"""Rithmic ledger recovery and authoritative account projection."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from logging import Logger
from typing import Any


class RithmicLedgerRecoveryService:
    """Own Rithmic startup recovery and verified account publication."""

    def __init__(
        self,
        *,
        profile: str,
        account_id: str | None,
        reconcile_owned_orders: Callable[[str, str | None], dict[str, Any]],
        now_seconds: Callable[[], float],
        publish_authoritative_balance: Callable[..., None],
        logger: Logger,
    ) -> None:
        self._profile = profile
        self._account_id = account_id
        self._reconcile_owned_orders = reconcile_owned_orders
        self._now_seconds = now_seconds
        self._publish_authoritative_balance = publish_authoritative_balance
        self._logger = logger

    def reconcile_startup(self) -> dict[str, Any]:
        """Return the existing fail-closed startup reconciliation envelope."""
        try:
            summary = self._reconcile_owned_orders(
                self._profile,
                self._account_id,
            )
        except Exception:
            self._logger.exception("Startup order reconciliation failed")
            return {
                "recoverable_count": 0,
                "unresolved_count": 1,
                "verification_blocked_count": 1,
                "auto_resume_safe": False,
            }

        try:
            self.publish_authoritative_summary(summary)
        except Exception:
            self._logger.exception(
                "Startup authoritative account reconciliation failed"
            )
            return {
                **summary,
                "auto_resume_safe": False,
                "account_context_error": True,
            }

        self._logger.info(
            "Startup order reconciliation complete: %s recoverable orders",
            summary["recoverable_count"],
        )
        return summary

    def publish_authoritative_summary(self, summary: dict[str, Any]) -> None:
        """Publish exact account values only after ledger verification passes."""
        if summary.get("auto_resume_safe") is not True:
            raise RuntimeError("rithmic_account_summary_not_authoritative")
        ledger = summary.get("ledger_verification")
        if not isinstance(ledger, dict) or ledger.get("verification_blocked"):
            raise RuntimeError("rithmic_account_ledger_verification_blocked")
        account_id = ledger.get("account_id")
        if account_id != self._account_id:
            raise RuntimeError("rithmic_account_summary_identity_mismatch")
        currency = ledger.get("account_currency")
        account_summary = ledger.get("account_summary")
        if not isinstance(account_summary, dict):
            raise RuntimeError("rithmic_account_summary_missing")
        raw_balance = account_summary.get("account_balance")
        if raw_balance is None:
            raise RuntimeError("rithmic_account_balance_missing")
        raw_day_pnl = account_summary.get("day_pnl")
        if raw_day_pnl is None:
            raise RuntimeError("rithmic_account_day_pnl_missing")
        try:
            balance = Decimal(str(raw_balance))
            day_pnl = Decimal(str(raw_day_pnl))
        except ArithmeticError as exc:
            raise RuntimeError("rithmic_account_balance_invalid") from exc
        if not balance.is_finite() or not day_pnl.is_finite():
            raise RuntimeError("rithmic_account_balance_invalid")
        raw_source_timestamp = account_summary.get("timestamp_ms")
        try:
            source_timestamp_ms = (
                int(raw_source_timestamp) if raw_source_timestamp is not None else None
            )
        except (TypeError, ValueError) as exc:
            raise RuntimeError("rithmic_account_timestamp_invalid") from exc
        self._publish_authoritative_balance(
            venue="rithmic",
            account_id=account_id,
            currency=str(currency or ""),
            balance=balance,
            day_pnl=day_pnl,
            observed_at_ms=int(self._now_seconds() * 1000),
            source_timestamp_ms=source_timestamp_ms,
        )
