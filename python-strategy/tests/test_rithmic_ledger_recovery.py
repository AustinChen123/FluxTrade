"""Owner tests for Rithmic ledger recovery and account publication."""

from decimal import Decimal, InvalidOperation
import inspect
import logging
from unittest.mock import MagicMock

import pytest

from src.core.adapters.rithmic_ledger_recovery import (
    RithmicLedgerRecoveryService,
)
from src.core.engine import StrategyEngine


def _summary(
    *,
    account_id: str = "ACCOUNT",
    currency: str | None = "USD",
    balance: object = "50000.25",
    day_pnl: object = "0",
    timestamp_ms: object = 1704067200000,
) -> dict[str, object]:
    return {
        "recoverable_count": 2,
        "unresolved_count": 0,
        "verification_blocked_count": 0,
        "auto_resume_safe": True,
        "ledger_verification": {
            "account_id": account_id,
            "account_currency": currency,
            "verification_blocked": False,
            "account_summary": {
                "account_balance": balance,
                "day_pnl": day_pnl,
                "timestamp_ms": timestamp_ms,
            },
        },
    }


def _service(
    *,
    summary: dict[str, object] | None = None,
    reconcile_error: Exception | None = None,
    publish_error: Exception | None = None,
) -> tuple[RithmicLedgerRecoveryService, MagicMock, MagicMock]:
    reconcile = MagicMock()
    if reconcile_error is not None:
        reconcile.side_effect = reconcile_error
    else:
        reconcile.return_value = summary if summary is not None else _summary()
    publish = MagicMock(side_effect=publish_error)
    service = RithmicLedgerRecoveryService(
        profile="test",
        account_id="ACCOUNT",
        reconcile_owned_orders=reconcile,
        now_seconds=lambda: 1704067201,
        publish_authoritative_balance=publish,
        logger=logging.getLogger("src.core.engine"),
    )
    return service, reconcile, publish


def test_startup_success_returns_original_summary_and_logs_once(caplog):
    summary = _summary()
    service, reconcile, publish = _service(summary=summary)

    with caplog.at_level(logging.INFO, logger="src.core.engine"):
        result = service.reconcile_startup()

    assert result is summary
    reconcile.assert_called_once_with("test", "ACCOUNT")
    publish.assert_called_once_with(
        venue="rithmic",
        account_id="ACCOUNT",
        currency="USD",
        balance=Decimal("50000.25"),
        day_pnl=Decimal("0"),
        observed_at_ms=1704067201000,
        source_timestamp_ms=1704067200000,
    )
    records = [
        record
        for record in caplog.records
        if record.name == "src.core.engine"
        and record.getMessage().startswith("Startup order reconciliation")
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].getMessage() == (
        "Startup order reconciliation complete: 2 recoverable orders"
    )
    assert records[0].exc_info is None


def test_startup_ledger_error_returns_fixed_blocked_result_and_logs_once(caplog):
    error = RuntimeError("ledger offline")
    service, reconcile, publish = _service(reconcile_error=error)

    with caplog.at_level(logging.INFO, logger="src.core.engine"):
        result = service.reconcile_startup()

    assert result == {
        "recoverable_count": 0,
        "unresolved_count": 1,
        "verification_blocked_count": 1,
        "auto_resume_safe": False,
    }
    reconcile.assert_called_once_with("test", "ACCOUNT")
    publish.assert_not_called()
    records = [
        record
        for record in caplog.records
        if record.name == "src.core.engine"
        and record.getMessage().startswith("Startup order reconciliation")
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert records[0].getMessage() == "Startup order reconciliation failed"
    assert records[0].exc_info is not None
    assert records[0].exc_info[1] is error


def test_startup_projection_error_marks_result_unsafe_and_logs_once(caplog):
    summary = _summary()
    error = ValueError("authoritative cache offline")
    service, _, publish = _service(summary=summary, publish_error=error)

    with caplog.at_level(logging.INFO, logger="src.core.engine"):
        result = service.reconcile_startup()

    assert result == {
        **summary,
        "auto_resume_safe": False,
        "account_context_error": True,
    }
    publish.assert_called_once()
    records = [
        record
        for record in caplog.records
        if record.name == "src.core.engine"
        and record.getMessage().startswith("Startup")
    ]
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert records[0].getMessage() == (
        "Startup authoritative account reconciliation failed"
    )
    assert records[0].exc_info is not None
    assert records[0].exc_info[1] is error


_INVALID_CASES = (
    ("not_authoritative", "rithmic_account_summary_not_authoritative", None),
    ("ledger_missing", "rithmic_account_ledger_verification_blocked", None),
    ("ledger_wrong_type", "rithmic_account_ledger_verification_blocked", None),
    ("ledger_blocked", "rithmic_account_ledger_verification_blocked", None),
    ("wrong_account", "rithmic_account_summary_identity_mismatch", None),
    ("summary_missing", "rithmic_account_summary_missing", None),
    ("balance_missing", "rithmic_account_balance_missing", None),
    ("day_pnl_missing", "rithmic_account_day_pnl_missing", None),
    ("balance_invalid", "rithmic_account_balance_invalid", InvalidOperation),
    ("day_pnl_invalid", "rithmic_account_balance_invalid", InvalidOperation),
    ("balance_nonfinite", "rithmic_account_balance_invalid", None),
    ("day_pnl_nonfinite", "rithmic_account_balance_invalid", None),
    ("timestamp_invalid", "rithmic_account_timestamp_invalid", ValueError),
)


def _invalid_summary(case: str) -> dict[str, object]:
    summary = _summary()
    ledger = summary["ledger_verification"]
    assert isinstance(ledger, dict)
    account_summary = ledger["account_summary"]
    assert isinstance(account_summary, dict)
    if case == "not_authoritative":
        summary["auto_resume_safe"] = False
    elif case == "ledger_missing":
        del summary["ledger_verification"]
    elif case == "ledger_wrong_type":
        summary["ledger_verification"] = []
    elif case == "ledger_blocked":
        ledger["verification_blocked"] = True
    elif case == "wrong_account":
        ledger["account_id"] = "OTHER"
    elif case == "summary_missing":
        del ledger["account_summary"]
    elif case == "balance_missing":
        account_summary["account_balance"] = None
    elif case == "day_pnl_missing":
        account_summary["day_pnl"] = None
    elif case == "balance_invalid":
        account_summary["account_balance"] = "not-a-decimal"
    elif case == "day_pnl_invalid":
        account_summary["day_pnl"] = "not-a-decimal"
    elif case == "balance_nonfinite":
        account_summary["account_balance"] = "NaN"
    elif case == "day_pnl_nonfinite":
        account_summary["day_pnl"] = "Infinity"
    elif case == "timestamp_invalid":
        account_summary["timestamp_ms"] = "not-an-int"
    else:
        raise AssertionError(f"unhandled test case: {case}")
    return summary


@pytest.mark.parametrize(("case", "message", "cause_type"), _INVALID_CASES)
def test_summary_validation_rejects_before_publication(
    case: str,
    message: str,
    cause_type: type[BaseException] | None,
):
    service, _, publish = _service()

    with pytest.raises(RuntimeError) as caught:
        service.publish_authoritative_summary(_invalid_summary(case))

    assert caught.value.args == (message,)
    if cause_type is None:
        assert caught.value.__cause__ is None
    else:
        assert type(caught.value.__cause__) is cause_type
    publish.assert_not_called()


@pytest.mark.parametrize(
    ("balance", "day_pnl", "timestamp_ms", "expected_timestamp"),
    [
        (50000, 2, -1, -1),
        ("50000.2500", "-2.50", None, None),
    ],
)
def test_current_convertible_values_remain_accepted(
    balance: object,
    day_pnl: object,
    timestamp_ms: object,
    expected_timestamp: int | None,
):
    summary = _summary(
        balance=balance,
        day_pnl=day_pnl,
        timestamp_ms=timestamp_ms,
    )
    service, _, publish = _service()

    service.publish_authoritative_summary(summary)

    assert publish.call_args.kwargs == {
        "venue": "rithmic",
        "account_id": "ACCOUNT",
        "currency": "USD",
        "balance": Decimal(str(balance)),
        "day_pnl": Decimal(str(day_pnl)),
        "observed_at_ms": 1704067201000,
        "source_timestamp_ms": expected_timestamp,
    }


def test_publisher_exception_propagates_without_translation():
    error = ValueError("authoritative_balance_currency_missing")
    summary = _summary(currency=None)
    service, _, publish = _service(publish_error=error)

    with pytest.raises(ValueError) as caught:
        service.publish_authoritative_summary(summary)

    assert caught.value is error
    assert publish.call_args.kwargs["currency"] == ""


def test_engine_compatibility_method_contains_only_one_owner_delegation():
    source = inspect.getsource(
        StrategyEngine._publish_authoritative_account_summary
    )

    assert source.count("publish_authoritative_summary") == 1
    assert "summary.get" not in source
    assert "Decimal(" not in source
    assert "logger." not in source
