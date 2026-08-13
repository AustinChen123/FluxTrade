import logging
import time
from collections.abc import Iterator, Mapping
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol, cast
from src.core.models import Signal, SignalType, Position, PositionSide
from src.core.redis_factory import create_redis_client
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_rules.daily_loss_circuit_breaker import DailyLossCircuitBreakerRule
from src.core.risk_rules.existing_position_entry import ExistingPositionEntryRule
from src.core.risk_rules.max_position_notional import MaxPositionNotionalRule
from src.core.risk_rules.order_rate_limit import OrderRateLimitRule
from src.core.risk_rules.price_sanity_check import PriceSanityCheckRule
from src.core.risk_rules.single_order_notional import SingleOrderNotionalRule
from src.core.product_registry import InstrumentSpec, calculate_notional_exposure
from src.core.runtime_environment import RuntimeEnvironment

if TYPE_CHECKING:
    from src.core.capital_allocator import CapitalAllocator

logger = logging.getLogger(__name__)


class _SyncRedisPipeline(Protocol):
    def delete(self, *keys: str | bytes) -> object: ...

    def hset(self, name: str, mapping: Mapping[str, str]) -> object: ...

    def execute(self) -> list[object]: ...


class _SyncRedis(Protocol):
    def ping(self) -> bool: ...

    def close(self) -> None: ...

    def hget(self, name: str, key: str) -> str | None: ...

    def hgetall(self, name: str) -> dict[str, str]: ...

    def hset(self, name: str, mapping: Mapping[str, str]) -> int: ...

    def scan_iter(self, match: str) -> Iterator[str | bytes]: ...

    def pipeline(self, *, transaction: bool) -> _SyncRedisPipeline: ...


class AccountService:
    """Interface for accessing account data via Redis."""

    def __init__(self):
        self.redis: _SyncRedis | None = None
        self._authoritative_balance_key: str | None = None
        self._authoritative_venue: str | None = None
        self._authoritative_account_id: str | None = None
        self._authoritative_balance_max_age_seconds: float | None = None
        try:
            redis_client = cast(_SyncRedis, create_redis_client())
            redis_client.ping()  # Check connection
            self.redis = redis_client
        except Exception as e:
            logger.warning("AccountService: Redis connection failed: %s", e)

    def close(self):
        if self.redis:
            self.redis.close()

    def configure_authoritative_balance(
        self,
        *,
        venue: str,
        account_id: str,
        max_age_seconds: float,
        runtime_environment: RuntimeEnvironment,
    ) -> None:
        if not venue or not account_id:
            raise ValueError("authoritative account identity is required")
        if max_age_seconds <= 0:
            raise ValueError("authoritative balance max age must be positive")
        normalized_venue = venue.strip().lower()
        self._authoritative_balance_key = runtime_environment.key(
            f"account:{normalized_venue}:{account_id}"
        )
        self._authoritative_venue = normalized_venue
        self._authoritative_account_id = account_id
        self._authoritative_balance_max_age_seconds = max_age_seconds

    def replace_authoritative_balance(
        self,
        *,
        venue: str,
        account_id: str,
        currency: str,
        balance: Decimal,
        day_pnl: Decimal,
        observed_at_ms: int,
        source_timestamp_ms: int | None,
    ) -> None:
        if not self.redis or not self._authoritative_balance_key:
            raise RuntimeError("authoritative_balance_cache_unavailable")
        normalized_venue = venue.strip().lower()
        if normalized_venue != self._authoritative_venue:
            raise ValueError("authoritative_balance_venue_mismatch")
        if account_id != self._authoritative_account_id:
            raise ValueError("authoritative_balance_account_mismatch")
        if not currency:
            raise ValueError("authoritative_balance_currency_missing")
        if not balance.is_finite() or not day_pnl.is_finite():
            raise ValueError("authoritative_balance_not_finite")
        if observed_at_ms <= 0:
            raise ValueError("authoritative_balance_observation_time_invalid")

        self.redis.hset(
            self._authoritative_balance_key,
            mapping={
                "venue": normalized_venue,
                "account_id": account_id,
                "currency": currency,
                "balance": str(balance),
                "day_pnl": str(day_pnl),
                "day_start_nav": str(balance - day_pnl),
                "observed_at_ms": str(observed_at_ms),
                "source_timestamp_ms": (
                    str(source_timestamp_ms)
                    if source_timestamp_ms is not None
                    else ""
                ),
            },
        )

    def get_balance(self) -> Decimal:
        if self._authoritative_balance_key is not None:
            return self._get_authoritative_balance_snapshot()["balance"]
        if not self.redis:
            return Decimal("0")
        
        # Assuming single account 'main' for now as per Lua script usage
        balance = self.redis.hget("state:balance:main", "free")
        return Decimal(balance) if balance else Decimal("0")

    def get_daily_nav_context(self) -> tuple[Decimal, Decimal] | None:
        if getattr(self, "_authoritative_balance_key", None) is None:
            return None
        snapshot = self._get_authoritative_balance_snapshot()
        return snapshot["day_start_nav"], snapshot["balance"]

    def _get_authoritative_balance_snapshot(self) -> dict[str, Decimal]:
        if not self.redis:
            raise RuntimeError("authoritative_balance_cache_unavailable")
        balance_key = cast(str, self._authoritative_balance_key)
        data = self.redis.hgetall(balance_key)
        if not data:
            raise RuntimeError("authoritative_balance_snapshot_missing")
        if data.get("venue") != self._authoritative_venue:
            raise RuntimeError("authoritative_balance_venue_mismatch")
        if data.get("account_id") != self._authoritative_account_id:
            raise RuntimeError("authoritative_balance_account_mismatch")
        if not data.get("currency"):
            raise RuntimeError("authoritative_balance_currency_missing")
        try:
            observed_at_ms = int(data["observed_at_ms"])
            balance = Decimal(data["balance"])
            day_pnl = Decimal(data["day_pnl"])
            day_start_nav = Decimal(data["day_start_nav"])
        except (KeyError, ValueError, ArithmeticError) as exc:
            raise RuntimeError("authoritative_balance_snapshot_invalid") from exc
        max_age = self._authoritative_balance_max_age_seconds
        if max_age is None:
            raise RuntimeError("authoritative_balance_freshness_unconfigured")
        age_ms = int(time.time() * 1000) - observed_at_ms
        if age_ms < 0 or age_ms > int(max_age * 1000):
            raise RuntimeError("authoritative_balance_snapshot_stale")
        if not all(
            value.is_finite() for value in (balance, day_pnl, day_start_nav)
        ):
            raise RuntimeError("authoritative_balance_snapshot_invalid")
        if day_start_nav != balance - day_pnl:
            raise RuntimeError("authoritative_balance_snapshot_inconsistent")
        return {
            "balance": balance,
            "day_pnl": day_pnl,
            "day_start_nav": day_start_nav,
        }

    def get_position(self, strategy_id: str, product_id: str) -> Optional[Position]:
        if not self.redis:
            return None

        key = f"state:position:{strategy_id}:{product_id}"
        data = self.redis.hgetall(key)
        
        if not data:
            return None

        qty_str = data.get("quantity", "0")
        qty = Decimal(qty_str)
        
        if qty == 0:
            return None

        # Determine Side & Abs Quantity
        if qty > 0:
            side = PositionSide.LONG
            abs_qty = qty
        else:
            side = PositionSide.SHORT
            abs_qty = abs(qty)

        # Entry Price (tracked by Lua or external updater)
        entry_price = Decimal(data.get("entry_price", "0"))
        
        # Unrealized PnL (Not strictly tracked in Redis Hash yet, placeholder)
        unrealized_pnl = Decimal("0")

        return Position(
            strategy_id=strategy_id,
            product_id=product_id,
            side=side,
            quantity=abs_qty,
            entry_price=entry_price,
            unrealized_pnl=unrealized_pnl
        )

    def get_position_for_exit(
        self,
        strategy_id: str,
        product_id: str,
    ) -> Optional[Position]:
        """Load position truth for an EXIT decision, distinguishing flat from unavailable."""
        if type(self).get_position is not AccountService.get_position:
            return self.get_position(strategy_id, product_id)
        if not getattr(self, "redis", None):
            raise RuntimeError("position_state_unavailable")
        return self.get_position(strategy_id, product_id)

    def get_all_positions(self) -> list[Position]:
        redis_client = getattr(self, "redis", None)
        if redis_client is None:
            return []

        positions: list[Position] = []
        for key in redis_client.scan_iter("state:position:*"):
            key_text = key.decode() if isinstance(key, bytes) else str(key)
            prefix = "state:position:"
            if not key_text.startswith(prefix):
                continue
            try:
                strategy_id, exchange, symbol = key_text[len(prefix):].rsplit(":", 2)
            except ValueError:
                continue
            product_id = f"{exchange}:{symbol}"
            position = self.get_position(strategy_id, product_id)
            if position is not None:
                positions.append(position)
        return positions

    def replace_positions_for_products(
        self,
        positions: list[Position],
        product_ids: list[str] | tuple[str, ...],
        *,
        timestamp_ms: int,
    ) -> dict[str, int]:
        """Atomically replace a scoped local cache from authoritative positions."""
        products = {str(product_id) for product_id in product_ids if str(product_id)}
        if not products:
            return {"removed": 0, "written": 0}
        if not self.redis:
            raise RuntimeError("authoritative_position_cache_unavailable")

        keys = []
        prefix = "state:position:"
        for key in self.redis.scan_iter(f"{prefix}*"):
            key_text = key.decode() if isinstance(key, bytes) else str(key)
            if not key_text.startswith(prefix):
                continue
            try:
                _, exchange, symbol = key_text[len(prefix):].rsplit(":", 2)
            except ValueError:
                continue
            if f"{exchange}:{symbol}" in products:
                keys.append(key)

        pipeline = self.redis.pipeline(transaction=True)
        if keys:
            pipeline.delete(*keys)
        seen = set()
        for position in positions:
            if position.product_id not in products:
                raise ValueError("authoritative_position_product_out_of_scope")
            if position.product_id in seen:
                raise ValueError("duplicate_authoritative_position_product")
            seen.add(position.product_id)
            quantity = (
                -position.quantity
                if position.side == PositionSide.SHORT
                else position.quantity
            )
            pipeline.hset(
                f"state:position:LIVE:{position.product_id}",
                mapping={
                    "quantity": str(quantity),
                    "entry_price": str(position.entry_price),
                    "last_update": str(timestamp_ms),
                },
            )
        pipeline.execute()
        return {"removed": len(keys), "written": len(positions)}

class RiskManager:
    def __init__(
        self,
        account_service: AccountService,
        capital_allocator: Optional["CapitalAllocator"] = None,
        max_exposure_per_strategy: Optional[Decimal] = None,
        risk_config: Optional[RiskConfig] = None,
        single_order_rule: Optional[SingleOrderNotionalRule] = None,
        daily_loss_rule: Optional[DailyLossCircuitBreakerRule] = None,
        price_sanity_rule: Optional[PriceSanityCheckRule] = None,
        order_rate_limit_rule: Optional[OrderRateLimitRule] = None,
        redis_client=None,
        max_position_rule: Optional[MaxPositionNotionalRule] = None,
        existing_position_entry_rule: Optional[ExistingPositionEntryRule] = None,
        state_manager: Optional[Any] = None,
        daily_nav_service: Optional[Any] = None,
        instrument_spec_resolver: Optional[
            Callable[[str], InstrumentSpec | None]
        ] = None,
        lifecycle_id_resolver: Callable[[str], str] | None = None,
    ):
        self.account_service = account_service
        self.risk_config = risk_config or RiskConfig.from_env()
        self.single_order_rule = single_order_rule or SingleOrderNotionalRule(self.risk_config)
        self.daily_loss_rule = daily_loss_rule or DailyLossCircuitBreakerRule(self.risk_config)
        self.price_sanity_rule = price_sanity_rule or PriceSanityCheckRule(self.risk_config)
        rate_limit_redis = redis_client or getattr(account_service, "redis", None)
        self.order_rate_limit_rule = order_rate_limit_rule
        if self.order_rate_limit_rule is None and rate_limit_redis is not None:
            self.order_rate_limit_rule = OrderRateLimitRule(
                self.risk_config,
                rate_limit_redis,
            )
        self.max_position_rule = max_position_rule or MaxPositionNotionalRule(self.risk_config)
        self.existing_position_entry_rule = (
            existing_position_entry_rule or ExistingPositionEntryRule()
        )
        self.capital_allocator = capital_allocator
        self.state_manager = state_manager
        self.daily_nav_service = daily_nav_service
        self.instrument_spec_resolver = instrument_spec_resolver
        self.lifecycle_id_resolver = lifecycle_id_resolver or (lambda value: value)
        self.max_exposure_per_strategy = (
            max_exposure_per_strategy or self.risk_config.max_position_notional
        )

    def check_risk(
        self,
        signal: Signal,
        current_price: Optional[Decimal] = None,
        *,
        best_bid: Optional[Decimal] = None,
        best_ask: Optional[Decimal] = None,
        daily_start_nav: Optional[Decimal] = None,
        current_nav: Optional[Decimal] = None,
        snapshot_date: Optional[date] = None,
    ) -> tuple[bool, str]:
        """
        Evaluates the signal against risk rules.
        Returns (True, "PASS") if safe to proceed, (False, reason) otherwise.

        Args:
            signal: The trading signal to evaluate.
            current_price: Current market price for exposure calculation.
                           Falls back to entry_price if not provided.
        """
        if signal.type == SignalType.NO_SIGNAL:
            return True, "NO_SIGNAL"

        is_entry = signal.type in [SignalType.LONG, SignalType.SHORT]
        instrument_spec = self._instrument_spec(signal.product_id) if is_entry else None

        balance: Decimal | None = None
        if is_entry and daily_start_nav is None and current_nav is None:
            context_loader = getattr(
                self.account_service,
                "get_daily_nav_context",
                None,
            )
            if callable(context_loader):
                nav_context = cast(
                    tuple[Decimal, Decimal] | None,
                    context_loader(),
                )
                if nav_context is not None:
                    daily_start_nav, current_nav = nav_context
                    balance = current_nav

        # Rule 1: Balance / Capital check. Risk-reducing signals must not depend
        # on account-balance availability.
        if self.capital_allocator is not None:
            # Per-strategy capital check
            available = self.capital_allocator.get_available(signal.strategy_id)
            if is_entry and available <= Decimal("0"):
                msg = f"REJECT: No available capital for strategy {signal.strategy_id} (available={available})"
                logger.warning("RISK_REJECTED: %s signal_type=%s", msg, signal.type)
                return False, msg
        else:
            # Global balance check (backward-compatible)
            if is_entry and balance is None:
                balance = self.account_service.get_balance()
            if balance is not None and balance <= 0:
                msg = f"REJECT: Account balance is {balance} (<= 0)"
                logger.warning("RISK_REJECTED: %s signal_type=%s", msg, signal.type)
                return False, msg

        # Rule 2: Single-order notional check.
        if is_entry:
            nav = balance if balance is not None else self.account_service.get_balance()
            rule_status, rule_reason = self.single_order_rule.evaluate(
                signal,
                nav,
                instrument_spec,
            )
            if rule_status == RuleStatus.REJECT:
                msg = f"REJECT: {rule_reason}"
                logger.warning("RISK_REJECTED: %s", msg)
                return False, msg

        # Rule 3: Daily loss circuit breaker when NAV context is available.
        if is_entry and (daily_start_nav is not None or current_nav is not None):
            if daily_start_nav is None and current_nav is not None and self.daily_nav_service is not None:
                daily_start_nav = self.daily_nav_service.get_start_nav(
                    signal.strategy_id,
                    snapshot_date or date.today(),
                )
                if daily_start_nav is None:
                    msg = "REJECT: daily_loss_missing_start_nav_snapshot"
                    logger.warning("RISK_REJECTED: %s", msg)
                    return False, msg
            if daily_start_nav is None or current_nav is None:
                msg = "REJECT: daily_loss_missing_nav_context"
                logger.warning("RISK_REJECTED: %s", msg)
                return False, msg
            rule_status, rule_reason = self.daily_loss_rule.evaluate(
                start_nav=daily_start_nav,
                current_nav=current_nav,
            )
            if rule_status in {RuleStatus.REJECT, RuleStatus.CIRCUIT_BREAKER_TRIGGERED}:
                msg = f"REJECT: {rule_reason}"
                if rule_status == RuleStatus.CIRCUIT_BREAKER_TRIGGERED:
                    self._transition_strategy_to_error(signal.strategy_id, rule_reason or msg)
                logger.warning("RISK_REJECTED: %s", msg)
                return False, msg

        # Rule 4: Price sanity check when market-depth context is available.
        if is_entry and (best_bid is not None or best_ask is not None):
            rule_status, rule_reason = self.price_sanity_rule.evaluate(
                signal,
                best_bid=best_bid,
                best_ask=best_ask,
            )
            if rule_status == RuleStatus.REJECT:
                msg = f"REJECT: {rule_reason}"
                logger.warning("RISK_REJECTED: %s", msg)
                return False, msg

        # Rule 5: Max Exposure Check (uses current market price)
        position = self.account_service.get_position(signal.strategy_id, signal.product_id)
        if position:
            price_for_exposure = current_price if current_price is not None else position.entry_price

            if is_entry:
                if (
                    not self.risk_config.allow_same_side_reentry
                    and self.existing_position_entry_rule is not None
                ):
                    rule_status, rule_reason = self.existing_position_entry_rule.evaluate(
                        signal,
                        position,
                    )
                    if rule_status == RuleStatus.REJECT:
                        msg = f"REJECT: {rule_reason}"
                        logger.warning("RISK_REJECTED: %s", msg)
                        return False, msg

                if signal.quantity is None:
                    current_exposure = calculate_notional_exposure(
                        position.quantity,
                        price_for_exposure,
                        instrument_spec,
                    )
                    if current_exposure > self.risk_config.max_position_notional:
                        msg = (
                            "REJECT: Max exposure reached "
                            f"({current_exposure} > {self.risk_config.max_position_notional})"
                        )
                        logger.warning("RISK_REJECTED: %s", msg)
                        return False, msg
                else:
                    rule_status, rule_reason = self.max_position_rule.evaluate(
                        signal,
                        position,
                        price_for_exposure,
                        instrument_spec,
                    )
                    if rule_status == RuleStatus.REJECT:
                        msg = f"REJECT: Max exposure reached ({rule_reason})"
                        logger.warning("RISK_REJECTED: %s", msg)
                        return False, msg

            # Per-strategy exposure limit (only when capital_allocator present)
            if self.capital_allocator is not None and is_entry:
                current_exposure = calculate_notional_exposure(
                    position.quantity,
                    price_for_exposure,
                    instrument_spec,
                )
                if current_exposure >= self.max_exposure_per_strategy:
                    msg = (
                        f"REJECT: Strategy {signal.strategy_id} max exposure reached "
                        f"({current_exposure} >= {self.max_exposure_per_strategy})"
                    )
                    logger.warning("RISK_REJECTED: %s", msg)
                    return False, msg

        # Rule 6: Order rate limit. This records only after prior checks pass.
        if is_entry and self.order_rate_limit_rule is not None:
            rule_status, rule_reason = self.order_rate_limit_rule.try_record_order(
                signal.strategy_id,
            )
            if rule_status == RuleStatus.REJECT:
                msg = f"REJECT: {rule_reason}"
                logger.warning("RISK_REJECTED: %s", msg)
                return False, msg

        return True, "PASS"

    def _instrument_spec(self, product_id: str) -> InstrumentSpec | None:
        if self.instrument_spec_resolver is None:
            return None
        return self.instrument_spec_resolver(product_id)

    def _transition_strategy_to_error(self, strategy_id: str, reason: str) -> None:
        if self.state_manager is None:
            return
        try:
            self.state_manager.transition_to_error(
                self.lifecycle_id_resolver(strategy_id),
                reason,
                actor="system",
            )
        except Exception as exc:
            logger.error(
                "Failed to transition %s to ERROR after risk circuit breaker: %s",
                strategy_id,
                exc,
            )

    def calculate_position_size(self, entry_price: Decimal, stop_loss_price: Decimal, risk_percent: Decimal = Decimal("0.02")) -> Decimal:
        """
        Calculates position size based on risk percentage and stop loss distance.
        Risk = |Entry - SL| * Size
        Size = Risk / |Entry - SL|
        """
        balance = self.account_service.get_balance()
        if balance <= 0:
            return Decimal("0")

        risk_amount = balance * risk_percent
        price_diff = abs(entry_price - stop_loss_price)

        if price_diff == 0:
            return Decimal("0")

        size = risk_amount / price_diff
        
        # Optional: Add rounding logic here based on exchange precision if known
        # For now, return raw Decimal
        return round(size, 4)
