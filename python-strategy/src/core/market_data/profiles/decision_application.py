"""Pending terminal evidence, not durable proof of callback execution or fills.

The dispatcher must never label a started/partial callback SKIPPED. These pure
DTOs cannot observe caller behavior; persistence and receipt atomicity are separate.
"""

from dataclasses import asdict, dataclass
import hashlib
import json

from src.core.product_registry import validate_product_id
from .read_types import _hex, _integer, _safe


class DecisionApplicationError(ValueError):
    def __init__(self) -> None:
        super().__init__("MARKET_DATA_DECISION_INVALID")


def _product(value: str) -> None:
    if type(value) is not str or len(value) > 64:
        raise ValueError
    validate_product_id(value)


def _trigger(timeframe: str, bar_start_ms: int) -> str:
    _safe(timeframe, 32)
    _integer(bar_start_ms)
    return f"{timeframe}:{bar_start_ms}"


def _bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class MarketDataDecisionKey:
    environment: str
    execution_scope_id: str
    strategy_id: str
    strategy_version: str
    config_hash: str
    product_id: str
    trigger_kind: str
    trigger_id: str

    def __post_init__(self) -> None:
        try:
            for value in (
                self.environment,
                self.execution_scope_id,
                self.strategy_id,
                self.strategy_version,
            ):
                _safe(value, 128)
            _hex(self.config_hash)
            _product(self.product_id)
            if (
                type(self.trigger_kind) is not str
                or self.trigger_kind != "CANDLE"
                or type(self.trigger_id) is not str
                or len(self.trigger_id) > 52
            ):
                raise ValueError
            timeframe, start = self.trigger_id.split(":")
            if (
                not start.isascii()
                or not start.isdecimal()
                or len(start) > 19
                or _trigger(timeframe, int(start)) != self.trigger_id
            ):
                raise ValueError
        except ValueError:
            raise DecisionApplicationError() from None

    @classmethod
    def for_candle(
        cls,
        *,
        environment: str,
        execution_scope_id: str,
        strategy_id: str,
        strategy_version: str,
        config_hash: str,
        product_id: str,
        timeframe: str,
        bar_start_ms: int,
    ) -> "MarketDataDecisionKey":
        try:
            trigger = _trigger(timeframe, bar_start_ms)
        except ValueError:
            raise DecisionApplicationError() from None
        return cls(
            environment,
            execution_scope_id,
            strategy_id,
            strategy_version,
            config_hash,
            product_id,
            "CANDLE",
            trigger,
        )

    @property
    def canonical_bytes(self) -> bytes:
        return _bytes({"schema_version": 1, **asdict(self)})

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @property
    def input_id(self) -> str:
        return self.digest


@dataclass(frozen=True, slots=True)
class MarketDataDecisionOutcome:
    key: MarketDataDecisionKey
    disposition: str
    input_id: str | None
    input_digest: str | None
    reason: str | None = None
    contract_version: int = 1
    signal_suppressed: bool = False
    suppression_reason: str | None = None

    def __post_init__(self) -> None:
        try:
            if (
                type(self.key) is not MarketDataDecisionKey
                or type(self.disposition) is not str
                or self.disposition not in ("APPLIED", "SKIPPED")
                or type(self.contract_version) is not int
                or self.contract_version != 1
                or type(self.signal_suppressed) is not bool
            ):
                raise ValueError
            if (self.input_id is None) != (self.input_digest is None):
                raise ValueError
            if self.input_id is not None:
                _hex(self.input_id)
                _hex(self.input_digest)  # type: ignore[arg-type]
                if self.input_id != self.key.input_id:
                    raise ValueError
            if self.disposition == "APPLIED":
                if self.input_id is None or self.reason is not None:
                    raise ValueError
            else:
                _safe(self.reason)  # type: ignore[arg-type]
                if self.signal_suppressed or self.reason not in (
                    "INPUT_COMMIT_UNCONFIRMED",
                    "INPUT_STORE_FAILED",
                ):
                    raise ValueError
            if self.signal_suppressed:
                _safe(self.suppression_reason)  # type: ignore[arg-type]
                if self.suppression_reason != "SNAPSHOT_REVOKED":
                    raise ValueError
            elif self.suppression_reason is not None:
                raise ValueError
        except ValueError:
            raise DecisionApplicationError() from None

    @property
    def canonical_bytes(self) -> bytes:
        return _bytes(asdict(self))

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class MarketDataDecisionBatch:
    environment: str
    execution_scope_id: str
    product_id: str
    timeframe: str
    bar_start_ms: int
    participants: tuple[MarketDataDecisionKey, ...]
    outcomes: tuple[MarketDataDecisionOutcome, ...]

    def __post_init__(self) -> None:
        try:
            _safe(self.environment, 128)
            _safe(self.execution_scope_id, 128)
            _product(self.product_id)
            trigger = _trigger(self.timeframe, self.bar_start_ms)
            if (
                type(self.participants) is not tuple
                or type(self.outcomes) is not tuple
                or any(
                    type(key) is not MarketDataDecisionKey for key in self.participants
                )
                or any(
                    type(outcome) is not MarketDataDecisionOutcome
                    for outcome in self.outcomes
                )
            ):
                raise ValueError
            keys = {key.canonical_bytes for key in self.participants}
            actual = {outcome.key.canonical_bytes for outcome in self.outcomes}
            if (
                len(keys) != len(self.participants)
                or len(actual) != len(self.outcomes)
                or keys != actual
            ):
                raise ValueError
            if len({key.strategy_id for key in self.participants}) != len(
                self.participants
            ):
                raise ValueError
            if any(
                key.environment != self.environment
                or key.execution_scope_id != self.execution_scope_id
                or key.product_id != self.product_id
                or key.trigger_id != trigger
                for key in self.participants
            ):
                raise ValueError
            object.__setattr__(
                self,
                "participants",
                tuple(sorted(self.participants, key=lambda key: key.canonical_bytes)),
            )
            object.__setattr__(
                self,
                "outcomes",
                tuple(
                    sorted(
                        self.outcomes, key=lambda outcome: outcome.key.canonical_bytes
                    )
                ),
            )
        except ValueError:
            raise DecisionApplicationError() from None

    @property
    def canonical_bytes(self) -> bytes:
        return _bytes({"schema_version": 1, **asdict(self)})

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()
