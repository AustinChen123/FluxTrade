"""Pending terminal evidence, not durable proof of callback execution or fills.

The dispatcher must never label a started/partial callback SKIPPED. These pure
DTOs cannot observe caller behavior; persistence and receipt atomicity are separate.
"""

from dataclasses import asdict, dataclass, fields
import hashlib
import json
from typing import Any

from src.core.product_registry import validate_product_id
from .decision_identity import validate_strategy_version
from .read_types import _hex, _integer, _safe
from .publication import _canonical

MAX_DECISION_BATCH_PARTICIPANTS = 256
MAX_DECISION_BATCH_BYTES = 1024 * 1024
MAX_DECISION_BATCH_NODES = 65536
MAX_DECISION_BATCH_DEPTH = 16  # Shared detached JSON helper's fixed depth domain.


def _batch_bytes(value: dict[str, Any]) -> bytes:
    # Detached bounded JSON only; this is not the HTTP profile wire decoder.
    return _canonical(
        value, max_bytes=MAX_DECISION_BATCH_BYTES, max_nodes=MAX_DECISION_BATCH_NODES
    ).encode("utf-8")


def _shape(value: Any, cls: Any) -> dict[str, Any]:
    if type(value) is not dict or set(value) != {field.name for field in fields(cls)}:
        raise ValueError
    return value.copy()


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
            ):
                _safe(value, 128)
            validate_strategy_version(self.strategy_version)
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
                or len(self.participants) > MAX_DECISION_BATCH_PARTICIPANTS
                or len(self.outcomes) > MAX_DECISION_BATCH_PARTICIPANTS
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
            self.canonical_bytes
        except ValueError:
            raise DecisionApplicationError() from None

    @property
    def canonical_bytes(self) -> bytes:
        data = {"schema_version": 1, **asdict(self)}
        data["participants"] = list(data["participants"])
        data["outcomes"] = list(data["outcomes"])
        return _batch_bytes(data)

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_canonical_bytes(cls, raw: bytes) -> "MarketDataDecisionBatch":
        def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
            if len({key for key, _ in items}) != len(items):
                raise ValueError
            return dict(items)

        def reject(value: str) -> Any:
            raise ValueError

        try:
            if type(raw) is not bytes or len(raw) > MAX_DECISION_BATCH_BYTES:
                raise ValueError
            data = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=pairs,
                parse_float=reject,
                parse_constant=reject,
            )
            if _batch_bytes(data) != raw or type(data) is not dict:
                raise ValueError
            version = data.pop("schema_version")
            if type(version) is not int or version != 1:
                raise ValueError
            data = _shape(data, cls)
            for name in ("participants", "outcomes"):
                if (
                    type(data[name]) is not list
                    or len(data[name]) > MAX_DECISION_BATCH_PARTICIPANTS
                ):
                    raise ValueError
            data["participants"] = tuple(
                MarketDataDecisionKey(**_shape(row, MarketDataDecisionKey))
                for row in data["participants"]
            )
            outcomes = []
            for row in data["outcomes"]:
                item = _shape(row, MarketDataDecisionOutcome)
                item["key"] = MarketDataDecisionKey(
                    **_shape(item["key"], MarketDataDecisionKey)
                )
                outcomes.append(MarketDataDecisionOutcome(**item))
            data["outcomes"] = tuple(outcomes)
            result = cls(**data)
            if result.canonical_bytes != raw:
                raise ValueError
            return result
        except (ValueError, TypeError, KeyError, OverflowError, RecursionError):
            raise DecisionApplicationError() from None
