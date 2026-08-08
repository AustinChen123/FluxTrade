"""Canonical identity envelope for one trading parity run."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Annotated, ClassVar, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    ModelWrapValidatorHandler,
    StringConstraints,
    model_validator,
)

from src.core.backtest.endpoint_state import ReplayEndpointState
from src.core.canonical_mapping import snapshot_mapping
from src.validation.trading_outcome import (
    FillObservation,
    FinancialOutcome,
    JournalObservation,
    OrderObservation,
    SignalObservation,
    TradingOutcome,
)

__all__ = ["TradingParityRun"]


_GitIdentity = Annotated[
    str,
    StringConstraints(
        strict=True,
        pattern=r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$",
    ),
]
_Sha256Identity = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$"),
]
_SUBCLASS_ERROR = "TradingParityRun subclasses are unsupported"


class TradingParityRun(BaseModel):
    """Strict provenance and outcome identity for one parity matrix cell."""

    model_config = ConfigDict(
        frozen=True, strict=True, extra="forbid", revalidate_instances="always"
    )
    schema_version: ClassVar[str] = "fluxtrade.trading_parity_run.v2"

    role: Literal["BL", "BB", "CL", "CB"]
    source_version: Literal["baseline", "candidate"]
    mode: Literal["live_like", "backtest"]
    revision_sha: _GitIdentity
    tree_oid: _GitIdentity
    runtime_source_sha256: _Sha256Identity
    input_sha256: _Sha256Identity
    configuration_sha256: _Sha256Identity
    runner_sha256: _Sha256Identity
    loaded_artifact_sha256: _Sha256Identity
    native_matcher_sha256: _Sha256Identity
    outcome: TradingOutcome
    _string_fields: ClassVar[frozenset[str]] = frozenset(
        {
            "role",
            "source_version",
            "mode",
            "revision_sha",
            "tree_oid",
            "runtime_source_sha256",
            "input_sha256",
            "configuration_sha256",
            "runner_sha256",
            "loaded_artifact_sha256",
            "native_matcher_sha256",
        }
    )

    @model_validator(mode="wrap")
    @classmethod
    def reject_direct_model_extra(
        cls, value: object, handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if cls is not TradingParityRun or (
            type(value) is not TradingParityRun and isinstance(value, TradingParityRun)
        ):
            raise ValueError(_SUBCLASS_ERROR)
        if (
            cls is TradingParityRun
            and type(value) is cls
            and value.model_extra is not None
        ):
            raise ValueError("parity run contains unexpected fields")
        return handler(value)

    @model_validator(mode="before")
    @classmethod
    def require_outcome_instance(cls, data: object) -> object:
        if not isinstance(data, Mapping):
            raise ValueError("parity runs require mapping input")
        projected = snapshot_mapping(
            data,
            invalid_key_error="parity run field names must be exact strings",
        )
        if set(projected) - set(cls.model_fields):
            raise ValueError("parity runs forbid unexpected fields")
        for field in cls._string_fields & projected.keys():
            if type(projected[field]) is not str:
                raise ValueError(f"{field} must be a string")
        if "outcome" in projected:
            outcome = projected["outcome"]
            if type(outcome) is not TradingOutcome:
                raise ValueError("outcome must be an exact TradingOutcome")
            if outcome.model_extra is not None:
                raise ValueError("outcome contains unexpected fields")
            values = outcome.__dict__
            sequences = (
                (values.get("signals"), SignalObservation),
                (values.get("order_observations"), OrderObservation),
                (values.get("fills"), FillObservation),
                (values.get("journal"), JournalObservation),
            )
            if any(
                type(items) is not tuple
                or any(type(item) is not expected for item in items)
                for items, expected in sequences
            ):
                raise ValueError("outcome observations must have exact canonical types")
            if (
                type(values.get("endpoint_state")) is not ReplayEndpointState
                or type(values.get("financial")) is not FinancialOutcome
            ):
                raise ValueError("outcome summaries must have exact canonical types")
            projected["outcome"] = TradingOutcome.model_validate(dict(values))
        return projected

    @model_validator(mode="after")
    def validate_matrix_cell(self) -> Self:
        expected = {
            "BL": ("baseline", "live_like"),
            "BB": ("baseline", "backtest"),
            "CL": ("candidate", "live_like"),
            "CB": ("candidate", "backtest"),
        }
        if (self.source_version, self.mode) != expected[self.role]:
            raise ValueError(
                "role, source_version, and mode must identify one exact parity cell"
            )
        return self

    def canonical_bytes(self) -> bytes:
        if type(self) is not TradingParityRun:
            raise ValueError(_SUBCLASS_ERROR)
        if self.model_extra is not None:
            raise ValueError("parity run contains unexpected fields")
        validated = TradingParityRun.model_validate(dict(self.__dict__))
        self = validated
        values = [
            self.schema_version,
            self.role,
            self.source_version,
            self.mode,
            self.revision_sha,
            self.tree_oid,
            self.runtime_source_sha256,
            self.input_sha256,
            self.configuration_sha256,
            self.runner_sha256,
            self.loaded_artifact_sha256,
            self.native_matcher_sha256,
            self.outcome.sha256(),
        ]
        return json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()

    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()
