"""Canonical identity envelope for one trading parity run."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Annotated, ClassVar, Literal, Self

from pydantic import BaseModel, ConfigDict, StringConstraints, model_validator

from src.validation.trading_outcome import TradingOutcome

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


class TradingParityRun(BaseModel):
    """Strict provenance and outcome identity for one parity matrix cell."""

    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")
    schema_version: ClassVar[str] = "fluxtrade.trading_parity_run.v1"

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

    @model_validator(mode="before")
    @classmethod
    def require_outcome_instance(cls, data: object) -> object:
        if (
            isinstance(data, Mapping)
            and "outcome" in data
            and not isinstance(data["outcome"], TradingOutcome)
        ):
            raise ValueError("outcome must be an instantiated TradingOutcome")
        return data

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
