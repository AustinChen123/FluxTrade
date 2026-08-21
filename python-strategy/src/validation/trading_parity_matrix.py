"""Pure identity-first comparison for the four-run parity matrix."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import ClassVar, Literal

from pydantic import ValidationError

from src.validation.trading_outcome import OutcomeDifference, TradingOutcome
from src.validation.trading_parity import TradingParityRun

__all__ = [
    "FourRunParityReport",
    "InvalidParityInputIdentity",
    "TradingParityMismatch",
    "compare_four_run_parity",
]


ComparisonName = Literal["BL_BB", "CL_CB", "BL_CL", "BB_CB"]
ComparisonResult = Literal["exact_match"]
ComparisonRecord = tuple[ComparisonName, ComparisonResult]
RunRole = Literal["BL", "BB", "CL", "CB"]
RunDigestRecord = tuple[RunRole, str]
RunDigests = tuple[
    RunDigestRecord,
    RunDigestRecord,
    RunDigestRecord,
    RunDigestRecord,
]
ComparisonRecords = tuple[
    ComparisonRecord,
    ComparisonRecord,
    ComparisonRecord,
    ComparisonRecord,
]

_ROLE_ORDER = ("BL", "BB", "CL", "CB")
_TYPED_ROLE_ORDER: tuple[RunRole, RunRole, RunRole, RunRole] = (
    "BL",
    "BB",
    "CL",
    "CB",
)
_COMPARISONS: ComparisonRecords = (
    ("BL_BB", "exact_match"),
    ("CL_CB", "exact_match"),
    ("BL_CL", "exact_match"),
    ("BB_CB", "exact_match"),
)
_IDENTICAL_ALL = (
    "input_sha256",
    "configuration_sha256",
    "loaded_artifact_sha256",
    "native_matcher_sha256",
)
_IDENTICAL_WITHIN_MODE = ("runtime_source_sha256", "runner_sha256")


class InvalidParityInputIdentity(ValueError):
    """Canonical REPLAN disposition for malformed or incomparable inputs."""

    classification = "INVALID_INPUT_IDENTITY"
    canonical_stop_action = "REPLAN"

    def __init__(self, field: str) -> None:
        super().__init__("parity input identity is invalid")
        self.field = field


class TradingParityMismatch(AssertionError):
    """First deterministic semantic difference in the required matrix."""

    def __init__(
        self, comparison: ComparisonName, difference: OutcomeDifference
    ) -> None:
        super().__init__("trading parity mismatch")
        self.comparison = comparison
        self.difference = difference


def _exact_digest(value: object) -> bool:
    return (
        type(value) is str
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


@dataclass(frozen=True, slots=True)
class FourRunParityReport:
    """Successful exact-match result for all four required comparisons."""

    schema_version: ClassVar[str] = "fluxtrade.four_run_parity_report.v1"
    run_digests: RunDigests
    comparisons: ComparisonRecords

    def __post_init__(self) -> None:
        if type(self.run_digests) is not tuple or len(self.run_digests) != 4:
            raise ValueError("run_digests must be one exact four-item tuple")
        for actual, expected_role in zip(
            self.run_digests, _TYPED_ROLE_ORDER, strict=True
        ):
            if (
                type(actual) is not tuple
                or len(actual) != 2
                or type(actual[0]) is not str
                or actual[0] != expected_role
                or not _exact_digest(actual[1])
            ):
                raise ValueError(
                    "run_digests must use the fixed role and SHA-256 ledger"
                )
        if type(self.comparisons) is not tuple or len(self.comparisons) != 4:
            raise ValueError("comparisons must be one exact four-item tuple")
        for actual, expected in zip(self.comparisons, _COMPARISONS, strict=True):
            if (
                type(actual) is not tuple
                or len(actual) != 2
                or any(type(value) is not str for value in actual)
                or actual != expected
            ):
                raise ValueError("comparisons must use the fixed exact-match ledger")

    def canonical_bytes(self) -> bytes:
        validated = FourRunParityReport(self.run_digests, self.comparisons)
        return json.dumps(
            [
                validated.schema_version,
                [list(record) for record in validated.run_digests],
                [list(record) for record in validated.comparisons],
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()

    def sha256(self) -> str:
        return hashlib.sha256(FourRunParityReport.canonical_bytes(self)).hexdigest()


def _invalid(field: str) -> InvalidParityInputIdentity:
    return InvalidParityInputIdentity(field)


def _revalidate_run(value: object) -> TradingParityRun:
    if type(value) is not TradingParityRun:
        raise _invalid("matrix")
    try:
        return TradingParityRun.model_validate(dict(value.__dict__))
    except (ValidationError, ValueError) as error:
        raise _invalid("matrix") from error


def _require_equal(left: TradingParityRun, right: TradingParityRun, field: str) -> None:
    if getattr(left, field) != getattr(right, field):
        raise _invalid(field)


def _validated_roles(runs: object) -> dict[str, TradingParityRun]:
    if type(runs) is not tuple or len(runs) != 4:
        raise _invalid("matrix")
    validated = tuple(_revalidate_run(run) for run in runs)
    by_role = {run.role: run for run in validated}
    if len(by_role) != 4 or tuple(sorted(by_role)) != tuple(sorted(_ROLE_ORDER)):
        raise _invalid("matrix")
    return by_role


def _validate_identity(by_role: dict[str, TradingParityRun]) -> None:
    bl, bb, cl, cb = (by_role[role] for role in _ROLE_ORDER)
    for field in ("revision_sha", "tree_oid"):
        _require_equal(bl, bb, field)
        _require_equal(cl, cb, field)
        if getattr(bl, field) == getattr(cl, field):
            raise _invalid(field)
    for field in _IDENTICAL_ALL:
        for run in (bb, cl, cb):
            _require_equal(bl, run, field)
    for field in _IDENTICAL_WITHIN_MODE:
        _require_equal(bl, cl, field)
        _require_equal(bb, cb, field)


def compare_four_run_parity(runs: object) -> FourRunParityReport:
    """Validate identities first, then require exact outcomes in fixed order."""
    by_role = _validated_roles(runs)
    _validate_identity(by_role)
    pairs: tuple[tuple[ComparisonName, TradingParityRun, TradingParityRun], ...] = (
        ("BL_BB", by_role["BL"], by_role["BB"]),
        ("CL_CB", by_role["CL"], by_role["CB"]),
        ("BL_CL", by_role["BL"], by_role["CL"]),
        ("BB_CB", by_role["BB"], by_role["CB"]),
    )
    completed: list[ComparisonRecord] = []
    for comparison, expected, actual in pairs:
        difference = TradingOutcome.first_difference(expected.outcome, actual.outcome)
        if difference is not None:
            raise TradingParityMismatch(comparison, difference)
        completed.append((comparison, "exact_match"))
    digests: RunDigests = (
        ("BL", TradingParityRun.sha256(by_role["BL"])),
        ("BB", TradingParityRun.sha256(by_role["BB"])),
        ("CL", TradingParityRun.sha256(by_role["CL"])),
        ("CB", TradingParityRun.sha256(by_role["CB"])),
    )
    return FourRunParityReport(
        run_digests=(digests[0], digests[1], digests[2], digests[3]),
        comparisons=(completed[0], completed[1], completed[2], completed[3]),
    )
