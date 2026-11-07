"""Single source of truth for versioned Spider artifact byte limits."""

from collections.abc import Mapping

DEFAULT_LIMIT = 16_777_216
LEG32 = 33_554_432
PAID_JOURNAL_LIMIT = LEG32
GROSS_REPORT_LIMIT = LEG32

# T1.3. `None` means a separately-derived v2 value; J1 does not activate v2.
POLICY_TABLE: dict[tuple[str, str, str], int | None] = {
    ("P3", "v1", "journal.jsonl"): LEG32,
    ("P3", "v1", "other"): DEFAULT_LIMIT,
    ("P3", "v2", "journal.jsonl"): None,
    ("P3", "v2", "reconciliation.json"): None,
    ("P3", "v2", "endpoint.json"): None,
    ("P3", "v2", "other"): DEFAULT_LIMIT,
    ("paid_execution", "v1", "journal.jsonl"): PAID_JOURNAL_LIMIT,
    ("paid_floating", "v1", "journal.jsonl"): PAID_JOURNAL_LIMIT,
    ("paid_gross", "v1", "journal.jsonl"): PAID_JOURNAL_LIMIT,
    ("paid_gross", "v1", "report.jsonl"): GROSS_REPORT_LIMIT,
    ("paid_execution", "v1", "other"): DEFAULT_LIMIT,
    ("paid_floating", "v1", "other"): DEFAULT_LIMIT,
    ("paid_gross", "v1", "other"): DEFAULT_LIMIT,
    ("P1", "v1", "other"): DEFAULT_LIMIT,
    ("P2", "v1", "other"): DEFAULT_LIMIT,
}

# P3-v2 structural policy. Delivery attempts are admitted by the common row
# schema but are not writable P3-v2 journal rows (the executor skips them).
P3_V2_COVERAGE_KINDS = (
    "HISTORICAL_MARKET_STEP",
    "HISTORICAL_MARKET_CACHE",
    "HISTORICAL_TIMER",
)
P3_V2_RECORD_KINDS = frozenset(
    {
        "HISTORICAL_MARKET_RESULT",
        "SNAPSHOT_FACT",
        "CALLBACK_RESULT",
        "SOURCE_GROUP_RESULT",
    }
)
P3_V2_COVERAGE_BASIS: dict[str, tuple[str, ...] | None] = {
    "HISTORICAL_MARKET_RESULT": ("HISTORICAL_MARKET_STEP",),
    "SNAPSHOT_FACT": P3_V2_COVERAGE_KINDS,
    "CALLBACK_RESULT": P3_V2_COVERAGE_KINDS,
    "SOURCE_GROUP_RESULT": None,
}
P3_V2_MULTIPLIERS: dict[str, tuple[int, int]] = {
    "HISTORICAL_MARKET_RESULT": (11, 10),
    "SNAPSHOT_FACT": (12, 5),
    "CALLBACK_RESULT": (27, 10),
}
P3_V2_ROW_CAPS = {
    "HISTORICAL_MARKET_RESULT": 5_933,
    "SNAPSHOT_FACT": 1_572,
    "CALLBACK_RESULT": 1_544,
}
P3_V2_DEFS_PER_MARKET = 5
P3_V2_DEFCAP = 425
P3_V2_ROW_MAX = 1_048_576
P3_V2_MAX_DEFINITION_BYTES = 4_096
P3_V2_HEADER_BYTES = 0  # JSONL has no separate file prelude.
P3_V2_WIRE_CEILING = 512 * 1_048_576
P3_V2_MATERIAL_CEILING = 512 * 1_048_576
P3_V2_EXP_MAX = 6
P3_V2_JOURNAL_CAP = min(P3_V2_WIRE_CEILING, P3_V2_MATERIAL_CEILING // P3_V2_EXP_MAX)
P3_V2_ROW_MIN = 1_591
P3_V2_RECONCILIATION_FLOOR = DEFAULT_LIMIT
P3_V2_ENDPOINT_FLOOR = DEFAULT_LIMIT
P3_V2_X2_COEFFICIENT = (19_048_383, 6_119)
P3_V2_E2_COEFFICIENT = (10_790_164, 6_119)


def _ceil_div(numerator: int, denominator: int) -> int:
    return (numerator + denominator - 1) // denominator


def p3_v2_scope_refused(structural_bytes: int, l_wire: int) -> bool:
    """Apply D2.3 registration refusal at inclusive byte ceilings."""
    if (
        type(structural_bytes) is not int
        or type(l_wire) is not int
        or min(structural_bytes, l_wire) < 0
    ):
        raise ValueError("INVALID_ARTIFACT")
    return structural_bytes > P3_V2_JOURNAL_CAP or l_wire > P3_V2_JOURNAL_CAP


def p3_v2_scope_budget(coverage: Mapping[str, int]) -> dict[str, object]:
    """Return the pure P3-v2 registration and runtime budgets for valid coverage."""
    if not isinstance(coverage, Mapping) or any(
        kind not in P3_V2_COVERAGE_KINDS for kind in coverage
    ):
        raise ValueError("INVALID_ARTIFACT")
    if any(type(count) is not int or count < 0 for count in coverage.values()):
        raise ValueError("INVALID_ARTIFACT")
    counts = {kind: coverage.get(kind, 0) for kind in P3_V2_COVERAGE_KINDS}
    total = sum(counts.values())
    market_steps = counts["HISTORICAL_MARKET_STEP"]
    if total == 0:
        raise ValueError("INVALID_ARTIFACT")

    row_counts = {
        kind: _ceil_div(sum(counts[key] for key in basis) * numerator, denominator)
        for kind, basis in P3_V2_COVERAGE_BASIS.items()
        if basis is not None
        for numerator, denominator in (P3_V2_MULTIPLIERS[kind],)
    }
    kind_budgets = {
        kind: row_counts[kind] * P3_V2_ROW_CAPS[kind] for kind in row_counts
    }
    table_max = P3_V2_DEFS_PER_MARKET * market_steps
    table_bytes_max = table_max * P3_V2_DEFCAP
    structural_bytes = P3_V2_HEADER_BYTES + sum(kind_budgets.values()) + table_bytes_max
    l_wire = market_steps * P3_V2_ROW_MIN
    refused = p3_v2_scope_refused(structural_bytes, l_wire)
    u_wire = None if refused else P3_V2_JOURNAL_CAP
    x2 = max(
        P3_V2_RECONCILIATION_FLOOR,
        _ceil_div(P3_V2_X2_COEFFICIENT[0] * total, P3_V2_X2_COEFFICIENT[1]),
    )
    e2 = max(
        P3_V2_ENDPOINT_FLOOR,
        _ceil_div(P3_V2_E2_COEFFICIENT[0] * total, P3_V2_E2_COEFFICIENT[1]),
    )
    return {
        "row_counts": row_counts,
        "kind_budgets": kind_budgets,
        "table_max": table_max,
        "table_bytes_max": table_bytes_max,
        "structural_bytes": structural_bytes,
        "B_SG": P3_V2_JOURNAL_CAP - structural_bytes,
        "L_wire": l_wire,
        "U_wire": u_wire,
        "J2": P3_V2_JOURNAL_CAP if not refused else None,
        "X2": x2,
        "E2": e2,
        "refused": refused,
    }


def artifact_limit(family: str, encoding: str, artifact: str) -> int | None:
    """Return the active byte cap, or None for a deferred/unknown policy cell."""
    category = (
        artifact
        if artifact
        in ("journal.jsonl", "report.jsonl", "endpoint.json", "reconciliation.json")
        else "other"
    )
    specific = (family, encoding, category)
    if specific in POLICY_TABLE:
        return POLICY_TABLE[specific]
    return POLICY_TABLE.get((family, encoding, "other"))


def oversize_reason(family: str, encoding: str, artifact: str) -> str:
    """Keep historical v1 limit failures distinguishable from artifact mismatch."""
    if family == "P3" and encoding == "v1" and artifact == "journal.jsonl":
        return "ARTIFACT_LIMIT_EXCEEDED"
    return "ARTIFACT_MISMATCH"
