"""Single source of truth for versioned Spider artifact byte limits."""

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


def artifact_limit(family: str, encoding: str, artifact: str) -> int | None:
    """Return the active byte cap, or None for a deferred/unknown policy cell."""
    category = artifact if artifact in ("journal.jsonl", "report.jsonl", "endpoint.json", "reconciliation.json") else "other"
    specific = (family, encoding, category)
    if specific in POLICY_TABLE:
        return POLICY_TABLE[specific]
    return POLICY_TABLE.get((family, encoding, "other"))


def oversize_reason(family: str, encoding: str, artifact: str) -> str:
    """Keep historical v1 limit failures distinguishable from artifact mismatch."""
    if family == "P3" and encoding == "v1" and artifact == "journal.jsonl":
        return "ARTIFACT_LIMIT_EXCEEDED"
    return "ARTIFACT_MISMATCH"
