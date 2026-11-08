"""Exact size accounting for P3-v2 journal rows and v1 projections."""

from collections.abc import Sequence
from typing import cast

from src.core.backtest.spider_artifact_limits import (
    P3_V2_EXP_MAX,
    P3_V2_MATERIAL_CEILING,
    P3_V2_ROW_MAX,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes

_MARKET_SCHEMA_V1 = "spider_journal_record_v1"
_MARKET_SCHEMA_V2 = "spider_journal_record_v2"


def canonical_jsonl_row_len(row_v1: object) -> int:
    """Return canonical v1 row bytes including the JSONL newline."""
    return len(canonical_bytes(row_v1)) + 1


def _replace_refs(
    row: dict[str, object],
    path: tuple[str, ...],
    prior: Sequence[object],
    definitions: list[object],
) -> None:
    value: object = row
    for key in path[:-1]:
        if type(value) is not dict or key not in value:
            raise ValueError("ARTIFACT_MISMATCH")
        value = value[key]
    if type(value) is not dict or type(value.get(path[-1])) is not list:
        raise ValueError("ARTIFACT_MISMATCH")
    indexes = cast(list[object], value[path[-1]])
    resolved = []
    for index in indexes:
        if (
            type(index) is not int
            or index < 0
            or index >= len(prior) + len(definitions)
        ):
            raise ValueError("ARTIFACT_MISMATCH")
        resolved.append(
            prior[index] if index < len(prior) else definitions[index - len(prior)]
        )
    value[path[-1]] = resolved


def materialize_v1_row(
    row_v2: dict[str, object],
    prior_definitions: Sequence[object],
    contexts: tuple[object, object],
) -> tuple[dict[str, object], list[object]]:
    """Restore one wire row's refs, contexts and v1 schema without copying history."""
    definitions = row_v2.pop("order_defs", [])
    if type(definitions) is not list:
        raise ValueError("ARTIFACT_MISMATCH")
    new_definitions = cast(list[object], definitions)
    paths: tuple[tuple[str, ...], ...] = ()
    kind = row_v2["record_kind"]
    if kind == "HISTORICAL_MARKET_RESULT":
        paths = (
            ("payload", "request", "working_orders"),
            ("payload", "working_orders_snapshot"),
        )
    elif kind == "SOURCE_GROUP_RESULT":
        paths = tuple(
            ("payload", side, "open_orders_fact", "immutable_payload", "rows")
            for side in ("owner_evidence_before", "owner_evidence_after")
        )
    elif kind == "SNAPSHOT_FACT":
        fact = cast(
            dict[str, object], cast(dict[str, object], row_v2["payload"])["fact"]
        )
        immutable = fact.get("immutable_payload")
        if (
            fact.get("snapshot_kind") == "OPEN_ORDERS"
            and type(immutable) is dict
            and immutable.get("outcome") == "SUCCESS"
        ):
            paths = (("payload", "fact", "immutable_payload", "rows"),)
    for path in paths:
        _replace_refs(row_v2, path, prior_definitions, new_definitions)
    row_v2["schema_version"] = _MARKET_SCHEMA_V1
    row_v2["configuration_context"], row_v2["historical_context"] = contexts
    return row_v2, new_definitions


def _reference_lists(row: dict[str, object]) -> list[list[object]]:
    """Return only the fixed D5.2 slots, in their protocol traversal order."""
    payload = cast(dict[str, object], row["payload"])
    kind = row["record_kind"]
    if kind == "HISTORICAL_MARKET_RESULT":
        request = cast(dict[str, object], payload["request"])
        return [
            cast(list[object], request["working_orders"]),
            cast(list[object], payload["working_orders_snapshot"]),
        ]
    if kind == "SOURCE_GROUP_RESULT":
        result: list[list[object]] = []
        for side in ("owner_evidence_before", "owner_evidence_after"):
            evidence = cast(dict[str, object], payload[side])
            fact = cast(dict[str, object], evidence["open_orders_fact"])
            immutable = cast(dict[str, object], fact["immutable_payload"])
            result.append(cast(list[object], immutable["rows"]))
        return result
    if kind == "SNAPSHOT_FACT":
        fact = cast(dict[str, object], payload["fact"])
        if fact.get("snapshot_kind") == "OPEN_ORDERS":
            immutable_value = fact.get("immutable_payload")
            if type(immutable_value) is not dict:
                raise ValueError("ARTIFACT_MISMATCH")
            immutable = cast(dict[str, object], immutable_value)
            if immutable.get("outcome") == "SUCCESS":
                rows = immutable.get("rows")
                if type(rows) is not list:
                    raise ValueError("ARTIFACT_MISMATCH")
                return [cast(list[object], rows)]
    return []


def _definition_member_bytes(definitions: list[object]) -> int:
    return len(b'"order_defs":') + len(canonical_bytes(definitions)) + 1


def _context_member_bytes(key: bytes, value: object) -> int:
    return len(key) + len(canonical_bytes(value)) + 1


def projected_v1_jsonl_row_len(
    row_v2: dict[str, object],
    prior_definitions: Sequence[object],
    configuration_context: object,
    historical_context: object,
    *,
    prior_definition_count: int | None = None,
) -> int:
    """Compute exact projected v1 bytes from a canonical v2 semantic row.

    This uses T2's member-framing equation, including the JSONL newline. The
    caller supplies definitions accumulated before this row; row-local defs are
    appended first for purposes of resolving same-row references (D5.4).
    """
    if row_v2.get("schema_version") != _MARKET_SCHEMA_V2:
        raise ValueError("INVALID_ARTIFACT")
    definitions = row_v2.get("order_defs", [])
    if type(definitions) is not list:
        raise ValueError("INVALID_ARTIFACT")
    prior_count = (
        len(prior_definitions)
        if prior_definition_count is None
        else prior_definition_count
    )
    if prior_count < 0 or prior_count > len(prior_definitions):
        raise ValueError("INVALID_ARTIFACT")
    new_definitions = cast(list[object], definitions)
    refs = _reference_lists(row_v2)
    digit_bytes = 0
    definition_bytes = 0
    for refs_in_slot in refs:
        for reference in refs_in_slot:
            if (
                type(reference) is not int
                or reference < 0
                or reference >= prior_count + len(new_definitions)
            ):
                raise ValueError("INVALID_ARTIFACT")
            digit_bytes += len(str(reference))
            definition = (
                prior_definitions[reference]
                if reference < prior_count
                else new_definitions[reference - prior_count]
            )
            definition_bytes += len(canonical_bytes(definition))

    wire_len = len(canonical_bytes(row_v2)) + 1
    definition_member = (
        _definition_member_bytes(cast(list[object], definitions))
        if "order_defs" in row_v2
        else 0
    )
    context_bytes = _context_member_bytes(
        b'"configuration_context":', configuration_context
    ) + _context_member_bytes(b'"historical_context":', historical_context)
    projected = (
        wire_len - definition_member - digit_bytes + definition_bytes + context_bytes
    )
    if projected < 1:
        raise ValueError("INVALID_ARTIFACT")
    return projected


def projected_jsonl_budget_ok(
    *, wire_bytes: int, projected_bytes: int, row_projected_bytes: int
) -> bool:
    """Apply inclusive T2/D2 material, ratio, and expanded-row ceilings."""
    if any(
        type(value) is not int or value < 0
        for value in (wire_bytes, projected_bytes, row_projected_bytes)
    ):
        raise ValueError("INVALID_ARTIFACT")
    if wire_bytes == 0:
        return projected_bytes == 0 and row_projected_bytes == 0
    return (
        projected_bytes <= P3_V2_MATERIAL_CEILING
        and projected_bytes <= P3_V2_EXP_MAX * wire_bytes
        and row_projected_bytes <= P3_V2_ROW_MAX
    )


def assert_schema_width_identity() -> None:
    """Protect the zero-byte schema-name restoration assumption from T2."""
    if len(_MARKET_SCHEMA_V1) != len(_MARKET_SCHEMA_V2):
        raise ValueError("ARTIFACT_MISMATCH")
