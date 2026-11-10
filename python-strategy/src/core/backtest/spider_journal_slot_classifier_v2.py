# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Exact-shape classifier for the shared D5.2 slot table."""

from src.core.backtest.spider_journal_slots_v2 import (
    P3_V2_SLOT_TABLE,
    SLOT_LIST,
    SLOT_MISSING,
    SLOT_NULL,
)


def classify_p3_v2_slots(
    row: object,
) -> (
    tuple[tuple[tuple[str, ...], frozenset[str], str, dict[str, object], str], ...]
    | None
):
    """Return ordered slot observations, empty for valid no-slot rows, None if malformed."""
    if type(row) is not dict:
        return None
    kind = row.get("record_kind")
    if type(kind) is not str:
        return None
    selected = None
    for known_kind, slots, selector in P3_V2_SLOT_TABLE:
        if kind == known_kind:
            selected = (slots, selector)
            break
    if selected is None:
        return None
    slots, selector = selected
    payload = row.get("payload")
    if type(payload) is not dict:
        return None
    if selector is not None:
        fact = payload.get("fact")
        if type(fact) is not dict:
            return None
        snapshot_kind = fact.get("snapshot_kind")
        if type(snapshot_kind) is not str:
            return None
        if snapshot_kind != selector[0]:
            return ()
        immutable = fact.get("immutable_payload")
        if type(immutable) is not dict:
            return None
        outcome = immutable.get("outcome")
        if type(outcome) is not str:
            return None
        if outcome != selector[1]:
            return ()

    observations = []
    for path, required_fields in slots:
        parent: object = row
        for name in path[:-1]:
            if type(parent) is not dict or name not in parent:
                return None
            parent = parent[name]
        if type(parent) is not dict:
            return None
        leaf = path[-1]
        if leaf not in parent:
            state = SLOT_MISSING
        elif parent[leaf] is None:
            state = SLOT_NULL
        elif type(parent[leaf]) is list:
            state = SLOT_LIST
        else:
            return None
        observations.append((path, required_fields, state, parent, leaf))
    return tuple(observations)
