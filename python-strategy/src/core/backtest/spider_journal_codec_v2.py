# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Pure B1 order interning for P3-v2 journal rows."""

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.backtest.spider_journal_slots_v2 import SLOT_LIST
from src.core.backtest.spider_journal_slot_classifier_v2 import classify_p3_v2_slots

_V1_SCHEMA = "spider_journal_record_v1"
_V2_SCHEMA = "spider_journal_record_v2"
_CODEC_UNSUPPORTED = "CODEC_UNSUPPORTED_ROW"

_EMPTY_STATE: tuple[bytes, ...] = ()


def _set_path(row: dict[str, object], path: tuple[str, ...], value: object) -> None:
    current = row
    for name in path[:-1]:
        child = current.get(name)
        if type(child) is not dict:
            raise ValueError(_CODEC_UNSUPPORTED)
        copied = child.copy()
        current[name] = copied
        current = copied
    current[path[-1]] = value


def _checked_state(state: object) -> tuple[bytes, ...]:
    if type(state) is not tuple:
        raise ValueError(_CODEC_UNSUPPORTED)
    checked: list[bytes] = []
    for item in state:
        if type(item) is not bytes:
            raise ValueError(_CODEC_UNSUPPORTED)
        checked.append(item)
    typed_state = tuple(checked)
    if len(set(typed_state)) != len(typed_state):
        raise ValueError(_CODEC_UNSUPPORTED)
    return typed_state


def encode_p3_v2_row(
    row: object, prior_state: object = _EMPTY_STATE
) -> tuple[dict[str, object], tuple[bytes, ...]]:
    """Intern D5.2 slots and return a new row and immutable next table state.

    The caller owns publication: it may adopt the returned state only after
    the corresponding encoded row has been written successfully.
    """
    if type(row) is not dict:
        raise ValueError(_CODEC_UNSUPPORTED)
    source = row
    if type(source.get("schema_version")) is not str:
        raise ValueError(_CODEC_UNSUPPORTED)
    if source.get("schema_version") != _V1_SCHEMA:
        raise ValueError(_CODEC_UNSUPPORTED)
    state = _checked_state(prior_state)
    if "order_defs" in source:
        raise ValueError(_CODEC_UNSUPPORTED)
    slots = classify_p3_v2_slots(source)
    if slots is None:
        raise ValueError(_CODEC_UNSUPPORTED)

    indexes = {encoded: index for index, encoded in enumerate(state)}
    appended: list[bytes] = []
    row_definitions: list[dict[str, object]] = []
    references: list[tuple[tuple[str, ...], list[int]]] = []

    for path, expected_fields, slot_state, parent, leaf in slots:
        if slot_state != SLOT_LIST:
            continue
        slot = parent[leaf]
        if type(slot) is not list:
            raise ValueError(_CODEC_UNSUPPORTED)
        row_indexes: list[int] = []
        for order in slot:
            if type(order) is not dict or set(order) != expected_fields:
                raise ValueError(_CODEC_UNSUPPORTED)
            try:
                encoded = canonical_bytes(order)
            except (TypeError, ValueError, RecursionError):
                raise ValueError(_CODEC_UNSUPPORTED) from None
            index = indexes.get(encoded)
            if index is None:
                index = len(state) + len(appended)
                try:
                    definition = decode_canonical(encoded)
                except (TypeError, ValueError, RecursionError):
                    raise ValueError(_CODEC_UNSUPPORTED) from None
                if type(definition) is not dict:
                    raise ValueError(_CODEC_UNSUPPORTED)
                indexes[encoded] = index
                appended.append(encoded)
                row_definitions.append(definition)
            row_indexes.append(index)
        references.append((path, row_indexes))

    result = source.copy()
    result["schema_version"] = _V2_SCHEMA
    for path, row_indexes in references:
        _set_path(result, path, row_indexes)
    if row_definitions:
        result["order_defs"] = row_definitions
    return result, state + tuple(appended)
