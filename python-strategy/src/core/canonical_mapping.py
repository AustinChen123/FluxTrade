"""Canonical mapping snapshots."""

from collections.abc import Mapping


def snapshot_mapping[KeyT, ValueT](
    value: Mapping[KeyT, ValueT], *, invalid_key_error: str
) -> dict[KeyT, ValueT]:
    if isinstance(value, dict):
        pairs = tuple(dict.items(value))
        for key, _ in pairs:
            if type(key) is not str:
                raise ValueError(invalid_key_error)
        return dict(pairs)

    keys = tuple(value)
    for key in keys:
        if type(key) is not str:
            raise ValueError(invalid_key_error)

    snapshot: dict[KeyT, ValueT] = {}
    for key in keys:
        if key not in snapshot:
            snapshot[key] = value[key]
    return snapshot
