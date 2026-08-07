"""Behavior-preserving canonical mapping snapshots."""

from collections.abc import Mapping


def snapshot_mapping[KeyT, ValueT](
    value: Mapping[KeyT, ValueT], *, invalid_key_error: str
) -> dict[KeyT, ValueT]:
    _ = invalid_key_error
    return dict(value)
