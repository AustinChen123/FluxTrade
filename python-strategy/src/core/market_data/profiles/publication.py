"""Verified publication inputs only; callers must never include secrets in JSON.

No persistence, timestamps assigned by a store, or source-completeness inference.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import cast

from .types import VolumeProfileContent

MAX_JSON_BYTES = 65_536
MAX_JSON_DEPTH = 16
MAX_JSON_NODES = 4096


def _canonical(value: dict[str, object]) -> str:
    """Root depth is zero; keys and values count as nodes. UTF-8 bytes include syntax."""
    if type(value) is not dict:
        raise ValueError("expected exact JSON object")
    pieces: list[str] = []
    active: set[int] = set()
    nodes = size = 0

    def emit(text: str) -> None:
        nonlocal size
        size += len(text.encode("utf-8"))
        if size > MAX_JSON_BYTES:
            raise ValueError("JSON byte limit exceeded")
        pieces.append(text)

    def walk(item: object, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > MAX_JSON_NODES or depth > MAX_JSON_DEPTH:
            raise ValueError("JSON node/depth limit exceeded")
        kind = type(item)
        if kind is dict or kind is list:
            if id(item) in active:
                raise ValueError("cyclic JSON")
            active.add(id(item))
            container = cast(dict[str, object] | list[object], item)
            if len(container) > MAX_JSON_NODES:
                raise ValueError("JSON node limit exceeded")
            if kind is dict:
                obj = cast(dict[str, object], item)
                if any(
                    type(key) is not str or len(key) > MAX_JSON_BYTES for key in obj
                ):
                    raise ValueError("expected bounded exact string keys")
                emit("{")
                for index, key in enumerate(sorted(obj)):
                    if index:
                        emit(",")
                    walk(key, depth + 1)
                    emit(":")
                    walk(obj[key], depth + 1)
                emit("}")
            else:
                emit("[")
                for index, child in enumerate(cast(list[object], item)):
                    if index:
                        emit(",")
                    walk(child, depth + 1)
                emit("]")
            active.remove(id(item))
        elif kind is str or kind is int or kind is bool or item is None:
            if kind is str and "\x00" in cast(str, item):
                raise ValueError("JSON text contains forbidden NUL")
            if kind is str and len(cast(str, item)) > MAX_JSON_BYTES:
                raise ValueError("JSON string limit exceeded")
            if kind is int and not -(1 << 63) <= cast(int, item) < (1 << 64):
                raise ValueError("JSON integer outside signed64/unsigned64 domain")
            emit(
                json.dumps(
                    item, ensure_ascii=False, allow_nan=False, separators=(",", ":")
                )
            )
        else:
            raise ValueError("unsupported JSON value type")

    try:
        walk(value, 0)
    except UnicodeError:
        raise ValueError("invalid UTF-8 JSON text") from None
    return "".join(pieces)


@dataclass(frozen=True, slots=True, init=False)
class CanonicalJsonObject:
    """Detached canonical JSON: <=64 KiB, depth<=16, <=4096 key/value nodes.

    Exact dict/list/str/int/bool/None only; cycles rejected. Integers must be
    in [-2^63, 2^64-1]; larger values must be supplied as strings.
    Each thaw returns a fresh mutable tree. Callers must exclude all secrets.
    """

    text: str

    def __init__(self, value: dict[str, object]) -> None:
        object.__setattr__(self, "text", _canonical(value))

    def thaw(self) -> dict[str, object]:
        return cast(dict[str, object], json.loads(self.text))


@dataclass(frozen=True, slots=True)
class VerifiedProfilePublication:
    content: VolumeProfileContent
    source_manifest: CanonicalJsonObject
    reconciliation: CanonicalJsonObject
    source_available_at: datetime | None
    availability_basis: str
    raw_retention_state: str

    def __post_init__(self) -> None:
        if type(self.content) is not VolumeProfileContent:
            raise ValueError("expected exact profile content")
        if any(
            type(value) is not CanonicalJsonObject
            for value in (self.source_manifest, self.reconciliation)
        ):
            raise ValueError("expected canonical JSON metadata")
        stamp = self.source_available_at
        if stamp is not None:
            try:
                valid = type(stamp) is datetime and stamp.tzinfo is not None and stamp.utcoffset() == timedelta(0)
            except Exception:
                raise ValueError("invalid UTC availability timestamp") from None
            if not valid:
                raise ValueError("invalid UTC availability timestamp")
            object.__setattr__(self, "source_available_at", stamp.replace(tzinfo=timezone.utc))
        if type(self.availability_basis) is not str or self.availability_basis not in (
            "OBSERVED",
            "MODELED",
        ):
            raise ValueError("invalid availability basis")
        retention_states = ("PRESENT", "DELETED", "NOT_STORED")
        if type(self.raw_retention_state) is not str or self.raw_retention_state not in retention_states:
            raise ValueError("invalid raw retention state")

    @property
    def quality(self) -> str:
        return "VERIFIED"

    @property
    def content_sha256(self) -> str:
        return self.content.content_sha256
