from __future__ import annotations

import json
from queue import Empty, Full, Queue
from threading import RLock
from typing import Mapping, cast


MAX_EVENT_SUBSCRIBERS = 16
MAX_QUEUED_EVENT_FRAMES = 64
HEARTBEAT_SECONDS = 15.0

_CLOSED = object()
_ALLOWED_RESOURCES = frozenset({"ga_job", "evolution_epoch"})


class InvalidInvalidationRecord(ValueError):
    """An invalid event record was rejected before any subscriber fan-out."""


class EventConnectionCapacity(RuntimeError):
    """The stream hub is full or closed and cannot admit another subscriber."""


class SubscriptionClosed(RuntimeError):
    """A subscription was closed by its owner, the hub, or queue overflow."""


class InvalidationSubscription:
    def __init__(self, hub: ControlPlaneInvalidationHub) -> None:
        self._hub = hub
        self._frames: Queue[bytes | object] = Queue(maxsize=MAX_QUEUED_EVENT_FRAMES)
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def next_frame(self, timeout: float) -> bytes | None:
        try:
            frame = self._frames.get(timeout=timeout)
        except Empty:
            if self._closed:
                raise SubscriptionClosed
            return None
        if frame is _CLOSED:
            raise SubscriptionClosed
        return cast(bytes, frame)

    def close(self) -> None:
        self._hub._release(self)


class ControlPlaneInvalidationHub:
    """Bounded in-process fan-out for canonical Control Plane invalidations."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._subscriptions: set[InvalidationSubscription] = set()
        self._closed = False

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._subscriptions)

    def subscribe(self) -> InvalidationSubscription:
        with self._lock:
            if self._closed or len(self._subscriptions) >= MAX_EVENT_SUBSCRIBERS:
                raise EventConnectionCapacity
            subscription = InvalidationSubscription(self)
            self._subscriptions.add(subscription)
            return subscription

    def publish(self, record: Mapping[str, object]) -> None:
        frame = _encode_invalidation(record)
        with self._lock:
            if self._closed:
                raise EventConnectionCapacity
            for subscription in tuple(self._subscriptions):
                try:
                    subscription._frames.put_nowait(frame)
                except Full:
                    self._close_locked(subscription)

    def close(self) -> None:
        with self._lock:
            self._closed = True
            for subscription in tuple(self._subscriptions):
                self._close_locked(subscription)

    def _release(self, subscription: InvalidationSubscription) -> None:
        with self._lock:
            self._close_locked(subscription)

    def _close_locked(self, subscription: InvalidationSubscription) -> None:
        if subscription._closed:
            return
        subscription._closed = True
        self._subscriptions.discard(subscription)
        while True:
            try:
                subscription._frames.get_nowait()
            except Empty:
                break
        subscription._frames.put_nowait(_CLOSED)


def _encode_invalidation(record: Mapping[str, object]) -> bytes:
    if set(record) != {"schema_version", "resource", "identity", "revision"}:
        raise InvalidInvalidationRecord("invalid event record")
    schema_version = record.get("schema_version")
    resource = record.get("resource")
    identity = record.get("identity")
    revision = record.get("revision")
    if (
        type(schema_version) is not int
        or schema_version != 1
        or not isinstance(resource, str)
        or resource not in _ALLOWED_RESOURCES
        or not isinstance(identity, str)
        or not identity
        or len(identity) > 128
        or not identity.isprintable()
        or type(revision) is not int
        or revision <= 0
    ):
        raise InvalidInvalidationRecord("invalid event record")
    encoded = json.dumps(
        {
            "schema_version": 1,
            "resource": resource,
            "identity": identity,
            "revision": revision,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"event: invalidate\ndata: {encoded}\n\n".encode("utf-8")
