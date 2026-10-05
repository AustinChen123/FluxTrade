from __future__ import annotations

import http.client
import json
import socket
import time
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any, Iterator, cast

import pytest

from src.control_plane.app import ControlPlaneApp, EventStreamResponse, HttpResponse
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.invalidation import (
    MAX_QUEUED_EVENT_FRAMES,
    ControlPlaneInvalidationHub,
    EventConnectionCapacity,
    InvalidInvalidationRecord,
    SubscriptionClosed,
)
from src.control_plane.server import make_handler, serve


ORIGIN = "https://fluxtrade.example.ts.net"
OPERATOR_CAPABILITY = "example.com/cap/fluxtrade-operator"
STEP_UP_CAPABILITY = "example.com/cap/fluxtrade-step-up"
API_KEY = "test-api-key"


def _record(
    *, resource: str = "ga_job", identity: str = "job-1", revision: int = 1
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "resource": resource,
        "identity": identity,
        "revision": revision,
    }


def test_hub_emits_exact_canonical_invalidation_frame() -> None:
    hub = ControlPlaneInvalidationHub()
    stream = hub.subscribe()

    hub.publish(_record())

    assert stream.next_frame(timeout=0) == (
        b"event: invalidate\ndata: "
        b'{"schema_version":1,"resource":"ga_job","identity":"job-1","revision":1}'
        b"\n\n"
    )
    assert hub.active_count == 1
    stream.close()
    assert hub.active_count == 0


@pytest.mark.parametrize(
    "record",
    [
        {},
        {"schema_version": 2, "resource": "ga_job", "identity": "job-1", "revision": 1},
        {"schema_version": 1, "resource": "other", "identity": "job-1", "revision": 1},
        {"schema_version": 1, "resource": "ga_job", "identity": "", "revision": 1},
        {
            "schema_version": 1,
            "resource": "ga_job",
            "identity": "x\nsecret",
            "revision": 1,
        },
        {
            "schema_version": 1,
            "resource": "ga_job",
            "identity": "x" * 129,
            "revision": 1,
        },
        {"schema_version": 1, "resource": "ga_job", "identity": "job-1", "revision": 0},
        {
            "schema_version": 1,
            "resource": "ga_job",
            "identity": "job-1",
            "revision": True,
        },
        {
            "schema_version": 1,
            "resource": "ga_job",
            "identity": "job-1",
            "revision": 1,
            "payload": {"private": "not allowed"},
        },
    ],
)
def test_invalid_records_are_rejected_before_fanout(record) -> None:
    hub = ControlPlaneInvalidationHub()
    stream = hub.subscribe()

    with pytest.raises(InvalidInvalidationRecord):
        hub.publish(record)

    assert stream.next_frame(timeout=0) is None
    assert hub.active_count == 1
    stream.close()


def test_hub_capacity_is_sixteen_and_closed_slots_are_released() -> None:
    hub = ControlPlaneInvalidationHub()
    streams = [hub.subscribe() for _ in range(16)]

    with pytest.raises(EventConnectionCapacity):
        hub.subscribe()

    streams[3].close()
    replacement = hub.subscribe()
    assert hub.active_count == 16

    hub.close()
    assert hub.active_count == 0
    with pytest.raises(SubscriptionClosed):
        replacement.next_frame(timeout=0)
    with pytest.raises(EventConnectionCapacity):
        hub.subscribe()


def test_slow_subscriber_overflow_closes_only_that_subscriber() -> None:
    hub = ControlPlaneInvalidationHub()
    slow = hub.subscribe()
    healthy = hub.subscribe()

    for revision in range(1, 66):
        hub.publish(_record(revision=revision))
        assert healthy.next_frame(timeout=0) is not None

    assert hub.active_count == 1
    with pytest.raises(SubscriptionClosed):
        slow.next_frame(timeout=0)
    hub.publish(_record(revision=66))
    assert healthy.next_frame(timeout=0) is not None
    healthy.close()
    assert hub.active_count == 0


def test_subscription_timeout_represents_heartbeat_due() -> None:
    hub = ControlPlaneInvalidationHub()
    stream = hub.subscribe()

    assert stream.next_frame(timeout=0.001) is None

    hub.close()


def _app() -> ControlPlaneApp:
    return ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key=API_KEY,
        browser_auth=BrowserSessionAuth(
            allowed_origin=ORIGIN,
            operator_capability=OPERATOR_CAPABILITY,
            step_up_capability=STEP_UP_CAPABILITY,
        ),
    )


def _identity_headers(*capabilities: str) -> dict[str, str]:
    return {
        "Origin": ORIGIN,
        "Tailscale-User-Login": "reader@example.com",
        "Tailscale-App-Capabilities": json.dumps(
            {capability: [{}] for capability in capabilities}
        ),
    }


def _session_headers(app: ControlPlaneApp, *capabilities: str) -> dict[str, str]:
    issued = app.handle(
        "POST",
        "/api/v1/auth/session",
        headers=_identity_headers(*capabilities),
    )
    assert issued.status_code == 201
    cookie = dict(issued.headers)["Set-Cookie"].split(";", 1)[0]
    return {**_identity_headers(*capabilities), "Cookie": cookie}


@contextmanager
def _running_http_server(app: ControlPlaneApp) -> Iterator[ThreadingHTTPServer]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(app))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        app.close_event_streams()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


def _http_response(
    server: ThreadingHTTPServer,
    path: str,
    headers: dict[str, str] | None = None,
) -> tuple[http.client.HTTPConnection, http.client.HTTPResponse]:
    address = cast(tuple[str, int], server.server_address)
    connection = http.client.HTTPConnection(*address, timeout=3)
    connection.request("GET", path, headers=headers or {})
    return connection, connection.getresponse()


def test_http_stream_requires_session_and_operator_capability() -> None:
    app = _app()
    with _running_http_server(app) as server:
        anonymous, anonymous_response = _http_response(server, "/api/v1/events")
        assert anonymous_response.status == 401
        assert json.loads(anonymous_response.read()) == {"error": "unauthorized"}
        assert anonymous_response.getheader("Cache-Control") == "no-store"
        anonymous.close()

        api_key_only, api_key_response = _http_response(
            server, "/api/v1/events", {"Authorization": f"Bearer {API_KEY}"}
        )
        assert api_key_response.status == 403
        assert json.loads(api_key_response.read()) == {"error": "forbidden"}
        assert api_key_response.getheader("Cache-Control") == "no-store"
        api_key_only.close()

        no_operator, no_operator_response = _http_response(
            server, "/api/v1/events", _session_headers(app)
        )
        assert no_operator_response.status == 403
        assert json.loads(no_operator_response.read()) == {"error": "forbidden"}
        assert no_operator_response.getheader("Cache-Control") == "no-store"
        no_operator.close()
        assert app.invalidation_hub.active_count == 0


def test_http_stream_has_exact_headers_heartbeat_frame_and_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.control_plane import server as server_module

    monkeypatch.setattr(server_module, "_EVENT_HEARTBEAT_SECONDS", 0.01)
    app = _app()
    headers = _session_headers(app, OPERATOR_CAPABILITY)
    with _running_http_server(app) as server:
        connection, response = _http_response(server, "/api/v1/events", headers)
        assert response.status == 200
        assert response.getheader("Content-Type") == "text/event-stream"
        assert response.getheader("Cache-Control") == "no-store"
        assert response.getheader("Content-Length") is None
        assert app.invalidation_hub.active_count == 1
        assert response.read(len(b": heartbeat\n\n")) == b": heartbeat\n\n"

        app.invalidation_hub.publish(
            _record(resource="evolution_epoch", identity="epoch-1", revision=2)
        )
        expected = (
            b"event: invalidate\ndata: "
            b'{"schema_version":1,"resource":"evolution_epoch",'
            b'"identity":"epoch-1","revision":2}\n\n'
        )
        assert response.read(len(expected)) == expected

        app.close_event_streams()
        assert response.read(1) == b""
        connection.close()
        assert app.invalidation_hub.active_count == 0


def test_heartbeat_is_emitted_during_continuous_event_backlog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.control_plane import server as server_module

    class ControlledClock:
        current = 0.0

        def monotonic(self) -> float:
            now = self.current
            self.current += 8.0
            return now

    clock = ControlledClock()
    monkeypatch.setattr(
        server_module,
        "time",
        SimpleNamespace(monotonic=clock.monotonic),
    )
    app = _app()
    subscription = app.invalidation_hub.subscribe()
    app.invalidation_hub.publish(_record(revision=1))
    event_frames: list[bytes] = []
    heartbeat_frames: list[bytes] = []
    heartbeats_before_last_event: list[int] = []

    class FakeConnection:
        def settimeout(self, timeout: float) -> None:
            assert timeout > 0

    class BackloggedWriter:
        def write(self, data: bytes) -> int:
            if data == b": heartbeat\n\n":
                heartbeat_frames.append(data)
            elif data.startswith(b"event: invalidate\n"):
                event_frames.append(data)
                if len(event_frames) < 5:
                    app.invalidation_hub.publish(
                        _record(revision=len(event_frames) + 1)
                    )
                else:
                    heartbeats_before_last_event.append(len(heartbeat_frames))
                    subscription.close()
            return len(data)

    handler_type = make_handler(app)
    handler = cast(Any, object.__new__(handler_type))
    handler.connection = FakeConnection()
    handler.close_connection = False
    handler.send_response = lambda status: None
    handler.send_header = lambda name, value: None
    handler.end_headers = lambda: None
    handler._event_peer_closed = lambda: False
    handler.wfile = BackloggedWriter()

    handler._handle_event_stream(EventStreamResponse(subscription))

    assert len(event_frames) == 5
    assert [
        json.loads(frame.split(b"data: ", 1)[1].split(b"\n", 1)[0])["revision"]
        for frame in event_frames
    ] == [1, 2, 3, 4, 5]
    assert heartbeats_before_last_event[0] >= 2
    assert all(frame == b": heartbeat\n\n" for frame in heartbeat_frames)
    assert app.invalidation_hub.active_count == 0


def test_http_disconnect_releases_subscription_on_broken_pipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.control_plane import server as server_module

    monkeypatch.setattr(server_module, "_EVENT_HEARTBEAT_SECONDS", 0.01)
    app = _app()
    with _running_http_server(app) as server:
        connection, response = _http_response(
            server,
            "/api/v1/events",
            _session_headers(app, OPERATOR_CAPABILITY),
        )
        assert response.status == 200
        assert app.invalidation_hub.active_count == 1
        if connection.sock is not None:
            connection.sock.shutdown(socket.SHUT_RDWR)
        response.close()
        connection.close()
        deadline = time.monotonic() + 2
        while app.invalidation_hub.active_count and time.monotonic() < deadline:
            time.sleep(0.01)
        assert app.invalidation_hub.active_count == 0


def test_http_capacity_response_is_finite_and_no_store() -> None:
    app = _app()
    subscriptions = [app.invalidation_hub.subscribe() for _ in range(16)]
    try:
        response = app.handle(
            "GET",
            "/api/v1/events",
            headers=_session_headers(app, OPERATOR_CAPABILITY),
        )
        assert isinstance(response, HttpResponse)
        assert response.status_code == 503
        assert response.body == {"error": "event_connection_capacity"}
        assert dict(response.headers) == {"Cache-Control": "no-store"}
        assert app.invalidation_hub.active_count == 16
    finally:
        app.close_event_streams()
        assert app.invalidation_hub.active_count == 0
        for subscription in subscriptions:
            subscription.close()


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return cast(tuple[str, int], listener.getsockname())[1]


def _wait_until_listening(port: int) -> None:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.01)
    raise AssertionError("isolated test server did not start")


def test_serve_shutdown_closes_active_stream_within_five_seconds() -> None:
    app = _app()
    stop_event = Event()
    port = _free_port()
    server_thread = Thread(
        target=serve,
        args=(app, "127.0.0.1", port),
        kwargs={"stop_event": stop_event},
        daemon=True,
    )
    server_thread.start()
    connection: http.client.HTTPConnection | None = None
    try:
        _wait_until_listening(port)
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        connection.request(
            "GET",
            "/api/v1/events",
            headers=_session_headers(app, OPERATOR_CAPABILITY),
        )
        response = connection.getresponse()
        assert response.status == 200
        started = time.monotonic()
        stop_event.set()
        server_thread.join(timeout=4.5)
        assert not server_thread.is_alive()
        assert time.monotonic() - started < 5
        assert response.read(1) == b""
        assert app.invalidation_hub.active_count == 0
    finally:
        stop_event.set()
        if connection is not None:
            connection.close()
        server_thread.join(timeout=5)
        app.shutdown(timeout=0)


def test_slow_socket_overflow_releases_blocked_writer_within_write_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.control_plane import server as server_module

    monkeypatch.setattr(server_module, "_EVENT_STREAM_WRITE_TIMEOUT_SECONDS", 0.25)
    app = _app()
    frame_write_started = Event()
    frame_write_finished = Event()
    handler_finished = Event()
    write_durations: list[float] = []

    class ObservedWriter:
        def __init__(self, writer) -> None:
            self._writer = writer

        def write(self, data: bytes) -> int:
            started = time.monotonic()
            is_event_frame = data.startswith(b"event: invalidate\n")
            if is_event_frame:
                frame_write_started.set()
            try:
                return self._writer.write(data)
            finally:
                elapsed = time.monotonic() - started
                write_durations.append(elapsed)
                if is_event_frame:
                    frame_write_finished.set()

        def __getattr__(self, name: str) -> Any:
            return getattr(self._writer, name)

    base_handler = make_handler(app)

    class ObservedHandler(base_handler):
        def setup(self) -> None:
            super().setup()
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
            self.wfile = cast(Any, ObservedWriter(self.wfile))

        def handle(self) -> None:
            try:
                super().handle()
            finally:
                handler_finished.set()

    class SmallSendBufferServer(ThreadingHTTPServer):
        def get_request(self):
            request, client_address = super().get_request()
            request.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
            return request, client_address

    server = SmallSendBufferServer(("127.0.0.1", 0), ObservedHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client: socket.socket | None = None
    try:
        client = socket.socket()
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 512)
        client.settimeout(3)
        client.connect(cast(tuple[str, int], server.server_address))
        request_headers = _session_headers(app, OPERATOR_CAPABILITY)
        request = (
            "GET /api/v1/events HTTP/1.0\r\n"
            + "".join(f"{name}: {value}\r\n" for name, value in request_headers.items())
            + "\r\n"
        )
        client.sendall(request.encode("ascii"))
        response_headers = bytearray()
        while not response_headers.endswith(b"\r\n\r\n"):
            response_headers.extend(client.recv(1))
        assert response_headers.startswith(b"HTTP/1.0 200")
        assert b"Content-Type: text/event-stream\r\n" in response_headers
        assert app.invalidation_hub.active_count == 1

        blocked_write_observed = False
        for revision in range(1, 2_000):
            # Advance only after the preceding frame write completes. This
            # prevents the hub queue from overflowing before TCP backpressure
            # has actually stalled the handler's writer.
            frame_write_started.clear()
            frame_write_finished.clear()
            app.invalidation_hub.publish(_record(identity="x" * 128, revision=revision))
            assert frame_write_started.wait(timeout=1), write_durations
            if not frame_write_finished.wait(timeout=0.05):
                blocked_write_observed = True
                break
            if app.invalidation_hub.active_count == 0:
                break

        assert blocked_write_observed, write_durations
        assert app.invalidation_hub.active_count == 1
        for revision in range(2_000, 2_000 + MAX_QUEUED_EVENT_FRAMES + 1):
            app.invalidation_hub.publish(_record(identity="x" * 128, revision=revision))

        assert app.invalidation_hub.active_count == 0
        release_started = time.monotonic()
        assert handler_finished.wait(timeout=1)
        assert time.monotonic() - release_started < 1
    finally:
        app.close_event_streams()
        if client is not None:
            client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()
