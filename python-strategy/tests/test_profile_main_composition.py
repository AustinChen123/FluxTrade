from unittest.mock import Mock

import pytest

from src.control_plane import main
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.profile_http_query import ProfileQueryService
from src.core.market_data.profiles.read_repository import ProfileReadRepository


def test_default_reader_and_service_are_wired_without_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = Mock(side_effect=AssertionError("no DB at construction"))
    monkeypatch.setattr(
        main, "create_engine", Mock(side_effect=AssertionError("no new engine"))
    )
    app = main.build_control_plane_app(
        redis_client=Mock(),
        db_session_factory=factory,
        job_store=InMemoryJobStore(),
        parameter_search_evaluator=Mock(),
        api_key="key",
    )
    try:
        service = app.profile_query_service
        assert type(service) is ProfileQueryService
        assert type(service._provider) is ProfileReadRepository
        assert service._provider._sessions is factory
        assert service._utc is main._utc_ms and service._monotonic is main._monotonic_ms
        factory.assert_not_called()
        response = app.handle(
            "GET",
            "/api/v1/market-data/volume-profiles?invalid=1",
            headers={"X-API-Key": "key"},
        )
        assert response.status_code == 400 and response.body == {
            "error": "INVALID_REQUEST"
        }
        factory.assert_not_called()
    finally:
        assert app.shutdown(1)


def test_injected_service_identity_no_default_reader(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = ProfileQueryService(Mock(), utc_ms=lambda: 0, monotonic_ms=lambda: 0)
    reader = Mock(
        side_effect=AssertionError("injected service must bypass default reader")
    )
    monkeypatch.setattr(main, "ProfileReadRepository", reader)
    app = main.build_control_plane_app(
        redis_client=Mock(),
        db_session_factory=Mock(),
        job_store=InMemoryJobStore(),
        parameter_search_evaluator=Mock(),
        profile_query_service=service,
    )
    try:
        assert app.profile_query_service is service
        reader.assert_not_called()
    finally:
        assert app.shutdown(1)


@pytest.mark.parametrize("nanoseconds", [0, 999999, 1000000, 1234567890123456789])
def test_clocks_use_integer_nanoseconds(
    monkeypatch: pytest.MonkeyPatch, nanoseconds: int
) -> None:
    utc, mono = Mock(return_value=nanoseconds), Mock(return_value=nanoseconds)
    monkeypatch.setattr(main.time, "time_ns", utc)
    monkeypatch.setattr(main.time, "monotonic_ns", mono)
    monkeypatch.setattr(
        main.time, "time", Mock(side_effect=AssertionError("float clock forbidden"))
    )
    monkeypatch.setattr(
        main.time,
        "monotonic",
        Mock(side_effect=AssertionError("float clock forbidden")),
    )
    for clock in (main._utc_ms, main._monotonic_ms):
        value = clock()
        assert type(value) is int and value == nanoseconds // 1000000 and value >= 0
    utc.assert_called_once()
    mono.assert_called_once()
