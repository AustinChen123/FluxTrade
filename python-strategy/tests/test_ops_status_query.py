from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
import pytest

from src.control_plane.ops_status_query import OpsStatusQuery
from src.core.orm_models import Base, SystemEvent
from src.core.runtime_environment import RuntimeEnvironment


class FakeRedis:
    def __init__(self, state: Any = b"OK", listeners: int = 1, fail: bool = False) -> None:
        self.state, self.listeners, self.fail = state, listeners, fail

    def get(self, key: str) -> Any:
        assert key == "fluxtrade:test:system:state"
        if self.fail:
            raise RuntimeError("secret detail")
        return self.state

    def pubsub_numsub(self, channel: str) -> list[tuple[str, int]]:
        assert channel == "cmd:strategy:control"
        return [(channel, self.listeners)]


def test_authority_matrix_and_listener_are_independent():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine, tables=[SystemEvent.__table__])
    factory = sessionmaker(bind=engine)
    with factory() as session:
        for durable in ("OK", "LOCKDOWN", "BROKEN"):
            session.add(SystemEvent(event_type="ops", event_subtype="kill_switch_state", payload={"state": durable}))
        session.commit()
    query = OpsStatusQuery(FakeRedis(b"LOCKDOWN", 0), factory, RuntimeEnvironment("test"))
    assert query.read() == {"state": "LOCKDOWN", "redis_state": "LOCKDOWN", "durable_state": None, "listener_available": False}
    with factory() as session:
        session.query(SystemEvent).delete()
        session.add(SystemEvent(event_type="ops", event_subtype="kill_switch_state", payload={"state": "OK"}))
        session.commit()
    assert OpsStatusQuery(FakeRedis(b"OK"), factory, RuntimeEnvironment("test")).read()["state"] == "OK"
    for redis_state in (None, b"invalid"):
        result = OpsStatusQuery(FakeRedis(redis_state), factory, RuntimeEnvironment("test")).read()
        assert result["state"] == "UNKNOWN"
        assert result["redis_state"] is None
    engine.dispose()


@pytest.mark.parametrize(
    ("redis_state", "durable_state", "expected"),
    [
        ("LOCKDOWN", "LOCKDOWN", "LOCKDOWN"),
        ("LOCKDOWN", "OK", "LOCKDOWN"),
        ("LOCKDOWN", None, "LOCKDOWN"),
        ("OK", "LOCKDOWN", "LOCKDOWN"),
        ("OK", "OK", "OK"),
        ("OK", None, "UNKNOWN"),
        (None, "LOCKDOWN", "LOCKDOWN"),
        (None, "OK", "UNKNOWN"),
        (None, None, "UNKNOWN"),
        ("bad", "OK", "UNKNOWN"),
        (b"\xff", "OK", "UNKNOWN"),
        ({}, "OK", "UNKNOWN"),
        ([], "OK", "UNKNOWN"),
        ("OK", {"state": "OK"}, "UNKNOWN"),
        ("OK", [], "UNKNOWN"),
    ],
)
def test_two_authority_classification_matrix(redis_state: Any, durable_state: Any, expected: str) -> None:
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine, tables=[SystemEvent.__table__])
    factory = sessionmaker(bind=engine)
    with factory() as session:
        if durable_state is not None:
            session.add(SystemEvent(event_type="ops", event_subtype="kill_switch_state", payload={"state": durable_state}))
            session.commit()
    result = OpsStatusQuery(FakeRedis(redis_state), factory, RuntimeEnvironment("test")).read()
    assert result["state"] == expected
    assert result["redis_state"] == (redis_state if isinstance(redis_state, str) and redis_state in {"OK", "LOCKDOWN"} else None)
    assert result["durable_state"] == (durable_state if isinstance(durable_state, str) and durable_state in {"OK", "LOCKDOWN"} else None)
    engine.dispose()


def test_status_query_failure_is_propagated_for_sanitized_http_handling():
    class BrokenSession:
        def __call__(self) -> Session:
            raise RuntimeError("database password")

    try:
        OpsStatusQuery(FakeRedis(), BrokenSession(), RuntimeEnvironment("test")).read()
    except RuntimeError as exc:
        assert "database password" in str(exc)
    else:
        raise AssertionError("expected query failure")
