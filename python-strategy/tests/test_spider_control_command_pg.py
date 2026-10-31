"""Durable scoped STOP on the actual isolated PostgreSQL runtime projection."""

from threading import RLock
import os
from typing import Any, cast

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.core.command_router import CommandResult
from src.core.orm_models import StrategyState, StrategyStateTransition
from src.core.spider_management.orm import (
    SpiderControlReceipt,
    SpiderRuntimeScope,
)
from src.core.spider_management.runtime_owner import (
    RuntimeScopeKey,
    SpiderRuntimeOwner,
)
from src.core.strategy_state_manager import StrategyStateManager
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_financial_delivery_pg import (
    _install_owner,
    operation_pg as _operation_pg,
)
from test_spider_runtime_owner_pg import _owned_dispatch

fresh_pg_db = _migration_fresh_pg_db
pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


class _RedisProbe:
    def publish(self, _channel: str, _message: str) -> int:
        return 1


def _owner_with_lifecycle(engine: Engine):
    install_owner, scope, operation_id, order_manager, _prepared, readback, _confirmation, source = (
        _install_owner(engine)
    )
    factory = sessionmaker(engine)
    manager = StrategyStateManager(factory, _RedisProbe())
    lifecycle_lock = RLock()
    order_event_lock = RLock()
    owner = SpiderRuntimeOwner(
        factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            cast(dict[str, Any], readback["effective"])["confirmation_request_id"]: source
        },
        strategy_state_manager=manager,
        lifecycle_lock=lambda _strategy_id: lifecycle_lock,
        order_event_lock=order_event_lock,
    )
    assert install_owner.readback(scope, operation_id) == readback
    return owner, scope, operation_id, factory, readback


def _command(scope: RuntimeScopeKey, *, key: str, version: int) -> dict[str, Any]:
    return {
        "command": "STOP",
        "strategy_id": scope.strategy_id,
        "params": {
            "strategy_id": scope.strategy_id,
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "actor": "api_key",
            "idempotency_key": key,
            "expected_version": version,
        },
    }


def _dispatch(owner: SpiderRuntimeOwner, message: dict[str, Any]) -> CommandResult:
    results: list[CommandResult | None] = []
    _owned_dispatch(owner, results).dispatch(message)
    assert len(results) == 1 and results[0] is not None
    return cast(CommandResult, results[0])


def test_stop_is_atomic_idempotent_and_does_not_replace_held_operation(operation_pg: Engine):
    engine = operation_pg
    owner, scope, operation_id, factory, initial = _owner_with_lifecycle(engine)
    start_state_version = cast(int, initial["state_version"])
    before_plan = initial["initial_plan_id"]
    before_missing = initial["initial_plan_missing_members"]

    stopped = _dispatch(owner, _command(scope, key="stop-key", version=start_state_version))
    assert stopped.success is True and stopped.completed is True
    assert stopped.data is not None
    assert stopped.data["disposition"] == "COMPLETED"
    assert stopped.data["reason_code"] == "SPIDER_STOPPED"
    stop_readback = cast(dict[str, Any], stopped.data["readback"])
    assert stop_readback["runtime_control"] == "STOP"
    assert stop_readback["phase"] == "INCOMPLETE"
    assert stop_readback["operation_id"] == operation_id
    assert cast(dict[str, object], stop_readback["effective"])["generation"] == cast(
        dict[str, object], initial["effective"]
    )["generation"]
    assert stop_readback["initial_plan_id"] == before_plan
    assert stop_readback["initial_plan_missing_members"] == before_missing
    assert stop_readback["trading_ready"] is False

    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope.control_mode,
                   SpiderRuntimeScope.state_version,
                   SpiderRuntimeScope.control_barrier_sequence,
                   SpiderRuntimeScope.last_received_sequence,
                   SpiderRuntimeScope.current_operation_id,
                   SpiderRuntimeScope.generation,
                   SpiderRuntimeScope.initial_plan_id)
            .where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).one()
        receipt = session.execute(
            select(SpiderControlReceipt).where(
                SpiderControlReceipt.strategy_id == scope.strategy_id,
                SpiderControlReceipt.idempotency_key == "stop-key",
            )
        ).scalar_one()
        after_state = session.execute(
            select(StrategyState.status, StrategyState.version).where(
                StrategyState.strategy_id == scope.strategy_id
            )
        ).one()
        transition_count = session.scalar(
            select(func.count()).select_from(StrategyStateTransition).where(
                StrategyStateTransition.strategy_id == scope.strategy_id,
                StrategyStateTransition.to_status == "STOPPED",
            )
        )
    assert runtime.control_mode == "STOP"
    assert runtime.state_version == start_state_version + 1
    assert runtime.control_barrier_sequence == runtime.last_received_sequence
    assert runtime.current_operation_id == operation_id
    assert runtime.generation == cast(
        dict[str, object], initial["effective"]
    )["generation"]
    assert runtime.initial_plan_id == before_plan
    assert receipt.before_state_version == start_state_version
    assert receipt.after_state_version == start_state_version + 1
    assert receipt.barrier_sequence == runtime.last_received_sequence
    assert receipt.outcome == "COMPLETED"
    assert after_state == ("STOPPED", start_state_version + 1)
    assert transition_count == 1

    retry = _dispatch(
        owner, _command(scope, key="stop-key", version=start_state_version)
    )
    assert retry.success is True and retry.completed is True
    assert retry.data == stopped.data
    with factory() as session:
        assert session.scalar(
            select(func.count()).select_from(StrategyStateTransition).where(
                StrategyStateTransition.strategy_id == scope.strategy_id,
                StrategyStateTransition.to_status == "STOPPED",
            )
        ) == 1
        assert session.scalar(
            select(func.count()).select_from(SpiderControlReceipt).where(
                SpiderControlReceipt.strategy_id == scope.strategy_id,
            )
        ) == 1

    conflict = _dispatch(
        owner, _command(scope, key="stop-key", version=start_state_version + 1)
    )
    assert conflict.success is False and conflict.completed is True
    assert conflict.data is not None
    assert conflict.data["reason_code"] == "SPIDER_CONTROL_KEY_CONFLICT"
    assert conflict.data["readback"] is not None

    stale = _dispatch(
        owner, _command(scope, key="new-stop-key", version=start_state_version)
    )
    assert stale.success is False and stale.completed is True
    assert stale.data is not None
    assert stale.data["reason_code"] == "SPIDER_CONTROL_STALE"
    assert stale.data["readback"] is not None

    install_key = _dispatch(
        owner, _command(scope, key="start-confirmation", version=start_state_version + 1)
    )
    assert install_key.success is False and install_key.completed is True
    assert install_key.data is not None
    assert install_key.data["reason_code"] == "SPIDER_CONTROL_KEY_CONFLICT"
    assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})


def test_stop_flush_failure_rolls_back_receipt_runtime_and_strategy_projection(
    operation_pg: Engine,
):
    engine = operation_pg
    initial_owner, scope, _operation_id, order_manager, _prepared, initial, _confirmation, source = (
        _install_owner(engine)
    )
    with engine.connect() as connection:
        original_state = connection.execute(
            text("SELECT status, version FROM strategy_state WHERE strategy_id=:id"),
            {"id": scope.strategy_id},
        ).one()
    observed = {"receipt_visible": False, "raised": False}

    class FailAfterReceiptFlushSession(Session):
        def flush(self, objects=None) -> None:
            super().flush(objects)
            if not observed["raised"]:
                count = self.connection().execute(
                    text("SELECT count(*) FROM spider_control_receipt WHERE idempotency_key='rollback-stop'")
                ).scalar_one()
                if count == 1:
                    observed["receipt_visible"] = True
                    observed["raised"] = True
                    raise RuntimeError("injected STOP transaction failure")

    factory = sessionmaker(engine, class_=FailAfterReceiptFlushSession)
    manager = StrategyStateManager(factory, _RedisProbe())
    owner = SpiderRuntimeOwner(
        factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            cast(dict[str, Any], initial["effective"])["confirmation_request_id"]: source
        },
        strategy_state_manager=manager,
        lifecycle_lock=lambda _strategy_id: RLock(),
        order_event_lock=RLock(),
    )
    result = _dispatch(
        owner,
        _command(
            scope,
            key="rollback-stop",
            version=cast(int, initial["state_version"]),
        ),
    )
    assert observed == {"receipt_visible": True, "raised": True}
    assert result.success is False and result.completed is False
    assert result.data is not None
    assert result.data["disposition"] == "UNKNOWN"
    assert result.data["readback"] is None
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT count(*) FROM spider_control_receipt WHERE idempotency_key='rollback-stop'")
        ).scalar_one() == 0
        assert connection.execute(
            text("SELECT control_mode FROM spider_runtime_scope WHERE strategy_id=:id"),
            {"id": scope.strategy_id},
        ).scalar_one() == "RUN"
        assert connection.execute(
            text("SELECT status, version FROM strategy_state WHERE strategy_id=:id"),
            {"id": scope.strategy_id},
        ).one() == original_state
    assert initial_owner.readback(scope, _operation_id) is not None


def test_stop_commit_response_loss_is_proved_by_same_key_independent_readback(
    operation_pg: Engine,
):
    engine = operation_pg
    _initial_owner, scope, _operation_id, order_manager, _prepared, initial, _confirmation, source = (
        _install_owner(engine)
    )
    close_state = {"pending": True, "writer_closed": False}

    class LoseFirstWriterCloseSession(Session):
        def close(self) -> None:
            super().close()
            if close_state["pending"]:
                close_state["pending"] = False
                close_state["writer_closed"] = True
                raise RuntimeError("injected post-commit close loss")

    factory = sessionmaker(engine, class_=LoseFirstWriterCloseSession)
    manager = StrategyStateManager(factory, _RedisProbe())
    owner = SpiderRuntimeOwner(
        factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            cast(dict[str, Any], initial["effective"])["confirmation_request_id"]: source
        },
        strategy_state_manager=manager,
        lifecycle_lock=lambda _strategy_id: RLock(),
        order_event_lock=RLock(),
    )
    result = _dispatch(
        owner,
        _command(scope, key="close-loss-stop", version=cast(int, initial["state_version"])),
    )
    assert close_state == {"pending": False, "writer_closed": True}
    assert result.success is True and result.completed is True
    assert result.data is not None and result.data["disposition"] == "COMPLETED"
    assert result.data["readback"] is not None
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT count(*) FROM spider_control_receipt WHERE idempotency_key='close-loss-stop'")
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT status, version FROM strategy_state WHERE strategy_id=:id"),
            {"id": scope.strategy_id},
        ).one() == ("STOPPED", cast(int, initial["state_version"]) + 1)


def test_exact_retry_writer_error_uses_proved_same_key_receipt(operation_pg: Engine):
    engine = operation_pg
    _install, scope, _operation_id, order_manager, _prepared, initial, _confirmation, source = (
        _install_owner(engine)
    )
    ordinary_factory = sessionmaker(engine)
    manager = StrategyStateManager(ordinary_factory, _RedisProbe())
    initial_owner = SpiderRuntimeOwner(
        ordinary_factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            cast(dict[str, Any], initial["effective"])["confirmation_request_id"]: source
        },
        strategy_state_manager=manager,
        lifecycle_lock=lambda _strategy_id: RLock(),
        order_event_lock=RLock(),
    )
    original = _command(
        scope, key="preexisting-stop", version=cast(int, initial["state_version"])
    )
    first = _dispatch(initial_owner, original)
    assert first.success is True

    fault = {"pending": True}

    class FailBeforeWriterQuerySession(Session):
        def execute(self, *args, **kwargs):
            if fault["pending"]:
                fault["pending"] = False
                raise RuntimeError("injected retry writer failure")
            return super().execute(*args, **kwargs)

    failing_factory = sessionmaker(engine, class_=FailBeforeWriterQuerySession)
    retry_owner = SpiderRuntimeOwner(
        failing_factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            cast(dict[str, Any], initial["effective"])["confirmation_request_id"]: source
        },
        strategy_state_manager=StrategyStateManager(failing_factory, _RedisProbe()),
        lifecycle_lock=lambda _strategy_id: RLock(),
        order_event_lock=RLock(),
    )
    retry = _dispatch(retry_owner, original)
    assert fault == {"pending": False}
    assert retry.success is True and retry.completed is True
    assert retry.data == first.data
    with engine.connect() as connection:
        assert connection.execute(
            text("SELECT count(*) FROM spider_control_receipt WHERE idempotency_key='preexisting-stop'")
        ).scalar_one() == 1
        assert connection.execute(
            text("SELECT count(*) FROM strategy_state_transitions WHERE strategy_id=:id AND to_status='STOPPED'"),
            {"id": scope.strategy_id},
        ).scalar_one() == 1


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_advanced_stop_retry_requires_valid_current_stop_receipt(
    operation_pg: Engine, damage: str
):
    engine = operation_pg
    owner, scope, _operation_id, _factory, initial = _owner_with_lifecycle(engine)
    version = cast(int, initial["state_version"])
    original = _command(scope, key="historical-stop", version=version)
    first = _dispatch(owner, original)
    assert first.success is True
    second = _dispatch(owner, _command(scope, key="current-stop", version=version + 1))
    assert second.success is True
    with engine.connect() as connection:
        current_before_retry = connection.execute(
            text("SELECT control_mode, state_version, control_barrier_sequence FROM spider_runtime_scope WHERE strategy_id=:id"),
            {"id": scope.strategy_id},
        ).one()

    with engine.begin() as connection:
        if damage == "missing":
            connection.execute(
                text("DELETE FROM spider_control_receipt WHERE idempotency_key='current-stop'")
            )
        else:
            connection.execute(
                text("UPDATE spider_control_receipt SET request_payload=:payload WHERE idempotency_key='current-stop'"),
                {"payload": b"{}"},
            )

    retry = _dispatch(owner, original)
    assert retry.success is False and retry.completed is False
    assert retry.data is not None
    assert retry.data["disposition"] == "UNKNOWN"
    assert retry.data["reason_code"] == "SPIDER_CONTROL_READBACK_UNKNOWN"
    with engine.connect() as connection:
        current = connection.execute(
            text("SELECT control_mode, state_version, control_barrier_sequence FROM spider_runtime_scope WHERE strategy_id=:id"),
            {"id": scope.strategy_id},
        ).one()
        assert current == current_before_retry
        assert current[0] == "STOP" and current[1] == version + 2
        assert connection.execute(
            text("SELECT count(*) FROM strategy_state_transitions WHERE strategy_id=:id AND to_status='STOPPED'"),
            {"id": scope.strategy_id},
        ).scalar_one() == 2
