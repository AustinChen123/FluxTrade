"""Caller-owned cancellation atomicity, not runtime STOP orchestration."""

import os
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session, sessionmaker

from src.core.strategy_activation_request_store import (
    ProfileActivationRequestStore,
    ProfileActivationRequestStatus,
)
from src.core.strategy_deactivation_service import (
    cancel_pending_profile_activation_request,
)
from src.core.strategy_state_manager import StrategyStateManager
from test_profile_activation_request import request
from test_strategy_activation_request_store_pg import prepare
from test_strategy_state_request_atomic_pg import evidence

pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


@pytest.mark.parametrize("rollback", [False, True])
def test_cancel_state_request_audit_atomicity(profile_repository_pg, rollback):
    engine, value = profile_repository_pg, request()
    prepare(engine, value)
    store = ProfileActivationRequestStore(sessionmaker(engine))
    admitted = store.admit(value, current=value.intent)
    manager = StrategyStateManager(MagicMock(), MagicMock())
    error = RuntimeError("fixed rollback")
    result = None
    terminal_evidence = (
        ("STOPPED", 1),
        [
            (
                "READY",
                "STOPPED",
                "operator",
                "ACTIVATION_CANCELLED",
                admitted.requested_at,
            )
        ],
        "CANCELLED",
    )
    with Session(engine) as writer:
        try:
            with writer.begin():
                result = cancel_pending_profile_activation_request(
                    writer,
                    manager,
                    environment=value.intent.key.environment,
                    strategy_id=value.intent.key.strategy_id,
                    actor="operator",
                    terminal_at=admitted.requested_at,
                    expected_version=0,
                )
                assert result is not None
                assert evidence(writer) == terminal_evidence
                if rollback:
                    raise error
        except RuntimeError as caught:
            assert rollback and caught is error
        else:
            assert not rollback
    assert result is not None
    expected = (("READY", 0), [], "PENDING") if rollback else terminal_evidence
    with Session(engine) as reader:
        assert reader is not writer
        assert evidence(reader) == expected
    confirmed = ProfileActivationRequestStore(sessionmaker(engine)).confirm(value)
    assert confirmed == (admitted if rollback else result.record)
    if not rollback:
        assert confirmed is not None
        assert confirmed.status is ProfileActivationRequestStatus.CANCELLED
        assert confirmed.terminal_reason == "ACTIVATION_CANCELLED"
        assert confirmed.terminal_at == admitted.requested_at
        assert result.transition.version == 1
