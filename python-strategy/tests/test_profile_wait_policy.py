"""Profile persistence recording contracts; real lock timeout acceptance is separate."""
from contextlib import nullcontext
from dataclasses import FrozenInstanceError
from decimal import Decimal
from functools import partial

import pytest
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles.jobs import ProfileIngestJobStore
from src.core.market_data.profiles.repository import ProfileRepository, TransactionWaitPolicy
from test_profile_jobs import SPEC, harness as job_harness
from test_profile_publication import publication
from test_profile_repository import harness as publication_harness


@pytest.mark.parametrize("lock,statement", [
    (True, 10), (1, True), (0, 1), (-1, 1), (60001, 60001), (1, 300001),
    (2, 1), (1, 0), (Decimal("1"), 2), (1, "2"),
])
def test_invalid_wait_policy_is_rejected(lock, statement) -> None:
    with pytest.raises(ValueError, match="^invalid profile transaction wait policy$"):
        TransactionWaitPolicy(lock, statement)


def test_wait_policy_boundaries_defaults_and_immutability() -> None:
    default = TransactionWaitPolicy()
    assert (default.lock_timeout_ms, default.statement_timeout_ms) == (2000, 10000)
    for lock, statement in ((1, 1), (60000, 60000), (60000, 300000)):
        assert TransactionWaitPolicy(lock, statement).statement_timeout_ms == statement
    with pytest.raises(FrozenInstanceError):
        setattr(default, "lock_timeout_ms", 1)


@pytest.mark.parametrize("owner", ["publication", "job"])
def test_custom_wait_policy_order_bound_values_and_read_only_unchanged(owner: str) -> None:
    policy = TransactionWaitPolicy(7, 19)
    if owner == "publication":
        _, session, _ = publication_harness()
        store = ProfileRepository(lambda: nullcontext(session), policy)
        result = store.publish(publication())
        read = partial(store.get_verified, result.snapshot_id)
    else:
        _, session, _, _ = job_harness()
        job_store = ProfileIngestJobStore(lambda: nullcontext(session), policy)
        job_store.register(SPEC)
        read = partial(job_store.get, SPEC.id)
    statements = [call.args[0] for call in session.execute.call_args_list]
    assert str(statements[0]) == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    setting = statements[1].compile()
    assert setting.params == {"lock_timeout": "7ms", "statement_timeout": "19ms"}
    assert "set_config('lock_timeout', :lock_timeout, true)" in str(setting)
    assert "set_config('statement_timeout', :statement_timeout, true)" in str(setting)
    assert "7ms" not in str(setting) and "19ms" not in str(setting)
    session.execute.reset_mock()
    assert read() is not None
    assert not any("set_config" in str(c.args[0]) or "SET TRANSACTION" in str(c.args[0])
                   for c in session.execute.call_args_list)


@pytest.mark.parametrize("owner", ["publication", "job"])
@pytest.mark.parametrize("failure_index", [1, 2])
def test_timeout_error_propagates_without_subsequent_mutation(owner: str, failure_index: int) -> None:
    if owner == "publication":
        _, session, _ = publication_harness()
        operation = partial(ProfileRepository(lambda: nullcontext(session)).publish, publication())
    else:
        _, session, _, _ = job_harness()
        operation = partial(ProfileIngestJobStore(lambda: nullcontext(session)).register, SPEC)
    failure = DBAPIError("timeout", None, Exception("timeout"))
    session.execute.side_effect = [None] * failure_index + [failure]
    with pytest.raises(DBAPIError) as caught:
        operation()
    assert caught.value is failure and session.execute.call_count == failure_index + 1
    assert session.begin.return_value.__exit__.call_args.args[0] is DBAPIError
    assert not any(str(c.args[0]).startswith(("INSERT", "UPDATE")) for c in session.execute.call_args_list[:-1])
