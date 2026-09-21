"""Recording-session contracts only; PostgreSQL concurrency remains a separate gate."""

from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles.publication import CanonicalJsonObject
from src.core.market_data.profiles.repository import (
    ProfileIntegrityError,
    ProfileRepository,
)
from test_profile_publication import publication


def harness(corrupt: bool = False) -> tuple[ProfileRepository, MagicMock, list[str]]:
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = False
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    session.scalar.side_effect = [None, now]
    header: dict[str, Any] = {}
    bins: list[dict[str, Any]] = []
    events: list[str] = []

    def execute(statement: Any, parameters: Any = None) -> MagicMock:
        sql = str(statement)
        events.append(sql)
        result = MagicMock()
        if sql.startswith("INSERT INTO volume_profile_snapshot"):
            header.update(parameters)
            assert header["quality"] == "PARTIAL" and header["published_at"] is None
        elif sql.startswith("INSERT INTO volume_profile_bin"):
            bins.extend(parameters)
        elif sql.startswith("UPDATE"):
            header.update(quality="VERIFIED", published_at=now)
        result.mappings.return_value.one_or_none.return_value = dict(header) or None
        result.mappings.return_value.one.return_value = dict(header)
        result.mappings.return_value.all.return_value = bins[:-1] if corrupt else bins
        return result

    session.execute.side_effect = execute
    return ProfileRepository(lambda: nullcontext(session)), session, events


def test_publish_readback_order_and_idempotency() -> None:
    repo, session, events = harness()
    result = repo.publish(publication())
    assert result.snapshot_id == publication().content_sha256
    assert result.revision == 1 and not result.already_present
    assert result.publication == publication()
    assert events[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "set_config" in events[1]
    assert "pg_advisory_xact_lock" in events[2]
    assert events[3].startswith("SELECT volume_profile_snapshot.")
    update_index = next(i for i, sql in enumerate(events) if sql.startswith("UPDATE"))
    assert "ORDER BY volume_profile_bin.bin_index" in events[update_index - 1]
    session.flush.assert_called_once()
    assert session.begin.return_value.__exit__.call_args.args == (None, None, None)
    events.clear()
    changed = replace(
        publication(),
        source_manifest=CanonicalJsonObject({"new": 1}),
        reconciliation=CanonicalJsonObject({"new": 2}),
        source_available_at=result.computed_at,
        availability_basis="MODELED",
        raw_retention_state="DELETED",
    )
    repeated = repo.publish(changed)
    assert repeated.already_present and repeated.publication == publication()
    assert not any(sql.startswith(("INSERT", "UPDATE")) for sql in events)
    assert repo.get_verified(result.snapshot_id) == repo.publish(publication())


def test_corrupt_readback_rolls_back_without_promotion() -> None:
    repo, session, events = harness(corrupt=True)
    with pytest.raises(ProfileIntegrityError, match="^profile integrity check failed$"):
        repo.publish(publication())
    assert not any(sql.startswith("UPDATE") for sql in events)
    assert (
        session.begin.return_value.__exit__.call_args.args[0] is ProfileIntegrityError
    )


def test_commit_failure_propagates_instead_of_returning_success() -> None:
    repo, session, _ = harness()
    failure = RuntimeError("commit failed")
    session.begin.return_value.__exit__.side_effect = failure
    with pytest.raises(RuntimeError) as caught:
        repo.publish(publication())
    assert caught.value is failure


@pytest.mark.parametrize("field,value", [("quality", "PARTIAL"), ("base_volume", 999)])
def test_existing_invalid_content_is_not_repaired(field: str, value: object) -> None:
    repo, session, events = harness()
    repo.publish(publication())
    execute = session.execute.side_effect

    def damaged(statement: Any, parameters: Any = None) -> MagicMock:
        result = execute(statement, parameters)
        row = result.mappings.return_value.one_or_none.return_value
        if row is not None:
            row[field] = value
        return result

    session.execute.side_effect = damaged
    events.clear()
    with pytest.raises(ProfileIntegrityError):
        repo.publish(publication())
    assert not any(sql.startswith(("INSERT", "UPDATE")) for sql in events)


def test_missing_and_unsupported_session_fail_closed() -> None:
    repo, session, _ = harness()
    assert repo.get_verified("absent") is None
    session.get_bind.return_value.dialect.name = "sqlite"
    session.execute.reset_mock()
    with pytest.raises(ValueError, match="requires PostgreSQL"):
        repo.publish(publication())
    session.execute.assert_not_called()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = True
    with pytest.raises(ValueError, match="fresh transaction"):
        repo.publish(publication())
