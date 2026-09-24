"""Recording-session contracts only; PostgreSQL concurrency remains a separate gate."""

from contextlib import nullcontext
import hashlib
import json
from dataclasses import asdict, replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import RowMapping

from src.core.market_data.profiles import repository
from src.core.market_data.profiles.publication import CanonicalJsonObject
from src.core.market_data.profiles.repository import (
    ProfileIntegrityError,
    ProfileRepository,
)
from test_profile_publication import publication


def verified_rows() -> tuple[RowMapping, list[RowMapping]]:
    value = publication()
    content = value.content
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    header = dict(**asdict(content), id=content.content_sha256, content_sha256=content.content_sha256,
                  period="1d", timezone="UTC", revision=1, base_volume=content.base_volume,
                  quote_volume=content.quote_volume, aggregate_count=content.aggregate_count,
                  occupied_bins=content.occupied_bins, source_manifest={}, reconciliation={},
                  source_available_at=None, availability_basis="OBSERVED", raw_retention_state="PRESENT",
                  quality="VERIFIED", computed_at=now, published_at=now)
    return cast(RowMapping, header), [cast(RowMapping, asdict(b)) for b in content.bins]


def test_verify_queries_once_with_identity_order_and_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    row, bins = verified_rows()
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = bins
    delegate = MagicMock(return_value=publication())
    monkeypatch.setattr(repository, "_verify_rows", delegate)
    assert repository._verify(session, row, "PARTIAL", publication()) == publication()
    session.execute.assert_called_once()
    statement = session.execute.call_args.args[0].compile(dialect=postgresql.dialect())
    assert "WHERE volume_profile_bin.snapshot_id = %(snapshot_id_1)s" in str(statement)
    assert "ORDER BY volume_profile_bin.bin_index" in str(statement)
    assert statement.params == {"snapshot_id_1": row["id"]}
    delegate.assert_called_once_with(row, bins, quality="PARTIAL", expected=publication())


@pytest.mark.parametrize("field,value", [
    ("id", "a" * 64), ("content_sha256", "b" * 64), ("period", "1h"), ("timezone", "other"),
    ("revision", True), ("revision", 0), ("revision", 1 << 63), ("base_volume", 3),
    ("quote_volume", 35), ("base_volume", Decimal("4")), ("quote_volume", Decimal("36")),
    ("aggregate_count", 99), ("occupied_bins", 99), ("quality", "PARTIAL"),
    ("computed_at", None), ("published_at", None), ("source_available_at", datetime(2026, 1, 1)),
    ("source_manifest", []), ("reconciliation", {"invalid": Decimal(1)}),
    ("availability_basis", "other"), ("raw_retention_state", "other"),
    ("product_id", "invalid"), ("window_end_ms", 1), ("bin_step", Decimal("NaN")),
])
def test_row_verifier_matches_session_verifier_for_corruption(field: str, value: Any) -> None:
    row, bins = verified_rows()
    cast(dict[str, Any], row)[field] = value
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = bins
    for action in (lambda: repository._verify_rows(row, bins), lambda: repository._verify(session, row)):
        with pytest.raises(ProfileIntegrityError, match="^profile integrity check failed$") as caught:
            action()
        assert type(caught.value) is ProfileIntegrityError


@pytest.mark.parametrize("damage", ["missing", "reverse", "duplicate", "volume", "count", "metadata", "expected", "partial_published"])
def test_bin_metadata_expected_and_quality_invariants_share_one_verifier(damage: str) -> None:
    row, bins = verified_rows()
    quality, expected = "VERIFIED", publication()
    if damage == "missing":
        bins.pop()
    elif damage == "reverse":
        bins.reverse()
    elif damage == "duplicate":
        bins.append(bins[-1])
    elif damage in ("volume", "count"):
        cast(dict[str, Any], bins[0])["base_volume" if damage == "volume" else "aggregate_count"] = 0
    elif damage == "metadata":
        cast(dict[str, Any], row)["source_manifest"] = {"changed": True}
    elif damage == "expected":
        expected = replace(expected, raw_retention_state="DELETED")
    else:
        quality = "PARTIAL"
        cast(dict[str, Any], row)["quality"] = quality
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = bins
    for action in (lambda: repository._verify_rows(row, bins, quality=quality, expected=expected),
                   lambda: repository._verify(session, row, quality=quality, expected=expected)):
        with pytest.raises(ProfileIntegrityError, match="^profile integrity check failed$"):
            action()


@pytest.mark.parametrize("quality", ["VERIFIED", "PARTIAL"])
def test_valid_rows_and_session_verifiers_are_equivalent(quality: str) -> None:
    row, bins = verified_rows()
    cast(dict[str, Any], row)["quality"] = quality
    if quality == "PARTIAL":
        cast(dict[str, Any], row)["published_at"] = None
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = bins
    assert repository._verify_rows(row, bins, quality=quality, expected=publication()) == publication()
    assert repository._verify(session, row, quality=quality, expected=publication()) == publication()


@pytest.mark.parametrize("duplicate", [False, True])
def test_bin_order_rejection_is_not_masked_by_digest_or_expected_mismatch(duplicate: bool) -> None:
    row, bins = verified_rows()
    payload = json.loads(publication().content.content_bytes)
    if duplicate:
        cast(dict[str, Any], bins[1])["bin_index"] = bins[0]["bin_index"]
        payload["bins"][1]["bin_index"] = payload["bins"][0]["bin_index"]
    else:
        bins.reverse()
        payload["bins"].reverse()
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    cast(dict[str, Any], row).update(id=digest, content_sha256=digest)
    session = MagicMock()
    session.execute.return_value.mappings.return_value.all.return_value = bins
    # Totals/counts and digest match these malformed-order rows; expected is deliberately absent.
    for action in (lambda: repository._verify_rows(row, bins), lambda: repository._verify(session, row)):
        with pytest.raises(ProfileIntegrityError):
            action()


def harness(corrupt: bool = False) -> tuple[ProfileRepository, MagicMock, list[str]]:
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = False
    session.begin.return_value.__enter__.side_effect = lambda: setattr(session.in_transaction, "return_value", True)
    session.begin.return_value.__exit__.side_effect = lambda *_: setattr(session.in_transaction, "return_value", False)
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
