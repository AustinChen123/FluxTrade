"""End-to-end migration round-trip integration test (Task 0.7).

Verifies the full Alembic revision chain (base → head) on a real PostgreSQL
database:

1. ``test_full_upgrade_to_head`` — upgrade ``base`` → ``head`` and assert that
   the selected HEAD tables, columns, indexes and CHECK constraints
   exists with the expected shape.
2. ``test_sample_data_insertion_after_upgrade`` — insert representative rows
   into the new tables and verify that CHECK constraints and partial unique
   indexes behave correctly (positive + negative cases).
3. ``test_full_downgrade_to_base`` — downgrade ``head`` → ``base`` and assert
   that every HEAD schema object is gone (tables dropped, ALTERed columns
   removed).
4. ``test_round_trip_idempotent`` — upgrade → downgrade → upgrade twice and
   compare the resulting schema fingerprints to confirm idempotency.

Each test runs against an isolated database created via ``CREATE DATABASE``
on the explicitly selected PostgreSQL instance (Fixture Plan B). The database
is dropped at fixture teardown. The module requires
``FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS=1`` before checking connectivity, so a
default ``pytest`` run cannot create or drop databases.

Marked ``integration`` to keep it out of unit-only runs.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from threading import Barrier, Event
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import event, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import DBAPIError, IntegrityError, TimeoutError as PoolTimeoutError
from sqlalchemy.orm import Session, sessionmaker
from src.core.database_url import build_postgres_url
from src.core.market_data.profiles import jobs as profile_jobs
from src.core.market_data.profiles import invalidation as profile_invalidation
from src.core.market_data.profiles import repository as profile_repository
from src.core.market_data.profiles.handoff import parse_handoff
from src.core.market_data.profiles.ingest import ProfileIngestProcess
from src.core.market_data.profiles.publication import CanonicalJsonObject, VerifiedProfilePublication
from src.core.market_data.profiles.read_connection import ProfileReadConnection, ProfileReadConnectionConfig
from src.core.market_data.profiles.read_repository import ProfileReadRepository, ProfileReadTooLarge
from src.core.market_data.profiles.read_types import DailyProfileRef, OrderedProfileManifest
from src.core.market_data.profiles.types import ProfileBin, VolumeProfileContent

# Alembic is imported lazily inside tests so that import-time failures do not
# break collection on environments where alembic is missing.
ALEMBIC_INI = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "database", "alembic.ini")
)

pytestmark = pytest.mark.integration

if os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1":
    pytest.skip(
        "set FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS=1 to run destructive "
        "PostgreSQL migration tests",
        allow_module_level=True,
    )


# --------------------------------------------------------------------------- #
# Connection helpers
# --------------------------------------------------------------------------- #


def _admin_url() -> str:
    # Connect to the maintenance ``postgres`` database for CREATE/DROP DATABASE.
    return _target_url("postgres")


def _target_url(db_name: str) -> str:
    url = build_postgres_url({
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "fluxtrade"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "fluxtrade"),
        "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
        "POSTGRES_DB": db_name,
    })
    return url.set(drivername="postgresql+psycopg2").render_as_string(hide_password=False)


def _pg_reachable() -> bool:
    try:
        engine = sa.create_engine(_admin_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return True
    except Exception:  # pragma: no cover - environment dependent
        return False


# --------------------------------------------------------------------------- #
# Explicitly enabled connectivity gate
# --------------------------------------------------------------------------- #


if not _pg_reachable():  # pragma: no cover - environment dependent
    raise RuntimeError(
        "PostgreSQL migration test service unavailable after explicit enablement"
    )


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture()
def fresh_pg_db() -> Iterator[str]:
    """Create an isolated PostgreSQL database for the test, drop on exit.

    Returns the database name. The database is empty (no migrations applied)
    so each test owns the full upgrade/downgrade lifecycle.
    """
    db_name = f"test_migrations_{os.getpid()}_{int(time.time() * 1000)}"
    admin = sa.create_engine(_admin_url(), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    admin.dispose()
    try:
        yield db_name
    finally:
        admin = sa.create_engine(_admin_url(), isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            # Terminate any lingering connections from alembic before dropping.
            conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :db AND pid <> pg_backend_pid()"
                ),
                {"db": db_name},
            )
            conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}"'))
        admin.dispose()


def _alembic_config(db_name: str):
    """Build an Alembic Config pointed at ``db_name`` using the project ini."""
    from alembic.config import Config

    cfg = Config(ALEMBIC_INI)
    # ``script_location`` in alembic.ini uses %(here)s, which Alembic resolves
    # against the .ini file's directory — no extra fix-up needed.
    cfg.set_main_option("sqlalchemy.url", _target_url(db_name).replace("%", "%%"))
    cfg.attributes["expected_isolated_database"] = db_name
    return cfg


def _upgrade(db_name: str, target: str = "head") -> None:
    from alembic import command

    command.upgrade(_alembic_config(db_name), target)


_VP_BASE = Decimal("12345678901234567890.1234567890123456789012345678")
_VP_QUOTE = Decimal("98765432109876543210.9876543210987654321098765432")


def _insert_vp_snapshot(
    conn: sa.Connection, snapshot_id: str = "vp", revision: int = 1,
    digest: str = "a" * 64,
) -> None:
    conn.execute(text("""
        INSERT INTO volume_profile_snapshot (
            id, product_id, window_start_ms, window_end_ms, period, timezone,
            grid_id, bin_origin, bin_step, algorithm_version, revision, content_sha256,
            source_manifest, base_volume, quote_volume, aggregate_count, occupied_bins,
            quality, computed_at, published_at, availability_basis, reconciliation,
            raw_retention_state)
        VALUES (:id, 'vp-product', 0, 86400000, '1d', 'UTC', 'grid', -10, 10,
            'vp-v1', :revision, :digest, CAST(:manifest AS JSONB),
            :base, :quote, 1, 1, 'VERIFIED', now(), now(), 'OBSERVED',
            CAST(:manifest AS JSONB), 'NOT_STORED')
    """), {"id": snapshot_id, "revision": revision, "digest": digest,
            "base": _VP_BASE, "quote": _VP_QUOTE, "manifest": json.dumps({"fixture": True})})


@pytest.fixture
def volume_profile_pg(fresh_pg_db: str) -> Iterator[Engine]:
    """Migrated disposable schema only; not repository publication acceptance."""
    _upgrade(fresh_pg_db)
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO exchange (id, name) VALUES ('vp-exchange', 'Fixture')"))
            conn.execute(text("INSERT INTO product (id, exchange_id, base_asset, quote_asset) "
                              "VALUES ('vp-product', 'vp-exchange', 'BTC', 'USDT')"))
            _insert_vp_snapshot(conn)
            conn.execute(text("INSERT INTO volume_profile_bin VALUES ('vp', -2, :base, :quote, 1)"),
                         {"base": _VP_BASE, "quote": _VP_QUOTE})
        yield engine
    finally:
        engine.dispose()


def test_volume_profile_decimal_json_fk_and_cascade_schema(volume_profile_pg: Engine) -> None:
    """Exact unconstrained NUMERIC roundtrip and sparse-bin FK, not publication."""
    engine = volume_profile_pg
    with engine.connect() as conn:
        for table in ("volume_profile_snapshot", "volume_profile_bin"):
            row = conn.execute(text(f"SELECT base_volume, quote_volume FROM {table}")).one()
            assert row == (_VP_BASE, _VP_QUOTE)
            assert all(isinstance(value, Decimal) for value in row)
        row = conn.execute(text("SELECT source_manifest, reconciliation FROM volume_profile_snapshot")).one()
        assert row == ({"fixture": True}, {"fixture": True})
        assert conn.execute(text("SELECT bin_index FROM volume_profile_bin")).scalar_one() == -2
    for statement in (
        "UPDATE volume_profile_snapshot SET product_id = 'missing'",
        "INSERT INTO volume_profile_bin VALUES ('missing', 0, 1, 1, 1)",
    ):
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(text(statement))
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM volume_profile_snapshot WHERE id = 'vp'"))
        assert conn.execute(text("SELECT count(*) FROM volume_profile_bin")).scalar_one() == 0


def test_volume_profile_all_numeric_columns_reject_special_values(volume_profile_pg: Engine) -> None:
    """Each failed UPDATE has its own rolled-back transaction and unchanged value."""
    for table, columns in (
        ("volume_profile_snapshot", ("bin_origin", "bin_step", "base_volume", "quote_volume")),
        ("volume_profile_bin", ("base_volume", "quote_volume")),
    ):
        for column in columns:
            with volume_profile_pg.connect() as conn:
                before = conn.execute(text(f"SELECT {column} FROM {table}")).scalar_one()
            for special in ("NaN", "Infinity", "-Infinity"):
                with pytest.raises(IntegrityError):
                    with volume_profile_pg.begin() as conn:
                        conn.execute(text(f"UPDATE {table} SET {column} = CAST(:value AS NUMERIC)"),
                                     {"value": special})
                with volume_profile_pg.connect() as conn:
                    assert conn.execute(text(f"SELECT {column} FROM {table}")).scalar_one() == before


def test_volume_profile_logical_uniqueness_and_daily_checks(volume_profile_pg: Engine) -> None:
    for revision, digest in [(1, "b" * 64), (2, "a" * 64)]:
        with pytest.raises(IntegrityError):
            with volume_profile_pg.begin() as conn:
                _insert_vp_snapshot(conn, "duplicate", revision, digest)
    for assignment in (
        "window_start_ms = 1, window_end_ms = 86400001", "window_end_ms = 172800000",
        "window_start_ms = -86400000, window_end_ms = 0",
        "period = '1h'", "timezone = 'OTHER'", "published_at = NULL", "quality = 'PARTIAL'",
        "source_manifest = '[]'::jsonb", "reconciliation = 'null'::jsonb",
    ):
        with pytest.raises(IntegrityError):
            with volume_profile_pg.begin() as conn:
                conn.execute(text(f"UPDATE volume_profile_snapshot SET {assignment}"))
    with volume_profile_pg.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM volume_profile_snapshot")).scalar_one() == 1
        assert conn.execute(text("SELECT revision, window_start_ms, quality, "
                                 "published_at IS NOT NULL FROM volume_profile_snapshot")).one() == (1, 0, "VERIFIED", True)
    with volume_profile_pg.begin() as conn:
        _insert_vp_snapshot(conn, "vp-revision-2", 2, "b" * 64)
    with volume_profile_pg.connect() as conn:
        assert conn.execute(text("SELECT revision, content_sha256 FROM volume_profile_snapshot "
                                 "ORDER BY revision")).all() == [(1, "a" * 64), (2, "b" * 64)]


@pytest.fixture
def profile_repository_pg(fresh_pg_db: str) -> Iterator[Engine]:
    """Repository acceptance on isolated PG; intentionally hostile default isolation."""
    _upgrade(fresh_pg_db)
    engine = sa.create_engine(_target_url(fresh_pg_db), isolation_level="REPEATABLE READ")
    try:
        with engine.begin() as conn:
            assert conn.execute(text("SELECT id FROM exchange WHERE id = 'BINANCE'")).scalar_one() == "BINANCE"
            conn.execute(text("INSERT INTO product (id, exchange_id, base_asset, quote_asset) "
                              "VALUES ('BINANCE:BTCUSDT-SPOT', 'BINANCE', 'BTC', 'USDT')"))
        yield engine
    finally:
        engine.dispose()


def _publication_pg(variant: bool = False) -> VerifiedProfilePublication:
    bins = (ProfileBin(-2, Decimal("1.1234567890123456789012345678"), Decimal("12.34"), 2),
            ProfileBin(7, Decimal("0.2") if variant else Decimal("0.1"), Decimal("5.67"), 1))
    content = VolumeProfileContent("BINANCE:BTCUSDT-SPOT", 0, 86400000, "g1",
                                   Decimal("0"), Decimal("10"), "vp-v1", bins)
    return VerifiedProfilePublication(content, CanonicalJsonObject({"pages": [{"id": 7}]}),
                                      CanonicalJsonObject({"checks": {"exact": True}}),
                                      datetime(2026, 1, 1, tzinfo=timezone.utc), "OBSERVED", "PRESENT")


def test_profile_authenticated_route_pg_native_end_to_end(profile_repository_pg: Engine) -> None:
    """Real migrated PG -> reader -> native merge -> authenticated app (no socket)."""
    from unittest.mock import Mock
    from urllib.parse import urlencode
    from src.control_plane.app import ControlPlaneApp
    from src.control_plane.backtest_jobs import BacktestJobExecutor
    from src.control_plane.profile_http_query import ProfileQueryService

    engine = profile_repository_pg
    day = 86400000
    content = VolumeProfileContent(
        "BINANCE:BTCUSDT-SPOT", 0, day, "btc_spot_usdt_10_v1", Decimal(0), Decimal(10),
        "vp-v1", (ProfileBin(2, Decimal("1.20"), Decimal("24.00"), 2),),
    )
    publication = VerifiedProfilePublication(
        content, CanonicalJsonObject({"source": "integration"}), CanonicalJsonObject({"exact": True}),
        datetime(1970, 1, 2, tzinfo=timezone.utc), "OBSERVED", "PRESENT",
    )
    sessions = sessionmaker(engine)
    published = profile_repository.ProfileRepository(sessions).publish(publication)
    utc, mono = Mock(side_effect=[day, day, day]), Mock(side_effect=[0, 0])
    service = ProfileQueryService(ProfileReadRepository(sessions), utc_ms=utc, monotonic_ms=mono)
    app = ControlPlaneApp(BacktestJobExecutor(run_inline=True), api_key="integration-key", profile_query_service=service)
    statements: list[str] = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture)
    try:
        path = "/api/v1/market-data/volume-profiles?" + urlencode(dict(
            product_id=content.product_id, base_grid_id=content.grid_id, output_grid_id=content.grid_id,
            algorithm_version=content.algorithm_version, start_ms=0, end_ms=day,
            purpose="LIVE_QUERY", freshness_policy_id="utc_complete_strict_v1",
        ))
        assert app.handle("GET", path).status_code == 401
        utc.assert_not_called()
        mono.assert_not_called()
        assert statements == []
        response = app.handle("GET", path, headers={"X-API-Key": "integration-key"})
        assert response.status_code == 200
        body = response.body
        assert body["schema_version"] == 2 and body["data_kind"] == "VOLUME_PROFILE"
        assert body["source_available_at_ms"] == day
        assert body["profile_kind"] == "DAILY" and body["validation_basis"] == "SERVER_PINNED_READ"
        assert (body["snapshot_id"], body["revision"], body["content_sha256"]) == (
            published.snapshot_id, published.revision, content.content_sha256)
        ref = DailyProfileRef(published.snapshot_id, published.revision, content.content_sha256, 0, day)
        manifest = OrderedProfileManifest(content.product_id, content.grid_id, content.algorithm_version, (ref,))
        assert body["manifest"] == json.loads(manifest.canonical_bytes)
        assert body["manifest_digest"] == manifest.manifest_digest
        assert body["window"] == {"start_ms": 0, "end_ms": day}
        assert body["coverage"] == {"expected_days": 1, "complete_days": 1}
        assert body["grid"] == {"origin": "0", "step": "10", "unit": "USDT"}
        assert body["bins"] == [{"bin_index": 2, "base_volume": "1.2", "quote_volume": "24", "aggregate_count": 2}]
        assert (body["base_volume"], body["quote_volume"], body["aggregate_count"]) == ("1.2", "24", 2)
        assert body["poc"] == {"bin_index": 2, "low": "20", "high_exclusive": "30"}
        assert body["validation_started_at_ms"] == body["validation_completed_at_ms"] == body["served_at_ms"] == day
        assert body["validation_expires_at_ms"] == day + 300000
        assert body["validation_max_age_ms"] == body["validation_remaining_ms"] == 300000
        assert not {"status", "composite_id", "merge_algorithm_version"} & body.keys()
        assert utc.call_count == 3 and mono.call_count == 2 and statements
    finally:
        event.remove(engine, "before_cursor_execute", capture)
        assert app.shutdown(1)


def _profile_counts(engine: Engine) -> tuple[int, int]:
    with engine.connect() as conn:
        return (conn.execute(text("SELECT count(*) FROM volume_profile_snapshot")).scalar_one(),
                conn.execute(text("SELECT count(*) FROM volume_profile_bin")).scalar_one())


@contextmanager
def _profile_reader_pg(db_name: str) -> Iterator[tuple[ProfileReadRepository, Engine]]:
    """Capture the actual lazy engine through a no-SQL session, not private state."""
    owner = ProfileReadConnection(ProfileReadConnectionConfig(make_url(_target_url(db_name))))
    try:
        with owner.sessions() as session:
            engine = session.get_bind()
            assert isinstance(engine, Engine) and not session.in_transaction()
        yield ProfileReadRepository(owner.sessions), engine
    finally:
        owner.close()


def _profile_reader_seed(engine: Engine, *, with_events: bool = True):
    repo = profile_repository.ProfileRepository(sessionmaker(engine))
    invalidations = profile_invalidation.ProfileInvalidationStore(sessionmaker(engine))
    original = _publication_pg()
    values, published, expected_events = [], [], []
    for day, bins in enumerate((original.content.bins, original.content.bins[:1], ())):
        value = replace(original, content=replace(original.content, window_start_ms=day * 86400000,
                                                 window_end_ms=(day + 1) * 86400000, bins=bins))
        result = repo.publish(value)
        values.append(value)
        published.append(result)
        expected_events.append(tuple(invalidations.append_confirmed(profile_invalidation.ConfirmedInvalidationRequest(
            f"read_{day}_{number}", result.snapshot_id, "BAD_DATA", None, "acceptance")).event
            for number in range(len(bins) if with_events else 0)))
    manifest = OrderedProfileManifest(original.content.product_id, original.content.grid_id, original.content.algorithm_version,
        tuple(DailyProfileRef(result.snapshot_id, result.revision, result.content_sha256,
                             value.content.window_start_ms, value.content.window_end_ms)
              for result, value in zip(published, values)))
    return values, published, expected_events, manifest


def test_profile_reader_pg_exact_daily_batch_content_and_revocations(profile_repository_pg: Engine, fresh_pg_db: str) -> None:
    values, published, events, manifest = _profile_reader_seed(profile_repository_pg)
    with _profile_reader_pg(fresh_pg_db) as (reader, _):
        batch = reader.get_manifest(manifest)
        assert batch is not None and batch.manifest == manifest
        assert tuple(day.ref for day in batch.days) == manifest.days
        assert [len(day.publication.content.bins) for day in batch.days] == [2, 1, 0]
        assert [len(day.invalidations) for day in batch.days] == [2, 1, 0]
        for index, day in enumerate(batch.days):
            assert day.publication == values[index]
            assert day.invalidations == events[index]
            assert day.computed_at == published[index].computed_at
            assert day.published_at == published[index].published_at
            assert all(event.snapshot_id == day.ref.snapshot_id for event in day.invalidations)
            assert reader.get_verified(day.ref.snapshot_id) == day
        assert batch.days[-1].publication.content.base_volume == Decimal("0")
        assert batch.days[-1].publication.content.quote_volume == Decimal("0")


@pytest.mark.parametrize("case", ["missing", "partial", "revision", "digest", "scope"])
def test_profile_reader_pg_pinned_classification(profile_repository_pg: Engine, fresh_pg_db: str, case: str) -> None:
    _, published, _, manifest = _profile_reader_seed(profile_repository_pg)
    if case in ("missing", "revision", "digest"):
        ref = manifest.days[1]
        changed = replace(ref, **{"missing": {"snapshot_id": "f" * 64}, "revision": {"revision": 2},
                                 "digest": {"content_sha256": "e" * 64}}[case])
        manifest = replace(manifest, days=(manifest.days[0], changed, manifest.days[2]))
    elif case == "scope":
        manifest = replace(manifest, base_grid_id="other")
    else:
        with profile_repository_pg.begin() as conn:
            conn.execute(text("UPDATE volume_profile_snapshot SET quality='PARTIAL',published_at=NULL WHERE id=:id"),
                         {"id": published[1].snapshot_id})
    with _profile_reader_pg(fresh_pg_db) as (reader, _):
        if case in ("missing", "partial"):
            assert reader.get_manifest(manifest) is None
            assert reader.get_verified(manifest.days[1].snapshot_id) is None
        else:
            with pytest.raises(profile_repository.ProfileIntegrityError):
                reader.get_manifest(manifest)


@pytest.mark.parametrize("damage", ["numeric", "metadata", "computed_at", "published_at", "source_available_at"])
def test_profile_reader_pg_guarded_header_stops_before_payload(profile_repository_pg: Engine, fresh_pg_db: str, damage: str) -> None:
    result = profile_repository.ProfileRepository(sessionmaker(profile_repository_pg)).publish(_publication_pg())
    with profile_repository_pg.begin() as conn:
        if damage == "numeric":
            conn.execute(text("UPDATE volume_profile_snapshot SET bin_origin=CAST(:value AS numeric) WHERE id=:id"),
                         {"id": result.snapshot_id, "value": "1" + "0" * 80})
        elif damage == "metadata":
            conn.execute(text("UPDATE volume_profile_snapshot SET source_manifest=CAST(:value AS jsonb) WHERE id=:id"),
                         {"id": result.snapshot_id, "value": json.dumps({"large": "x" * 131072})})
        else:
            # Current snapshot schema permits these timestamps; the reader must guard driver conversion.
            assert damage in ("computed_at", "published_at", "source_available_at")
            conn.execute(text(f"UPDATE volume_profile_snapshot SET {damage}='10000-01-01 00:00:00+00'::timestamptz WHERE id=:id"),
                         {"id": result.snapshot_id})
    statements = []

    def capture(_conn: Any, _cursor: Any, statement: str, _params: Any, _context: Any, _many: bool) -> None:
        if "FROM volume_profile_" in statement or "FROM market_data_invalidation" in statement:
            statements.append(statement)

    with _profile_reader_pg(fresh_pg_db) as (reader, engine):
        event.listen(engine, "before_cursor_execute", capture)
        try:
            expected = ProfileReadTooLarge if damage in ("numeric", "metadata") else profile_repository.ProfileIntegrityError
            with pytest.raises(expected):
                reader.get_verified(result.snapshot_id)
        finally:
            event.remove(engine, "before_cursor_execute", capture)
    assert len(statements) == 1 and " AS source_manifest_bytes" in statements[0]
    assert "FROM volume_profile_snapshot" in statements[0]
    assert "FROM volume_profile_bin" not in statements[0] and "market_data_invalidation" not in statements[0]


@pytest.mark.parametrize("batch", [False, True], ids=["single_snapshot", "manifest"])
def test_profile_reader_pg_repeatable_read_excludes_mid_read_invalidation(
    profile_repository_pg: Engine, fresh_pg_db: str, batch: bool,
) -> None:
    values, published, prior_events, manifest = _profile_reader_seed(profile_repository_pg, with_events=batch)
    target = published[1].snapshot_id
    injected, settings = [], []

    def after_header(_conn: Any, cursor: Any, statement: str, _params: Any, _context: Any, _many: bool) -> None:
        if injected or " AS source_manifest_bytes" not in statement or "FROM volume_profile_snapshot" not in statement:
            return
        with cursor.connection.cursor() as probe:
            probe.execute("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),current_setting('TimeZone')")
            settings.append(probe.fetchone())
        # Independent writer commit completes synchronously before the reader fetches its payload.
        with profile_repository_pg.begin() as writer:
            writer.execute(text("INSERT INTO market_data_invalidation(event_id,snapshot_id,reason_code,source) "
                                "VALUES ('mid_read',:id,'BAD_DATA','acceptance')"), {"id": target})
        injected.append(True)

    with _profile_reader_pg(fresh_pg_db) as (reader, engine):
        event.listen(engine, "after_cursor_execute", after_header)
        try:
            if batch:
                before = reader.get_manifest(manifest)
                assert before is not None
                assert [day.invalidations for day in before.days] == prior_events
                after = reader.get_manifest(manifest)
                assert after is not None and tuple(day.ref for day in after.days) == manifest.days
                assert [day.publication for day in after.days] == values
                assert [len(day.invalidations) for day in after.days] == [2, 2, 0]
                assert after.days[0] == before.days[0] and after.days[2] == before.days[2]
                assert tuple(e for e in after.days[1].invalidations if e.event_id != "mid_read") == prior_events[1]
                assert sum(e.event_id == "mid_read" and e.snapshot_id == target for e in after.days[1].invalidations) == 1
            else:
                before_day = reader.get_verified(target)
                assert before_day is not None and before_day.invalidations == ()
                after_day = reader.get_verified(target)
                assert after_day is not None and after_day.publication == before_day.publication == values[1]
                assert len(after_day.invalidations) == 1 and after_day.invalidations[0].event_id == "mid_read"
                assert after_day.invalidations[0].snapshot_id == target
        finally:
            event.remove(engine, "after_cursor_execute", after_header)
    assert injected == [True] and settings == [("repeatable read", "on", "UTC")]


def test_profile_reader_pg_connection_settings_read_only_and_new_call_recovery(profile_repository_pg: Engine, fresh_pg_db: str) -> None:
    value = _publication_pg()
    published = profile_repository.ProfileRepository(sessionmaker(profile_repository_pg)).publish(value)
    with _profile_reader_pg(fresh_pg_db) as (reader, _):
        with pytest.raises(DBAPIError) as caught:
            with reader._transaction() as session:
                settings = session.execute(text("SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only'),"
                    "current_setting('TimeZone'),current_setting('lock_timeout'),current_setting('statement_timeout')")).one()
                assert settings == ("repeatable read", "on", "UTC", "250ms", "1500ms")
                driver: Any = session.connection().connection.driver_connection
                # Inspect only the timeout field; never retain or print the complete DSN.
                assert driver.get_dsn_parameters()["connect_timeout"] == "2"
                session.execute(text("INSERT INTO market_data_invalidation(event_id,snapshot_id,reason_code,source) "
                                     "VALUES ('readonly_probe',:id,'BAD_DATA','acceptance')"), {"id": published.snapshot_id})
        assert getattr(caught.value.orig, "pgcode", None) == "25006"
        recovered = reader.get_verified(published.snapshot_id)
        assert recovered is not None and recovered.publication == value and recovered.invalidations == ()


def test_profile_reader_pg_pool_saturation_and_fresh_session_recovery(profile_repository_pg: Engine, fresh_pg_db: str) -> None:
    # The migrated fixture owns the isolated database; the tested pool is independent.
    assert profile_repository_pg is not None
    owner = ProfileReadConnection(ProfileReadConnectionConfig(make_url(_target_url(fresh_pg_db))))
    try:
        with owner.sessions() as first, owner.sessions() as second:
            first_pid = first.execute(text("SELECT pg_backend_pid()")).scalar_one()
            second_pid = second.execute(text("SELECT pg_backend_pid()")).scalar_one()
            assert type(first_pid) is int and type(second_pid) is int and first_pid != second_pid
            started = time.monotonic()
            with pytest.raises(PoolTimeoutError) as caught:
                with owner.sessions() as third:
                    third.execute(text("SELECT pg_backend_pid()"))
            assert type(caught.value) is PoolTimeoutError
            assert 0.15 <= time.monotonic() - started < 5  # Pool wait only, not an E2E SLA.
        with owner.sessions() as fresh:
            assert fresh.execute(text("SELECT pg_backend_pid()")).scalar_one() in (first_pid, second_pid)
    finally:
        owner.close()


def test_profile_reader_pg_lock_timeout_no_retry_then_new_public_call(profile_repository_pg: Engine, fresh_pg_db: str) -> None:
    value = _publication_pg()
    published = profile_repository.ProfileRepository(sessionmaker(profile_repository_pg)).publish(value)
    blocked = []

    def capture(_conn: Any, _cursor: Any, statement: str, _params: Any, _context: Any, _many: bool) -> None:
        if "FROM volume_profile_snapshot" in statement and " AS source_manifest_bytes" in statement:
            blocked.append(True)

    with _profile_reader_pg(fresh_pg_db) as (reader, engine):
        event.listen(engine, "before_cursor_execute", capture)
        try:
            with profile_repository_pg.connect() as holder:
                try:
                    holder.execute(text("SET LOCAL statement_timeout='3000ms'"))
                    holder.execute(text("LOCK TABLE volume_profile_snapshot IN ACCESS EXCLUSIVE MODE"))
                    started = time.monotonic()
                    with pytest.raises(DBAPIError) as caught:
                        reader.get_verified(published.snapshot_id)
                    assert getattr(caught.value.orig, "pgcode", None) == "55P03"
                    assert 0.15 <= time.monotonic() - started < 5
                    assert blocked == [True]
                finally:
                    holder.rollback()
        finally:
            event.remove(engine, "before_cursor_execute", capture)
        recovered = reader.get_verified(published.snapshot_id)  # Explicit new call, not automatic retry.
        assert recovered is not None and recovered.publication == value


def test_profile_reader_pg_statement_timeout_then_new_public_call(profile_repository_pg: Engine, fresh_pg_db: str) -> None:
    value = _publication_pg()
    published = profile_repository.ProfileRepository(sessionmaker(profile_repository_pg)).publish(value)
    with _profile_reader_pg(fresh_pg_db) as (reader, _):
        started: float | None = None  # Duration measurement only; no financial arithmetic.
        with pytest.raises(DBAPIError) as caught:
            with reader._transaction() as session:
                started = time.monotonic()
                session.execute(text("SELECT pg_sleep(2)"))
        assert getattr(caught.value.orig, "pgcode", None) == "57014"
        assert started is not None
        assert 1.2 <= time.monotonic() - started < 5  # Statement cancellation, not an E2E SLA.
        recovered = reader.get_verified(published.snapshot_id)
        assert recovered is not None and recovered.publication == value


def test_profile_repository_exact_roundtrip_idempotency_and_revisions(profile_repository_pg: Engine) -> None:
    engine = profile_repository_pg
    repo = profile_repository.ProfileRepository(sessionmaker(engine))
    original = _publication_pg()
    first = repo.publish(original)
    assert first.revision == 1 and not first.already_present
    assert first.snapshot_id == original.content_sha256
    read = profile_repository.ProfileRepository(sessionmaker(engine)).get_verified(first.snapshot_id)
    assert read == replace(first, already_present=True)
    changed = replace(original, source_manifest=CanonicalJsonObject({"changed": True}),
                      reconciliation=CanonicalJsonObject({}), source_available_at=None,
                      availability_basis="MODELED", raw_retention_state="DELETED")
    assert repo.publish(changed) == read
    assert _profile_counts(engine) == (1, 2)
    with engine.connect() as conn:
        row = conn.execute(text("SELECT * FROM volume_profile_snapshot")).mappings().one()
        assert type(row["base_volume"]) is Decimal and row["base_volume"] == original.content.base_volume
        assert row["quote_volume"] == original.content.quote_volume
        assert row["source_manifest"] == original.source_manifest.thaw()
        assert row["reconciliation"] == original.reconciliation.thaw()
        assert row["source_available_at"] == original.source_available_at
        assert row["quality"] == "VERIFIED" and row["published_at"] == first.published_at
        assert first.computed_at.tzinfo is timezone.utc and first.published_at.tzinfo is timezone.utc
    second = repo.publish(_publication_pg(True))
    assert second.revision == 2 and second.snapshot_id != first.snapshot_id
    assert repo.get_verified(first.snapshot_id) == read
    assert repo.get_verified(second.snapshot_id) == replace(second, already_present=True)
    assert _profile_counts(engine) == (2, 4)


@pytest.mark.parametrize("different", [False, True])
def test_profile_repository_isolation_override_and_concurrent_convergence(
    profile_repository_pg: Engine, different: bool,
) -> None:
    engine = profile_repository_pg
    barrier = Barrier(2, timeout=10)
    observed: list[str] = []

    def inspect_isolation(conn: sa.Connection, cursor: Any, statement: str,
                          parameters: Any, context: Any, executemany: bool) -> None:
        if "pg_advisory_xact_lock" in statement:
            # Same DBAPI connection and current transaction, before lock execution.
            with cursor.connection.cursor() as probe:
                probe.execute("SHOW transaction_isolation")
                observed.append(probe.fetchone()[0])

    def worker(variant: bool) -> profile_repository.PublishedProfile:
        with engine.connect() as conn:
            assert conn.get_isolation_level() == "REPEATABLE READ"
            barrier.wait()
            return profile_repository.ProfileRepository(sessionmaker(bind=conn)).publish(_publication_pg(variant))

    event.listen(engine, "before_cursor_execute", inspect_isolation)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            a, b = pool.submit(worker, False), pool.submit(worker, different)
            results = [a.result(timeout=20), b.result(timeout=20)]
    finally:
        event.remove(engine, "before_cursor_execute", inspect_isolation)
    assert observed == ["read committed", "read committed"]
    assert {r.revision for r in results} == ({1, 2} if different else {1})
    assert sum(not r.already_present for r in results) == (2 if different else 1)
    assert len({r.snapshot_id for r in results}) == (2 if different else 1)
    assert _profile_counts(engine) == ((2, 4) if different else (1, 2))


@pytest.mark.parametrize("phase", ["PARTIAL", "VERIFIED"])
def test_profile_repository_readback_fault_rolls_back_every_row(
    profile_repository_pg: Engine, monkeypatch: pytest.MonkeyPatch, phase: str,
) -> None:
    original_verify = profile_repository._verify
    reached: list[str] = []

    def faulty(session: Session, row: sa.engine.RowMapping, quality: str = "VERIFIED",
               expected: VerifiedProfilePublication | None = None) -> VerifiedProfilePublication:
        checked = original_verify(session, row, quality, expected)
        if quality == phase:
            reached.append(quality)
            raise profile_repository.ProfileIntegrityError("injected readback failure")
        return checked

    monkeypatch.setattr(profile_repository, "_verify", faulty)
    with pytest.raises(profile_repository.ProfileIntegrityError, match="^injected readback failure$"):
        profile_repository.ProfileRepository(sessionmaker(profile_repository_pg)).publish(_publication_pg())
    assert reached == [phase]
    assert _profile_counts(profile_repository_pg) == (0, 0)


def test_profile_repository_uncommitted_partial_is_invisible(
    profile_repository_pg: Engine, monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_verify = profile_repository._verify
    reached, release = Event(), Event()

    def blocked(session: Session, row: sa.engine.RowMapping, quality: str = "VERIFIED",
                expected: VerifiedProfilePublication | None = None) -> VerifiedProfilePublication:
        checked = original_verify(session, row, quality, expected)
        if quality == "PARTIAL":
            reached.set()
            assert release.wait(10), "publication release timed out"
        return checked

    monkeypatch.setattr(profile_repository, "_verify", blocked)
    repo = profile_repository.ProfileRepository(sessionmaker(profile_repository_pg))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(repo.publish, _publication_pg())
        try:
            assert reached.wait(10), "partial readback not reached"
            assert _profile_counts(profile_repository_pg) == (0, 0)
            assert repo.get_verified(_publication_pg().content_sha256) is None
        finally:
            release.set()
        published = future.result(timeout=20)
    assert repo.get_verified(published.snapshot_id) == replace(published, already_present=True)
    assert _profile_counts(profile_repository_pg) == (1, 2)


def _job_store(engine: Engine) -> profile_jobs.ProfileIngestJobStore:
    return profile_jobs.ProfileIngestJobStore(sessionmaker(engine))


def _job_spec(job_id: str = "job") -> profile_jobs.JobSpec:
    return profile_jobs.JobSpec(job_id, "BINANCE:BTCUSDT-SPOT", 0, 86400000, "g1", "vp-v1", "a" * 64)


def _job_row(engine: Engine, job_id: str = "job") -> dict[str, Any]:
    with engine.connect() as conn:
        return dict(conn.execute(text("SELECT * FROM volume_profile_ingest_job WHERE id=:id"),
                                 {"id": job_id}).mappings().one())


def test_profile_job_register_checkpoint_retry_fail_and_isolation(profile_repository_pg: Engine) -> None:
    engine, duration = profile_repository_pg, timedelta(minutes=5)
    store, spec = _job_store(engine), _job_spec()
    observed: list[str] = []

    def observe(conn: sa.Connection, cursor: Any, statement: str,
                parameters: Any, context: Any, executemany: bool) -> None:
        if "volume_profile_ingest_job" in statement and (statement.startswith("INSERT") or "FOR UPDATE" in statement):
            with cursor.connection.cursor() as probe:
                probe.execute("SHOW transaction_isolation")
                observed.append(probe.fetchone()[0])

    event.listen(engine, "before_cursor_execute", observe)
    try:
        initial = store.register(spec)
        assert initial.status == "RETRYABLE" and initial.attempt == 0
        assert store.register(spec) == initial
        with pytest.raises(profile_jobs.JobConflict):
            store.register(replace(spec, config_sha256="b" * 64))
        with engine.connect() as conn:
            assert conn.get_isolation_level() == "REPEATABLE READ"
            before = conn.execute(text("SELECT clock_timestamp()")).scalar_one()
        claim = store.claim_next("worker", duration)
        assert claim is not None and claim.attempt == 1 and claim.lease_expires_at.tzinfo is timezone.utc
        assert claim.lease_expires_at >= before + duration
        cursor, manifest = CanonicalJsonObject({"id": 17}), CanonicalJsonObject({"pages": [{"sha": "x"}]})
        updated = store.checkpoint(claim, cursor, manifest, duration)
        assert updated.lease_expires_at >= claim.lease_expires_at
        assert updated.source_cursor == cursor and updated.progress_manifest == manifest
        before_register = _job_row(engine)
        store.register(spec)
        assert _job_row(engine) == before_register
        retry = store.retry(updated, timedelta(hours=1), "RETRY", "bounded detail")
        assert retry.status == "RETRYABLE" and retry.lease_owner is retry.lease_expires_at is None
        assert store.claim_next("worker", duration) is None
        with engine.begin() as conn:
            conn.execute(text("UPDATE volume_profile_ingest_job SET retry_after_at=clock_timestamp()-interval '1 second'"))
        again = store.claim_next("worker", duration)
        assert again is not None and again.attempt == 2 and again.source_cursor == cursor
        assert store.fail(again, "FAILED", "bounded detail").status == "FAILED"
        assert store.claim_next("worker", duration) is None
    finally:
        event.remove(engine, "before_cursor_execute", observe)
    assert observed and set(observed) == {"read committed"}


def test_profile_job_skip_locked_uses_second_without_waiting_for_first(profile_repository_pg: Engine) -> None:
    engine, store = profile_repository_pg, _job_store(profile_repository_pg)
    store.register(_job_spec("first"))
    store.register(_job_spec("second"))

    def worker() -> profile_jobs.JobClaim | None:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION statement_timeout = '2s'"))
            conn.commit()
            return profile_jobs.ProfileIngestJobStore(sessionmaker(bind=conn)).claim_next("worker", timedelta(minutes=5))

    with engine.begin() as locked:
        locked.execute(text("SELECT id FROM volume_profile_ingest_job WHERE id='first' FOR UPDATE")).one()
        with ThreadPoolExecutor(max_workers=1) as pool:
            claim = pool.submit(worker).result(timeout=5)
        assert claim is not None and claim.spec.id == "second"
    first = store.claim_next("worker", timedelta(minutes=5))
    assert first is not None and first.spec.id == "first"


def test_exact_job_claim_scope_skip_locked_and_due_takeover(profile_repository_pg: Engine) -> None:
    engine, store, duration = profile_repository_pg, _job_store(profile_repository_pg), timedelta(minutes=5)
    for name in ("first", "requested", "other"):
        store.register(_job_spec(name))
    untouched = {name: _job_row(engine, name) for name in ("first", "other")}
    policy = profile_repository.TransactionWaitPolicy(100, 500)
    with engine.begin() as locked:
        locked.execute(text("SET LOCAL statement_timeout = '2s'"))
        locked.execute(text("SELECT id FROM volume_profile_ingest_job WHERE id='requested' FOR UPDATE")).one()
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(profile_jobs.ProfileIngestJobStore(sessionmaker(engine), policy).claim,
                                 "requested", "worker", duration)
            assert future.result(timeout=3) is None
    first = store.claim("requested", "worker", duration)
    assert first is not None and first.spec.id == "requested" and first.attempt == 1
    cursor, manifest = CanonicalJsonObject({"id": 42}), CanonicalJsonObject({"page": 2})
    first = store.checkpoint(first, cursor, manifest, duration)
    store.retry(first, timedelta(hours=1), "RETRY", "bounded detail")
    assert store.claim("requested", "worker", duration) is None
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_ingest_job SET retry_after_at=clock_timestamp()-interval '1 second' WHERE id='requested'"))
    second = store.claim("requested", "worker", duration)
    assert second is not None and second.attempt == 2
    assert store.claim("requested", "new", duration) is None
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_ingest_job SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id='requested'"))
    takeover = store.claim("requested", "new", duration)
    assert takeover is not None and takeover.attempt == 3
    assert takeover.source_cursor == cursor and takeover.progress_manifest == manifest
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_ingest_job SET attempt=9223372036854775807, lease_expires_at=clock_timestamp()-interval '1 second' WHERE id='requested'"))
    before = _job_row(engine, "requested")
    with pytest.raises(profile_jobs.JobIntegrityError, match="job attempt exhausted"):
        store.claim("requested", "worker", duration)
    assert _job_row(engine, "requested") == before
    assert {name: _job_row(engine, name) for name in untouched} == untouched


def test_completed_job_restart_recovery_is_read_only_without_row_or_advisory_lock(profile_repository_pg: Engine) -> None:
    engine, value = profile_repository_pg, _publication_pg()
    claim = _atomic_claim(engine)
    published = _job_store(engine).publish_and_complete(claim, value)
    before = _job_row(engine)
    statements: list[str] = []

    def observe(conn: sa.Connection, cursor: Any, statement: str,
                parameters: Any, context: Any, executemany: bool) -> None:
        statements.append(statement.upper())

    @contextmanager
    def read_only() -> Iterator[Session]:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION default_transaction_read_only = on"))
            conn.execute(text("SET SESSION statement_timeout = '500ms'"))
            conn.commit()
            event.listen(conn, "before_cursor_execute", observe)
            try:
                with Session(conn) as session:
                    yield session
            finally:
                event.remove(conn, "before_cursor_execute", observe)
                conn.rollback()
                conn.execute(text("RESET default_transaction_read_only"))
                conn.execute(text("RESET statement_timeout"))
                conn.commit()

    with engine.begin() as locked:
        locked.execute(text("SET LOCAL statement_timeout = '2s'"))
        locked.execute(text("SELECT id FROM volume_profile_ingest_job WHERE id='job' FOR UPDATE")).one()
        with ThreadPoolExecutor(max_workers=1) as pool:
            recovered = pool.submit(profile_jobs.ProfileIngestJobStore(read_only).recover_completed,
                                    claim.spec, value).result(timeout=3)
    assert recovered is not None and recovered.recovered_after_commit and recovered.profile.already_present
    assert recovered.profile.publication == value and recovered.job == published.job
    assert statements and all(s.startswith("SELECT") and "FOR UPDATE" not in s and "PG_ADVISORY" not in s for s in statements)
    assert _job_row(engine) == before and _profile_counts(engine) == (1, 2)


@pytest.mark.parametrize("damage", ["spec", "metadata", "digest", "bin", "missing", "partial"])
def test_completed_job_restart_recovery_rejects_invalid_evidence(profile_repository_pg: Engine, damage: str) -> None:
    engine, value = profile_repository_pg, _publication_pg()
    store, spec = _job_store(engine), _job_spec()
    assert store.recover_completed(spec, value) is None
    store.register(spec)
    assert store.recover_completed(spec, value) is None
    claim = store.claim(spec.id, "worker", timedelta(minutes=5))
    assert claim is not None
    assert store.recover_completed(spec, value) is None
    store.publish_and_complete(claim, value)
    if damage == "spec":
        spec = replace(spec, config_sha256="b" * 64)
    elif damage == "metadata":
        value = replace(value, reconciliation=CanonicalJsonObject({"changed": True}))
    else:
        with engine.begin() as conn:
            if damage == "digest":
                conn.execute(text("UPDATE volume_profile_snapshot SET content_sha256=:digest"), {"digest": "b" * 64})
            elif damage == "bin":
                conn.execute(text("UPDATE volume_profile_bin SET base_volume=base_volume+1 WHERE bin_index=-2"))
            elif damage == "missing":
                # Schema permits staging-only DONE; publication recovery requires a snapshot.
                conn.execute(text("UPDATE volume_profile_ingest_job SET completed_snapshot_id=NULL"))
            else:
                conn.execute(text("UPDATE volume_profile_snapshot SET quality='PARTIAL', published_at=NULL"))
    before = _job_row(engine)
    error = profile_jobs.JobIntegrityError if damage == "missing" else profile_jobs.JobCompletionError
    with pytest.raises(error):
        _job_store(engine).recover_completed(spec, value)
    assert _job_row(engine) == before


def test_profile_job_expired_takeover_fences_every_stale_mutation(profile_repository_pg: Engine) -> None:
    engine, store = profile_repository_pg, _job_store(profile_repository_pg)
    store.register(_job_spec())
    first = store.claim_next("old", timedelta(minutes=5))
    assert first is not None
    cursor, manifest = CanonicalJsonObject({"id": 42}), CanonicalJsonObject({"page": 2})
    first = store.checkpoint(first, cursor, manifest, timedelta(minutes=5))
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_ingest_job SET lease_expires_at=clock_timestamp()-interval '1 second'"))
    second = store.claim_next("new", timedelta(minutes=5))
    assert second is not None and second.attempt == first.attempt + 1
    assert second.source_cursor == cursor and second.progress_manifest == manifest
    before = _job_row(engine)
    for mutate in (
        lambda: store.checkpoint(first, cursor, manifest, timedelta(minutes=5)),
        lambda: store.retry(first, timedelta(0), "RETRY", "detail"),
        lambda: store.fail(first, "FAILED", "detail"),
        lambda: store.complete(first, "a" * 64),
    ):
        with pytest.raises(profile_jobs.LeaseLost):
            mutate()
        assert _job_row(engine) == before


@pytest.mark.parametrize("phase", ["checkpoint", "fail", "complete"])
def test_profile_job_post_update_reconstruction_failure_rolls_back(
    profile_repository_pg: Engine, monkeypatch: pytest.MonkeyPatch, phase: str,
) -> None:
    engine, store = profile_repository_pg, _job_store(profile_repository_pg)
    snapshot = profile_repository.ProfileRepository(sessionmaker(engine)).publish(_publication_pg())
    store.register(_job_spec())
    claim = store.claim_next("worker", timedelta(minutes=5))
    assert claim is not None
    before, reached = _job_row(engine), []
    original = profile_jobs._state

    def broken(row: sa.engine.RowMapping) -> profile_jobs.JobState:
        state = original(row)
        if row["status"] in ("FAILED", "DONE") or row["source_cursor"] == {"new": 1}:
            reached.append(True)
            raise profile_jobs.JobIntegrityError("injected reconstruction failure")
        return state

    monkeypatch.setattr(profile_jobs, "_state", broken)
    with pytest.raises(profile_jobs.JobIntegrityError, match="^injected reconstruction failure$"):
        if phase == "checkpoint":
            store.checkpoint(claim, CanonicalJsonObject({"new": 1}), CanonicalJsonObject({}), timedelta(minutes=5))
        elif phase == "fail":
            store.fail(claim, "FAILED", "detail")
        else:
            store.complete(claim, snapshot.snapshot_id)
    assert reached == [True] and _job_row(engine) == before


@pytest.mark.parametrize("case", ["valid", "window", "grid", "product", "algorithm", "partial", "missing", "corrupt"])
def test_profile_job_verified_completion_identity_and_integrity(profile_repository_pg: Engine, case: str) -> None:
    engine, store = profile_repository_pg, _job_store(profile_repository_pg)
    snapshot = profile_repository.ProfileRepository(sessionmaker(engine)).publish(_publication_pg())
    spec = _job_spec()
    changes: dict[str, Any] = {
        "window": {"window_start_ms": 86400000, "window_end_ms": 172800000},
        "grid": {"grid_id": "other"}, "product": {"product_id": "BINANCE:ETHUSDT-PERP"},
        "algorithm": {"algorithm_version": "vp-v2"},
    }.get(case, {})
    store.register(replace(spec, **changes))
    claim = store.claim_next("worker", timedelta(minutes=5))
    assert claim is not None
    if case in ("partial", "corrupt"):
        with engine.begin() as conn:
            sql = ("UPDATE volume_profile_snapshot SET quality='PARTIAL', published_at=NULL" if case == "partial"
                   else "UPDATE volume_profile_bin SET base_volume=base_volume+1")
            conn.execute(text(sql))
    before = _job_row(engine)
    if case != "valid":
        with pytest.raises(profile_jobs.JobCompletionError, match="^job completion verification failed$"):
            store.complete(claim, "f" * 64 if case == "missing" else snapshot.snapshot_id)
        assert _job_row(engine) == before
    else:
        done = store.complete(claim, snapshot.snapshot_id)
        assert done.status == "DONE" and done.completed_snapshot_id == snapshot.snapshot_id
        assert done.lease_owner is done.lease_expires_at is done.retry_after_at is None
        assert done.last_error_code is done.last_error_detail is None
        assert store.get(spec.id) == done


_WAIT_POLICY = profile_repository.TransactionWaitPolicy(100, 500)


def _assert_profile_lock_timeout(engine: Engine, action: Callable[[sa.Connection], object]) -> None:
    """Broad deadline is a hang guard, not a performance assertion."""
    def worker() -> None:
        with engine.connect() as conn:
            settings = text("SELECT current_setting('lock_timeout'), current_setting('statement_timeout')")
            original = conn.execute(settings).one()
            conn.execute(text("SET SESSION statement_timeout = '2s'"))
            guarded = conn.execute(settings).one()
            conn.commit()
            observed: list[tuple[str, str]] = []

            def observe(connection: sa.Connection, cursor: Any, statement: str,
                        parameters: Any, context: Any, executemany: bool) -> None:
                if "pg_advisory_xact_lock" in statement or "FOR UPDATE" in statement or statement.startswith("INSERT"):
                    with cursor.connection.cursor() as probe:
                        probe.execute("SHOW lock_timeout")
                        lock = probe.fetchone()[0]
                        probe.execute("SHOW statement_timeout")
                        observed.append((lock, probe.fetchone()[0]))

            event.listen(conn, "before_cursor_execute", observe)
            try:
                started = time.monotonic()
                with pytest.raises(DBAPIError) as caught:
                    action(conn)
                assert time.monotonic() - started < 3
                assert getattr(caught.value.orig, "pgcode", None) == "55P03"
                assert observed and set(observed) == {("100ms", "500ms")}
                assert conn.execute(settings).one() == guarded
                conn.commit()
            finally:
                event.remove(conn, "before_cursor_execute", observe)
                conn.rollback()
                conn.execute(text("SELECT set_config('statement_timeout', :value, false)"), {"value": original[1]})
                conn.commit()
                assert conn.execute(settings).one() == original
                conn.rollback()

    with ThreadPoolExecutor(max_workers=1) as pool:
        pool.submit(worker).result(timeout=5)


def test_profile_advisory_lock_timeout_rolls_back_and_release_allows_publish(profile_repository_pg: Engine) -> None:
    engine, value = profile_repository_pg, _publication_pg()
    first, second = profile_repository._lock_parts(profile_repository._logical(value.content))
    with engine.begin() as holder:
        holder.execute(text("SET LOCAL statement_timeout = '2s'"))
        holder.execute(text("SELECT pg_advisory_xact_lock(:a, :b)"), {"a": first, "b": second})
        _assert_profile_lock_timeout(engine, lambda conn: profile_repository.ProfileRepository(
            sessionmaker(bind=conn), _WAIT_POLICY).publish(value))
        assert _profile_counts(engine) == (0, 0)
    result = profile_repository.ProfileRepository(sessionmaker(engine), _WAIT_POLICY).publish(value)
    assert not result.already_present and _profile_counts(engine) == (1, 2)


def test_profile_job_row_lock_timeout_preserves_claim_until_release(profile_repository_pg: Engine) -> None:
    engine, store = profile_repository_pg, _job_store(profile_repository_pg)
    store.register(_job_spec())
    claim = store.claim_next("worker", timedelta(minutes=5))
    assert claim is not None
    before = _job_row(engine)
    with engine.begin() as holder:
        holder.execute(text("SET LOCAL statement_timeout = '2s'"))
        holder.execute(text("SELECT id FROM volume_profile_ingest_job WHERE id='job' FOR UPDATE")).one()
        _assert_profile_lock_timeout(engine, lambda conn: profile_jobs.ProfileIngestJobStore(
            sessionmaker(bind=conn), _WAIT_POLICY).checkpoint(
                claim, CanonicalJsonObject({"id": 3}), CanonicalJsonObject({"page": 1}), timedelta(minutes=5)))
        assert _job_row(engine) == before
    updated = store.checkpoint(claim, CanonicalJsonObject({"id": 3}), CanonicalJsonObject({"page": 1}), timedelta(minutes=5))
    assert updated.attempt == claim.attempt and updated.source_cursor.thaw() == {"id": 3}


def test_profile_job_uncommitted_unique_conflict_times_out_then_registers(profile_repository_pg: Engine) -> None:
    engine = profile_repository_pg
    with engine.connect() as holder:
        transaction = holder.begin()
        try:
            holder.execute(text("SET LOCAL statement_timeout = '2s'"))
            holder.execute(text("INSERT INTO volume_profile_ingest_job "
                "(id, product_id, window_start_ms, window_end_ms, grid_id, algorithm_version, "
                "config_sha256, status, source_cursor, attempt, progress_manifest) VALUES "
                "('job', 'BINANCE:BTCUSDT-SPOT', 0, 86400000, 'g1', 'vp-v1', :digest, "
                "'RETRYABLE', '{}'::jsonb, 0, '{}'::jsonb)"), {"digest": "a" * 64})
            _assert_profile_lock_timeout(engine, lambda conn: profile_jobs.ProfileIngestJobStore(
                sessionmaker(bind=conn), _WAIT_POLICY).register(_job_spec()))
            with engine.connect() as observer:
                assert observer.execute(text("SELECT count(*) FROM volume_profile_ingest_job")).scalar_one() == 0
        finally:
            transaction.rollback()
    registered = profile_jobs.ProfileIngestJobStore(sessionmaker(engine), _WAIT_POLICY).register(_job_spec())
    assert registered.status == "RETRYABLE" and registered.attempt == 0


def _atomic_claim(engine: Engine, job_id: str = "job") -> profile_jobs.JobClaim:
    store = _job_store(engine)
    store.register(_job_spec(job_id))
    claim = store.claim_next(job_id, timedelta(minutes=5))
    assert claim is not None and claim.spec.id == job_id
    return claim


@pytest.fixture
def process_assembler(tmp_path: Path) -> tuple[Path, profile_jobs.JobSpec]:
    """Real subprocess, strict fixture wire; not a claim of official provenance."""
    wire = json.loads((Path(__file__).parent / "fixtures/profile_handoff_v1.json").read_text())["wire_bytes"]
    helper = tmp_path / "assembler"
    helper.write_text(f"#!{sys.executable}\n" + """import argparse, json, pathlib, sys
p = argparse.ArgumentParser()
for name in ('staging-root', 'job-id', 'start-ms'):
    p.add_argument('--' + name, required=True)
a = p.parse_args()
root = pathlib.Path(a.staging_root)
(root / 'invoked').write_text('yes')
assert sys.stdin.buffer.read() == b''
mode = (root / 'mode').read_text() if (root / 'mode').exists() else ''
if mode == 'exit': raise SystemExit(7)
if mode == 'parse': print('SECRET malformed'); raise SystemExit(0)
""" + f"wire = json.loads({wire!r})\n" + """wire['job_id'] = a.job_id if mode != 'identity' else 'other-job'
print(json.dumps(wire, sort_keys=True, separators=(',', ':')))
""")
    helper.chmod(0o700)
    return helper, parse_handoff(wire.encode()).spec


@pytest.mark.parametrize("ack_loss", [False, True])
def test_ingest_process_pg_publish_and_read_only_restart(
    profile_repository_pg: Engine, process_assembler: tuple[Path, profile_jobs.JobSpec], ack_loss: bool,
) -> None:
    engine, (helper, spec) = profile_repository_pg, process_assembler
    committed = False
    failure = DBAPIError("commit acknowledgment", None, Exception("injected ACK loss"))

    def observe(conn: sa.Connection, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
        nonlocal committed
        if statement.startswith("UPDATE volume_profile_ingest_job") and "DONE" in parameters.values():
            committed = True

    @contextmanager
    def sessions() -> Iterator[Session]:
        with Session(engine) as session:
            yield session
            if ack_loss and committed:
                raise failure

    event.listen(engine, "after_cursor_execute", observe)
    try:
        owner = ProfileIngestProcess(profile_jobs.ProfileIngestJobStore(sessions), str(helper))
        if ack_loss:
            with pytest.raises(DBAPIError) as caught:
                owner.run(spec, str(helper.parent), "worker")
            assert caught.value is failure
        else:
            assert owner.run(spec, str(helper.parent), "worker").status == "PUBLISHED"
    finally:
        event.remove(engine, "after_cursor_execute", observe)
    before = _job_row(engine, spec.id)
    assert before["status"] == "DONE" and _profile_counts(engine) == (1, 2)
    sql: list[str] = []

    def record(conn: sa.Connection, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool) -> None:
        sql.append(statement.upper())

    event.listen(engine, "before_cursor_execute", record)
    try:
        result = ProfileIngestProcess(_job_store(engine), str(helper)).run(spec, str(helper.parent), "restart")
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert result.status == "RECOVERED" and result.publication is not None
    assert result.publication.profile.revision == 1 and result.publication.profile.already_present
    assert sql and all(s.startswith("SELECT") and "FOR UPDATE" not in s and "PG_ADVISORY" not in s for s in sql)
    assert result.publication.profile == profile_repository.ProfileRepository(sessionmaker(engine)).get_verified(result.publication.profile.snapshot_id)
    assert _job_row(engine, spec.id) == before and _profile_counts(engine) == (1, 2)


@pytest.mark.parametrize("mode", ["exit", "parse", "identity"])
def test_ingest_process_pg_failure_retries_only_requested_job(
    profile_repository_pg: Engine, process_assembler: tuple[Path, profile_jobs.JobSpec], mode: str,
) -> None:
    engine, (helper, spec) = profile_repository_pg, process_assembler
    store = _job_store(engine)
    store.register(replace(spec, id="other"))
    before = _job_row(engine, "other")
    (helper.parent / "mode").write_text(mode)
    result = ProfileIngestProcess(store, str(helper)).run(spec, str(helper.parent), "worker")
    assert result.status == "RETRYABLE" and _profile_counts(engine) == (0, 0)
    row = _job_row(engine, spec.id)
    assert row["status"] == "RETRYABLE" and row["last_error_code"] == "ASSEMBLY_FAILED"
    assert row["last_error_detail"] == "offline assembly validation failed"
    assert _job_row(engine, "other") == before


def test_ingest_process_pg_busy_locked_and_not_due_never_assemble(
    profile_repository_pg: Engine, process_assembler: tuple[Path, profile_jobs.JobSpec],
) -> None:
    engine, (helper, spec) = profile_repository_pg, process_assembler
    store = _job_store(engine)
    store.register(spec)
    store.register(replace(spec, id="other"))
    before = _job_row(engine, "other")
    owner = ProfileIngestProcess(store, str(helper))
    with engine.begin() as locked:
        locked.execute(text("SET LOCAL statement_timeout='2s'"))
        locked.execute(text("SELECT id FROM volume_profile_ingest_job WHERE id=:id FOR UPDATE"), {"id": spec.id}).one()
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(owner.run, spec, str(helper.parent), "worker").result(timeout=3).status == "UNAVAILABLE"
    claim = store.claim(spec.id, "owner", timedelta(minutes=5))
    assert claim is not None
    assert owner.run(spec, str(helper.parent), "worker").status == "UNAVAILABLE"
    store.retry(claim, timedelta(hours=1), "RETRY", "bounded")
    assert owner.run(spec, str(helper.parent), "worker").status == "UNAVAILABLE"
    assert not (helper.parent / "invoked").exists() and _job_row(engine, "other") == before


def test_ingest_process_pg_takeover_before_publish_fences_old_process(
    profile_repository_pg: Engine, process_assembler: tuple[Path, profile_jobs.JobSpec], monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.core.market_data.profiles import ingest
    engine, (helper, spec) = profile_repository_pg, process_assembler
    original = ingest._assemble
    taken: list[profile_jobs.JobClaim] = []

    def assemble_then_takeover(argv: list[str], policy: ingest.IngestPolicy) -> bytes:
        result = original(argv, policy)
        with engine.begin() as conn:
            conn.execute(text("UPDATE volume_profile_ingest_job SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=:id"), {"id": spec.id})
        claim = _job_store(engine).claim(spec.id, "new", timedelta(minutes=5))
        assert claim is not None
        taken.append(claim)
        return result

    monkeypatch.setattr(ingest, "_assemble", assemble_then_takeover)
    with pytest.raises(profile_jobs.LeaseLost):
        ProfileIngestProcess(_job_store(engine), str(helper)).run(spec, str(helper.parent), "old")
    assert taken[0].attempt == 2 and _profile_counts(engine) == (0, 0)
    row = _job_row(engine, spec.id)
    assert row["lease_owner"] == "new" and row["status"] == "RUNNING"


def _retention_publication(engine: Engine):
    fixture = json.loads((Path(__file__).parent / "fixtures/profile_handoff_v1.json").read_text())
    parsed = parse_handoff(fixture["wire_bytes"].encode())
    store = _job_store(engine)
    store.register(parsed.spec)
    claim = store.claim(parsed.spec.id, "retention", timedelta(minutes=5))
    assert claim is not None
    store.publish_and_complete(claim, parsed.publication)
    return parsed


def _retention_rows(engine: Engine) -> tuple[list[dict[str, Any]], ...]:
    with engine.connect() as conn:
        return tuple([dict(row) for row in conn.execute(text(f"SELECT * FROM {table} ORDER BY {order}")).mappings()]
                     for table, order in (("volume_profile_ingest_job", "id"), ("volume_profile_snapshot", "id"),
                                          ("volume_profile_bin", "snapshot_id, bin_index")))


@pytest.mark.parametrize("ack_unknown", [False, True])
def test_retention_cas_exact_readback_idempotency_and_commit_recovery(profile_repository_pg: Engine, ack_unknown: bool) -> None:
    engine = profile_repository_pg
    parsed = _retention_publication(engine)
    store = _job_store(engine)
    before = _retention_rows(engine)
    present = store.recover_completed_snapshot(parsed.spec)
    assert present is not None and present.profile.publication == parsed.publication
    failure = DBAPIError("commit acknowledgment", None, Exception("injected"))

    @contextmanager
    def lost_ack() -> Iterator[Session]:
        with Session(engine) as session:
            yield session
            raise failure  # Inner transaction already committed.

    if ack_unknown:
        with pytest.raises(DBAPIError) as caught:
            profile_jobs.ProfileIngestJobStore(lost_ack).mark_raw_deleted(parsed.spec, parsed.publication.content_sha256)
        assert caught.value is failure
    else:
        assert not store.mark_raw_deleted(parsed.spec, parsed.publication.content_sha256).already_deleted
    retry = store.mark_raw_deleted(parsed.spec, parsed.publication.content_sha256)
    assert retry.already_deleted and retry.completion.profile.revision == 1
    assert retry.completion.profile.publication == replace(parsed.publication, raw_retention_state="DELETED")
    assert store.recover_completed_snapshot(parsed.spec) == retry.completion
    with pytest.raises(profile_jobs.JobCompletionError):
        store.recover_completed(parsed.spec, parsed.publication)
    after = _retention_rows(engine)
    assert after[0] == before[0] and after[2] == before[2]
    assert after[1] == [{**before[1][0], "raw_retention_state": "DELETED"}]


@pytest.mark.parametrize("damage", ["spec", "id", "non_done", "non_done_without_snapshot", "done_null_snapshot",
                                         "done_zero_attempt", "partial", "bin", "not_stored", "job", "config",
                                         "hours", "recon", "availability", "submillisecond", "beyond_max_ms"])
def test_retention_rejects_corruption_without_mutating(profile_repository_pg: Engine, damage: str) -> None:
    engine = profile_repository_pg
    parsed = _retention_publication(engine)
    spec, identifier = parsed.spec, parsed.publication.content_sha256
    if damage == "spec":
        spec = replace(spec, config_sha256="e" * 64)
    elif damage == "id":
        identifier = "e" * 64
    else:
        with engine.begin() as conn:
            if damage in ("non_done", "non_done_without_snapshot"):
                conn.execute(text("UPDATE volume_profile_ingest_job SET status='RETRYABLE', completed_snapshot_id=NULL"))
                if damage == "non_done_without_snapshot":
                    conn.execute(text("DELETE FROM volume_profile_snapshot"))
            elif damage == "done_null_snapshot":
                # Schema permits NULL for staged DONE jobs; this recovery requires a published snapshot.
                conn.execute(text("UPDATE volume_profile_ingest_job SET completed_snapshot_id=NULL"))
            elif damage == "done_zero_attempt":
                conn.execute(text("UPDATE volume_profile_ingest_job SET attempt=0"))
            elif damage in ("submillisecond", "beyond_max_ms"):
                available = (datetime(1970, 1, 2, microsecond=1, tzinfo=timezone.utc)
                             if damage == "submillisecond" else datetime.max.replace(tzinfo=timezone.utc))
                conn.execute(text("UPDATE volume_profile_snapshot SET source_available_at=:v"), {"v": available})
            elif damage == "partial":
                conn.execute(text("UPDATE volume_profile_snapshot SET quality='PARTIAL', published_at=NULL"))
            elif damage == "bin":
                conn.execute(text("UPDATE volume_profile_bin SET base_volume=base_volume+1"))
            elif damage == "not_stored":
                conn.execute(text("UPDATE volume_profile_snapshot SET raw_retention_state='NOT_STORED'"))
            elif damage == "availability":
                conn.execute(text("UPDATE volume_profile_snapshot SET availability_basis='MODELED'"))
            else:
                manifest = parsed.publication.source_manifest.thaw()
                recon = parsed.publication.reconciliation.thaw()
                if damage == "job":
                    manifest["job_id"] = "other"
                elif damage == "config":
                    manifest["config_sha256"] = "e" * 64
                elif damage == "hours":
                    manifest["hours"] = []
                else:
                    recon["actual_aggregate_trade_count"] = 99
                conn.execute(text("UPDATE volume_profile_snapshot SET source_manifest=CAST(:m AS jsonb), "
                                  "reconciliation=CAST(:r AS jsonb)"), {"m": json.dumps(manifest), "r": json.dumps(recon)})
    before = _retention_rows(engine)
    store = _job_store(engine)
    with pytest.raises(profile_jobs.JobCompletionError):
        store.mark_raw_deleted(spec, identifier)
    if damage in ("non_done", "non_done_without_snapshot"):
        assert store.recover_completed_snapshot(spec) is None
    elif damage != "id":
        with pytest.raises(profile_jobs.JobCompletionError):
            store.recover_completed_snapshot(spec)
    assert _retention_rows(engine) == before


def test_retention_two_callers_converge(profile_repository_pg: Engine) -> None:
    engine = profile_repository_pg
    parsed = _retention_publication(engine)
    barrier = Barrier(2)

    def run():
        barrier.wait(timeout=3)
        return _job_store(engine).mark_raw_deleted(parsed.spec, parsed.publication.content_sha256)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run) for _ in range(2)]
        results = [future.result(timeout=5) for future in futures]
    assert sorted(result.already_deleted for result in results) == [False, True]
    assert results[0].completion == results[1].completion and _profile_counts(engine) == (1, 2)


def test_retention_post_update_verification_failure_rolls_back(profile_repository_pg: Engine,
                                                               monkeypatch: pytest.MonkeyPatch) -> None:
    engine = profile_repository_pg
    parsed = _retention_publication(engine)
    before = _retention_rows(engine)
    verify = profile_jobs._verify
    observed = []

    def reject_deleted(session: Session, row: Any, **kwargs: Any):
        result = verify(session, row, **kwargs)
        if row["raw_retention_state"] == "DELETED":
            observed.append(row["raw_retention_state"])
            raise profile_repository.ProfileIntegrityError("injected readback failure")
        return result

    monkeypatch.setattr(profile_jobs, "_verify", reject_deleted)
    with pytest.raises(profile_jobs.JobCompletionError):
        _job_store(engine).mark_raw_deleted(parsed.spec, parsed.publication.content_sha256)
    assert observed == ["DELETED"] and _retention_rows(engine) == before


@pytest.mark.parametrize("table", ["volume_profile_ingest_job", "volume_profile_snapshot"])
def test_retention_lock_timeout_preserves_all_rows(profile_repository_pg: Engine, table: str) -> None:
    engine = profile_repository_pg
    parsed = _retention_publication(engine)
    before = _retention_rows(engine)
    with engine.begin() as holder:
        holder.execute(text("SET LOCAL statement_timeout='2s'"))
        holder.execute(text(f"SELECT id FROM {table} FOR UPDATE")).all()
        _assert_profile_lock_timeout(engine, lambda conn: profile_jobs.ProfileIngestJobStore(
            sessionmaker(bind=conn), _WAIT_POLICY).mark_raw_deleted(parsed.spec, parsed.publication.content_sha256))
    assert _retention_rows(engine) == before
    assert not _job_store(engine).mark_raw_deleted(parsed.spec, parsed.publication.content_sha256).already_deleted


def test_retention_completed_recovery_reads_through_row_locks_without_writes(profile_repository_pg: Engine) -> None:
    engine = profile_repository_pg
    parsed = _retention_publication(engine)
    statements: list[str] = []

    @contextmanager
    def read_only() -> Iterator[Session]:
        with engine.connect() as conn:
            conn.execute(text("SET SESSION default_transaction_read_only=on"))
            conn.execute(text("SET SESSION statement_timeout='500ms'"))
            conn.commit()

            def observe(connection: Any, cursor: Any, statement: str, parameters: Any, context: Any, many: bool) -> None:
                statements.append(statement.upper())

            event.listen(conn, "before_cursor_execute", observe)
            try:
                with Session(conn) as session:
                    yield session
            finally:
                event.remove(conn, "before_cursor_execute", observe)
                conn.rollback()
                conn.execute(text("RESET default_transaction_read_only"))
                conn.execute(text("RESET statement_timeout"))
                conn.commit()

    with engine.begin() as holder:
        holder.execute(text("SET LOCAL statement_timeout='2s'"))
        holder.execute(text("SELECT id FROM volume_profile_ingest_job FOR UPDATE")).all()
        holder.execute(text("SELECT id FROM volume_profile_snapshot FOR UPDATE")).all()
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(profile_jobs.ProfileIngestJobStore(read_only).recover_completed_snapshot,
                                 parsed.spec).result(timeout=3)
    assert result is not None and result.profile.publication == parsed.publication
    assert statements and all(sql.startswith("SELECT") and "FOR UPDATE" not in sql and "PG_ADVISORY" not in sql
                              for sql in statements)
    assert _job_store(engine).recover_completed_snapshot(replace(parsed.spec, id="absent")) is None


def test_atomic_profile_commit_ack_unknown_recovers_exact_committed_result(profile_repository_pg: Engine) -> None:
    engine, value = profile_repository_pg, _publication_pg()
    claim = _atomic_claim(engine)
    failure = DBAPIError("commit acknowledgment", None, Exception("injected ACK loss"))

    @contextmanager
    def ack_unknown() -> Iterator[Session]:
        with Session(engine) as session:
            yield session
            # The owner's inner session.begin() has already committed successfully.
            raise failure

    with pytest.raises(DBAPIError) as caught:
        profile_jobs.ProfileIngestJobStore(ack_unknown).publish_and_complete(claim, value)
    assert caught.value is failure
    before = _job_row(engine)
    assert before["status"] == "DONE" and _profile_counts(engine) == (1, 2)
    persisted = profile_repository.ProfileRepository(sessionmaker(engine)).get_verified(value.content_sha256)
    assert persisted is not None and persisted.publication == value and persisted.revision == 1
    recovered = _job_store(engine).publish_and_complete(claim, value)
    assert recovered.recovered_after_commit and recovered.profile == persisted
    assert _job_row(engine) == before and _profile_counts(engine) == (1, 2)
    changed = replace(value, reconciliation=CanonicalJsonObject({"changed": True}))
    with pytest.raises(profile_jobs.JobCompletionError):
        _job_store(engine).publish_and_complete(claim, changed)
    assert _job_row(engine) == before and _profile_counts(engine) == (1, 2)
    assert profile_repository.ProfileRepository(sessionmaker(engine)).get_verified(value.content_sha256) == persisted


def test_atomic_profile_final_fence_rolls_back_snapshot_and_injected_expiry(profile_repository_pg: Engine) -> None:
    engine, value = profile_repository_pg, _publication_pg()
    claim = _atomic_claim(engine)
    before, reached = _job_row(engine), []

    def expire(connection: sa.Connection, cursor: Any, statement: str,
               parameters: Any, context: Any, executemany: bool) -> None:
        if statement.startswith("UPDATE volume_profile_ingest_job"):
            with cursor.connection.cursor() as injection:
                injection.execute("SELECT quality FROM volume_profile_snapshot WHERE id=%s", (value.content_sha256,))
                assert injection.fetchone() == ("VERIFIED",)
                injection.execute("UPDATE volume_profile_ingest_job SET lease_expires_at="
                                  "clock_timestamp()-interval '1 second' WHERE id=%s", (claim.spec.id,))
                reached.append(True)

    event.listen(engine, "before_cursor_execute", expire)
    try:
        with pytest.raises(profile_jobs.LeaseLost):
            _job_store(engine).publish_and_complete(claim, value)
    finally:
        event.remove(engine, "before_cursor_execute", expire)
    assert reached == [True] and _profile_counts(engine) == (0, 0)
    assert _job_row(engine) == before
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_ingest_job SET lease_expires_at=clock_timestamp()-interval '1 second'"))
    replacement = _job_store(engine).claim_next("replacement", timedelta(minutes=5))
    assert replacement is not None and replacement.attempt == claim.attempt + 1
    result = _job_store(engine).publish_and_complete(replacement, value)
    assert result.profile.revision == 1 and result.job.status == "DONE"


def test_atomic_profile_stale_takeover_cannot_publish(profile_repository_pg: Engine) -> None:
    engine, value = profile_repository_pg, _publication_pg()
    old = _atomic_claim(engine)
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_ingest_job SET lease_expires_at=clock_timestamp()-interval '1 second'"))
    new = _job_store(engine).claim_next("new", timedelta(minutes=5))
    assert new is not None and new.attempt == old.attempt + 1
    before = _job_row(engine)
    with pytest.raises(profile_jobs.LeaseLost):
        _job_store(engine).publish_and_complete(old, value)
    assert _profile_counts(engine) == (0, 0) and _job_row(engine) == before
    assert _job_store(engine).publish_and_complete(new, value).job.status == "DONE"


@pytest.mark.parametrize("different", [False, True])
def test_atomic_profiles_concurrent_jobs_converge_without_duplicate_bins(
    profile_repository_pg: Engine, different: bool,
) -> None:
    engine = profile_repository_pg
    claims = [_atomic_claim(engine, "first"), _atomic_claim(engine, "second")]
    values = [_publication_pg(), _publication_pg(different)]
    barrier = Barrier(2, timeout=10)

    def worker(index: int) -> profile_jobs.JobPublicationResult:
        barrier.wait()
        return _job_store(engine).publish_and_complete(claims[index], values[index])

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker, index) for index in range(2)]
        results = [future.result(timeout=20) for future in futures]
    assert {r.profile.revision for r in results} == ({1, 2} if different else {1})
    assert sum(not r.profile.already_present for r in results) == (2 if different else 1)
    assert _profile_counts(engine) == ((2, 4) if different else (1, 2))
    for claim, value, result in zip(claims, values, results):
        assert result.job.status == "DONE" and result.job.completed_snapshot_id == value.content_sha256
        assert not result.recovered_after_commit and result.profile.publication == value
        assert _job_store(engine).get(claim.spec.id) == result.job
        read = profile_repository.ProfileRepository(sessionmaker(engine)).get_verified(value.content_sha256)
        assert read is not None and read.publication == value


def test_atomic_profile_existing_digest_different_metadata_keeps_second_job_running(profile_repository_pg: Engine) -> None:
    engine, value = profile_repository_pg, _publication_pg()
    first = _job_store(engine).publish_and_complete(_atomic_claim(engine, "first"), value)
    second = _atomic_claim(engine, "second")
    before = _job_row(engine, "second")
    changed = replace(value, source_manifest=CanonicalJsonObject({"different": True}))
    with pytest.raises(profile_jobs.JobCompletionError):
        _job_store(engine).publish_and_complete(second, changed)
    assert _job_row(engine, "second") == before and _profile_counts(engine) == (1, 2)
    read = profile_repository.ProfileRepository(sessionmaker(engine)).get_verified(value.content_sha256)
    assert read is not None and read.publication == value and read.revision == first.profile.revision == 1


@pytest.mark.parametrize("lock_kind", ["job", "advisory"])
def test_atomic_profile_bounded_wait_has_no_partial_mutation(profile_repository_pg: Engine, lock_kind: str) -> None:
    engine, value = profile_repository_pg, _publication_pg()
    claim = _atomic_claim(engine)
    before = _job_row(engine)
    with engine.begin() as holder:
        holder.execute(text("SET LOCAL statement_timeout = '2s'"))
        if lock_kind == "job":
            holder.execute(text("SELECT id FROM volume_profile_ingest_job WHERE id='job' FOR UPDATE")).one()
        else:
            first, second = profile_repository._lock_parts(profile_repository._logical(value.content))
            holder.execute(text("SELECT pg_advisory_xact_lock(:a, :b)"), {"a": first, "b": second})
        _assert_profile_lock_timeout(engine, lambda conn: profile_jobs.ProfileIngestJobStore(
            sessionmaker(bind=conn), _WAIT_POLICY).publish_and_complete(claim, value))
        assert _job_row(engine) == before and _profile_counts(engine) == (0, 0)
    result = profile_jobs.ProfileIngestJobStore(sessionmaker(engine), _WAIT_POLICY).publish_and_complete(claim, value)
    assert result.job.status == "DONE" and result.profile.revision == 1 and _profile_counts(engine) == (1, 2)


def _downgrade(db_name: str, target: str = "base") -> None:
    from alembic import command

    command.downgrade(_alembic_config(db_name), target)


# --------------------------------------------------------------------------- #
# Schema introspection helpers
# --------------------------------------------------------------------------- #


def _table_names(engine: Engine) -> set[str]:
    insp = sa.inspect(engine)
    return set(insp.get_table_names())


def _column_names(engine: Engine, table: str) -> set[str]:
    insp = sa.inspect(engine)
    return {c["name"] for c in insp.get_columns(table)}


def _index_names(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = 'public' AND tablename = :t"
            ),
            {"t": table},
        ).fetchall()
    return {r[0] for r in rows}


def _check_constraint_names(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT con.conname
                FROM pg_constraint con
                JOIN pg_class cls ON cls.oid = con.conrelid
                JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                WHERE ns.nspname = 'public'
                  AND cls.relname = :t
                  AND con.contype = 'c'
                """
            ),
            {"t": table},
        ).fetchall()
    return {r[0] for r in rows}


def _insert_order_identity_prerequisites(
    engine: Engine,
    *,
    exchange_id: str,
    product_id: str,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO exchange (id, name) VALUES (:id, :id) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"id": exchange_id},
        )
        conn.execute(
            text(
                "INSERT INTO product (id, exchange_id, base_asset, quote_asset) "
                "VALUES (:product, :exchange, 'BASE', 'QUOTE') "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {"product": product_id, "exchange": exchange_id},
        )
        conn.execute(
            text(
                "INSERT INTO strategy (id, name, configuration_json) "
                "VALUES ('identity-test', 'Identity Test', '{}')"
            )
        )


def _insert_scoped_order(
    engine: Engine,
    *,
    order_id: str,
    exchange_id: str,
    product_id: str,
    profile: str | None,
    account_id: str | None,
    client_order_id: str,
    exchange_order_id: str,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                'INSERT INTO "order" '
                "(id, exchange_order_id, strategy_id, product_id, exchange_id, "
                "account_profile, account_id, type, side, quantity, status, "
                "timestamp, client_order_id) VALUES "
                "(:id, :exchange_order_id, 'identity-test', :product_id, "
                ":exchange_id, :profile, :account_id, 'market', 'buy', 1, "
                "'open', 1, :client_order_id)"
            ),
            {
                "id": order_id,
                "exchange_order_id": exchange_order_id,
                "product_id": product_id,
                "exchange_id": exchange_id,
                "profile": profile,
                "account_id": account_id,
                "client_order_id": client_order_id,
            },
        )


def _schema_fingerprint(engine: Engine) -> tuple:
    """Return a stable snapshot of the public schema for diff comparison."""
    insp = sa.inspect(engine)
    fp: list[tuple] = []
    for table in sorted(insp.get_table_names()):
        cols = tuple(
            sorted((c["name"], str(c["type"])) for c in insp.get_columns(table))
        )
        idx = tuple(sorted(_index_names(engine, table)))
        chk = tuple(sorted(_check_constraint_names(engine, table)))
        fp.append((table, cols, idx, chk))
    return tuple(fp)


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


def test_decision_input_constraints_and_immutable_dml(
    profile_repository_pg: Engine,
) -> None:
    engine = profile_repository_pg
    table = sa.Table("market_data_decision_input", sa.MetaData(), autoload_with=engine)
    row = dict(
        input_id="a" * 64,
        environment="live",
        execution_scope_id="deployment",
        strategy_id="s",
        strategy_version="v1",
        config_hash="b" * 64,
        product_id="BINANCE:BTCUSDT-SPOT",
        trigger_kind="CANDLE",
        trigger_id="1m:0",
        requirements_digest="c" * 64,
        policy_digest="d" * 64,
        input_digest="e" * 64,
        decision_time_ms=0,
        contract_version=1,
        canonical_payload=b"{}",
    )
    with engine.begin() as conn:
        first = dict(
            conn.execute(table.insert().values(**row).returning(table)).mappings().one()
        )
    stamp = first["recorded_at"]
    assert stamp.utcoffset() == timedelta(0) and stamp.microsecond % 1000 == 0
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            accepted = conn.execute(
                table.insert()
                .values(
                    **{
                        **row,
                        "input_id": "1" * 64,
                        "strategy_id": "trigger_max",
                        "trigger_id": "1m:9223372036854775807",
                    }
                )
                .returning(table.c.trigger_id)
            ).scalar_one()
            assert accepted == "1m:9223372036854775807"
        finally:
            transaction.rollback()
    with pytest.raises(DBAPIError) as caught, engine.begin() as conn:
        conn.execute(
            table.insert().values(
                **{
                    **row,
                    "input_id": "2" * 64,
                    "strategy_id": "trigger_overflow",
                    "trigger_id": "1m:9223372036854775808",
                }
            )
        )
    assert getattr(caught.value.orig, "pgcode", None) == "23514"
    invalid = [
        (name, "INVALID!")
        for name in (
            "environment",
            "execution_scope_id",
            "strategy_id",
            "strategy_version",
        )
    ]
    invalid += [
        (name, "A" * 64)
        for name in (
            "input_id",
            "config_hash",
            "requirements_digest",
            "policy_digest",
            "input_digest",
        )
    ]
    invalid += [
        ("product_id", "BINANCE:ETHUSDT-SPOT"),
        ("product_id", "bad"),
        ("trigger_kind", "OTHER"),
        ("trigger_id", "1m:00"),
        ("trigger_id", "1m:-1"),
        ("trigger_id", "1m:12345678901234567890"),
        ("trigger_id", "bad:tf:0"),
        ("decision_time_ms", -1),
        ("decision_time_ms", 2**63),
        ("contract_version", 2),
        ("canonical_payload", b""),
        ("canonical_payload", b"x" * (8388608 + 1)),
    ]
    for name, value in invalid:
        candidate = {**row, "input_id": "f" * 64, "strategy_id": "other", name: value}
        with pytest.raises(DBAPIError), engine.begin() as conn:
            conn.execute(table.insert().values(**candidate))
    for name in row:
        with pytest.raises(DBAPIError), engine.begin() as conn:
            conn.execute(
                table.insert().values(
                    **{**row, "input_id": "f" * 64, "strategy_id": "other", name: None}
                )
            )
    for timestamp in (
        "-infinity",
        "infinity",
        "10000-01-01 00:00:00+00",
        "2026-01-01 00:00:00.000001+00",
    ):
        with pytest.raises(DBAPIError), engine.begin() as conn:
            conn.execute(
                table.insert().values(
                    **{
                        **row,
                        "input_id": "f" * 64,
                        "strategy_id": "other",
                        "recorded_at": sa.cast(timestamp, sa.DateTime(timezone=True)),
                    }
                )
            )
    with pytest.raises(DBAPIError) as caught, engine.begin() as conn:
        conn.execute(table.insert().values(**{**row, "input_id": "f" * 64}))
    assert getattr(caught.value.orig, "pgcode", None) == "23505"
    with pytest.raises(DBAPIError) as caught, engine.begin() as conn:
        conn.execute(table.insert().values(**{**row, "strategy_id": "different_key"}))
    assert getattr(caught.value.orig, "pgcode", None) == "23505"
    for statement in (
        "UPDATE market_data_decision_input SET strategy_id=strategy_id",
        "UPDATE market_data_decision_input SET strategy_id='changed' WHERE false",
        "DELETE FROM market_data_decision_input",
        "DELETE FROM market_data_decision_input WHERE false",
        "TRUNCATE market_data_decision_input",
    ):
        with pytest.raises(DBAPIError) as caught, engine.begin() as conn:
            conn.execute(text(statement))
        assert getattr(caught.value.orig, "pgcode", None) == "55000"
        with engine.connect() as conn:
            assert dict(conn.execute(sa.select(table)).mappings().one()) == first


def test_decision_input_upgrade_downgrade_artifacts(fresh_pg_db: str) -> None:
    _upgrade(fresh_pg_db)
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        for index, present in enumerate((True, False, True)):
            with engine.connect() as conn:
                assert (
                    conn.execute(
                        text("SELECT to_regclass('market_data_decision_input')")
                    ).scalar_one()
                    is not None
                ) is present
                assert (
                    conn.execute(
                        text(
                            "SELECT to_regprocedure('reject_market_data_decision_input_mutation()')"
                        )
                    ).scalar_one()
                    is not None
                ) is present
                assert conn.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger WHERE tgname='market_data_decision_input_append_only' AND NOT tgisinternal"
                    )
                ).scalar_one() == int(present)
            if index == 0:
                _downgrade(fresh_pg_db, "7d3a9c02e5f8")
            elif index == 1:
                _upgrade(fresh_pg_db)
    finally:
        engine.dispose()


def test_decision_input_store_concurrent_pins(profile_repository_pg: Engine) -> None:
    from sqlalchemy import event
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from src.core.market_data.profiles.decision_input_store import DecisionInputStore, DecisionInputConflict
    from test_profile_decision_input import sample

    engine = profile_repository_pg.execution_options(isolation_level="REPEATABLE READ")
    value = sample()
    for conflict in (False, True):
        first = replace(value, key=replace(value.key, strategy_id=f"concurrent_{conflict}"))
        second = replace(first, requirements=(), context=replace(first.context, profiles=())) if conflict else first
        barrier = Barrier(2)
        isolation = []

        @contextmanager
        def sessions():
            with Session(engine) as session:
                yield session

        def capture(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("SELECT market_data_decision_input"):
                with conn.connection.cursor() as probe:
                    probe.execute("SHOW transaction_isolation")
                    isolation.append(probe.fetchone()[0])

        def pin(item):
            barrier.wait(timeout=5)
            try:
                return DecisionInputStore(sessions).pin(item)
            except DecisionInputConflict:
                return None

        event.listen(engine, "after_cursor_execute", capture)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(pin, item) for item in (first, second)]
                results = [future.result(timeout=10) for future in futures]
            assert isolation and set(isolation) == {"read committed"}
            if conflict:
                assert sum(result is None for result in results) == 1
            else:
                assert sorted(result.already_present for result in results if result is not None) == [False, True]
        finally:
            event.remove(engine, "after_cursor_execute", capture)
        with engine.connect() as conn:
            assert conn.execute(text("SELECT count(*) FROM market_data_decision_input WHERE strategy_id=:strategy"), {"strategy": first.key.strategy_id}).scalar_one() == 1


def test_decision_input_store_commit_faults_and_wait(profile_repository_pg: Engine) -> None:
    from contextlib import nullcontext
    from sqlalchemy import event
    from src.core.market_data.profiles.repository import TransactionWaitPolicy
    from src.core.market_data.profiles.decision_input_store import DecisionInputStore
    from test_profile_decision_input import sample

    engine = profile_repository_pg
    value = sample()
    error = DBAPIError(None, None, RuntimeError("lost ACK"))

    @contextmanager
    def ack_lost():
        with Session(engine) as session:
            yield session
        raise error

    store = DecisionInputStore(sessionmaker(engine))
    with pytest.raises(DBAPIError) as caught:
        DecisionInputStore(ack_lost).pin(value)
    assert caught.value is error
    confirmed = store.confirm(value)
    assert confirmed is not None and confirmed.already_present and confirmed.value == value
    assert store.confirm(value) == confirmed

    rolled = replace(value, key=replace(value.key, strategy_id="rolled"))
    def abort(session):
        raise error
    with Session(engine) as session:
        event.listen(session, "before_commit", abort)
        with pytest.raises(DBAPIError) as caught:
            DecisionInputStore(lambda: nullcontext(session)).pin(rolled)
        assert caught.value is error
    assert store.get(rolled.key) is None

    from src.core.market_data.profiles.decision_input_store import _values
    from src.core.market_data.profiles.orm import MarketDataDecisionInput as InputRow
    waiting = replace(value, key=replace(value.key, strategy_id="waiting"))
    inserts = []
    def count_insert(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("INSERT INTO market_data_decision_input"):
            inserts.append(statement)
    with engine.connect() as holder:
        transaction = holder.begin()
        try:
            holder.execute(sa.insert(InputRow).values(**_values(waiting)))
            short = DecisionInputStore(sessionmaker(engine), TransactionWaitPolicy(25, 1000))
            event.listen(engine, "before_cursor_execute", count_insert)
            try:
                with pytest.raises(DBAPIError) as caught:
                    short.pin(waiting)
            finally:
                event.remove(engine, "before_cursor_execute", count_insert)
            assert getattr(caught.value.orig, "pgcode", None) == "55P03"
            assert len(inserts) == 1
        finally:
            transaction.rollback()
    assert store.get(waiting.key) is None
    assert not store.pin(waiting).already_present


@pytest.mark.parametrize("damage", ["payload", "key", "time", "digest"])
def test_decision_input_store_corruption(profile_repository_pg: Engine, damage: str) -> None:
    from src.core.market_data.profiles.decision_input_store import DecisionInputStore, DecisionInputIntegrityError, _values
    from src.core.market_data.profiles.orm import MarketDataDecisionInput as InputRow
    from test_profile_decision_input import sample

    value = sample()
    values = _values(value)
    changes = {"payload": {"canonical_payload": b"{}"}, "key": {"strategy_version": "other"},
               "time": {"decision_time_ms": value.decision_time_ms + 1}, "digest": {"input_digest": "f" * 64}}
    with profile_repository_pg.begin() as conn:
        conn.execute(sa.insert(InputRow).values(**{**values, **changes[damage]}))
    store = DecisionInputStore(sessionmaker(profile_repository_pg))
    for action in (lambda: store.get(value.key), lambda: store.confirm(value), lambda: store.pin(value)):
        with pytest.raises(DecisionInputIntegrityError):
            action()


def test_terminal_schema_contract_and_guards(profile_repository_pg: Engine) -> None:
    from dataclasses import asdict
    from src.core.market_data.profiles.decision_input_store import _values
    from test_profile_decision_input import sample
    from test_profile_decision_application import batch, key

    engine = profile_repository_pg
    metadata = sa.MetaData()
    receipt, header, outcome, inputs = [sa.Table(name, metadata, autoload_with=engine) for name in (
        "market_data_application", "market_data_decision_batch", "market_data_decision_outcome", "market_data_decision_input")]
    empty = batch()
    receipt_values = dict(environment="live", product_id=empty.product_id, timeframe="1m", timestamp=0,
                          open=Decimal(1), high=Decimal(1), low=Decimal(1), close=Decimal(1), volume=Decimal(0))
    base = dict(environment="live", product_id=empty.product_id, timeframe="1m", bar_start_ms=0,
                execution_scope_id="deployment", contract_version=1, participant_count=0,
                canonical_payload=empty.canonical_bytes, batch_digest=empty.digest)
    with engine.begin() as conn:
        assert conn.execute(receipt.insert().values(**receipt_values).returning(receipt.c.decision_contract_version)).scalar_one() is None
    with pytest.raises(DBAPIError) as caught, engine.begin() as conn:
        conn.execute(header.insert().values(**base))
    assert getattr(caught.value.orig, "pgcode", None) == "23503"
    with engine.begin() as conn:
        conn.execute(receipt.update().values(decision_contract_version=1))
    for field, value in (("participant_count", -1), ("participant_count", 257), ("canonical_payload", b""),
                         ("canonical_payload", b"x" * 1048577), ("batch_digest", "A" * 64),
                         ("bar_start_ms", -1), ("execution_scope_id", "bad!"), ("contract_version", 2),
                         ("recorded_at", sa.cast("infinity", sa.DateTime(timezone=True)))):
        with pytest.raises(DBAPIError), engine.begin() as conn:
            conn.execute(header.insert().values(**{**base, field: value}))
    # SQL resource boundaries are independent of codec completeness: later helper validates payload claims.
    for changes in ({"participant_count": 256}, {"canonical_payload": empty.canonical_bytes + b" " * (1048576 - len(empty.canonical_bytes))}):
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                conn.execute(header.insert().values(**{**base, **changes}))
            finally:
                tx.rollback()
    with engine.begin() as conn:
        recorded = conn.execute(header.insert().values(**base).returning(header.c.recorded_at)).scalar_one()
        assert recorded.utcoffset() == timedelta(0) and recorded.microsecond % 1000 == 0
        conn.execute(inputs.insert().values(**_values(sample())))
    terminal = dict(**asdict(key()), timeframe="1m", bar_start_ms=0, contract_version=1,
                    disposition="APPLIED", input_id=sample().input_id, input_digest=sample().input_digest,
                    reason=None, signal_suppressed=False, suppression_reason=None)
    for changes in ({}, {"signal_suppressed": True, "suppression_reason": "SNAPSHOT_REVOKED"},
                    {"disposition": "SKIPPED", "reason": "INPUT_STORE_FAILED", "input_id": None, "input_digest": None},
                    {"disposition": "SKIPPED", "reason": "INPUT_COMMIT_UNCONFIRMED"}):
        with engine.connect() as conn:
            tx = conn.begin()
            try:
                conn.execute(outcome.insert().values(**{**terminal, **changes}))
            finally:
                tx.rollback()
    for changes in ({"input_id": None, "input_digest": None}, {"input_digest": None}, {"input_id": "f" * 64},
                    {"input_digest": "BAD"}, {"execution_scope_id": "other"}, {"trigger_id": "1m:1"},
                    {"disposition": "OTHER"}, {"reason": "INPUT_STORE_FAILED"},
                    {"disposition": "SKIPPED", "reason": None}, {"disposition": "SKIPPED", "reason": "PROFILE_NOT_READY"},
                    {"disposition": "SKIPPED", "reason": "INPUT_STORE_FAILED", "signal_suppressed": True, "suppression_reason": "SNAPSHOT_REVOKED"},
                    {"signal_suppressed": True}, {"suppression_reason": "SNAPSHOT_REVOKED"}):
        with pytest.raises(DBAPIError), engine.begin() as conn:
            conn.execute(outcome.insert().values(**{**terminal, **changes}))
    with engine.begin() as conn:
        conn.execute(outcome.insert().values(**terminal))
    for changes in ({}, {"strategy_version": "v2"}):
        with pytest.raises(DBAPIError) as caught, engine.begin() as conn:
            conn.execute(outcome.insert().values(**{**terminal, **changes}))
        assert getattr(caught.value.orig, "pgcode", None) == "23505"
    for table in (header, outcome):
        with engine.connect() as conn:
            before = conn.execute(sa.select(table)).mappings().all()
        for statement in (f"UPDATE {table.name} SET contract_version=contract_version", f"UPDATE {table.name} SET contract_version=1 WHERE false",
                          f"DELETE FROM {table.name}", f"DELETE FROM {table.name} WHERE false",
                          "TRUNCATE market_data_decision_batch, market_data_decision_outcome"):
            with pytest.raises(DBAPIError) as caught, engine.begin() as conn:
                conn.execute(text(statement))
            assert getattr(caught.value.orig, "pgcode", None) == "55000"
        with engine.connect() as conn:
            assert conn.execute(sa.select(table)).mappings().all() == before


def test_terminal_schema_roundtrip(fresh_pg_db: str) -> None:
    _upgrade(fresh_pg_db)
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        for index, stage in enumerate((True, False, True)):
            with engine.connect() as conn:
                for table in ("market_data_decision_batch", "market_data_decision_outcome"):
                    assert (conn.execute(text("SELECT to_regclass(:name)"), {"name": table}).scalar_one() is not None) is stage
                assert (conn.execute(text("SELECT to_regprocedure('reject_market_data_terminal_mutation()')")).scalar_one() is not None) is stage
                assert conn.execute(text("SELECT count(*) FROM information_schema.columns WHERE table_name='market_data_application' AND column_name='decision_contract_version'")).scalar_one() == int(stage)
                assert conn.execute(text("SELECT count(*) FROM pg_trigger WHERE tgname IN ('market_data_decision_batch_append_only','market_data_decision_outcome_append_only') AND NOT tgisinternal")).scalar_one() == 2 * int(stage)
            if index == 0:
                _downgrade(fresh_pg_db, "8e4b2c91a6d0")
            elif index == 1:
                _upgrade(fresh_pg_db)
    finally:
        engine.dispose()


# Selected non-legacy tables that must exist at HEAD and be gone
# after a full downgrade to ``base``.
HEAD_ONLY_TABLES = {
    "market_data_decision_batch",
    "market_data_decision_outcome",
    "market_data_invalidation",
    "market_data_decision_input",
    "volume_profile_snapshot",
    "volume_profile_bin",
    "volume_profile_ingest_job",
    "system_events",
    "strategy_state_transitions",
    "daily_nav_snapshots",
    "evolution_epochs",
    "gene_records",
}

# Pre-existing tables (created by revs 1–4) that must remain at every stage.
LEGACY_TABLES = {
    "exchange",
    "product",
    "strategy",
    "order",
    "signal_audit",
    "strategy_state",
}


def test_full_upgrade_to_head(fresh_pg_db: str) -> None:
    """Upgrade ``base`` → ``head`` and verify HEAD schema artifacts."""
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        tables = _table_names(engine)
        assert LEGACY_TABLES.issubset(tables), (
            f"Legacy tables missing after upgrade: {LEGACY_TABLES - tables}"
        )
        assert HEAD_ONLY_TABLES.issubset(tables), "HEAD tables missing after upgrade"

        # ``order`` must carry idempotency, audit, and account identity columns.
        order_cols = _column_names(engine, "order")
        for col in (
            "client_order_id",
            "intent_payload",
            "submitted_at",
            "acked_at",
            "last_reconciled_at",
            "account_profile",
            "account_id",
        ):
            assert col in order_cols, f"order.{col} missing after upgrade"

        # ``signal_audit`` must carry the 4 new payload/relation columns.
        sa_cols = _column_names(engine, "signal_audit")
        for col in (
            "client_order_id",
            "intent_payload",
            "outcome_payload",
            "signal_batch_id",
        ):
            assert col in sa_cols, f"signal_audit.{col} missing after upgrade"

        # ``strategy_state`` must gain the 5 new error/version columns.
        ss_cols = _column_names(engine, "strategy_state")
        for col in (
            "last_error_message",
            "entered_error_at",
            "recovered_at",
            "stopped_at",
            "version",
        ):
            assert col in ss_cols, f"strategy_state.{col} missing after upgrade"

        # Account-scoped and legacy partial unique order indexes.
        order_idx = _index_names(engine, "order")
        assert {
            "uq_order_identified_client_order_id",
            "uq_order_legacy_client_order_id",
            "uq_order_identified_exchange_order_id",
            "uq_order_legacy_exchange_order_id",
        } <= order_idx
        assert "uq_order_client_order_id" not in order_idx
        assert "idx_order_client_order_id" in order_idx
        assert "idx_order_strategy_status" in order_idx

        # Partial unique champion-per-strategy index.
        gene_idx = _index_names(engine, "gene_records")
        assert "uq_one_champion_per_strategy" in gene_idx

        # CHECK constraints landed by revs 5–7.
        assert "chk_system_events_type" in _check_constraint_names(
            engine, "system_events"
        )
        ss_checks = _check_constraint_names(engine, "strategy_state")
        assert "chk_error_state" in ss_checks
        assert "chk_stopped_state" in ss_checks
        assert "chk_nav_source" in _check_constraint_names(
            engine, "daily_nav_snapshots"
        )
        assert "chk_gene_role" in _check_constraint_names(engine, "gene_records")
        assert "chk_epoch_status" in _check_constraint_names(engine, "evolution_epochs")
        assert "chk_order_account_identity_complete" in _check_constraint_names(
            engine, "order"
        )
    finally:
        engine.dispose()


def test_sample_data_insertion_after_upgrade(fresh_pg_db: str) -> None:
    """Exercise CHECKs and partial unique indexes with real INSERTs."""
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        with engine.begin() as conn:
            # FK pre-requisites.
            conn.execute(
                text("INSERT INTO exchange (id, name) VALUES ('binance', 'Binance')")
            )
            conn.execute(
                text(
                    "INSERT INTO product (id, exchange_id, base_asset, quote_asset) "
                    "VALUES ('binance:BTC/USDT', 'binance', 'BTC', 'USDT')"
                )
            )
            conn.execute(
                text(
                    "INSERT INTO strategy (id, name, configuration_json) "
                    "VALUES ('strat-1', 'Test Strategy', '{}')"
                )
            )

            for order_id, profile, account_id in (
                ("order-no-identity", None, None),
                ("order-with-identity", "test", "ACCOUNT"),
            ):
                conn.execute(
                    text(
                        'INSERT INTO "order" '
                        "(id, strategy_id, product_id, exchange_id, account_profile, "
                        "account_id, type, side, quantity, status, timestamp) "
                        "VALUES (:id, 'strat-1', 'binance:BTC/USDT', 'binance', "
                        ":profile, :account_id, 'market', 'buy', 1, 'open', 1)"
                    ),
                    {"id": order_id, "profile": profile, "account_id": account_id},
                )

            for order_id, account_id in (
                ("order-account-a", "ACCOUNT-A"),
                ("order-account-b", "ACCOUNT-B"),
            ):
                conn.execute(
                    text(
                        'INSERT INTO "order" '
                        "(id, exchange_order_id, strategy_id, product_id, exchange_id, "
                        "account_profile, account_id, type, side, quantity, status, "
                        "timestamp, client_order_id) VALUES "
                        "(:id, 'SHARED-EXCHANGE', 'strat-1', 'binance:BTC/USDT', "
                        "'binance', 'ccxt:binance:live', :account_id, 'market', "
                        "'buy', 1, 'open', 1, 'shared-client')"
                    ),
                    {"id": order_id, "account_id": account_id},
                )

            conn.execute(
                text(
                    'INSERT INTO "order" '
                    "(id, exchange_order_id, strategy_id, product_id, exchange_id, "
                    "type, side, quantity, status, timestamp, client_order_id) "
                    "VALUES ('legacy-unique', 'LEGACY-EXCHANGE', 'strat-1', "
                    "'binance:BTC/USDT', 'binance', 'market', 'buy', 1, 'open', "
                    "1, 'legacy-client')"
                )
            )

            # Positive system_events insert.
            conn.execute(
                text(
                    "INSERT INTO system_events (event_type, payload) "
                    "VALUES ('reconcile', '{\"note\": \"ok\"}'::jsonb)"
                )
            )

        # Negative CHECK: invalid event_type must be rejected.
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO system_events (event_type, payload) "
                        "VALUES ('invalid_type', '{}'::jsonb)"
                    )
                )

        for index, (profile, account_id) in enumerate(
            (
                ("test", None),
                (None, "ACCOUNT"),
                ("", "ACCOUNT"),
                ("test", " "),
            )
        ):
            with pytest.raises(IntegrityError):
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            'INSERT INTO "order" '
                            "(id, strategy_id, product_id, exchange_id, "
                            "account_profile, account_id, type, side, quantity, "
                            "status, timestamp) "
                            "VALUES (:id, 'strat-1', 'binance:BTC/USDT', 'binance', "
                            ":profile, :account_id, 'market', 'buy', 1, 'open', 1)"
                        ),
                        {
                            "id": f"invalid-account-identity-{index}",
                            "profile": profile,
                            "account_id": account_id,
                        },
                    )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        'INSERT INTO "order" '
                        "(id, exchange_order_id, strategy_id, product_id, exchange_id, "
                        "account_profile, account_id, type, side, quantity, status, "
                        "timestamp, client_order_id) VALUES "
                        "('duplicate-identified', 'SHARED-EXCHANGE', 'strat-1', "
                        "'binance:BTC/USDT', 'binance', 'ccxt:binance:live', "
                        "'ACCOUNT-A', 'market', 'buy', 1, 'open', 1, "
                        "'shared-client')"
                    )
                )

        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        'INSERT INTO "order" '
                        "(id, exchange_order_id, strategy_id, product_id, exchange_id, "
                        "type, side, quantity, status, timestamp, client_order_id) "
                        "VALUES ('duplicate-legacy', 'LEGACY-EXCHANGE', 'strat-1', "
                        "'binance:BTC/USDT', 'binance', 'market', 'buy', 1, 'open', "
                        "1, 'legacy-client')"
                    )
                )

        # Negative CHECK: daily_nav_snapshots.source must be in whitelist.
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO daily_nav_snapshots "
                        "(strategy_id, snapshot_date, nav, base_currency, source) "
                        "VALUES ('strat-1', DATE '2026-01-01', 1000.0, 'USDT', 'random')"
                    )
                )

        # Positive nav snapshot insert (default source = 'eod_snapshot').
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO daily_nav_snapshots "
                    "(strategy_id, snapshot_date, nav, base_currency) "
                    "VALUES ('strat-1', DATE '2026-01-02', 1234.56789012, 'USDT')"
                )
            )

        # Evolution epoch + gene records (champion uniqueness).
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO evolution_epochs
                    (id, strategy_id, started_at, pop_size, max_generations,
                     seed, status, eval_pair, eval_start_date, eval_end_date,
                     eval_timeframe)
                    VALUES ('epoch-1', 'strat-1', NOW(), 10, 5, 42, 'running',
                            'BTC/USDT', DATE '2025-01-01', DATE '2025-06-01', '1h')
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO gene_records
                    (strategy_id, role, param_pack, score_total, score_breakdown,
                     max_drawdown, generation_index, candidate_id, epoch_id)
                    VALUES ('strat-1', 'champion', '{}'::jsonb, 1.0, '{}'::jsonb,
                            0.05, 0, 'champion', 'epoch-1')
                    """
                )
            )

        # Negative CHECK: invalid gene role.
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO gene_records
                        (strategy_id, role, param_pack, score_total,
                         score_breakdown, max_drawdown, generation_index,
                         candidate_id, epoch_id)
                        VALUES ('strat-1', 'bogus', '{}'::jsonb, 0.0, '{}'::jsonb,
                                0.0, 0, 'bogus', 'epoch-1')
                        """
                    )
                )

        # Partial unique: a second champion for the same strategy must fail.
        with pytest.raises(IntegrityError):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO gene_records
                        (strategy_id, role, param_pack, score_total,
                         score_breakdown, max_drawdown, generation_index,
                         candidate_id, epoch_id)
                        VALUES ('strat-1', 'champion', '{}'::jsonb, 2.0,
                                '{}'::jsonb, 0.05, 0, 'champion-2', 'epoch-1')
                        """
                    )
                )

        # ...but a challenger alongside the champion is allowed.
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO gene_records
                    (strategy_id, role, param_pack, score_total, score_breakdown,
                     max_drawdown, generation_index, candidate_id, epoch_id)
                    VALUES ('strat-1', 'challenger', '{}'::jsonb, 0.5, '{}'::jsonb,
                            1250.125, 0, 'challenger', 'epoch-1')
                    """
                )
            )

        # Read-back sanity: rows are visible.
        with engine.begin() as conn:
            n_events = conn.execute(
                text("SELECT COUNT(*) FROM system_events")
            ).scalar_one()
            n_genes = conn.execute(
                text("SELECT COUNT(*) FROM gene_records")
            ).scalar_one()
        assert n_events == 1
        assert n_genes == 2
    finally:
        engine.dispose()


def test_full_downgrade_to_base(fresh_pg_db: str) -> None:
    """Upgrade then downgrade fully; HEAD schema objects must be removed."""
    _upgrade(fresh_pg_db, "head")
    _downgrade(fresh_pg_db, "base")

    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        tables = _table_names(engine)
        # All selected non-legacy tables are gone.
        assert HEAD_ONLY_TABLES.isdisjoint(tables), "HEAD tables remain after downgrade"
        # And the original revs 1–4 tables are also gone (full downgrade).
        # alembic_version may or may not remain depending on Alembic version;
        # exclude it from comparison.
        residual = tables - {"alembic_version"}
        assert residual == set(), (
            f"Unexpected residual tables after full downgrade: {residual}"
        )
    finally:
        engine.dispose()


def test_ops_events_survive_downgrade_and_reupgrade(fresh_pg_db: str) -> None:
    _upgrade(fresh_pg_db, "4f7a2c9d1e6b")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    payloads = [
        '{"reason":"risk"}',
        '["operator",42]',
        '"scalar-payload"',
    ]
    try:
        with engine.begin() as conn:
            for index, payload in enumerate(payloads):
                conn.execute(
                    text(
                        "INSERT INTO system_events "
                        "(event_type, event_subtype, payload) "
                        "VALUES ('ops', :subtype, CAST(:payload AS jsonb))"
                    ),
                    {"subtype": f"kill_switch_{index}", "payload": payload},
                )
            conn.execute(
                text(
                    "INSERT INTO system_events "
                    "(event_type, event_subtype, payload) "
                    "VALUES ('system_error', 'existing_error', "
                    "CAST(:payload AS jsonb))"
                ),
                {"payload": '{"message":"existing"}'},
            )
        with engine.connect() as conn:
            original_identity = [
                tuple(row)
                for row in conn.execute(
                    text("SELECT id, event_subtype FROM system_events ORDER BY id")
                )
            ]

        _downgrade(fresh_pg_db, "fb8c6e6098e3")
        with engine.connect() as conn:
            downgraded = (
                conn.execute(
                    text(
                        "SELECT id, event_type, event_subtype, payload "
                        "FROM system_events ORDER BY id"
                    )
                )
                .mappings()
                .all()
            )
        assert [row["event_type"] for row in downgraded] == ["system_error"] * 4
        assert all(
            "__migration_4f7a2c9d1e6b_ops_event" in row["payload"]
            for row in downgraded[:3]
        )
        assert downgraded[3]["payload"] == {"message": "existing"}
        assert [(row["id"], row["event_subtype"]) for row in downgraded] == (
            original_identity
        )

        _upgrade(fresh_pg_db, "4f7a2c9d1e6b")
        with engine.connect() as conn:
            restored = (
                conn.execute(
                    text(
                        "SELECT id, event_type, event_subtype, payload::text AS payload "
                        "FROM system_events ORDER BY id"
                    )
                )
                .mappings()
                .all()
            )
        assert [row["event_type"] for row in restored] == ["ops"] * 3 + ["system_error"]
        assert [json.loads(row["payload"]) for row in restored[:3]] == [
            json.loads(payload) for payload in payloads
        ]
        assert json.loads(restored[3]["payload"]) == {"message": "existing"}
        assert [(row["id"], row["event_subtype"]) for row in restored] == (
            original_identity
        )
    finally:
        engine.dispose()


def _invalidation_seed(engine: Engine):
    repo = profile_repository.ProfileRepository(sessionmaker(engine))
    target = repo.publish(_publication_pg())
    replacement = repo.publish(_publication_pg(True))
    request = profile_invalidation.ConfirmedInvalidationRequest("event", target.snapshot_id, "BAD_DATA", replacement.snapshot_id, "audit")
    return profile_invalidation.ProfileInvalidationStore(sessionmaker(engine)), request


def _invalidation_rows(engine: Engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(text("SELECT * FROM market_data_invalidation ORDER BY recorded_at,event_id")).mappings()]


def test_invalidation_constraints_defaults_and_append_only_dml(profile_repository_pg: Engine) -> None:
    engine = profile_repository_pg
    store, request = _invalidation_seed(engine)
    with engine.connect() as conn:
        lower = conn.execute(text("SELECT date_trunc('milliseconds',clock_timestamp())")).scalar_one()
    first = store.append_confirmed(request)
    with engine.connect() as conn:
        upper = conn.execute(text("SELECT clock_timestamp()")).scalar_one()
    assert lower <= first.event.recorded_at <= upper
    assert first.event.recorded_at.utcoffset() == timedelta(0) and first.event.recorded_at.microsecond % 1000 == 0
    assert store.append_confirmed(request).event == first.event
    assert store.append_confirmed(request).already_present
    before = _invalidation_rows(engine), _retention_rows(engine)
    for statement in ("UPDATE market_data_invalidation SET source='changed'", "UPDATE market_data_invalidation SET source=source",
                      "UPDATE market_data_invalidation SET source=source WHERE false",
                      "DELETE FROM market_data_invalidation", "DELETE FROM market_data_invalidation WHERE false", "TRUNCATE market_data_invalidation"):
        with pytest.raises(DBAPIError) as caught, engine.begin() as conn:
            conn.execute(text(statement))
        assert getattr(caught.value.orig, "pgcode", None) == "55000"
        assert (_invalidation_rows(engine), _retention_rows(engine)) == before
    for identifier in (request.snapshot_id, request.replacement_snapshot_id):
        with pytest.raises(IntegrityError), engine.begin() as conn:
            conn.execute(text("DELETE FROM volume_profile_snapshot WHERE id=:id"), {"id": identifier})
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for field, value in (("event_id", "bad!"), ("reason_code", "é"), ("source", ""), ("snapshot_id", "c" * 64),
                         ("replacement_snapshot_id", request.snapshot_id), ("replacement_snapshot_id", "d" * 64),
                         ("recorded_at", timestamp.replace(microsecond=1))):
        params = dict(event_id="other", snapshot_id=request.snapshot_id, reason_code="BAD", source="audit",
                      replacement_snapshot_id=request.replacement_snapshot_id, recorded_at=timestamp)
        params[field] = value
        with pytest.raises(IntegrityError), engine.begin() as conn:
            conn.execute(text("INSERT INTO market_data_invalidation (event_id,snapshot_id,reason_code,source,replacement_snapshot_id,recorded_at) "
                              "VALUES (:event_id,:snapshot_id,:reason_code,:source,:replacement_snapshot_id,:recorded_at)"), params)
    assert (_invalidation_rows(engine), _retention_rows(engine)) == before
    for stamp in ("-infinity", "infinity", "10000-01-01 00:00:00+00"):
        with pytest.raises(IntegrityError), engine.begin() as conn:
            conn.execute(text("INSERT INTO market_data_invalidation(event_id,snapshot_id,reason_code,source,recorded_at) "
                              "VALUES ('invalid_time',:id,'BAD','audit',CAST(:stamp AS timestamptz))"),
                         {"id": request.snapshot_id, "stamp": stamp})
        assert (_invalidation_rows(engine), _retention_rows(engine)) == before
    for name, stamp in (("minimum_time", datetime(1, 1, 1, tzinfo=timezone.utc)),
                        ("maximum_time", datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc))):
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO market_data_invalidation(event_id,snapshot_id,reason_code,source,recorded_at) "
                              "VALUES (:event,:id,'BAD','audit',:stamp)"),
                         {"event": name, "id": request.snapshot_id, "stamp": stamp})
        persisted = store.get(name)
        assert persisted is not None and persisted.recorded_at == stamp
    assert len(_invalidation_rows(engine)) == 3 and _retention_rows(engine) == before[1]


def test_invalidation_reference_rules_and_corrupt_bins_remain_revocable(profile_repository_pg: Engine) -> None:
    engine = profile_repository_pg
    store, request = _invalidation_seed(engine)
    for changed in (replace(request, snapshot_id="d" * 64), replace(request, replacement_snapshot_id="d" * 64)):
        with pytest.raises(profile_invalidation.InvalidationReferenceError):
            store.append_confirmed(changed)
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_snapshot SET grid_id='other' WHERE id=:id"), {"id": request.replacement_snapshot_id})
    with pytest.raises(profile_invalidation.InvalidationReferenceError):
        store.append_confirmed(request)
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_snapshot SET grid_id='g1',quality='PARTIAL',published_at=NULL WHERE id=:id"), {"id": request.replacement_snapshot_id})
    with pytest.raises(profile_invalidation.InvalidationReferenceError):
        store.append_confirmed(request)
    with engine.begin() as conn:
        conn.execute(text("UPDATE volume_profile_bin SET base_volume=base_volume+1 WHERE snapshot_id=:id"), {"id": request.snapshot_id})
    before = _retention_rows(engine)
    first = store.append_confirmed(replace(request, replacement_snapshot_id=None))
    second = store.append_confirmed(replace(request, event_id="other", replacement_snapshot_id=None))
    assert first.event.snapshot_id == second.event.snapshot_id and len(_invalidation_rows(engine)) == 2
    assert _retention_rows(engine) == before


@pytest.mark.parametrize("mode", ["same", "conflict", "different_ids"])
def test_invalidation_concurrent_convergence_uses_read_committed(profile_repository_pg: Engine, mode: str) -> None:
    engine = profile_repository_pg
    _, request = _invalidation_seed(engine)
    other = replace(request, source="other") if mode == "conflict" else replace(request, event_id="other") if mode == "different_ids" else request
    barrier, observed = Barrier(2), []

    def observe(conn: Any, cursor: Any, statement: str, params: Any, context: Any, many: bool) -> None:
        if "FROM market_data_invalidation" in statement:
            with cursor.connection.cursor() as probe:
                probe.execute("SHOW transaction_isolation")
                observed.append(probe.fetchone()[0])

    def run(value: profile_invalidation.ConfirmedInvalidationRequest):
        barrier.wait(timeout=3)
        try:
            return profile_invalidation.ProfileInvalidationStore(sessionmaker(engine)).append_confirmed(value)
        except profile_invalidation.InvalidationConflict as error:
            return error

    event.listen(engine, "before_cursor_execute", observe)
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(run, value) for value in (request, other)]
            results = [future.result(timeout=5) for future in futures]
    finally:
        event.remove(engine, "before_cursor_execute", observe)
    assert observed and set(observed) == {"read committed"}
    successes = [r for r in results if isinstance(r, profile_invalidation.InvalidationAppendResult)]
    assert len(successes) == (1 if mode == "conflict" else 2)
    assert sum(r.already_present for r in successes) == int(mode == "same")
    assert len(_invalidation_rows(engine)) == (2 if mode == "different_ids" else 1)


def test_invalidation_unique_wait_timeout_then_release_retry(profile_repository_pg: Engine) -> None:
    engine = profile_repository_pg
    store, request = _invalidation_seed(engine)
    before = _retention_rows(engine)
    with engine.connect() as holder:
        transaction = holder.begin()
        try:
            holder.execute(text("SET LOCAL statement_timeout='2s'"))
            holder.execute(text("INSERT INTO market_data_invalidation(event_id,snapshot_id,reason_code,source) VALUES ('event',:id,'BAD_DATA','audit')"), {"id": request.snapshot_id})
            _assert_profile_lock_timeout(engine, lambda conn: profile_invalidation.ProfileInvalidationStore(
                sessionmaker(bind=conn), _WAIT_POLICY).append_confirmed(request))
            assert _invalidation_rows(engine) == [] and _retention_rows(engine) == before
        finally:
            transaction.rollback()
    assert not store.append_confirmed(request).already_present


@pytest.mark.parametrize("ack_loss", [False, True])
def test_invalidation_precommit_rollback_and_postcommit_ack_loss(profile_repository_pg: Engine, ack_loss: bool) -> None:
    engine = profile_repository_pg
    store, request = _invalidation_seed(engine)
    failure = DBAPIError("injected transaction failure", None, Exception("bounded"))

    def before_commit(session: Session) -> None:
        raise failure

    @contextmanager
    def sessions() -> Iterator[Session]:
        with Session(engine) as session:
            if not ack_loss:
                event.listen(session, "before_commit", before_commit)
            try:
                yield session
                if ack_loss:
                    raise failure
            finally:
                if not ack_loss:
                    event.remove(session, "before_commit", before_commit)

    before = _retention_rows(engine)
    with pytest.raises(DBAPIError) as caught:
        profile_invalidation.ProfileInvalidationStore(sessions).append_confirmed(request)
    assert caught.value is failure and _retention_rows(engine) == before
    rows = _invalidation_rows(engine)
    assert len(rows) == int(ack_loss)
    result = store.append_confirmed(request)
    assert result.already_present == ack_loss and len(_invalidation_rows(engine)) == 1
    if ack_loss:
        assert result.event.recorded_at == rows[0]["recorded_at"]


def test_invalidation_order_limits_read_settings_do_not_leak(profile_repository_pg: Engine) -> None:
    engine = profile_repository_pg
    _, request = _invalidation_seed(engine)
    with engine.begin() as conn:
        for name in ("a", "Z", "A"):
            conn.execute(text("INSERT INTO market_data_invalidation(event_id,snapshot_id,reason_code,source,recorded_at) "
                              "VALUES (:event,:id,'BAD','audit','2026-01-01T00:00:00Z')"), {"event": name, "id": request.snapshot_id})
    observed = []
    with engine.connect() as conn:
        settings = text("SELECT current_setting('transaction_isolation'), current_setting('transaction_read_only'), "
                        "current_setting('TimeZone'), current_setting('lock_timeout'), current_setting('statement_timeout')")
        conn.execute(text("SET SESSION TIME ZONE 'Europe/Berlin'"))
        baseline = conn.execute(settings).one()
        conn.commit()

        def observe(connection: Any, cursor: Any, statement: str, params: Any, context: Any, many: bool) -> None:
            if "FROM market_data_invalidation" in statement:
                with cursor.connection.cursor() as probe:
                    probe.execute(str(settings))
                    observed.append(probe.fetchone())

        event.listen(conn, "before_cursor_execute", observe)
        try:
            store = profile_invalidation.ProfileInvalidationStore(sessionmaker(bind=conn), _WAIT_POLICY)
            assert [item.event_id for item in store.list_for_snapshot(request.snapshot_id, limit=3)] == ["A", "Z", "a"]
            assert store.get("A") is not None
            with pytest.raises(profile_invalidation.InvalidationReadTooLarge):
                store.list_for_snapshot(request.snapshot_id, limit=2)
            assert observed and set(observed) == {("read committed", "on", "UTC", "100ms", "500ms")}
            assert conn.execute(settings).one() == baseline
        finally:
            event.remove(conn, "before_cursor_execute", observe)
            conn.rollback()
            conn.execute(text("RESET TIME ZONE"))
            conn.commit()


def _invalidation_artifacts(engine: Engine, present: bool) -> None:
    with engine.connect() as conn:
        assert (conn.execute(text("SELECT to_regclass('market_data_invalidation')")).scalar_one() is not None) == present
        assert (conn.execute(text("SELECT to_regprocedure('reject_market_data_invalidation_mutation()')")).scalar_one() is not None) == present
        count = conn.execute(text("SELECT count(*) FROM pg_trigger WHERE tgname='market_data_invalidation_append_only' AND NOT tgisinternal")).scalar_one()
        assert count == int(present)


def test_round_trip_idempotent(fresh_pg_db: str) -> None:
    """Upgrade → downgrade → upgrade twice: schema fingerprints must match."""
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        _invalidation_artifacts(engine, True)
        fp_first = _schema_fingerprint(engine)
    finally:
        engine.dispose()

    _downgrade(fresh_pg_db, "base")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        _invalidation_artifacts(engine, False)
    finally:
        engine.dispose()
    _upgrade(fresh_pg_db, "head")

    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        _invalidation_artifacts(engine, True)
        fp_second = _schema_fingerprint(engine)
    finally:
        engine.dispose()

    assert fp_first == fp_second, "Schema fingerprint diverged across round-trip"


def test_order_identity_compatible_data_survives_downgrade_and_reupgrade(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        _insert_order_identity_prerequisites(
            engine,
            exchange_id="BINANCE",
            product_id="BINANCE:BTCUSDT-PERP",
        )
        _insert_scoped_order(
            engine,
            order_id="identified",
            exchange_id="BINANCE",
            product_id="BINANCE:BTCUSDT-PERP",
            profile="ccxt:binance:live",
            account_id="ACCOUNT-A",
            client_order_id="identified-client",
            exchange_order_id="identified-exchange",
        )
        _insert_scoped_order(
            engine,
            order_id="legacy",
            exchange_id="BINANCE",
            product_id="BINANCE:BTCUSDT-PERP",
            profile=None,
            account_id=None,
            client_order_id="legacy-client",
            exchange_order_id="legacy-exchange",
        )
    finally:
        engine.dispose()

    _downgrade(fresh_pg_db, "9b7e2c4d6f10")
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        with engine.connect() as conn:
            rows = [
                tuple(row)
                for row in conn.execute(
                    text(
                        'SELECT id, account_profile, account_id FROM "order" '
                        "ORDER BY id"
                    )
                )
            ]
        assert rows == [
            ("identified", "ccxt:binance:live", "ACCOUNT-A"),
            ("legacy", None, None),
        ]
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    ("exchange_id", "product_id", "profile"),
    [
        ("BINANCE", "BINANCE:BTCUSDT-PERP", "ccxt:binance:live"),
        ("RITHMIC", "RITHMIC:MNQ-202509", "rithmic:lucid"),
    ],
)
def test_order_identity_incompatible_downgrade_keeps_scoped_indexes(
    fresh_pg_db: str,
    exchange_id: str,
    product_id: str,
    profile: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        _insert_order_identity_prerequisites(
            engine,
            exchange_id=exchange_id,
            product_id=product_id,
        )
        for suffix in ("A", "B"):
            _insert_scoped_order(
                engine,
                order_id=f"order-{suffix}",
                exchange_id=exchange_id,
                product_id=product_id,
                profile=profile,
                account_id=f"ACCOUNT-{suffix}",
                client_order_id="shared-client",
                exchange_order_id="shared-exchange",
            )
    finally:
        engine.dispose()

    with pytest.raises(RuntimeError, match="^order_identity_downgrade_collision$"):
        _downgrade(fresh_pg_db, "9b7e2c4d6f10")

    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        assert {
            "uq_order_identified_client_order_id",
            "uq_order_legacy_client_order_id",
            "uq_order_identified_exchange_order_id",
            "uq_order_legacy_exchange_order_id",
        } <= _index_names(engine, "order")
        with engine.connect() as conn:
            assert conn.execute(text('SELECT COUNT(*) FROM "order"')).scalar_one() == 2
            assert (
                conn.execute(
                    text("SELECT version_num FROM alembic_version")
                ).scalar_one()
                == "2f6c8a1e9b04"
            )
    finally:
        engine.dispose()


def _insert_research_prerequisites(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO exchange (id, name) VALUES ('RITHMIC', 'Rithmic')")
        )
        conn.execute(
            text(
                "INSERT INTO product "
                "(id, exchange_id, base_asset, quote_asset) "
                "VALUES "
                "('RITHMIC:MNQ-CONTINUOUS', 'RITHMIC', 'MNQ', 'USD')"
            )
        )


def _insert_importing_research_dataset(conn, dataset_id: str) -> None:
    conn.execute(
        text(
            """
            INSERT INTO research_dataset (
                id, product_id, timeframe, source, revision, timestamp_format,
                checksum_sha256, roll_policy, start_time, end_time, row_count,
                quality_status, lifecycle_state, sealed_at, metadata_json
            )
            VALUES (
                :dataset_id, 'RITHMIC:MNQ-CONTINUOUS', '1m',
                'rithmic-history', '2026-07-25', 'epoch_milliseconds',
                :checksum, 'vendor-front-month', 1704067200000, 1704067200000,
                1, 'validated', 'importing', NULL, '{}'
            )
            """
        ),
        {"dataset_id": dataset_id, "checksum": "0" * 64},
    )


def _insert_research_candle(conn, dataset_id: str, timestamp: int) -> None:
    conn.execute(
        text(
            """
            INSERT INTO research_candlestick (
                dataset_id, timestamp, open, high, low, close, volume,
                source_contract
            )
            VALUES (:dataset_id, :timestamp, 100, 101, 99, 100, 1, 'MNQH4')
            """
        ),
        {"dataset_id": dataset_id, "timestamp": timestamp},
    )


def test_research_dataset_postgres_seal_guards(fresh_pg_db: str) -> None:
    """Exercise the production PL/pgSQL immutability boundary."""
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    dataset_id = "postgres-seal-guards"
    try:
        _insert_research_prerequisites(engine)
        with engine.begin() as conn:
            _insert_importing_research_dataset(conn, dataset_id)
            _insert_research_candle(conn, dataset_id, 1704067200000)
            conn.execute(
                text(
                    "UPDATE research_dataset "
                    "SET lifecycle_state = 'sealed', sealed_at = NOW() "
                    "WHERE id = :dataset_id"
                ),
                {"dataset_id": dataset_id},
            )

        mutations = (
            (
                "INSERT INTO research_candlestick "
                "(dataset_id, timestamp, open, high, low, close, volume) "
                "VALUES (:dataset_id, 1704067260000, 100, 101, 99, 100, 1)",
                "sealed research dataset candles are immutable",
            ),
            (
                "UPDATE research_candlestick SET close = 100.5 "
                "WHERE dataset_id = :dataset_id",
                "sealed research dataset candles are immutable",
            ),
            (
                "DELETE FROM research_candlestick WHERE dataset_id = :dataset_id",
                "sealed research dataset candles are immutable",
            ),
            (
                "UPDATE research_dataset SET revision = 'mutated' "
                "WHERE id = :dataset_id",
                "sealed research dataset is immutable",
            ),
            (
                "DELETE FROM research_dataset WHERE id = :dataset_id",
                "sealed research dataset is immutable",
            ),
            (
                "TRUNCATE research_candlestick",
                "research dataset tables cannot be truncated",
            ),
        )
        for statement, message in mutations:
            with pytest.raises(DBAPIError, match=message):
                with engine.begin() as conn:
                    conn.execute(text(statement), {"dataset_id": dataset_id})

        with pytest.raises(
            DBAPIError,
            match="must be created in importing state",
        ):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO research_dataset (
                            id, product_id, timeframe, source, revision,
                            timestamp_format, checksum_sha256, roll_policy,
                            start_time, end_time, row_count, quality_status,
                            lifecycle_state, sealed_at, metadata_json
                        )
                        VALUES (
                            'direct-sealed', 'RITHMIC:MNQ-CONTINUOUS', '1m',
                            'rithmic-history', '2026-07-25',
                            'epoch_milliseconds', :checksum,
                            'vendor-front-month', 1704067200000, 1704067200000,
                            1, 'validated', 'sealed', NOW(), '{}'
                        )
                        """
                    ),
                    {"checksum": "0" * 64},
                )

        with engine.connect() as conn:
            state = conn.execute(
                text(
                    "SELECT lifecycle_state, COUNT(c.timestamp) "
                    "FROM research_dataset d "
                    "JOIN research_candlestick c ON c.dataset_id = d.id "
                    "WHERE d.id = :dataset_id "
                    "GROUP BY lifecycle_state"
                ),
                {"dataset_id": dataset_id},
            ).one()
        assert state == ("sealed", 1)
    finally:
        engine.dispose()


def test_research_dataset_postgres_rejects_invalid_seal_summary(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        _insert_research_prerequisites(engine)
        with engine.begin() as conn:
            _insert_importing_research_dataset(conn, "invalid-seal-summary")

        with pytest.raises(
            DBAPIError,
            match="seal summary does not match candles",
        ):
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "UPDATE research_dataset "
                        "SET lifecycle_state = 'sealed', sealed_at = NOW() "
                        "WHERE id = 'invalid-seal-summary'"
                    )
                )
    finally:
        engine.dispose()


def test_research_dataset_concurrent_import_has_one_winner(
    fresh_pg_db: str,
    tmp_path,
) -> None:
    """Two importers racing the same ID converge on one sealed dataset."""
    from sqlalchemy.orm import Session, sessionmaker

    from src.core.orm_models import ResearchDataset
    from src.core.research_datasets import (
        ResearchDatasetImporter,
        ResearchDatasetSpec,
    )

    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    _insert_research_prerequisites(engine)
    dataset_id = "postgres-concurrent-import"
    rendezvous = Barrier(2)

    class ConcurrentImportSession(Session):
        def get(self, entity, ident, **kwargs):
            result = super().get(entity, ident, **kwargs)
            if entity is ResearchDataset and ident == dataset_id and result is None:
                rendezvous.wait(timeout=10)
            return result

    factory = sessionmaker(bind=engine, class_=ConcurrentImportSession)
    csv_path = tmp_path / "concurrent.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume,source_contract\n"
        "1704067200000,100,101,99,100,1,MNQH4\n",
        encoding="utf-8",
    )
    spec = ResearchDatasetSpec(
        dataset_id=dataset_id,
        product_id="RITHMIC:MNQ-CONTINUOUS",
        timeframe="1m",
        source="rithmic-history",
        revision="2026-07-25",
        roll_policy="vendor-front-month",
    )

    def run_import():
        return ResearchDatasetImporter(session_factory=factory).import_csv(
            csv_path,
            spec,
        )

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _index: run_import(), range(2)))

        assert sorted(result.already_present for result in results) == [False, True]
        with engine.connect() as conn:
            assert conn.execute(
                text(
                    "SELECT lifecycle_state, row_count "
                    "FROM research_dataset WHERE id = :dataset_id"
                ),
                {"dataset_id": dataset_id},
            ).one() == ("sealed", 1)
            assert (
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM research_candlestick "
                        "WHERE dataset_id = :dataset_id"
                    ),
                    {"dataset_id": dataset_id},
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_research_dataset_downgrade_removes_guard_objects(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    try:
        with engine.connect() as conn:
            assert (
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM pg_proc "
                        "WHERE proname IN ("
                        "'reject_sealed_research_dataset_mutation', "
                        "'reject_sealed_research_candlestick_mutation', "
                        "'reject_research_table_truncate')"
                    )
                ).scalar_one()
                == 3
            )

        _downgrade(fresh_pg_db, "7a3c9e1b5d2f")

        with engine.connect() as conn:
            assert (
                conn.execute(
                    text(
                        "SELECT COUNT(*) FROM pg_proc "
                        "WHERE proname IN ("
                        "'reject_sealed_research_dataset_mutation', "
                        "'reject_sealed_research_candlestick_mutation', "
                        "'reject_research_table_truncate')"
                    )
                ).scalar_one()
                == 0
            )
        assert "research_dataset" not in _table_names(engine)
        assert "research_candlestick" not in _table_names(engine)
    finally:
        engine.dispose()
