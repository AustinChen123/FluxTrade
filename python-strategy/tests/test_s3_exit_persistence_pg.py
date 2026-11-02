"""PostgreSQL contract tests; destructive database creation is opt-in."""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterator
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from src.core.database_url import build_postgres_url
from src.core.live_candle_application import CandleCommitUnknown, LiveCandleApplicationService
from src.core.market_data.profiles.decision_application_store import (
    DecisionBatchConflict,
    DecisionBatchIntegrityError,
    MarketExitHandoffInput,
    append_exit_handoffs,
    read_exit_handoffs,
)
from src.core.models import Candlestick
from src.core.orm_models import MarketDataApplication, MarketExitHandoff, RithmicExitAttempt
from src.core.s3_exit_persistence import (
    AttemptClaim,
    ExitPersistenceError,
    ExitTarget,
    claim_prepared,
    create_or_link,
    mark_unresolved,
)


if os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1":
    pytest.skip("set FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS=1 to enable isolated PostgreSQL tests", allow_module_level=True)

def _url(database: str) -> str:
    url = build_postgres_url(dict(POSTGRES_USER=os.getenv("POSTGRES_USER", "fluxtrade"),
                                  POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD", "fluxtrade"),
                                  POSTGRES_HOST=os.getenv("POSTGRES_HOST", "localhost"),
                                  POSTGRES_PORT=os.getenv("POSTGRES_PORT", "5432"), POSTGRES_DB=database))
    return url.set(drivername="postgresql+psycopg2").render_as_string(hide_password=False)


@pytest.fixture
def migrated_pg() -> Iterator[tuple[sa.Engine, sessionmaker[Session]]]:
    name = f"s3prefixa_{os.getpid()}_{int(time.time() * 1000)}"
    admin = sa.create_engine(_url("postgres"), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{name}"'))
    except Exception as error:
        admin.dispose()
        pytest.skip(f"isolated PostgreSQL database service unavailable: {type(error).__name__}")
    admin.dispose()
    engine: sa.Engine | None = None
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "database"))
    cfg = Config(os.path.join(root, "alembic.ini"))
    cfg.set_main_option("sqlalchemy.url", _url(name).replace("%", "%%"))
    cfg.attributes["expected_isolated_database"] = name
    try:
        command.upgrade(cfg, "head")
        engine = sa.create_engine(_url(name))
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO exchange (id,name) VALUES ('S3TEST','S3 test')"))
            conn.execute(text("INSERT INTO product (id,exchange_id,base_asset,quote_asset) VALUES (:id,'S3TEST','MNQ','USD')"), {"id": "S3TEST:MNQ-202612"})
        yield engine, factory
    finally:
        if engine is not None:
            engine.dispose()
        cleanup = sa.create_engine(_url("postgres"), isolation_level="AUTOCOMMIT")
        with cleanup.connect() as conn:
            conn.execute(text("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=:db AND pid<>pg_backend_pid()"), {"db": name})
            conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
        cleanup.dispose()


def _payload() -> dict[str, object]:
    return {"kind": "EXIT", "strategy": "s3-test", "price": "1.25"}


def _handoff(*, timestamp: int = 1_800_000_000_000, signal_key: str = "signal-a", handoff_id: str | None = None, payload: dict[str, object] | None = None) -> MarketExitHandoffInput:
    payload = payload if payload is not None else _payload()
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    return MarketExitHandoffInput(handoff_id=handoff_id or str(uuid4()), environment="live", product_id="S3TEST:MNQ-202612",
                                  timeframe="1m", timestamp=timestamp, signal_key=signal_key, emission_ordinal=0,
                                  signal_payload=payload, signal_digest=digest, execution_scope_id="scope-a",
                                  strategy_id="strategy-a", strategy_version="1", config_hash="a" * 64,
                                  account_id="acct-a", account_profile="profile-a", cause_generation=2)


def _receipt(db: Session, timestamp: int) -> None:
    db.add(MarketDataApplication(environment="live", product_id="S3TEST:MNQ-202612", timeframe="1m", timestamp=timestamp,
                                 open=Decimal("1"), high=Decimal("2"), low=Decimal("0"), close=Decimal("1.5"),
                                 volume=Decimal("4"), decision_contract_version=None))
    db.flush()


def _candle(value: MarketExitHandoffInput) -> Candlestick:
    return Candlestick(product_id=value.product_id, timeframe=value.timeframe, timestamp=value.timestamp,
                       open=Decimal("1"), high=Decimal("2"), low=Decimal("0"), close=Decimal("1.5"), volume=Decimal("4"))


def test_receipt_and_handoffs_commit_atomically_and_duplicate_conflict_rolls_back(migrated_pg):
    _engine, factory = migrated_pg
    value = _handoff()
    duplicate_conflict = _handoff(signal_key=value.signal_key)
    with pytest.raises(DecisionBatchConflict):
        with factory() as db:
            _receipt(db, value.timestamp)
            append_exit_handoffs(db, (value, duplicate_conflict))
            db.commit()
    with factory() as db:
        assert db.get(MarketDataApplication, ("live", value.product_id, value.timeframe, value.timestamp)) is None
        assert db.query(MarketExitHandoff).count() == 0
    with factory() as db:
        _receipt(db, value.timestamp)
        assert append_exit_handoffs(db, (value,)) == (value,)
        db.commit()
    with factory() as db:
        assert db.query(MarketDataApplication).count() == 1
        assert db.query(MarketExitHandoff).count() == 1


def test_handoff_fk_and_per_signal_uniqueness_are_postgres_enforced(migrated_pg):
    engine, factory = migrated_pg
    value = _handoff()
    with factory() as db:
        db.add(MarketExitHandoff(**value.row))
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()
    with factory() as db:
        _receipt(db, value.timestamp)
        append_exit_handoffs(db, (value,))
        db.commit()
    with factory() as db:
        with pytest.raises(IntegrityError):
            with db.begin():
                db.add(MarketExitHandoff(**{**value.row, "handoff_id": str(uuid4())}))
    with factory() as db:
        with pytest.raises(DBAPIError, match="market_exit_handoff identity and signal payload are immutable"):
            with db.begin():
                db.execute(text("UPDATE market_exit_handoff SET signal_payload='{}'::jsonb WHERE handoff_id=:id"), {"id": value.handoff_id})


def test_active_target_partial_unique_index_releases_only_terminal_attempts(migrated_pg):
    engine, _factory = migrated_pg
    base = dict(account_id="acct-a", account_profile="profile-a", product_id="S3TEST:MNQ-202612", intent_key="intent-1", state="PREPARED")
    with engine.begin() as conn:
        conn.execute(sa.insert(RithmicExitAttempt).values(attempt_id=str(uuid4()), **base))
    with pytest.raises(IntegrityError):
        with engine.begin() as conn:
            conn.execute(sa.insert(RithmicExitAttempt).values(attempt_id=str(uuid4()), **{**base, "intent_key": "intent-2"}))
    with engine.begin() as conn:
        conn.execute(text("UPDATE rithmic_exit_attempt SET state='COMPLETED' WHERE intent_key='intent-1'"))
        conn.execute(sa.insert(RithmicExitAttempt).values(attempt_id=str(uuid4()), **{**base, "intent_key": "intent-3"}))


def test_create_link_active_target_claim_and_fence_are_durable(migrated_pg):
    _engine, factory = migrated_pg
    value = _handoff()
    target = ExitTarget("acct-a", "profile-a", value.product_id)
    with factory() as db:
        _receipt(db, value.timestamp)
        append_exit_handoffs(db, (value,))
        db.commit()
    with factory() as db:
        attempt_id = create_or_link(db, handoff_id=value.handoff_id, target=target, intent_key="intent-a")
    with factory() as db:
        assert db.get(MarketExitHandoff, value.handoff_id).attempt_id == attempt_id
    with factory() as db:
        claim = claim_prepared(
            db, attempt_id=attempt_id, worker_id="worker-a", expected_version=0,
            now=datetime.now(timezone.utc), lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
        )
    assert claim.fence == 1 and claim.version == 1 and claim.state == "PREPARED"
    with factory() as db:
        with pytest.raises(ExitPersistenceError, match="expected version"):
            claim_prepared(
                db, attempt_id=attempt_id, worker_id="worker-b", expected_version=0,
                now=datetime.now(timezone.utc), lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
            )
        assert db.get(RithmicExitAttempt, attempt_id).fence == 1


def test_handoff_payload_is_detached_and_every_committed_payload_matches_digest(migrated_pg):
    _engine, factory = migrated_pg
    source = {"nested": {"price": "1.25"}, "kind": "EXIT"}
    value = _handoff(payload=source)
    source["nested"]["price"] = "999"  # type: ignore[index]
    exposed_row = value.row
    exposed_row["signal_payload"]["nested"]["price"] = "888"
    value.signal_payload["nested"]["price"] = "777"
    with factory() as db:
        _receipt(db, value.timestamp)
        append_exit_handoffs(db, (value,))
        db.commit()
    with factory() as db, db.begin():
        rows = read_exit_handoffs(db, environment="live", product_id=value.product_id,
                                  timeframe=value.timeframe, bar_start_ms=value.timestamp)
        assert len(rows) == 1
        encoded = json.dumps(rows[0]["signal_payload"], sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
        assert hashlib.sha256(encoded).hexdigest() == rows[0]["signal_digest"] == value.signal_digest
        assert rows[0]["signal_payload"] == {"nested": {"price": "1.25"}, "kind": "EXIT"}


def test_handoff_read_fails_closed_when_stored_payload_digest_is_corrupt(migrated_pg):
    engine, factory = migrated_pg
    value = _handoff()
    with factory() as db:
        _receipt(db, value.timestamp)
        append_exit_handoffs(db, (value,))
        db.commit()
    with engine.begin() as conn:
        conn.execute(text("DROP TRIGGER trg_meh_identity_immutable ON market_exit_handoff"))
        conn.execute(text("UPDATE market_exit_handoff SET signal_payload='{}'::jsonb WHERE handoff_id=:id"), {"id": value.handoff_id})
    with factory() as db, db.begin():
        with pytest.raises(DecisionBatchIntegrityError):
            read_exit_handoffs(db, environment="live", product_id=value.product_id,
                               timeframe=value.timeframe, bar_start_ms=value.timestamp)


def test_claim_and_link_use_target_authority_before_attempt_lock(migrated_pg):
    """Class invariant: claim locks authority before attempt, matching create_or_link."""
    _engine, factory = migrated_pg
    value = _handoff()
    with factory() as db:
        _receipt(db, value.timestamp)
        append_exit_handoffs(db, (value,))
        db.commit()
    observed: list[str] = []
    boundaries: list[int] = []
    from sqlalchemy import event

    def capture(_conn, _cursor, statement, _parameters, _context, _many):
        if "FOR UPDATE" in statement.upper():
            if "rithmic_exit_target_authority" in statement:
                observed.append("authority")
            elif "rithmic_exit_attempt" in statement:
                observed.append("attempt")

    event.listen(_engine, "before_cursor_execute", capture)
    try:
        with factory() as db:
            attempt_id = create_or_link(db, handoff_id=value.handoff_id,
                                        target=ExitTarget("acct-a", "profile-a", value.product_id), intent_key="lock-order")
        boundaries.append(len(observed))
        with factory() as db:
            claim_prepared(db, attempt_id=attempt_id, worker_id="lock-check", expected_version=0,
                           now=datetime.now(timezone.utc), lease_until=datetime.now(timezone.utc) + timedelta(seconds=30))
        boundaries.append(len(observed))
    finally:
        event.remove(_engine, "before_cursor_execute", capture)
    assert boundaries[0] >= 2 and observed[:2] == ["authority", "attempt"]
    claim_locks = observed[boundaries[0]:boundaries[1]]
    assert claim_locks[:2] == ["authority", "attempt"]


@pytest.mark.parametrize("first_role", ["claim", "link"])
def test_two_session_create_and_claim_complete_without_deadlock(migrated_pg, first_role):
    """Exercise either legal winner without making the losing lock holder wait."""
    from sqlalchemy import event

    engine, factory = migrated_pg
    first = _handoff(signal_key="signal-a")
    second = _handoff(timestamp=1_800_000_000_001, signal_key="signal-b")
    with factory() as db:
        _receipt(db, first.timestamp)
        _receipt(db, second.timestamp)
        append_exit_handoffs(db, (first,))
        append_exit_handoffs(db, (second,))
        db.commit()
    target = ExitTarget("acct-a", "profile-a", first.product_id)
    with factory() as db:
        attempt_id = create_or_link(db, handoff_id=first.handoff_id, target=target, intent_key="concurrent-a")

    start = threading.Event()
    authority_locked = threading.Event()
    failures: list[BaseException] = []

    def after(_conn, _cursor, statement, _parameters, _context, _many):
        sql = statement.lower()
        if "rithmic_exit_target_authority" in sql and "for update" in sql:
            authority_locked.set()

    def claim():
        try:
            assert start.wait(5)
            with factory() as db:
                claim_prepared(db, attempt_id=attempt_id, worker_id="concurrent-worker", expected_version=0,
                               now=datetime.now(timezone.utc), lease_until=datetime.now(timezone.utc) + timedelta(seconds=30))
        except BaseException as error:
            failures.append(error)

    def link():
        try:
            assert start.wait(5)
            with factory() as db:
                create_or_link(db, handoff_id=second.handoff_id, target=target, intent_key="concurrent-b")
        except BaseException as error:
            failures.append(error)

    event.listen(engine, "after_cursor_execute", after)
    threads = [threading.Thread(target=claim, name="claim"), threading.Thread(target=link, name="link")]
    try:
        first = next(thread for thread in threads if thread.name == first_role)
        second_thread = next(thread for thread in threads if thread.name != first_role)
        first.start()
        start.set()
        assert authority_locked.wait(5)
        second_thread.start()
        first.join(timeout=15)
        second_thread.join(timeout=15)
    finally:
        event.remove(engine, "after_cursor_execute", after)
    assert all(not thread.is_alive() for thread in threads)
    assert failures == []
    with factory() as db:
        assert db.get(MarketExitHandoff, second.handoff_id).attempt_id == attempt_id
        assert db.get(RithmicExitAttempt, attempt_id).fence == 1


@pytest.mark.parametrize("advance_to_unresolved", [False, True])
def test_claim_discovery_revalidates_committed_concurrent_change(migrated_pg, advance_to_unresolved):
    """A discovery read cannot authorize a claim after another session commits."""
    from sqlalchemy import event

    engine, factory = migrated_pg
    value = _handoff()
    with factory() as db:
        _receipt(db, value.timestamp)
        append_exit_handoffs(db, (value,))
        db.commit()
    with factory() as db:
        attempt_id = create_or_link(db, handoff_id=value.handoff_id,
                                    target=ExitTarget("acct-a", "profile-a", value.product_id), intent_key="race")

    injected = False

    def commit_competitor(_conn, _cursor, statement, _parameters, _context, _many):
        nonlocal injected
        sql = statement.lower()
        if injected or "from rithmic_exit_attempt" not in sql or "for update" in sql:
            return
        injected = True
        with factory() as winner:
            claim = claim_prepared(winner, attempt_id=attempt_id, worker_id="winner", expected_version=0,
                                   now=datetime.now(timezone.utc),
                                   lease_until=datetime.now(timezone.utc) + timedelta(seconds=30))
        if advance_to_unresolved:
            with factory() as resolver:
                mark_unresolved(resolver, attempt_id=attempt_id, owner_id="winner", fence=claim.fence,
                                expected_version=claim.version, detail="concurrent resolution",
                                next_action_at=datetime.now(timezone.utc) + timedelta(minutes=1))

    event.listen(engine, "after_cursor_execute", commit_competitor)
    try:
        with factory() as stale_reader:
            with pytest.raises(ExitPersistenceError):
                claim_prepared(stale_reader, attempt_id=attempt_id, worker_id="stale-reader", expected_version=0,
                               now=datetime.now(timezone.utc),
                               lease_until=datetime.now(timezone.utc) + timedelta(seconds=30))
    finally:
        event.remove(engine, "after_cursor_execute", commit_competitor)
    assert injected
    with factory() as db:
        row = db.get(RithmicExitAttempt, attempt_id)
        assert row.owner_id == "winner"
        assert (row.fence, row.version) == ((1, 2) if advance_to_unresolved else (1, 1))
        assert row.state == ("UNRESOLVED" if advance_to_unresolved else "PREPARED")


def test_concurrent_same_version_claims_advance_fence_and_version_once(migrated_pg):
    """Under concurrent claims, the durable fence/version are monotonic and never duplicate."""
    engine, factory = migrated_pg
    value = _handoff()
    with factory() as db:
        _receipt(db, value.timestamp)
        append_exit_handoffs(db, (value,))
        db.commit()
    with factory() as db:
        attempt_id = create_or_link(db, handoff_id=value.handoff_id,
                                    target=ExitTarget("acct-a", "profile-a", value.product_id), intent_key="cas-invariant")
    barrier = threading.Barrier(3)
    successes: list[AttemptClaim] = []
    rejected: list[BaseException] = []

    def contender(worker: str) -> None:
        try:
            barrier.wait()
            with factory() as db:
                successes.append(claim_prepared(db, attempt_id=attempt_id, worker_id=worker, expected_version=0,
                                                now=datetime.now(timezone.utc),
                                                lease_until=datetime.now(timezone.utc) + timedelta(seconds=30)))
        except ExitPersistenceError as error:
            rejected.append(error)

    threads = [threading.Thread(target=contender, args=(name,)) for name in ("worker-a", "worker-b")]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=15)
    assert all(not thread.is_alive() for thread in threads)
    assert (len(successes), len(rejected)) == (1, 1)
    with factory() as db:
        row = db.get(RithmicExitAttempt, attempt_id)
        assert row.version == row.fence == 1
        assert row.owner_id == successes[0].owner_id


def test_shared_handoff_retry_returns_existing_attempt_even_after_terminal(migrated_pg):
    _engine, factory = migrated_pg
    first, shared = _handoff(signal_key="signal-a"), _handoff(timestamp=1_800_000_000_001, signal_key="signal-b")
    with factory() as db:
        _receipt(db, first.timestamp)
        _receipt(db, shared.timestamp)
        append_exit_handoffs(db, (first,))
        append_exit_handoffs(db, (shared,))
        db.commit()
    target = ExitTarget("acct-a", "profile-a", first.product_id)
    with factory() as db:
        attempt_id = create_or_link(db, handoff_id=first.handoff_id, target=target, intent_key="intent-a")
    with factory() as db:
        assert create_or_link(db, handoff_id=shared.handoff_id, target=target, intent_key="intent-b") == attempt_id
    with factory() as db, db.begin():
        db.execute(text("UPDATE rithmic_exit_attempt SET state='COMPLETED', next_action_at=NULL WHERE attempt_id=:id"), {"id": attempt_id})
    with factory() as db:
        assert create_or_link(db, handoff_id=shared.handoff_id, target=target, intent_key="intent-b") == attempt_id
    with factory() as db:
        assert db.query(RithmicExitAttempt).count() == 1
        assert db.get(MarketExitHandoff, shared.handoff_id).attempt_id == attempt_id


@pytest.mark.parametrize("table", ["market_exit_handoff", "rithmic_exit_attempt", "rithmic_exit_target_authority"])
def test_downgrade_refuses_each_nonempty_s3_table_and_preserves_schema(migrated_pg, table):
    engine, factory = migrated_pg
    value = _handoff()
    if table == "market_exit_handoff":
        with factory() as db:
            _receipt(db, value.timestamp)
            append_exit_handoffs(db, (value,))
            db.commit()
    elif table == "rithmic_exit_attempt":
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO rithmic_exit_attempt (attempt_id,account_id,account_profile,product_id,intent_key,state) VALUES (:id,'acct-a','profile-a',:product,'downgrade-attempt','PREPARED')"), {"id": str(uuid4()), "product": value.product_id})
    else:
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO rithmic_exit_target_authority (account_id,account_profile,product_id,dispatch_epoch,process_instance_id) VALUES ('acct-a','profile-a',:product,42,'process-a')"), {"product": value.product_id})
    cfg = Config(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "database", "alembic.ini")))
    cfg.set_main_option("sqlalchemy.url", engine.url.render_as_string(hide_password=False).replace("%", "%%"))
    cfg.attributes["expected_isolated_database"] = engine.url.database
    with pytest.raises(Exception, match="Cannot downgrade while S3 exit persistence rows exist"):
        command.downgrade(cfg, "6d8a1e2f4c90")
    with engine.connect() as conn:
        assert conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one() == 1
        assert conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "71d3a8c4e205"


def test_empty_s3_downgrade_succeeds(migrated_pg):
    engine, _factory = migrated_pg
    cfg = Config(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "database", "alembic.ini")))
    cfg.set_main_option("sqlalchemy.url", engine.url.render_as_string(hide_password=False).replace("%", "%%"))
    cfg.attributes["expected_isolated_database"] = engine.url.database
    command.downgrade(cfg, "6d8a1e2f4c90")
    with engine.connect() as conn:
        assert conn.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "6d8a1e2f4c90"
        assert conn.execute(text("SELECT to_regclass('rithmic_exit_target_authority')")).scalar_one() is None


def test_audit_insert_failure_rolls_back_attempt_and_handoff_link(migrated_pg):
    engine, factory = migrated_pg
    value = _handoff()
    with factory() as db:
        _receipt(db, value.timestamp)
        append_exit_handoffs(db, (value,))
        db.commit()
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE FUNCTION s3_fail_audit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.event_subtype = 'rithmic_exit_attempt_linked' THEN RAISE EXCEPTION 'audit unavailable'; END IF; RETURN NEW; END $$
        """))
        conn.execute(text("CREATE TRIGGER s3_fail_audit BEFORE INSERT ON system_events FOR EACH ROW EXECUTE FUNCTION s3_fail_audit()"))
    with factory() as db:
        with pytest.raises(ExitPersistenceError, match="failed to create or link"):
            create_or_link(db, handoff_id=value.handoff_id, target=ExitTarget("acct-a", "profile-a", value.product_id), intent_key="audit-fail")
    with factory() as db:
        assert db.query(RithmicExitAttempt).count() == 0
        assert db.get(MarketExitHandoff, value.handoff_id).attempt_id is None
        assert db.execute(text("SELECT count(*) FROM rithmic_exit_target_authority")).scalar_one() == 0


def test_commit_ack_loss_reconstructs_committed_handoffs_from_fresh_session(migrated_pg):
    _engine, factory = migrated_pg
    value = _handoff()
    calls = 0

    @contextmanager
    def ambiguous_factory():
        nonlocal calls
        db = factory()
        calls += 1
        if calls == 1:
            original_commit = db.commit

            def commit_then_disconnect():
                original_commit()
                raise OSError("simulated lost commit acknowledgement")

            db.commit = commit_then_disconnect  # type: ignore[method-assign]
        try:
            yield db
        finally:
            db.close()

    service = LiveCandleApplicationService(environment_identity=lambda: "live", db_session_factory=ambiguous_factory)
    service._persist(_candle(value), handoffs=(value,))
    with factory() as db:
        assert db.query(MarketDataApplication).count() == 1
        assert db.query(MarketExitHandoff).count() == 1
    assert calls == 2


def test_commit_failure_with_authoritative_absence_allows_replay(migrated_pg):
    _engine, factory = migrated_pg
    value = _handoff()

    @contextmanager
    def rolled_back_factory():
        db = factory()

        def rollback_then_disconnect():
            db.rollback()
            raise OSError("simulated pre-commit connection failure")

        db.commit = rollback_then_disconnect  # type: ignore[method-assign]
        try:
            yield db
        finally:
            db.close()

    service = LiveCandleApplicationService(environment_identity=lambda: "live", db_session_factory=rolled_back_factory)
    with pytest.raises(OSError, match="pre-commit"):
        service._persist(_candle(value), handoffs=(value,))
    with factory() as db:
        assert db.query(MarketDataApplication).count() == 0
        assert db.query(MarketExitHandoff).count() == 0


def test_commit_authority_unavailable_surfaces_commit_unknown(migrated_pg):
    _engine, factory = migrated_pg
    value = _handoff()
    calls = 0

    @contextmanager
    def unavailable_verify_factory():
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("verification database unavailable")
        db = factory()
        original_commit = db.commit

        def commit_then_disconnect():
            original_commit()
            raise OSError("simulated lost commit acknowledgement")

        db.commit = commit_then_disconnect  # type: ignore[method-assign]
        try:
            yield db
        finally:
            db.close()

    service = LiveCandleApplicationService(environment_identity=lambda: "live", db_session_factory=unavailable_verify_factory)
    with pytest.raises(CandleCommitUnknown):
        service._persist(_candle(value), handoffs=(value,))
