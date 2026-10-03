"""Committed PostgreSQL contract for linked sealed-source candle reads."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool

from src.control_plane.backtest_result_http_contract import (
    BacktestResultQuery,
    CandlesCursor,
    CandlesCursorBinding,
    encode_result_cursor,
    format_utc_milliseconds,
    verify_result_cursor,
    wire_decimal,
)
from src.core.backtest_result_owner import BacktestResultPersistenceOwner
from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.core.orm_models import ResearchDataset
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec
from test_backtest_result_owner_persistence import (
    _START,
    _database,
    _identity,
    _outcome,
)
from test_migrations import fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db
_KEY = bytes(range(32))
_OTHER_KEY = bytes(reversed(range(32)))
_PRODUCT = "RITHMIC:MNQ-CONTINUOUS"


def _sealed_source(
    factory: sessionmaker[Session],
    tmp_path,
    timeframe: str,
    offsets: list[int],
    *,
    product_id: str = _PRODUCT,
) -> str:
    dataset_id = "candles-source"
    csv_path = tmp_path / "candles.csv"
    continuous = product_id == _PRODUCT
    header = "timestamp,open,high,low,close,volume"
    if continuous:
        header += ",source_contract"
    lines = [header]
    for index, offset in enumerate(offsets):
        opening = Decimal("100.123456789012345678901") + index
        high = opening + Decimal("1.25")
        low = opening - Decimal("0.75")
        close = opening + Decimal("0.125")
        volume = Decimal("0.000000000000000000123456789") + index
        row = f"{_START + offset},{opening},{high},{low},{close},{volume}"
        lines.append(f"{row},MNQH4" if continuous else row)
    csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    ResearchDatasetImporter(session_factory=factory).import_csv(
        csv_path,
        ResearchDatasetSpec(
            dataset_id=dataset_id,
            product_id=product_id,
            timeframe=timeframe,
            source="p1ec-candles-test",
            revision="v1",
            roll_policy="vendor-front-month" if continuous else None,
        ),
    )
    return dataset_id


def _persist_result(
    factory: sessionmaker[Session], dataset_id: str, job_id: str = "candles-job"
):
    return BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_999_999_999
    ).persist(_identity(job_id=job_id, dataset_id=dataset_id), _outcome())


def _expected_item(offset: int, index: int) -> dict[str, str]:
    opening = Decimal("100.123456789012345678901") + index
    return {
        "timestamp": format_utc_milliseconds(_START + offset),
        "open": wire_decimal(opening),
        "high": wire_decimal(opening + Decimal("1.25")),
        "low": wire_decimal(opening - Decimal("0.75")),
        "close": wire_decimal(opening + Decimal("0.125")),
        "volume": wire_decimal(Decimal("0.000000000000000000123456789") + index),
    }


def test_candles_half_open_source_timeframe_paging_and_receipt_values(
    fresh_pg_db: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.control_plane.backtest_results import BacktestResultsQueryService

    engine, factory = _database(fresh_pg_db)
    dataset_id = _sealed_source(factory, tmp_path, "30s", [0, 30_000, 90_000])
    receipt = _persist_result(factory, dataset_id)
    source_calls: list[tuple[int, int]] = []
    original = ResearchDatabaseDataSource.get_candles

    def track_source(source, product_id, timeframe, start, end):
        source_calls.append((start, end))
        yield from original(source, product_id, timeframe, start, end)

    monkeypatch.setattr(ResearchDatabaseDataSource, "get_candles", track_source)
    service = BacktestResultsQueryService(factory, cursor_key=_KEY)
    query = BacktestResultQuery(start_ms=_START, end_ms=_START + 120_000, limit=2)

    first = service.list_candles("candles-job", query)
    repeated = service.list_candles("candles-job", query)
    assert first == repeated
    assert first["revision"] == 1
    assert first["items"] == [_expected_item(0, 0), _expected_item(30_000, 1)]
    assert first["next_cursor"] is not None
    cursor = verify_result_cursor(
        _KEY,
        first["next_cursor"],
        CandlesCursorBinding(
            "candles-job", receipt.result_digest, _START, _START + 120_000
        ),
    )
    assert cursor == CandlesCursor(
        "candles-job", receipt.result_digest, _START, _START + 120_000, _START + 30_000
    )

    tail = service.list_candles(
        "candles-job",
        BacktestResultQuery(
            start_ms=_START,
            end_ms=_START + 120_000,
            limit=3,
            cursor=first["next_cursor"],
        ),
    )
    assert tail == {
        "items": [_expected_item(90_000, 2)],
        "next_cursor": None,
        "revision": 1,
    }
    empty = service.list_candles(
        "candles-job",
        BacktestResultQuery(
            start_ms=_START + 60_000,
            end_ms=_START + 90_000,
        ),
    )
    assert empty == {"items": [], "next_cursor": None, "revision": 1}
    assert cast(QueuePool, engine.pool).checkedout() == 0
    from src.control_plane.backtest_result_http_contract import (
        InvalidBacktestResultHttpRequest,
    )

    for start, end in ((_START - 1, _START + 1), (_START, _START + 120_001)):
        with pytest.raises(InvalidBacktestResultHttpRequest):
            service.list_candles(
                "candles-job", BacktestResultQuery(start_ms=start, end_ms=end)
            )
    assert len(source_calls) == 4
    assert cast(QueuePool, engine.pool).checkedout() == 0
    engine.dispose()


def test_candles_invalid_cursor_rejected_before_source_queries(
    fresh_pg_db: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.control_plane.backtest_result_http_contract import (
        InvalidBacktestResultHttpRequest,
        TradesCursor,
    )
    from src.control_plane.backtest_results import BacktestResultsQueryService

    engine, factory = _database(fresh_pg_db)
    dataset_id = _sealed_source(factory, tmp_path, "1m", [0, 60_000])
    receipt = _persist_result(factory, dataset_id)
    calls: list[str] = []
    original_metadata = ResearchDatabaseDataSource.get_dataset_metadata
    original_candles = ResearchDatabaseDataSource.get_candles

    def count_metadata(source):
        calls.append("metadata")
        return original_metadata(source)

    def count_candles(source, product_id, timeframe, start, end):
        calls.append("candles")
        yield from original_candles(source, product_id, timeframe, start, end)

    monkeypatch.setattr(
        ResearchDatabaseDataSource, "get_dataset_metadata", count_metadata
    )
    monkeypatch.setattr(ResearchDatabaseDataSource, "get_candles", count_candles)
    start, end = _START, _START + 120_000
    valid_fields = ("candles-job", receipt.result_digest, start, end, _START)
    invalid_tokens = (
        encode_result_cursor(_OTHER_KEY, CandlesCursor(*valid_fields)),
        encode_result_cursor(
            _KEY,
            CandlesCursor("foreign-job", receipt.result_digest, start, end, _START),
        ),
        encode_result_cursor(
            _KEY, CandlesCursor("candles-job", "a" * 64, start, end, _START)
        ),
        encode_result_cursor(
            _KEY,
            CandlesCursor(
                "candles-job", receipt.result_digest, start + 1, end, start + 1
            ),
        ),
        encode_result_cursor(
            _KEY, TradesCursor("candles-job", receipt.result_digest, 0)
        ),
    )
    service = BacktestResultsQueryService(factory, cursor_key=_KEY)
    for token in invalid_tokens:
        with pytest.raises(InvalidBacktestResultHttpRequest):
            service.list_candles(
                "candles-job",
                BacktestResultQuery(start_ms=start, end_ms=end, cursor=token),
            )
    assert calls == []
    engine.dispose()


def test_candles_count_actual_rows_for_10000_bound_and_close_source(
    fresh_pg_db: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.control_plane.backtest_results import (
        BacktestResultsReadUnavailable,
        BacktestResultsQueryService,
    )
    from src.control_plane.backtest_result_http_contract import (
        InvalidBacktestResultHttpRequest,
    )

    engine, factory = _database(fresh_pg_db)
    # The source is 1m, but has a candle every 2m: a duration heuristic would
    # count nearly twice as many intervals as the actual stored rows.
    offsets = [index * 120_000 for index in range(10_001)]
    dataset_id = _sealed_source(factory, tmp_path, "1m", offsets)
    _persist_result(factory, dataset_id)
    service = BacktestResultsQueryService(factory, cursor_key=_KEY)
    exact_bound = service.list_candles(
        "candles-job",
        BacktestResultQuery(
            start_ms=_START,
            end_ms=_START + 9_999 * 120_000 + 60_000,
        ),
    )
    assert len(exact_bound["items"]) == 100
    assert exact_bound["next_cursor"] is not None

    with pytest.raises(InvalidBacktestResultHttpRequest):
        service.list_candles(
            "candles-job",
            BacktestResultQuery(
                start_ms=_START,
                end_ms=_START + 10_000 * 120_000 + 60_000,
            ),
        )
    assert cast(QueuePool, engine.pool).checkedout() == 0

    original = ResearchDatabaseDataSource.get_candles
    closed: list[bool] = []

    def broken_source(source, product_id, timeframe, start, end):
        generator = original(source, product_id, timeframe, start, end)
        try:
            yield next(generator)
            raise RuntimeError("source failure details must be hidden")
        finally:
            generator.close()
            closed.append(True)

    monkeypatch.setattr(ResearchDatabaseDataSource, "get_candles", broken_source)
    with pytest.raises(BacktestResultsReadUnavailable) as error:
        service.list_candles(
            "candles-job",
            BacktestResultQuery(start_ms=_START, end_ms=_START + 120_000),
        )
    assert error.value.__cause__ is None
    assert str(error.value) == "browser_result_backend_unavailable"
    assert closed == [True]
    assert cast(QueuePool, engine.pool).checkedout() == 0
    engine.dispose()


def test_candles_invalid_linked_source_uses_fixed_backend_failure(
    fresh_pg_db: str, tmp_path
) -> None:
    from src.control_plane.backtest_results import (
        BacktestResultsQueryService,
        BacktestResultsReadUnavailable,
    )

    engine, factory = _database(fresh_pg_db)
    _sealed_source(
        factory,
        tmp_path,
        "1m",
        [0, 60_000],
        product_id="BINANCE:BTCUSDT-PERP",
    )
    _persist_result(factory, "candles-source")
    service = BacktestResultsQueryService(factory, cursor_key=_KEY)
    with pytest.raises(BacktestResultsReadUnavailable) as wrong_product:
        service.list_candles(
            "candles-job",
            BacktestResultQuery(start_ms=_START, end_ms=_START + 120_000),
        )
    assert str(wrong_product.value) == "browser_result_backend_unavailable"
    assert wrong_product.value.__cause__ is None

    assert cast(QueuePool, engine.pool).checkedout() == 0
    engine.dispose()


def test_candles_missing_linked_source_uses_fixed_backend_failure(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import (
        BacktestResultsQueryService,
        BacktestResultsReadUnavailable,
    )

    engine, factory = _database(fresh_pg_db)
    # BacktestResultSummary.dataset_id has an existing FK to research_dataset,
    # so a truly absent linked source cannot be persisted; use importing state.
    with factory() as session:
        session.add(
            ResearchDataset(
                id="unsealed-source",
                product_id=_PRODUCT,
                timeframe="1m",
                source="p1ec-candles-test",
                revision="v1",
                timestamp_format="epoch_milliseconds",
                checksum_sha256="a" * 64,
                roll_policy="vendor-front-month",
                start_time=_START,
                end_time=_START,
                row_count=1,
                quality_status="validated",
                lifecycle_state="importing",
                sealed_at=None,
                metadata_json="{}",
            )
        )
        session.commit()
    _persist_result(factory, "unsealed-source")
    service = BacktestResultsQueryService(factory, cursor_key=_KEY)
    with pytest.raises(BacktestResultsReadUnavailable) as missing:
        service.list_candles(
            "candles-job",
            BacktestResultQuery(start_ms=_START, end_ms=_START + 1),
        )
    assert str(missing.value) == "browser_result_backend_unavailable"
    assert missing.value.__cause__ is None
    assert cast(QueuePool, engine.pool).checkedout() == 0
    engine.dispose()


def test_candles_reject_invalid_request_before_sql_and_absent_result_before_cursor(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_result_http_contract import (
        InvalidBacktestResultHttpRequest,
    )
    from src.control_plane.backtest_results import (
        BacktestResultsNotFound,
        BacktestResultsQueryService,
        BacktestResultsUnavailable,
    )

    calls: list[None] = []

    def forbidden_session():
        calls.append(None)
        raise AssertionError("request rejection must precede SQL")

    service = BacktestResultsQueryService(forbidden_session, cursor_key=_KEY)
    with pytest.raises(InvalidBacktestResultHttpRequest):
        service.list_candles(
            "../bad", BacktestResultQuery(start_ms=_START, end_ms=_START + 1)
        )
    with pytest.raises(InvalidBacktestResultHttpRequest):
        service.list_candles(
            "valid-job", BacktestResultQuery(start_ms=_START, end_ms=_START)
        )
    with pytest.raises(InvalidBacktestResultHttpRequest):
        service.list_candles(
            "valid-job",
            BacktestResultQuery(start_ms=_START, end_ms=_START + 1, limit=0),
        )
    assert calls == []

    engine, factory = _database(fresh_pg_db)
    absent = BacktestResultsQueryService(
        factory, cursor_key=_KEY, job_lookup=lambda _job_id: object()
    )
    invalid_cursor = encode_result_cursor(
        _KEY, CandlesCursor("valid-job", "a" * 64, _START, _START + 1, _START)
    )
    with pytest.raises(BacktestResultsUnavailable):
        absent.list_candles(
            "valid-job",
            BacktestResultQuery(
                start_ms=_START, end_ms=_START + 1, cursor=invalid_cursor
            ),
        )
    missing = BacktestResultsQueryService(factory, cursor_key=_KEY)
    with pytest.raises(BacktestResultsNotFound):
        missing.list_candles(
            "valid-job",
            BacktestResultQuery(
                start_ms=_START, end_ms=_START + 1, cursor=invalid_cursor
            ),
        )
    engine.dispose()
