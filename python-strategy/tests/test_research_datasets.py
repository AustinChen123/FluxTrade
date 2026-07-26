from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.core.orm_models import (
    Base,
    Exchange,
    Product,
    ResearchCandlestick,
    ResearchDataset,
)
from src.core.research_datasets import (
    ResearchCsvValidationError,
    ResearchDatasetConflictError,
    ResearchDatasetImporter,
    ResearchDatasetIntegrityError,
    ResearchDatasetSpec,
)


PRODUCT_ID = "RITHMIC:MNQ-CONTINUOUS"
TIMEFRAME = "1m"


@pytest.fixture
def session_factory():
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(
        engine,
        tables=[
            Exchange.__table__,
            Product.__table__,
            ResearchDataset.__table__,
            ResearchCandlestick.__table__,
        ],
    )
    with engine.begin() as connection:
        for statement in (
            """
            CREATE TRIGGER trg_test_research_dataset_insert_importing
            BEFORE INSERT ON research_dataset
            WHEN NEW.lifecycle_state <> 'importing' OR NEW.sealed_at IS NOT NULL
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'research dataset must be created in importing state'
                );
            END
            """,
            """
            CREATE TRIGGER trg_test_research_dataset_update_immutable
            BEFORE UPDATE ON research_dataset
            WHEN OLD.lifecycle_state = 'sealed'
            BEGIN
                SELECT RAISE(ABORT, 'sealed research dataset is immutable');
            END
            """,
            """
            CREATE TRIGGER trg_test_research_dataset_seal_summary
            BEFORE UPDATE ON research_dataset
            WHEN NEW.lifecycle_state = 'sealed'
              AND (
                  (SELECT COUNT(*) FROM research_candlestick
                   WHERE dataset_id = NEW.id) <> NEW.row_count
                  OR (SELECT MIN(timestamp) FROM research_candlestick
                      WHERE dataset_id = NEW.id) IS NOT NEW.start_time
                  OR (SELECT MAX(timestamp) FROM research_candlestick
                      WHERE dataset_id = NEW.id) IS NOT NEW.end_time
              )
            BEGIN
                SELECT RAISE(
                    ABORT,
                    'research dataset seal summary does not match candles'
                );
            END
            """,
            """
            CREATE TRIGGER trg_test_research_dataset_delete_immutable
            BEFORE DELETE ON research_dataset
            WHEN OLD.lifecycle_state = 'sealed'
            BEGIN
                SELECT RAISE(ABORT, 'sealed research dataset is immutable');
            END
            """,
            """
            CREATE TRIGGER trg_test_research_candle_insert_immutable
            BEFORE INSERT ON research_candlestick
            WHEN EXISTS (
                SELECT 1 FROM research_dataset
                WHERE id = NEW.dataset_id AND lifecycle_state = 'sealed'
            )
            BEGIN
                SELECT RAISE(ABORT, 'sealed research dataset candles are immutable');
            END
            """,
            """
            CREATE TRIGGER trg_test_research_candle_update_immutable
            BEFORE UPDATE ON research_candlestick
            WHEN EXISTS (
                SELECT 1 FROM research_dataset
                WHERE id IN (OLD.dataset_id, NEW.dataset_id)
                  AND lifecycle_state = 'sealed'
            )
            BEGIN
                SELECT RAISE(ABORT, 'sealed research dataset candles are immutable');
            END
            """,
            """
            CREATE TRIGGER trg_test_research_candle_delete_immutable
            BEFORE DELETE ON research_candlestick
            WHEN EXISTS (
                SELECT 1 FROM research_dataset
                WHERE id = OLD.dataset_id AND lifecycle_state = 'sealed'
            )
            BEGIN
                SELECT RAISE(ABORT, 'sealed research dataset candles are immutable');
            END
            """,
        ):
            connection.execute(text(statement))
    factory = sessionmaker(bind=engine)
    return factory


@pytest.fixture
def dataset_spec() -> ResearchDatasetSpec:
    return ResearchDatasetSpec(
        dataset_id="mnq-vendor-roll-2026-07",
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        source="rithmic-history",
        revision="2026-07-25",
        roll_policy="vendor-front-month",
        metadata={"calendar": "CME-equity-index"},
    )


def _write_csv(
    path,
    rows: list[str],
    *,
    include_source_contract: bool = True,
) -> None:
    header = "timestamp,open,high,low,close,volume"
    if include_source_contract:
        header += ",source_contract"
        rows = [f"{row},MNQH4" for row in rows]
    path.write_text(
        header + "\n" + "\n".join(rows),
        encoding="utf-8",
    )


def test_import_is_transactional_and_readable(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(
        path,
        [
            "1704067200000,100.10,101.20,99.50,100.80,12",
            "1704067260000,100.80,102.00,100.50,101.75,15.5",
        ],
    )

    result = ResearchDatasetImporter(
        session_factory=session_factory,
        batch_size=1,
    ).import_csv(path, dataset_spec)

    assert result.already_present is False
    assert result.row_count == 2
    with session_factory() as session:
        dataset = session.get(ResearchDataset, dataset_spec.dataset_id)
        assert dataset is not None
        assert dataset.source == "rithmic-history"
        assert dataset.roll_policy == "vendor-front-month"
        assert dataset.start_time == 1704067200000
        assert dataset.end_time == 1704067260000
        assert len(dataset.checksum_sha256) == 64
        assert dataset.lifecycle_state == "sealed"
        assert dataset.sealed_at is not None
        product = session.get(Product, PRODUCT_ID)
        exchange = session.get(Exchange, "RITHMIC")
        assert product is not None
        assert product.exchange_id == "RITHMIC"
        assert product.base_asset == "MNQ"
        assert product.quote_asset == "USD"
        assert exchange is not None
        assert exchange.name == "Rithmic"

    source = ResearchDatabaseDataSource(
        dataset_spec.dataset_id,
        session_factory=session_factory,
    )
    candles = list(
        source.get_candles(
            PRODUCT_ID,
            TIMEFRAME,
            1704067200000,
            1704067260000,
        )
    )
    assert [c.timestamp for c in candles] == [1704067200000, 1704067260000]
    assert candles[1].close == Decimal("101.75")
    assert source.get_available_range(PRODUCT_ID, TIMEFRAME) == (
        1704067200000,
        1704067260000,
    )
    assert source.validate() is True


def test_reimport_same_normalized_content_is_idempotent(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["1704067200000,100.10,101.20,99.50,100.80,12.0"])
    importer = ResearchDatasetImporter(session_factory=session_factory)
    first = importer.import_csv(path, dataset_spec)

    _write_csv(path, ["1704067200000,100.100,101.2,99.500,100.800,12"])
    second = importer.import_csv(path, dataset_spec)

    assert first.checksum_sha256 == second.checksum_sha256
    assert second.already_present is True
    with session_factory() as session:
        assert session.query(ResearchDataset).count() == 1
        assert session.query(ResearchCandlestick).count() == 1


def test_checksum_preserves_decimal_digits_beyond_context_precision(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(
        path,
        [
            "1704067200000,"
            "12345678901234567890123456781,"
            "12345678901234567890123456790,"
            "12345678901234567890123456780,"
            "12345678901234567890123456781,12"
        ],
    )
    importer = ResearchDatasetImporter(session_factory=session_factory)
    importer.import_csv(path, dataset_spec)
    _write_csv(
        path,
        [
            "1704067200000,"
            "12345678901234567890123456782,"
            "12345678901234567890123456790,"
            "12345678901234567890123456780,"
            "12345678901234567890123456782,12"
        ],
    )

    with pytest.raises(ResearchDatasetConflictError):
        importer.import_csv(path, dataset_spec)


def test_existing_dataset_id_rejects_changed_content(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["1704067200000,100,101,99,100,12"])
    importer = ResearchDatasetImporter(session_factory=session_factory)
    importer.import_csv(path, dataset_spec)
    _write_csv(path, ["1704067200000,100,102,99,101,12"])

    with pytest.raises(
        ResearchDatasetConflictError,
        match="different content or provenance",
    ):
        importer.import_csv(path, dataset_spec)


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (
            [
                "1704067200000,100,101,99,100,12",
                "1704067200000,100,101,99,100,12",
            ],
            "strictly increasing",
        ),
        (["1704067200000,100,99,98,100,12"], "high is below"),
        (["1704067200000,100,101,99,100,-1"], "must not be negative"),
        (["1704067200000,NaN,101,99,100,1"], "must be finite"),
        (["-1,100,101,99,100,1"], "outside the supported range"),
        (
            ["9223372036854775808,100,101,99,100,1"],
            "outside the supported range",
        ),
        (["1704067200000,100,101"], "invalid low"),
    ],
)
def test_invalid_csv_leaves_no_partial_dataset(
    tmp_path,
    session_factory,
    dataset_spec,
    rows,
    message,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, rows)

    with pytest.raises(ResearchCsvValidationError, match=message):
        ResearchDatasetImporter(session_factory=session_factory).import_csv(
            path,
            dataset_spec,
        )

    with session_factory() as session:
        assert session.query(ResearchDataset).count() == 0
        assert session.query(ResearchCandlestick).count() == 0


def test_source_is_bound_to_dataset_product_and_timeframe(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["1704067200000,100,101,99,100,12"])
    ResearchDatasetImporter(session_factory=session_factory).import_csv(
        path,
        dataset_spec,
    )
    source = ResearchDatabaseDataSource(
        dataset_spec.dataset_id,
        session_factory=session_factory,
    )

    assert list(
        source.get_candles(
            "RITHMIC:NQ-202609",
            TIMEFRAME,
            0,
            9_999_999_999_999,
        )
    ) == []
    assert source.get_candles_df(
        PRODUCT_ID,
        "5m",
        0,
        9_999_999_999_999,
    ).empty
    assert source.get_available_range("RITHMIC:NQ-202609", TIMEFRAME) is None


def test_rolled_dataset_rejects_executable_contract_identity():
    with pytest.raises(
        ValueError,
        match="rolled datasets require an EXCHANGE:ROOT-CONTINUOUS product_id",
    ):
        ResearchDatasetSpec(
            dataset_id="misidentified-roll",
            product_id="RITHMIC:MNQ-202609",
            timeframe=TIMEFRAME,
            source="rithmic-history",
            revision="2026-07-25",
            roll_policy="vendor-front-month",
        )


def test_continuous_dataset_requires_explicit_roll_policy():
    with pytest.raises(
        ValueError,
        match="continuous datasets require an explicit roll_policy",
    ):
        ResearchDatasetSpec(
            dataset_id="ambiguous-continuous",
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            source="rithmic-history",
            revision="2026-07-25",
        )


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        (
            ["1704067200000,100,101,99,100,12"],
            "must include a source_contract column",
        ),
        (
            ["1704067200000,100,101,99,100,12,"],
            "source_contract is required",
        ),
    ],
)
def test_rolled_dataset_requires_source_contract_for_every_row(
    tmp_path,
    session_factory,
    dataset_spec,
    rows,
    message,
):
    path = tmp_path / "candles.csv"
    if rows[0].endswith(","):
        path.write_text(
            "timestamp,open,high,low,close,volume,source_contract\n"
            + "\n".join(rows),
            encoding="utf-8",
        )
    else:
        _write_csv(path, rows, include_source_contract=False)

    with pytest.raises(ResearchCsvValidationError, match=message):
        ResearchDatasetImporter(session_factory=session_factory).import_csv(
            path,
            dataset_spec,
        )


def test_non_rolled_dataset_allows_missing_source_contract(
    tmp_path,
    session_factory,
):
    path = tmp_path / "candles.csv"
    _write_csv(
        path,
        ["1704067200000,100,101,99,100,12"],
        include_source_contract=False,
    )
    spec = ResearchDatasetSpec(
        dataset_id="mnq-contract-202609",
        product_id="RITHMIC:MNQ-202609",
        timeframe=TIMEFRAME,
        source="rithmic-history",
        revision="2026-07-25",
    )

    result = ResearchDatasetImporter(
        session_factory=session_factory
    ).import_csv(path, spec)

    assert result.row_count == 1


def test_dataframe_preserves_decimal_values(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["1704067200000,100.10,101.20,99.50,100.80,12"])
    ResearchDatasetImporter(session_factory=session_factory).import_csv(
        path,
        dataset_spec,
    )

    frame = ResearchDatabaseDataSource(
        dataset_spec.dataset_id,
        session_factory=session_factory,
    ).get_candles_df(PRODUCT_ID, TIMEFRAME, 0, 9_999_999_999_999)

    assert frame.iloc[0]["open"] == Decimal("100.10")
    assert isinstance(frame.iloc[0]["open"], Decimal)


@pytest.mark.parametrize(
    "mutation",
    (
        "insert_candle",
        "update_candle",
        "delete_candle",
        "update_dataset",
        "delete_dataset",
    ),
)
def test_sealed_dataset_rejects_all_content_and_metadata_mutations(
    tmp_path,
    session_factory,
    dataset_spec,
    mutation,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["1704067200000,100,101,99,100,12"])
    ResearchDatasetImporter(session_factory=session_factory).import_csv(
        path,
        dataset_spec,
    )
    with session_factory() as session:
        dataset = session.get(ResearchDataset, dataset_spec.dataset_id)
        candle = session.get(
            ResearchCandlestick,
            (dataset_spec.dataset_id, 1704067200000),
        )
        assert dataset is not None
        assert candle is not None
        if mutation == "insert_candle":
            session.add(
                ResearchCandlestick(
                    dataset_id=dataset_spec.dataset_id,
                    timestamp=1704067260000,
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100"),
                    volume=Decimal("1"),
                    source_contract="MNQH4",
                )
            )
        elif mutation == "update_candle":
            candle.close = Decimal("100.5")
        elif mutation == "delete_candle":
            session.delete(candle)
        elif mutation == "update_dataset":
            dataset.revision = "mutated"
        else:
            session.delete(dataset)
        with pytest.raises(
            IntegrityError,
            match="sealed research dataset",
        ):
            session.commit()

    source = ResearchDatabaseDataSource(
        dataset_spec.dataset_id,
        session_factory=session_factory,
    )

    assert source.validate() is True
    assert len(
        list(
            source.get_candles(
                PRODUCT_ID,
                TIMEFRAME,
                0,
                9_999_999_999_999,
            )
        )
    ) == 1


def test_unsealed_dataset_is_not_readable(
    session_factory,
    dataset_spec,
):
    with session_factory() as session:
        session.add(Exchange(id="RITHMIC", name="Rithmic"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="RITHMIC",
                base_asset="MNQ",
                quote_asset="USD",
            )
        )
        session.add(
            ResearchDataset(
                id=dataset_spec.dataset_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                source="rithmic-history",
                revision="pending",
                timestamp_format="epoch_milliseconds",
                checksum_sha256="0" * 64,
                roll_policy="vendor-front-month",
                start_time=1704067200000,
                end_time=1704067200000,
                row_count=1,
                quality_status="validated",
                lifecycle_state="importing",
                sealed_at=None,
                metadata_json="{}",
            )
        )
        session.commit()

    source = ResearchDatabaseDataSource(
        dataset_spec.dataset_id,
        session_factory=session_factory,
    )

    assert source.validate() is False
    with pytest.raises(ResearchDatasetIntegrityError):
        list(
            source.get_candles(
                PRODUCT_ID,
                TIMEFRAME,
                0,
                9_999_999_999_999,
            )
        )


def test_dataset_cannot_be_created_as_sealed(
    session_factory,
    dataset_spec,
):
    with session_factory() as session:
        session.add(Exchange(id="RITHMIC", name="Rithmic"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="RITHMIC",
                base_asset="MNQ",
                quote_asset="USD",
            )
        )
        session.add(
            ResearchDataset(
                id=dataset_spec.dataset_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                source="rithmic-history",
                revision="invalid-direct-seal",
                timestamp_format="epoch_milliseconds",
                checksum_sha256="0" * 64,
                roll_policy="vendor-front-month",
                start_time=1704067200000,
                end_time=1704067200000,
                row_count=1,
                quality_status="validated",
                lifecycle_state="sealed",
                sealed_at=datetime(2026, 7, 26, tzinfo=timezone.utc),
                metadata_json="{}",
            )
        )

        with pytest.raises(
            IntegrityError,
            match="must be created in importing state",
        ):
            session.commit()


def test_dataset_cannot_be_sealed_with_mismatched_summary(
    session_factory,
    dataset_spec,
):
    with session_factory() as session:
        session.add(Exchange(id="RITHMIC", name="Rithmic"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="RITHMIC",
                base_asset="MNQ",
                quote_asset="USD",
            )
        )
        dataset = ResearchDataset(
            id=dataset_spec.dataset_id,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            source="rithmic-history",
            revision="invalid-summary",
            timestamp_format="epoch_milliseconds",
            checksum_sha256="0" * 64,
            roll_policy="vendor-front-month",
            start_time=1704067200000,
            end_time=1704067200000,
            row_count=2,
            quality_status="validated",
            lifecycle_state="importing",
            sealed_at=None,
            metadata_json="{}",
        )
        session.add(dataset)
        session.flush()
        session.add(
            ResearchCandlestick(
                dataset_id=dataset_spec.dataset_id,
                timestamp=1704067200000,
                open=Decimal("100"),
                high=Decimal("101"),
                low=Decimal("99"),
                close=Decimal("100"),
                volume=Decimal("1"),
                source_contract="MNQH4",
            )
        )
        session.flush()
        dataset.lifecycle_state = "sealed"
        dataset.sealed_at = datetime(2026, 7, 26, tzinfo=timezone.utc)

        with pytest.raises(
            IntegrityError,
            match="seal summary does not match candles",
        ):
            session.commit()


def test_validate_checks_persistent_seal_without_scanning_candles(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["1704067200000,100,101,99,100,12"])
    ResearchDatasetImporter(session_factory=session_factory).import_csv(
        path,
        dataset_spec,
    )
    with session_factory() as session:
        engine = session.get_bind()
    statements: list[str] = []

    def capture_statement(
        _connection,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", capture_statement)
    try:
        assert ResearchDatabaseDataSource(
            dataset_spec.dataset_id,
            session_factory=session_factory,
        ).validate() is True
    finally:
        event.remove(engine, "before_cursor_execute", capture_statement)

    assert any("research_dataset" in statement for statement in statements)
    assert not any("research_candlestick" in statement for statement in statements)


def test_file_change_between_validation_and_insert_rolls_back(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["1704067200000,100,101,99,100,12"])

    class MutatingImporter(ResearchDatasetImporter):
        def _insert_rows(
            self,
            session,
            path,
            dataset_id,
            timestamp_format,
            *,
            require_source_contract,
        ):
            _write_csv(
                path,
                ["1704067200000,100,102,99,101,12"],
            )
            return super()._insert_rows(
                session,
                path,
                dataset_id,
                timestamp_format,
                require_source_contract=require_source_contract,
            )

    with pytest.raises(ResearchCsvValidationError, match="changed"):
        MutatingImporter(session_factory=session_factory).import_csv(
            path,
            dataset_spec,
        )

    with session_factory() as session:
        assert session.query(ResearchDataset).count() == 0
        assert session.query(ResearchCandlestick).count() == 0


@pytest.mark.parametrize(
    ("timestamp_format", "raw_timestamp"),
    [
        ("epoch_seconds", "1704067200"),
        ("iso8601", "2024-01-01T00:00:00Z"),
    ],
)
def test_explicit_timestamp_format_normalizes_to_milliseconds(
    tmp_path,
    session_factory,
    dataset_spec,
    timestamp_format,
    raw_timestamp,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, [f"{raw_timestamp},100,101,99,100,12"])
    spec = ResearchDatasetSpec(
        dataset_id=f"{dataset_spec.dataset_id}-{timestamp_format}",
        product_id=dataset_spec.product_id,
        timeframe=dataset_spec.timeframe,
        source=dataset_spec.source,
        revision=dataset_spec.revision,
        timestamp_format=timestamp_format,
        roll_policy=dataset_spec.roll_policy,
    )

    ResearchDatasetImporter(session_factory=session_factory).import_csv(path, spec)

    with session_factory() as session:
        dataset = session.get(ResearchDataset, spec.dataset_id)
        assert dataset is not None
        assert dataset.timestamp_format == timestamp_format
        assert dataset.start_time == 1704067200000


@pytest.mark.parametrize(
    "raw_timestamp",
    ["2024-01-01T00:00:00", "2024-01-01"],
)
def test_iso_timestamp_without_offset_is_rejected(
    tmp_path,
    session_factory,
    dataset_spec,
    raw_timestamp,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, [f"{raw_timestamp},100,101,99,100,12"])
    spec = ResearchDatasetSpec(
        dataset_id=f"{dataset_spec.dataset_id}-naive",
        product_id=dataset_spec.product_id,
        timeframe=dataset_spec.timeframe,
        source=dataset_spec.source,
        revision=dataset_spec.revision,
        timestamp_format="iso8601",
        roll_policy=dataset_spec.roll_policy,
    )

    with pytest.raises(
        ResearchCsvValidationError,
        match="must include a UTC offset",
    ):
        ResearchDatasetImporter(session_factory=session_factory).import_csv(
            path,
            spec,
        )


def test_iso_timestamp_with_non_utc_offset_is_normalized_to_utc(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["2024-01-01T08:00:00+08:00,100,101,99,100,12"])
    spec = ResearchDatasetSpec(
        dataset_id=f"{dataset_spec.dataset_id}-offset",
        product_id=dataset_spec.product_id,
        timeframe=dataset_spec.timeframe,
        source=dataset_spec.source,
        revision=dataset_spec.revision,
        timestamp_format="iso8601",
        roll_policy=dataset_spec.roll_policy,
    )

    ResearchDatasetImporter(session_factory=session_factory).import_csv(path, spec)

    with session_factory() as session:
        dataset = session.get(ResearchDataset, spec.dataset_id)
        assert dataset is not None
        assert dataset.start_time == 1704067200000


def test_epoch_milliseconds_before_2001_are_not_guessed_as_seconds(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    _write_csv(path, ["946684800000,100,101,99,100,12"])

    ResearchDatasetImporter(session_factory=session_factory).import_csv(
        path,
        dataset_spec,
    )

    with session_factory() as session:
        dataset = session.get(ResearchDataset, dataset_spec.dataset_id)
        assert dataset is not None
        assert dataset.start_time == 946684800000


def test_source_contract_is_persisted_and_part_of_checksum(
    tmp_path,
    session_factory,
    dataset_spec,
):
    path = tmp_path / "candles.csv"
    path.write_text(
        "timestamp,open,high,low,close,volume,FrontMonthContract\n"
        "1704067200000,100,101,99,100,12,MNQH4\n",
        encoding="utf-8",
    )
    importer = ResearchDatasetImporter(session_factory=session_factory)
    importer.import_csv(path, dataset_spec)

    with session_factory() as session:
        candle = session.get(
            ResearchCandlestick,
            (dataset_spec.dataset_id, 1704067200000),
        )
        assert candle is not None
        assert candle.source_contract == "MNQH4"

    path.write_text(
        "timestamp,open,high,low,close,volume,FrontMonthContract\n"
        "1704067200000,100,101,99,100,12,MNQM4\n",
        encoding="utf-8",
    )
    with pytest.raises(ResearchDatasetConflictError):
        importer.import_csv(path, dataset_spec)
