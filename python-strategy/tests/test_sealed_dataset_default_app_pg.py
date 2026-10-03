"""Isolated PostgreSQL acceptance for sealed research-data evaluation."""

from __future__ import annotations

import json
import time
from decimal import Decimal
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker

from test_migrations import (
    fresh_pg_db as _fresh_pg_db,
    _insert_research_prerequisites,
    _target_url,
    _upgrade,
)

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

PRODUCT = "RITHMIC:MNQ-CONTINUOUS"
TIMEFRAME = "1m"
START = 1_704_067_200_000
DATASET_ID = "p0c-default-app-oscillation-v1"
PRECISION_DATASET_ID = "p0c-decimal-roundtrip-v1"


def test_migrated_postgres_imports_runs_and_persists_sealed_search(
    fresh_pg_db: str,
    tmp_path,
) -> None:
    from src.control_plane.jobs import InMemoryJobStore
    from src.control_plane.main import build_control_plane_app
    from src.core.data_sources.research_database import ResearchDatabaseDataSource
    from src.core.orm_models import (
        EvolutionEpoch,
        GeneRecord,
        ResearchDataset,
        Strategy,
    )
    from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec

    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    app = None
    try:
        _insert_research_prerequisites(engine)
        sessions = sessionmaker(bind=engine)
        with sessions() as session:
            session.add(Strategy(id="golden_cross", name="Golden Cross"))
            session.commit()

        closes = [100, 99, 98, 105, 110, 95, 90, 108, 112, 90, 88, 110]
        rows = ["timestamp,open,high,low,close,volume,source_contract"]
        for index, close in enumerate(closes):
            timestamp = START + index * 60_000
            rows.append(f"{timestamp},{close},{close + 1},{close - 1},{close},1,MNQH4")
        candles_path = tmp_path / "native-sealed-search.csv"
        candles_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
        importer = ResearchDatasetImporter(session_factory=sessions)
        imported = importer.import_csv(
            candles_path,
            ResearchDatasetSpec(
                dataset_id=DATASET_ID,
                product_id=PRODUCT,
                timeframe=TIMEFRAME,
                source="synthetic-p0c",
                revision="v1",
                roll_policy="vendor-front-month",
            ),
        )
        assert imported.already_present is False
        assert imported.row_count == len(closes)

        precision = Decimal("101.123456789012345678901234567890123")
        precision_time = START + 20 * 60_000
        precision_path = tmp_path / "decimal-roundtrip.csv"
        precision_path.write_text(
            "timestamp,open,high,low,close,volume,source_contract\n"
            f"{precision_time},{precision},{precision + 1},"
            f"{precision - 1},{precision},0.1234567890123456789,MNQH4\n",
            encoding="utf-8",
        )
        precision_import = importer.import_csv(
            precision_path,
            ResearchDatasetSpec(
                dataset_id=PRECISION_DATASET_ID,
                product_id=PRODUCT,
                timeframe=TIMEFRAME,
                source="synthetic-p0c",
                revision="decimal-v1",
                roll_policy="vendor-front-month",
            ),
        )
        assert precision_import.already_present is False
        precision_source = ResearchDatabaseDataSource(
            PRECISION_DATASET_ID,
            session_factory=sessions,
        )
        precision_candles = list(
            precision_source.get_candles(
                PRODUCT,
                TIMEFRAME,
                precision_time,
                precision_time,
            )
        )
        assert len(precision_candles) == 1
        precision_candle = precision_candles[0]
        assert all(
            isinstance(value, Decimal)
            for value in (
                precision_candle.open,
                precision_candle.high,
                precision_candle.low,
                precision_candle.close,
                precision_candle.volume,
            )
        )
        assert (
            precision_candle.open,
            precision_candle.high,
            precision_candle.low,
            precision_candle.close,
            precision_candle.volume,
        ) == (
            precision,
            precision + 1,
            precision - 1,
            precision,
            Decimal("0.1234567890123456789"),
        )

        source = ResearchDatabaseDataSource(DATASET_ID, session_factory=sessions)
        assert source.validate() is True
        assert source.get_available_range(PRODUCT, TIMEFRAME) == (
            START,
            START + (len(closes) - 1) * 60_000,
        )
        with sessions() as session:
            dataset = session.get(ResearchDataset, DATASET_ID)
            assert dataset is not None
            assert dataset.id == DATASET_ID
            assert dataset.product_id == PRODUCT
            assert dataset.timeframe == TIMEFRAME
            assert dataset.source == "synthetic-p0c"
            assert dataset.revision == "v1"
            assert dataset.roll_policy == "vendor-front-month"
            assert dataset.lifecycle_state == "sealed"
            assert dataset.quality_status == "validated"
            assert dataset.sealed_at is not None
            assert dataset.checksum_sha256 == imported.checksum_sha256
            assert dataset.row_count == len(closes)

        store = InMemoryJobStore()
        app = build_control_plane_app(
            redis_client=MagicMock(),
            db_session_factory=sessions,
            job_store=store,
            api_key="p0c-test-operator-key",
            readiness_probe=lambda: None,
            profile_query_service=MagicMock(),
        )
        payload = {
            "strategy_type": "golden_cross",
            "strategy_id": "golden_cross",
            "product_id": PRODUCT,
            "timeframe": TIMEFRAME,
            "start_time": START,
            "end_time": START + (len(closes) - 1) * 60_000,
            "market_data": {"kind": "sealed_dataset", "dataset_id": DATASET_ID},
            "backtest": {
                "initial_balance": "1000",
                "maker_fee": "0.001",
                "taker_fee": "0.001",
            },
            "candidates": [
                {
                    "candidate_id": "small-windows",
                    "param_pack": {
                        "short_window": 2,
                        "long_window": 3,
                        "quantity": "1",
                    },
                }
            ],
        }
        submitted = app.handle(
            "POST",
            "/jobs/parameter-searches",
            body=json.dumps(payload),
            headers={"Authorization": "Bearer p0c-test-operator-key"},
        )
        assert submitted.status_code == 202
        job_id = submitted.body["job"]["id"]
        deadline = time.monotonic() + 15
        job = None
        while time.monotonic() < deadline:
            job = store.get(job_id)
            assert job is not None
            if job.finished_at is not None:
                break
            time.sleep(0.01)
        assert job is not None
        assert job.finished_at is not None

        delivered = app.handle(
            "GET",
            f"/jobs/{job_id}",
            headers={"Authorization": "Bearer p0c-test-operator-key"},
        )
        assert delivered.status_code == 200
        job_payload = delivered.body["job"]
        assert job_payload["status"] == "SUCCEEDED"
        assert job_payload["request"]["market_data"]["dataset_id"] == DATASET_ID
        result = job_payload["result"]
        evaluation = result["evaluations"][0]
        metrics = evaluation["metrics"]
        assert metrics["closed_trade_count"] > 0
        assert Decimal(evaluation["score_total"]) != 0
        assert metrics["fill_records"]
        assert metrics["decision_snapshots"]
        assert metrics["provenance"]["dataset_sha256"]
        assert metrics["provenance"]["dataset_candle_count"] == len(closes)

        with sessions() as session:
            epoch = session.get(EvolutionEpoch, result["epoch_id"])
            gene = session.query(GeneRecord).one()
        assert epoch is not None
        assert epoch.status == "completed"
        assert epoch.best_score == Decimal(evaluation["score_total"])
        assert gene.epoch_id == epoch.id
        assert gene.score_total == Decimal(evaluation["score_total"])
        assert gene.score_breakdown == metrics
        assert gene.score_breakdown["fill_records"] == metrics["fill_records"]
        assert (
            gene.score_breakdown["decision_snapshots"] == metrics["decision_snapshots"]
        )
        assert gene.score_breakdown["provenance"] == metrics["provenance"]
    except BaseException:
        if app is not None:
            app.shutdown(timeout=5)
        raise
    else:
        if app is not None:
            stopped = app.shutdown(timeout=5)
            assert stopped is True
    finally:
        engine.dispose()
