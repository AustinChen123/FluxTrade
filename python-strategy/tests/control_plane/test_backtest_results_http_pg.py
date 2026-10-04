from __future__ import annotations

import http.client
import json
from decimal import Decimal
from http.server import ThreadingHTTPServer
from threading import Thread
from typing import Any, cast
from unittest.mock import Mock

import pytest
from sqlalchemy import select
from sqlalchemy.pool import QueuePool

from src.control_plane.backtest_result_http_contract import (
    format_utc_milliseconds,
    wire_decimal,
)
from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.main import build_control_plane_app
from src.control_plane.models import BacktestJobRequest
from src.control_plane.server import make_handler
from src.core.orm_models import (
    BacktestClosedTrade,
    BacktestEquitySample,
    BacktestMonthlyReturn,
    BacktestPnlDistribution,
    BacktestResultSummary,
)
from test_backtest_result_owner_persistence import (
    _START,
    _database,
)
from test_backtest_results_candles_pg import (
    _expected_item,
    _persist_result,
    _sealed_source,
)
from test_migrations import fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

ORIGIN = "https://fluxtrade.example.ts.net"
OPERATOR = "example.com/cap/fluxtrade-operator"
PRODUCT = "RITHMIC:MNQ-CONTINUOUS"
JOB_ID = "ed2-http-result"


def _request_json(
    address: tuple[str, int],
    path: str,
    headers: dict[str, str],
    *,
    method: str = "GET",
) -> tuple[int, dict[str, str], dict[str, Any]]:
    connection = http.client.HTTPConnection(*address, timeout=5)
    try:
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        return response.status, dict(response.getheaders()), payload
    finally:
        connection.close()


def _trade_wire(job_id: str, row: BacktestClosedTrade) -> dict[str, str]:
    return {
        "id": f"{job_id}:{row.sequence}",
        "entry_time": format_utc_milliseconds(row.entry_time),
        "exit_time": format_utc_milliseconds(row.exit_time),
        "entry_price": wire_decimal(row.entry_price),
        "exit_price": wire_decimal(row.exit_price),
        "side": row.side,
        "quantity": wire_decimal(row.quantity),
        "pnl": wire_decimal(row.pnl),
        "fee": wire_decimal(row.fee),
    }


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else wire_decimal(value)


def test_default_composition_serves_committed_results_over_browser_http(
    fresh_pg_db: str, tmp_path
) -> None:
    engine, factory = _database(fresh_pg_db)
    app = None
    server = None
    thread = None
    try:
        dataset_id = _sealed_source(factory, tmp_path, "1m", [0, 60_000])
        candles = [_expected_item(0, 0), _expected_item(60_000, 1)]
        receipt = _persist_result(factory, dataset_id, job_id=JOB_ID)
        store = InMemoryJobStore()
        no_result = store.create(
            kind="backtest",
            request=BacktestJobRequest(
                strategy_id="missing-result",
                product_id=PRODUCT,
                timeframe="1m",
                candles_csv_path=str(tmp_path / "candles.csv"),
                signals_csv_path=str(tmp_path / "signals.csv"),
                start_time=_START,
                end_time=_START + 60_000,
            ),
        )
        auth = BrowserSessionAuth(
            allowed_origin=ORIGIN,
            operator_capability=OPERATOR,
            step_up_capability="example.com/cap/step-up",
        )
        app = build_control_plane_app(
            redis_client=Mock(),
            db_session_factory=factory,
            job_store=store,
            api_key="test-api-key",
            browser_auth=auth,
            readiness_probe=lambda: None,
            strategy_loader=lambda: {},
        )
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(app))
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        address = cast(tuple[str, int], server.server_address)
        identity_headers = {
            "Origin": ORIGIN,
            "Tailscale-User-Login": "reader@example.com",
            "Tailscale-App-Capabilities": json.dumps({OPERATOR: [{}]}),
        }
        connection = http.client.HTTPConnection(*address, timeout=5)
        try:
            connection.request("POST", "/api/v1/auth/session", headers=identity_headers)
            issued = connection.getresponse()
            issued.read()
            assert issued.status == 201
            set_cookie = issued.getheader("Set-Cookie")
            assert set_cookie is not None
            cookie = set_cookie.split(";", 1)[0]
        finally:
            connection.close()
        headers = {**identity_headers, "Cookie": cookie}

        with factory() as db:
            summary = db.scalars(
                select(BacktestResultSummary).where(
                    BacktestResultSummary.job_id == JOB_ID
                )
            ).one()
            equity = db.scalars(
                select(BacktestEquitySample)
                .where(BacktestEquitySample.summary_id == summary.id)
                .order_by(BacktestEquitySample.sequence)
            ).all()
            trades = db.scalars(
                select(BacktestClosedTrade)
                .where(BacktestClosedTrade.summary_id == summary.id)
                .order_by(BacktestClosedTrade.sequence)
            ).all()
            monthly = db.scalars(
                select(BacktestMonthlyReturn)
                .where(BacktestMonthlyReturn.summary_id == summary.id)
                .order_by(BacktestMonthlyReturn.month)
            ).all()
            distribution = db.scalars(
                select(BacktestPnlDistribution)
                .where(BacktestPnlDistribution.summary_id == summary.id)
                .order_by(BacktestPnlDistribution.sequence)
            ).all()
            expected_index = {
                "items": [
                    {
                        "job_id": summary.job_id,
                        "subject_id": summary.subject_id,
                        "dataset_id": summary.dataset_id,
                        "product_id": summary.product_id,
                        "timeframe": summary.timeframe,
                        "started_at": format_utc_milliseconds(summary.start_time),
                        "ended_at": format_utc_milliseconds(summary.end_time),
                        "completed_at": format_utc_milliseconds(
                            cast(int, summary.completed_at)
                        ),
                        "result_digest": receipt.result_digest,
                    }
                ],
                "next_cursor": None,
                "revision": 1,
            }
            expected_detail = {
                "job_id": summary.job_id,
                "strategy_id": summary.strategy_id,
                "subject_id": summary.subject_id,
                "dataset_id": summary.dataset_id,
                "product_id": summary.product_id,
                "timeframe": summary.timeframe,
                "started_at": format_utc_milliseconds(summary.start_time),
                "ended_at": format_utc_milliseconds(summary.end_time),
                "currency": summary.currency,
                "metrics": {
                    key: wire_decimal(cast(Decimal, getattr(summary, field)))
                    for key, field in (
                        ("net_pnl", "net_pnl"),
                        ("return_pct", "return_pct"),
                        ("max_drawdown", "max_drawdown"),
                        ("sharpe", "sharpe"),
                        ("sortino", "sortino"),
                        ("calmar", "calmar"),
                    )
                },
                "equity": [
                    {
                        "timestamp": format_utc_milliseconds(row.timestamp),
                        "equity": wire_decimal(row.equity),
                        "drawdown": wire_decimal(row.drawdown),
                    }
                    for row in equity
                ],
                "monthly_returns": [
                    {"month": row.month, "return_pct": wire_decimal(row.return_pct)}
                    for row in monthly
                ],
                "pnl_distribution": [
                    {
                        "lower": _decimal_or_none(row.lower),
                        "upper": _decimal_or_none(row.upper),
                        "count": row.count,
                    }
                    for row in distribution
                ],
                "trade_page": {
                    "items": [_trade_wire(JOB_ID, row) for row in trades],
                    "total_count": len(trades),
                    "next_cursor": None,
                },
                "input_digest": receipt.input_digest,
                "result_digest": receipt.result_digest,
                "revision": 1,
            }

        status, response_headers, index = _request_json(
            address, "/api/v1/backtest-results", headers
        )
        assert status == 200 and response_headers["Cache-Control"] == "no-store"
        assert index == expected_index
        status, _, detail = _request_json(
            address, f"/api/v1/backtest-results/{JOB_ID}", headers
        )
        assert status == 200 and detail == expected_detail

        status, _, first_trades = _request_json(
            address, f"/api/v1/backtest-results/{JOB_ID}/trades?limit=1", headers
        )
        assert status == 200
        assert first_trades["items"] == [_trade_wire(JOB_ID, trades[0])]
        trade_cursor = first_trades["next_cursor"]
        assert isinstance(trade_cursor, str) and trade_cursor
        replay = _request_json(
            address, f"/api/v1/backtest-results/{JOB_ID}/trades?limit=1", headers
        )[2]
        assert replay == first_trades
        tail = _request_json(
            address,
            f"/api/v1/backtest-results/{JOB_ID}/trades?limit=1&cursor={trade_cursor}",
            headers,
        )[2]
        assert tail == {
            "items": [_trade_wire(JOB_ID, trades[1])],
            "next_cursor": None,
            "revision": 1,
        }

        candle_path = (
            f"/api/v1/backtest-results/{JOB_ID}/candles?start={_START}"
            f"&end={_START + 120_000}&limit=1"
        )
        first_candles = _request_json(address, candle_path, headers)[2]
        candle_cursor = first_candles["next_cursor"]
        assert first_candles["items"] == candles[:1]
        assert isinstance(candle_cursor, str) and candle_cursor
        candle_tail = _request_json(
            address, f"{candle_path}&cursor={candle_cursor}", headers
        )[2]
        assert candle_tail == {
            "items": candles[1:],
            "next_cursor": None,
            "revision": 1,
        }

        status, _, result = _request_json(
            address, f"/api/v1/backtest-results/{no_result.id}", headers
        )
        assert status == 409 and result == {"error": "result_unavailable"}
        status, _, result = _request_json(
            address, "/api/v1/backtest-results/absent-result", headers
        )
        assert status == 404 and result == {"error": "result_not_found"}
        status, _, result = _request_json(
            address,
            f"/api/v1/backtest-results/{JOB_ID}",
            {"X-API-Key": "test-api-key"},
        )
        assert status == 403 and result == {"error": "forbidden"}
        assert cast(QueuePool, engine.pool).checkedout() == 0
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=5)
            assert not thread.is_alive()
        if app is not None:
            assert app.shutdown(timeout=2)
        engine.dispose()
