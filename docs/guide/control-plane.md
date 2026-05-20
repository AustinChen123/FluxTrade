# Control Plane

FluxTrade's control plane is the backend-facing API layer for running and
observing operational jobs. The first supported job type is a CSV-signal
backtest that reuses the existing backtest pipeline:

`CsvDataSource -> CsvSignalStrategy -> BacktestRunner -> StrategyEngine -> Rust matcher`

The control plane is intentionally framework-neutral. The core router can be
tested without an HTTP server, and a small stdlib HTTP adapter is available for
local operation.

## Run Locally

```bash
cd python-strategy
uv run python -m src.control_plane.main
```

Environment variables:

| Variable | Default | Description |
| --- | --- | --- |
| `CONTROL_PLANE_HOST` | `127.0.0.1` | Bind host |
| `CONTROL_PLANE_PORT` | `8080` | Bind port |

## Health Check

```bash
curl http://127.0.0.1:8080/health
```

Expected response:

```json
{"status":"ok"}
```

## Submit A Backtest Job

```bash
curl -X POST http://127.0.0.1:8080/jobs/backtests \
  -H 'Content-Type: application/json' \
  -d '{
    "strategy_id": "replay_v1",
    "product_id": "BINANCE:BTCUSDT-PERP",
    "timeframe": "15m",
    "candles_csv_path": "/absolute/path/to/candles.csv",
    "signals_csv_path": "/absolute/path/to/signals.csv",
    "start_time": 1700000000000,
    "end_time": 1700002700000,
    "initial_balance": "10000",
    "maker_fee": "0.0002",
    "taker_fee": "0.0006"
  }'
```

The job response contains the job ID, status, request payload, and result when
the configured executor runs inline. With the threaded executor, the initial
response is usually `QUEUED`; poll the job endpoint for completion.

## Inspect Jobs

```bash
curl http://127.0.0.1:8080/jobs
curl http://127.0.0.1:8080/jobs/<job_id>
```

## Strategy Status And Commands

When the control plane is constructed with a strategy control service, it can
wrap the existing `CommandRouter` and expose operator controls:

```bash
curl http://127.0.0.1:8080/strategies
curl http://127.0.0.1:8080/strategies/health
```

Submit a strategy command:

```bash
curl -X POST http://127.0.0.1:8080/strategies/strategy_1/commands \
  -H 'Content-Type: application/json' \
  -d '{
    "command": "STOP",
    "reason": "operator pause"
  }'
```

Supported commands:

- `START`
- `STOP`
- `RESUME`
- `FORCE_RECOVER`
- `RELOAD`

## CSV Formats

Candles use the existing `CsvDataSource` format:

```csv
timestamp,open,high,low,close,volume
1700000000000,50000,50100,49900,50000,100
```

Signals use the existing `CsvSignalStrategy` format:

```csv
timestamp,type,quantity
1700000000000,LONG,0.01
1700001800000,EXIT_LONG,0.01
```

## Current Limitations

- The default job store is in-memory; jobs are not durable across process restarts.
- The first job type is CSV-signal backtesting. Parameter search, strategy
  monitoring, and operator controls should be added as follow-up job types.
- Strategy command endpoints require wiring a live `StrategyControlService` over
  the running engine's `CommandRouter`; the default standalone server only
  exposes job endpoints.
- Authentication and authorization are not yet implemented.
- Production deployment should use a durable job store and a stronger HTTP
  framework adapter.
