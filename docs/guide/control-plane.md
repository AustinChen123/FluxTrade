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
| `CONTROL_PLANE_JOB_DB_PATH` | unset | Optional SQLite job database path. When set, job records survive process restarts. |

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
response is usually `QUEUED`; poll the job endpoint for completion. Set
`CONTROL_PLANE_JOB_DB_PATH` when local job history needs to persist across
control-plane restarts. On startup with the SQLite store enabled, jobs left in
`QUEUED` or `RUNNING` from a previous process are marked failed with an
interruption error so they can be retried explicitly.

## Inspect Jobs

```bash
curl http://127.0.0.1:8080/jobs
curl 'http://127.0.0.1:8080/jobs?limit=50&offset=0'
curl http://127.0.0.1:8080/jobs/<job_id>
```

## Cancel Or Retry Jobs

Queued backtest jobs can be cancelled before they start:

```bash
curl -X POST http://127.0.0.1:8080/jobs/<job_id>/cancel \
  -H 'Content-Type: application/json' \
  -d '{"reason":"operator cancelled"}'
```

Running jobs are not force-stopped by this endpoint. If a job has already
entered `BacktestRunner`, the control plane returns `409` instead of pretending
the running work was interrupted.

Failed or cancelled CSV-signal backtest jobs can be retried with the original
request payload:

```bash
curl -X POST http://127.0.0.1:8080/jobs/<job_id>/retry
```

## Submit A Parameter Search Job

Parameter search is exposed as a control-plane job type. The application
provides a `ParameterSearchEvaluator`, which turns one candidate parameter pack
into score, drawdown, and metrics. The first built-in evaluator is
`CsvSignalBacktestParameterEvaluator`: each candidate points to a signal CSV,
and the evaluator runs the existing BacktestRunner against a shared candle CSV.

```bash
curl -X POST http://127.0.0.1:8080/jobs/parameter-searches \
  -H 'Content-Type: application/json' \
  -d '{
    "strategy_id": "rsi_scalper",
    "product_id": "BINANCE:BTCUSDT-PERP",
    "timeframe": "15m",
    "start_time": 1700000000000,
    "end_time": 1700100000000,
    "objective": "maximize_score",
    "seed": 42,
    "backtest": {
      "candles_csv_path": "/absolute/path/to/candles.csv",
      "initial_balance": "10000",
      "maker_fee": "0.0002",
      "taker_fee": "0.0006"
    },
    "candidates": [
      {
        "candidate_id": "candidate_001",
        "param_pack": {
          "rsi_period": 14,
          "entry_threshold": 30,
          "signals_csv_path": "/absolute/path/to/candidate_001_signals.csv"
        }
      },
      {
        "candidate_id": "candidate_002",
        "param_pack": {
          "rsi_period": 21,
          "entry_threshold": 25,
          "signals_csv_path": "/absolute/path/to/candidate_002_signals.csv"
        }
      }
    ]
  }'
```

Supported objectives:

- `maximize_score`
- `maximize_return`
- `minimize_drawdown`

The default standalone server does not wire a parameter-search evaluator yet,
so this endpoint returns `503` until the process is constructed with one. The
CSV-signal evaluator is useful when another process generates candidate signals
from parameter packs and FluxTrade is responsible for durable evaluation,
ranking, and job history. When the executor is constructed with a database
session factory, completed searches also write an `evolution_epochs` row plus
one `gene_records` challenger row per evaluated candidate; the job result
includes `epoch_id`.

## Promote A Gene

When the control plane is constructed with `GeneControlService`, an operator can
promote one candidate gene to champion:

```bash
curl -X POST http://127.0.0.1:8080/genes/<gene_id>/promote \
  -H 'Content-Type: application/json' \
  -d '{
    "reason": "best search score",
    "actor": "operator"
  }'
```

Promotion retires any existing champion for the same strategy and writes
`gene_retire` / `gene_promote` system events in the same transaction.

Inspect persisted genes and evolution epochs:

```bash
curl http://127.0.0.1:8080/genes
curl 'http://127.0.0.1:8080/genes?strategy_id=rsi_scalper&role=champion&limit=50&offset=0'
curl http://127.0.0.1:8080/genes/<gene_id>

curl http://127.0.0.1:8080/evolution-epochs
curl 'http://127.0.0.1:8080/evolution-epochs?strategy_id=rsi_scalper&limit=50&offset=0'
curl http://127.0.0.1:8080/evolution-epochs/<epoch_id>
```

Inspect system events written by promotion, reconciliation, and operational
paths:

```bash
curl http://127.0.0.1:8080/system-events
curl 'http://127.0.0.1:8080/system-events?event_type=gene_promote&strategy_id=rsi_scalper&limit=50&offset=0'
curl 'http://127.0.0.1:8080/system-events?related_gene_id=123&limit=50&offset=0'
curl http://127.0.0.1:8080/system-events/<event_id>
```

List endpoints return `total`, `limit`, and `offset`. The default page is
`limit=100&offset=0`; `limit` must be between 1 and 500.

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

When the control plane is constructed with `StrategyStateQueryService`, it can
also expose durable strategy lifecycle state:

```bash
curl http://127.0.0.1:8080/strategy-states
curl 'http://127.0.0.1:8080/strategy-states/summary?stale_after_ms=120000'
curl 'http://127.0.0.1:8080/strategy-states?status=ACTIVE&limit=50&offset=0'
curl http://127.0.0.1:8080/strategy-states/<strategy_id>
curl 'http://127.0.0.1:8080/strategy-states/<strategy_id>/transitions?limit=50&offset=0'
```

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

- The default job store is in-memory. Set `CONTROL_PLANE_JOB_DB_PATH` to use
  the built-in SQLite job store for durable local operation.
- The first executable backtest job type is CSV-signal backtesting. Parameter
  search can evaluate candidate signal CSVs through BacktestRunner, but native
  strategy parameter generation/mutation/crossover is still future work.
- Gene promotion updates gene lifecycle state and writes audit events, but it
  does not hot-reload a live strategy yet.
- Job cancellation currently applies only to queued jobs. Running backtests need
  cooperative cancellation inside the runner before safe force-stop semantics
  can be exposed.
- Strategy command endpoints require wiring a live `StrategyControlService` over
  the running engine's `CommandRouter`; durable state endpoints require wiring a
  `StrategyStateQueryService`.
- Authentication and authorization are not yet implemented.
- Production deployment should use a stronger HTTP framework adapter and review
  whether SQLite durability is sufficient for the deployment model.
