# FluxTrade Python Strategy Service

## Control Plane

The control plane provides a backend-facing API layer for operational jobs.
The first supported job is a CSV-signal backtest using the existing
`BacktestRunner` pipeline.

Run locally:

```bash
uv run python -m src.control_plane.main
```

Set `CONTROL_PLANE_JOB_DB_PATH=/path/to/jobs.db` to persist control-plane job
records across local restarts.

Health check:

```bash
curl http://127.0.0.1:8080/health
```

Submit a backtest job:

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
    "end_time": 1700002700000
  }'
```

Inspect jobs:

```bash
curl http://127.0.0.1:8080/jobs
curl http://127.0.0.1:8080/jobs/<job_id>
curl -X POST http://127.0.0.1:8080/jobs/<job_id>/cancel \
  -H 'Content-Type: application/json' \
  -d '{"reason":"operator cancelled"}'
curl -X POST http://127.0.0.1:8080/jobs/<job_id>/retry
```

Parameter search jobs are available when the app is constructed with a
`ParameterSearchEvaluator`. The built-in CSV-signal evaluator expects each
candidate to provide a `signals_csv_path`:

```bash
curl -X POST http://127.0.0.1:8080/jobs/parameter-searches \
  -H 'Content-Type: application/json' \
  -d '{
    "strategy_id": "rsi_scalper",
    "product_id": "BINANCE:BTCUSDT-PERP",
    "timeframe": "15m",
    "start_time": 1700000000000,
    "end_time": 1700100000000,
    "backtest": {
      "candles_csv_path": "/absolute/path/to/candles.csv"
    },
    "candidates": [
      {
        "candidate_id":"a",
        "param_pack":{
          "rsi_period":14,
          "signals_csv_path":"/absolute/path/to/a_signals.csv"
        }
      },
      {
        "candidate_id":"b",
        "param_pack":{
          "rsi_period":21,
          "signals_csv_path":"/absolute/path/to/b_signals.csv"
        }
      }
    ]
  }'
```

When the app is wired with a live strategy control service, it can also expose:

```bash
curl http://127.0.0.1:8080/strategies
curl http://127.0.0.1:8080/strategies/health
curl -X POST http://127.0.0.1:8080/strategies/strategy_1/commands \
  -H 'Content-Type: application/json' \
  -d '{"command":"STOP","reason":"operator pause"}'
```
