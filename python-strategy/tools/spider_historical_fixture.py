"""Build validated historical input JSON from OKX 1m candle exports.

Example::

    uv run --no-sync python -m tools.spider_historical_fixture \
        --horizon-minutes 24 --run-id fin --output /path/to/F-IN.json

Candle and metadata roots are configurable CLI arguments. Window semantics are
[start_ms - 1 day, start_ms + horizon_minutes) with the upper bound exclusive.
Decimal is used for every price and quantity value.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from decimal import Decimal
from gzip import open as gzip_open
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any

from src.core.backtest.spider_historical_input import (
    BAR_DURATION_MS,
    SCHEMA_ID,
    SCHEMA_VERSION,
    HistoricalBar,
    HistoricalRunInput,
    InstrumentSpecEvidence,
    SourceManifest,
    _encode_historical_run_input_validated_run,
    _validate_historical_input_once,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest.spider_scenario_run import _P3_POLICY_SOURCE

_DEFAULT_START_MS = 1_793_257_200_000 - 89 * 86_400_000
_DEFAULT_CANDLE_ROOT = Path(os.environ.get(
    "SPIDER_HISTORICAL_CANDLE_ROOT", "/Volumes/Dev/Datasets/okx/candles_1m",
))
_DEFAULT_META_ROOT = Path(os.environ.get(
    "SPIDER_HISTORICAL_META_ROOT", "/Volumes/Dev/Datasets/okx/meta",
))


def _decimal_text(value: str | Decimal) -> str:
    number = Decimal(value)
    return format(number.normalize(), "f") if number else "0"


def _read_source(path: Path, product: str, kind: str, start: int, end: int) -> tuple[HistoricalBar, ...]:
    if not path.is_file():
        raise FileNotFoundError(path)
    rows: list[HistoricalBar] = []
    with gzip_open(path, "rt", encoding="utf-8", newline="") as stream:
        for line in stream:
            fields = line.rstrip("\n").split(",")
            timestamp = int(fields[0])
            if not start <= timestamp < end:
                continue
            is_trade = kind == "trade"
            row = HistoricalBar(
                product_id=product,
                bar_open_ms=timestamp,
                open=Decimal(_decimal_text(fields[1])),
                high=Decimal(_decimal_text(fields[2])),
                low=Decimal(_decimal_text(fields[3])),
                close=Decimal(_decimal_text(fields[4])),
                confirm=1,
                source_sequence=len(rows),
                volume=Decimal(_decimal_text(fields[5])) if is_trade else None,
            )
            rows.append(replace(row, source_row_hash=sha256(row.canonical_row(trade=is_trade)).hexdigest()))
    if not rows:
        raise ValueError(f"No {kind} bars for {product} in [{start}, {end})")
    return tuple(rows)


def _manifest(source_id: str, rows: tuple[HistoricalBar, ...], products: tuple[str, ...], *, trade: bool) -> SourceManifest:
    ordinal = {product: index for index, product in enumerate(products)}
    ordered = sorted(rows, key=lambda row: (ordinal[row.product_id], row.source_sequence))
    digest = sha256(b"".join(bytes.fromhex(row.source_row_hash) for row in ordered)).hexdigest()
    return SourceManifest(
        source_id,
        "OKX_PUBLIC_V5",
        "/api/v5/market/history-candles" if trade else "/api/v5/market/history-mark-price-candles",
        (("bar", "1m"),),
        "2026-10-29T12:38:00Z",
        200,
        "0" * 64,
        digest,
        "V1",
        "OKX_HISTORY_CANDLES_V1",
        tuple((product, product) for product in products),
        "CONTRACTS" if trade else "PRICE",
        len(rows),
        len(rows),
        min(row.bar_open_ms for row in rows),
        max(row.bar_open_ms for row in rows),
        "REJECT",
        "REJECT",
        "ARCHIVED_FIXTURE",
    )


def build_input(
    *,
    products: tuple[str, ...],
    horizon_minutes: int,
    start_ms: int,
    run_id: str,
    candle_root: Path = _DEFAULT_CANDLE_ROOT,
    meta_root: Path = _DEFAULT_META_ROOT,
) -> bytes:
    """Build and validate a frozen input from explicit read-only OKX source files."""
    if not products or len(products) != len(set(products)):
        raise ValueError("products must be a non-empty unique sequence")
    if horizon_minutes <= 0 or start_ms % BAR_DURATION_MS:
        raise ValueError("horizon must be positive and start must be minute aligned")
    if not run_id or not run_id[0].isalnum() or any(not (char.isalnum() or char in "_-") for char in run_id):
        raise ValueError("run_id must match [A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    if len(run_id) > 64:
        raise ValueError("run_id must match [A-Za-z0-9][A-Za-z0-9_-]{0,63}")

    start = start_ms
    warmup = start - 86_400_000
    end = start + horizon_minutes * 60_000
    instruments_value = json.loads((meta_root / "instruments_SWAP_20261029T071147Z.json").read_text())
    instruments_rows = instruments_value["data"] if isinstance(instruments_value, dict) else instruments_value
    instruments = {row["instId"]: row for row in instruments_rows}
    tiers_by_family: dict[str, Any] = json.loads((meta_root / "position_tiers_cross_20261029T071147Z.json").read_text())

    trade_rows: list[HistoricalBar] = []
    mark_rows: list[HistoricalBar] = []
    for product in products:
        trade_rows.extend(_read_source(candle_root / f"{product}_1m_trade.csv.gz", product, "trade", warmup, end))
        mark_rows.extend(_read_source(candle_root / f"{product}_1m_mark.csv.gz", product, "mark", warmup, end))
    order = {product: index for index, product in enumerate(products)}
    def key(row: HistoricalBar) -> tuple[int, int]:
        return row.bar_open_ms, order[row.product_id]

    trade = tuple(sorted(trade_rows, key=key))
    mark = tuple(sorted(mark_rows, key=key))
    first_mark = {
        product: next(row.close for row in mark if row.product_id == product and row.bar_open_ms >= start - 60_000)
        for product in products
    }

    configuration: dict[str, Any] = {
        "schema_version": "synthetic_multi_product_config_v1",
        "config_id": f"OKX_REAL_{run_id}",
        "seed_effective_at": 0,
        "cash": "1000",
        "leverage": "4",
        "products": [],
        "positions": [],
        "orders": [],
    }
    specs: list[InstrumentSpecEvidence] = []
    for ordinal, product in enumerate(products, start=1):
        instrument = instruments[product]
        tiers = tiers_by_family[product.replace("-SWAP", "")]
        tier_rows = [
            {
                "minimum_contracts": _decimal_text(row["minSz"]),
                "maximum_contracts": _decimal_text(row["maxSz"]),
                "mmr": _decimal_text(row["mmr"]),
                "imr": _decimal_text(row["imr"]),
                "max_leverage": _decimal_text(row["maxLever"]),
            }
            for row in tiers
        ]
        tier_version = [{"version": "tier-v1", "valid_from": 0, "valid_to": None, "rows": tier_rows}]
        configuration["products"].append(
            {
                "product_id": product,
                "instrument_code": ordinal,
                "taker_fee_rate": "0.0005",
                "liquidation_fee_rate": "0.0005",
                "specs": [{
                    "version": "spec-v1", "valid_from": 0, "valid_to": None,
                    "contract_value": _decimal_text(instrument["ctVal"]), "multiplier": "1",
                    "price_tick": _decimal_text(instrument["tickSz"]),
                    "quantity_step": _decimal_text(instrument["lotSz"]),
                    "minimum_quantity": _decimal_text(instrument["minSz"]),
                }],
                "tiers": tier_version,
                "marks": [{"valid_from": 0, "valid_to": end, "mark": _decimal_text(first_mark[product])}],
            }
        )
        specs.append(
            InstrumentSpecEvidence(
                product, start, Decimal(_decimal_text(instrument["ctVal"])), Decimal("1"),
                Decimal(_decimal_text(instrument["tickSz"])), Decimal(_decimal_text(instrument["lotSz"])),
                Decimal(_decimal_text(instrument["minSz"])), sha256(canonical_bytes(tier_version)).hexdigest(),
            )
        )
    configuration_raw = canonical_bytes(configuration)
    cache = {
        "schema_version": "SPIDER_POLICY_CACHE_V1", "running": True, "paused": False,
        "online": True, "ws_open": False, "order_id": 1,
        "capital": {"total": "1000", "usdt": "1000", "avail": "1000", "earn": "0", "position": "0"},
        "rows": [{"product_id": product, "name": product, "active": "true", "leverage": "4", "歩差": "0.01",
                   "單數": "10", "hold上限": "0.8", "hold下限": "-0.8", "hold": "0"} for product in products],
        "markets": [{"product_id": product, "price": _decimal_text(first_mark[product]),
                     "ctVal": _decimal_text(instruments[product]["ctVal"]), "lotSz": _decimal_text(instruments[product]["lotSz"]),
                     "minSz": _decimal_text(instruments[product]["minSz"]), "increment": _decimal_text(instruments[product]["tickSz"]),
                     "ratioHL": "0.1", "state": "live", "instIdCode": ordinal}
                    for ordinal, product in enumerate(products, start=1)],
        "replies": {}, "orders": {}, "positions": {}, "last_filled_price": {},
    }
    account = canonical_bytes({"cash": "1000", "orders": [], "positions": []})
    run = HistoricalRunInput(
        SCHEMA_ID, SCHEMA_VERSION, run_id, "account", "SPIDER_GRID_ORIGINAL_V1", "1", _P3_POLICY_SOURCE,
        "SPIDER_GRID_ORIGINAL_V1", (("defaultN", Decimal("10")),), configuration_raw,
        sha256(configuration_raw).hexdigest(), products, "OHLC4_OPEN_HIGH_LOW_CLOSE_V1", 1,
        start, end, BAR_DURATION_MS, warmup,
        _manifest("P3_ORACLE_TRADE_V1", trade, products, trade=True),
        _manifest("P3_ORACLE_MARK_V1", mark, products, trade=False),
        trade, mark, tuple(specs), tuple(replace(spec, effective_at_ms=end) for spec in specs),
        Decimal("10"), "MODELLING_ASSUMPTION", 5000, start + 5000, "UTC", 1, 5001, 2, 2,
        "POLL_ORDERED_V1", "MTM_PRESERVE_OPEN_V1", canonical_bytes(cache), account,
    )
    token = _validate_historical_input_once(run)
    return _encode_historical_run_input_validated_run(token.run)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--products", default="NEO-USDT-SWAP,GMX-USDT-SWAP")
    parser.add_argument("--horizon-minutes", type=int, required=True)
    parser.add_argument("--start-ms", type=int, default=_DEFAULT_START_MS)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--candle-root", type=Path, default=_DEFAULT_CANDLE_ROOT)
    parser.add_argument("--meta-root", type=Path, default=_DEFAULT_META_ROOT)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = build_input(
        products=tuple(part for part in args.products.split(",") if part),
        horizon_minutes=args.horizon_minutes, start_ms=args.start_ms, run_id=args.run_id,
        candle_root=args.candle_root, meta_root=args.meta_root,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(raw)
    print(json.dumps({"input_sha256": sha256(raw).hexdigest(), "bytes": len(raw)}, sort_keys=True))


if __name__ == "__main__":
    main()
