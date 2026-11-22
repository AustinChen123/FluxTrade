import json
import math
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class OkxBar:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float


@dataclass(frozen=True)
class CandleWindow:
    bars: tuple[OkxBar, ...]
    missing: tuple[int, ...]


def default_http_get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "FluxTrade public candle reader"}, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status != 200:
                raise ValueError(f"HTTP status {response.status}")
            return response.read()
    except urllib.error.HTTPError as error:
        raise ValueError(f"HTTP status {error.code}") from error


def fetch_closed_1m(inst_id: str, start_ms: int, end_ms: int, http_get: Callable[[str], bytes]) -> CandleWindow:
    if start_ms % 60000 != 0 or end_ms % 60000 != 0:
        raise ValueError("start_ms and end_ms must be minute-aligned")
    if start_ms >= end_ms:
        raise ValueError("start_ms must be less than end_ms")

    minute_count = (end_ms - start_ms) // 60000
    page_cap = (minute_count + 299) // 300 + 2
    bars_by_ts: dict[int, OkxBar] = {}
    after = end_ms

    for _ in range(page_cap):
        url = f"https://www.okx.com/api/v5/market/candles?instId={inst_id}&bar=1m&limit=300&after={after}"
        payload = json.loads(http_get(url))
        if not isinstance(payload, dict) or payload.get("code") != "0":
            raise ValueError("response code must be '0'")
        rows = payload.get("data")
        if not isinstance(rows, list):
            raise ValueError("data must be a list")
        if not rows:
            break

        parsed: list[tuple[int, OkxBar | None]] = []
        for row in rows:
            if not isinstance(row, list) or len(row) != 9 or any(not isinstance(field, str) for field in row):
                raise ValueError("each row must contain exactly nine strings")
            try:
                ts = int(row[0])
            except ValueError as error:
                raise ValueError("timestamp must be an integer minute") from error
            if str(ts) != row[0] or ts % 60000 != 0:
                raise ValueError("timestamp must be an integer minute")
            try:
                values = tuple(float(value) for value in row[1:5])
            except ValueError as error:
                raise ValueError("OHLC values must be finite and positive") from error
            if any(not math.isfinite(value) or value <= 0 for value in values):
                raise ValueError("OHLC values must be finite and positive")
            opening, high, low, close = values
            if high < max(opening, close, low) or low > min(opening, close, high):
                raise ValueError("high and low are inconsistent")
            bar = OkxBar(ts, opening, high, low, close)
            parsed.append((ts, bar if row[8] == "1" else None))

        for ts, bar in parsed:
            if bar is None or not start_ms <= ts < end_ms:
                continue
            previous = bars_by_ts.get(ts)
            if previous is not None and previous != bar:
                raise ValueError("conflicting duplicate timestamp")
            bars_by_ts[ts] = bar

        oldest = min(ts for ts, _ in parsed)
        if len(rows) < 300 or oldest <= start_ms:
            break
        after = oldest

    bars = tuple(bars_by_ts[ts] for ts in sorted(bars_by_ts))
    present = set(bars_by_ts)
    missing = tuple(ts for ts in range(start_ms, end_ms, 60000) if ts not in present)
    return CandleWindow(bars, missing)
