import { describe, expect, it } from "vitest";

import type {
  BacktestResultsCandlesPage,
  BacktestResultsDetail
} from "../../api";
import {
  buildTradeChartModel,
  projectBacktestCandlesPage,
  projectBacktestTradeIdentity,
  tradeCandleWindow,
  tradeIdFromChartData,
  type TradeChartSnapshot
} from "./tradeModel";

const snapshot = (overrides: Partial<TradeChartSnapshot> = {}): TradeChartSnapshot => ({
  strategyId: "strategy-1",
  productId: "CME:MNQ",
  timeframe: "5m",
  candles: [
    {
      timestamp: "2026-07-28T13:30:00.000Z",
      open: "19850.00",
      high: "19852.00",
      low: "19849.00",
      close: "19851.00",
      volume: "100"
    },
    {
      timestamp: "2026-07-28T13:35:00.000Z",
      open: "19851.00",
      high: "19854.00",
      low: "19850.00",
      close: "19853.00",
      volume: "120"
    }
  ],
  trades: [
    {
      id: "trade-1",
      side: "LONG",
      quantity: "1",
      entryTime: "2026-07-28T13:30:00.000Z",
      entryPrice: "19850.50",
      exitTime: "2026-07-28T13:35:00.000Z",
      exitPrice: "19853.00",
      fee: "1.12",
      pnl: "3.88"
    }
  ],
  ...overrides
});

type TradeDetailInput = Pick<
  BacktestResultsDetail,
  "job_id" | "strategy_id" | "product_id" | "timeframe" | "currency" |
  "started_at" | "ended_at" | "trade_page"
>;

function candlePage(
  items: BacktestResultsCandlesPage["items"]
): BacktestResultsCandlesPage {
  return { items, next_cursor: null, revision: 1 };
}

const candleWindow = {
  start: Date.parse("2026-07-28T13:30:00.000Z"),
  end: Date.parse("2026-07-28T13:35:00.000Z")
};

const wireTrade = {
  id: "job-1:0",
  entry_time: "2026-07-28T13:32:01.000Z",
  exit_time: "2026-07-28T13:36:00.000Z",
  entry_price: "19850.0000000000000001",
  exit_price: "19853.0000000000000002",
  side: "LONG",
  quantity: "0.000000000000000003",
  pnl: "0.000000000000000004",
  fee: "0.000000000000000005"
};

function tradeDetail(overrides: Partial<TradeDetailInput> = {}): TradeDetailInput {
  return {
    job_id: "job-1",
    strategy_id: "strategy-1",
    product_id: "CME:MNQ",
    timeframe: "5m",
    currency: "USDT",
    started_at: "2026-07-28T13:00:00.000Z",
    ended_at: "2026-07-28T14:00:00.000Z",
    trade_page: {
      items: [wireTrade],
      total_count: 1,
      next_cursor: null
    },
    ...overrides
  };
}

describe("trade chart model", () => {
  it("keeps exact candle Decimal strings and preserves identical-duplicate quality handling", () => {
    const candle = {
      timestamp: "2026-07-28T13:30:00.000Z",
      open: "19850.0000000000000000001",
      high: "19852.0000000000000000002",
      low: "19849.0000000000000000003",
      close: "19851.0000000000000000004",
      volume: "100.0000000000000000005"
    };
    const projected = projectBacktestCandlesPage(
      candlePage([candle, { ...candle }]),
      candleWindow
    );
    expect(projected).toEqual([candle, candle]);
    const chart = buildTradeChartModel(snapshot({ candles: projected ?? [] }));
    expect(chart.timestamps).toEqual([candle.timestamp]);
    expect(chart.skippedCandles).toBe(1);
  });

  it("rejects malformed, conflicting duplicate, and out-of-window candle pages", () => {
    const candle = {
      timestamp: "2026-07-28T13:30:00.000Z",
      open: "1", high: "2", low: "0", close: "1", volume: "3"
    };
    expect(projectBacktestCandlesPage(
      candlePage([{ ...candle, close: "1e2" }]), candleWindow
    )).toBeNull();
    expect(projectBacktestCandlesPage(
      candlePage([{ ...candle, timestamp: "2026-02-31T13:30:00.000Z" }]),
      candleWindow
    )).toBeNull();
    expect(projectBacktestCandlesPage(
      candlePage([{ ...candle, timestamp: "2026-07-28T13:35:00.000Z" }]),
      candleWindow
    )).toBeNull();
    expect(projectBacktestCandlesPage(
      candlePage([candle, { ...candle, close: "1.00" }]), candleWindow
    )).toBeNull();
  });

  it("projects only Trades metadata and preserves its exact quote and shared first page", () => {
    const metadata = projectBacktestTradeIdentity(tradeDetail());

    expect(metadata).toEqual({
      resultId: "job-1",
      strategyId: "strategy-1",
      productId: "CME:MNQ",
      currency: "USDT",
      timeframe: "5m",
      timeframeMs: 300_000,
      coverageStart: Date.parse("2026-07-28T13:00:00.000Z"),
      coverageEnd: Date.parse("2026-07-28T14:00:00.001Z"),
      tradePage: {
        items: [{
          id: wireTrade.id,
          side: "LONG",
          quantity: wireTrade.quantity,
          entryTime: wireTrade.entry_time,
          entryPrice: wireTrade.entry_price,
          exitTime: wireTrade.exit_time,
          exitPrice: wireTrade.exit_price,
          fee: wireTrade.fee,
          pnl: wireTrade.pnl
        }],
        totalCount: 1,
        nextCursor: null
      }
    });
  });

  it.each([
    ["1s", 1_000],
    ["2m", 120_000],
    ["3h", 10_800_000],
    ["4d", 345_600_000]
  ])("converts supported decision timeframe %s", (timeframe, milliseconds) => {
    expect(projectBacktestTradeIdentity(
      tradeDetail({ timeframe })
    )?.timeframeMs).toBe(milliseconds);
  });

  it.each([
    "0m",
    "-1m",
    "1M",
    "1.5m",
    "1x",
    " 1m",
    "9007199254740991d"
  ])("rejects malformed or unrepresentable timeframe %s", (timeframe) => {
    expect(projectBacktestTradeIdentity(
      tradeDetail({ timeframe })
    )).toBeNull();
  });

  it("pads unaligned trade instants using decision timeframe before clipping", () => {
    const metadata = projectBacktestTradeIdentity(tradeDetail());
    expect(metadata).not.toBeNull();
    expect(tradeCandleWindow(metadata!, {
      ...snapshot().trades[0],
      entryTime: wireTrade.entry_time,
      exitTime: wireTrade.exit_time
    })).toEqual({
      start: Date.parse("2026-07-28T13:20:00.000Z"),
      end: Date.parse("2026-07-28T13:50:00.000Z")
    });
  });

  it("clips to inclusive result coverage converted to a half-open end plus one millisecond", () => {
    const metadata = projectBacktestTradeIdentity(tradeDetail({
      started_at: "2026-07-28T13:30:00.000Z",
      ended_at: "2026-07-28T13:40:00.000Z",
      trade_page: {
        items: [{ ...wireTrade, exit_time: "2026-07-28T13:40:00.000Z" }],
        total_count: 1,
        next_cursor: null
      }
    }));
    expect(metadata?.coverageEnd).toBe(Date.parse("2026-07-28T13:40:00.001Z"));
    expect(metadata && tradeCandleWindow(metadata, {
      ...snapshot().trades[0],
      entryTime: wireTrade.entry_time,
      exitTime: "2026-07-28T13:40:00.000Z"
    })).toEqual({
      start: Date.parse("2026-07-28T13:30:00.000Z"),
      end: Date.parse("2026-07-28T13:40:00.001Z")
    });
  });

  it("preserves a single inclusive millisecond of result coverage", () => {
    const instant = "2026-07-28T13:40:00.000Z";
    const metadata = projectBacktestTradeIdentity(tradeDetail({
      timeframe: "1s",
      started_at: instant,
      ended_at: instant,
      trade_page: {
        items: [{ ...wireTrade, entry_time: instant, exit_time: instant }],
        total_count: 1,
        next_cursor: null
      }
    }));
    expect(metadata && tradeCandleWindow(metadata, {
      ...snapshot().trades[0], entryTime: instant, exitTime: instant
    })).toEqual({
      start: Date.parse(instant),
      end: Date.parse(instant) + 1
    });
  });

  it.each([
    ["reversed", "2026-07-28T13:36:00.000Z", "2026-07-28T13:32:01.000Z"],
    ["before coverage", "2026-07-28T12:59:59.999Z", "2026-07-28T13:00:00.000Z"],
    ["after coverage", "2026-07-28T14:00:00.001Z", "2026-07-28T14:00:00.001Z"],
    ["malformed UTC", "not-a-time", "not-a-time"]
  ])("rejects %s selected-trade timing", (_name, entryTime, exitTime) => {
    const metadata = projectBacktestTradeIdentity(tradeDetail());
    expect(metadata && tradeCandleWindow(metadata, {
      ...snapshot().trades[0], entryTime, exitTime
    })).toBeNull();
  });

  it("keeps exact candle and execution timestamps", () => {
    const model = buildTradeChartModel(snapshot());

    expect(model.timestamps).toEqual([
      "2026-07-28T13:30:00.000Z",
      "2026-07-28T13:35:00.000Z"
    ]);
    expect(model.candles).toEqual([
      [19850, 19851, 19849, 19852],
      [19851, 19853, 19850, 19854]
    ]);
    expect(model.markers.map((marker) => marker.value[0])).toEqual(
      model.timestamps
    );
    expect(model.skippedCandles).toBe(0);
    expect(model.skippedMarkers).toBe(0);
  });

  it("does not move an execution to the nearest candle", () => {
    const model = buildTradeChartModel(
      snapshot({
        trades: [
          {
            ...snapshot().trades[0],
            entryTime: "2026-07-28T13:30:01.000Z"
          }
        ]
      })
    );

    expect(model.markers).toHaveLength(1);
    expect(model.markers[0].event).toBe("exit");
    expect(model.skippedMarkers).toBe(1);
  });

  it.each(["2026-07-28T13:30:00Z", "2026-07-28T13:30:00+00:00"])(
    "matches equivalent UTC timestamp %s to the exact candle instant",
    (entryTime) => {
      const model = buildTradeChartModel(
        snapshot({
          trades: [{ ...snapshot().trades[0], entryTime }]
        })
      );

      expect(model.markers).toHaveLength(2);
      expect(model.markers[0].value[0]).toBe(
        "2026-07-28T13:30:00.000Z"
      );
      expect(model.skippedMarkers).toBe(0);
    }
  );

  it.each([
    "2026-02-29T13:30:00Z",
    "2026-02-31T13:30:00Z",
    "2026-07-28T24:00:00Z"
  ])("rejects invalid UTC calendar timestamp %s", (timestamp) => {
    const model = buildTradeChartModel(
      snapshot({
        candles: [{ ...snapshot().candles[0], timestamp }],
        trades: []
      })
    );

    expect(model.timestamps).toEqual([]);
    expect(model.skippedCandles).toBe(1);
  });

  it("drops invalid or duplicate candles without fabricating replacements", () => {
    const valid = snapshot().candles[0];
    const model = buildTradeChartModel(
      snapshot({
        candles: [
          valid,
          { ...valid },
          {
            ...snapshot().candles[1],
            high: "19849.00"
          }
        ]
      })
    );

    expect(model.timestamps).toEqual([valid.timestamp]);
    expect(model.skippedCandles).toBe(2);
    expect(model.markers).toHaveLength(1);
    expect(model.skippedMarkers).toBe(1);
  });

  it("deduplicates equivalent timestamp forms by exact instant", () => {
    const first = snapshot().candles[0];
    const model = buildTradeChartModel(
      snapshot({
        candles: [
          first,
          {
            ...first,
            timestamp: "2026-07-28T13:30:00+00:00"
          }
        ],
        trades: []
      })
    );

    expect(model.timestamps).toEqual([first.timestamp]);
    expect(model.skippedCandles).toBe(1);
  });

  it("rejects a descending candle without changing the accepted order", () => {
    const candles = snapshot().candles;
    const model = buildTradeChartModel(
      snapshot({ candles: [candles[1], candles[0]], trades: [] })
    );

    expect(model.timestamps).toEqual([candles[1].timestamp]);
    expect(model.skippedCandles).toBe(1);
  });

  it.each(["0x10", "1e2", "", "Infinity"])(
    "rejects non-decimal candle value %j",
    (open) => {
      const model = buildTradeChartModel(
        snapshot({
          candles: [{ ...snapshot().candles[0], open }],
          trades: []
        })
      );

      expect(model.timestamps).toEqual([]);
      expect(model.skippedCandles).toBe(1);
    }
  );

  it("accepts only a non-empty exact trade ID from chart data", () => {
    const model = buildTradeChartModel(snapshot());

    expect(tradeIdFromChartData(model.markers[0])).toBe("trade-1");
    expect(tradeIdFromChartData([19850, 19851, 19849, 19852])).toBeNull();
    expect(tradeIdFromChartData({ tradeId: "" })).toBeNull();
  });

  it("counts an invalid marker price without moving its other event", () => {
    const model = buildTradeChartModel(
      snapshot({
        trades: [{ ...snapshot().trades[0], entryPrice: "1e2" }]
      })
    );

    expect(model.markers.map((marker) => marker.event)).toEqual(["exit"]);
    expect(model.skippedMarkers).toBe(1);
  });

});
