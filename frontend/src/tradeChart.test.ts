import { describe, expect, it } from "vitest";

import {
  buildTradeChartModel,
  selectedTradeOption,
  tradeChartOption,
  tradeIdFromChartData,
  type TradeChartSnapshot
} from "./tradeChart";

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

describe("trade chart model", () => {
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

  it("isolates selected markers and ignores non-trade chart data", () => {
    const model = buildTradeChartModel(snapshot());
    const option = selectedTradeOption(model, "trade-1") as {
      series: [{ data: unknown[] }];
    };

    expect(option.series[0].data).toHaveLength(2);
    expect(tradeIdFromChartData(model.markers[0])).toBe("trade-1");
    expect(tradeIdFromChartData([19850, 19851, 19849, 19852])).toBeNull();
  });

  it("formats chart timestamps in UTC", () => {
    const option = tradeChartOption(
      buildTradeChartModel(snapshot()),
      {
        price: "Price",
        entry: "Entry",
        exit: "Exit",
        longEntry: "Long entry",
        longExit: "Long exit",
        shortEntry: "Short entry",
        shortExit: "Short exit"
      },
      "en",
      "light"
    ) as {
      tooltip: {
        formatter: (params: {
          seriesType: string;
          dataIndex: number;
        }) => string;
      };
      xAxis: {
        axisLabel: { formatter: (value: string) => string };
      };
    };

    expect(
      option.tooltip.formatter({
        seriesType: "candlestick",
        dataIndex: 0
      })
    ).toContain("13:30 UTC");
    expect(
      option.xAxis.axisLabel.formatter("2026-07-28T13:30:00.000Z")
    ).toContain("13:30");
  });
});
