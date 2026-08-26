import { describe, expect, it } from "vitest";

import {
  selectedTradeOption,
  tradeChartOption
} from "./tradeCharts";
import {
  buildTradeChartModel,
  type TradeChartSnapshot
} from "./tradeModel";

const snapshot: TradeChartSnapshot = {
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
  ]
};

describe("trade chart presentation", () => {
  it("projects every trade event into exactly one owned series", () => {
    const option = tradeChartOption(
      buildTradeChartModel(snapshot),
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
      series: { id: string; data: unknown[] }[];
    };

    expect(
      option.series.map((series) => [series.id, series.data.length])
    ).toEqual([
      ["candles", 2],
      ["long-entry", 1],
      ["long-exit", 1],
      ["short-entry", 0],
      ["short-exit", 0]
    ]);
  });

  it.each([
    ["trade-1", 2],
    ["missing", 0],
    [null, 0]
  ] as const)("isolates selected markers for %j", (tradeId, count) => {
    const model = buildTradeChartModel(snapshot);
    const option = selectedTradeOption(model, tradeId) as {
      series: [{ data: unknown[] }];
    };

    expect(option.series[0].data).toHaveLength(count);
  });

  it("formats chart timestamps in UTC", () => {
    const option = tradeChartOption(
      buildTradeChartModel(snapshot),
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
