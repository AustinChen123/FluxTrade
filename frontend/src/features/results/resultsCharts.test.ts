import { describe, expect, it } from "vitest";

import { demoBacktestSnapshot } from "./demo";
import { resultChartOption } from "./resultsCharts";

function chartOption(theme: "light" | "dark" = "light") {
  return resultChartOption(
    demoBacktestSnapshot,
    "zh-TW",
    theme,
    "權益",
    "回撤",
    new Intl.NumberFormat("zh-TW", {
      style: "currency",
      currency: "USD"
    })
  ) as {
    tooltip: {
      axisPointer: {
        label: { formatter: (params: { value: unknown }) => string };
      };
    };
    xAxis: Array<{ data: string[] }>;
    yAxis: Array<{
      axisLabel: { formatter: (value: number) => string };
    }>;
    series: Array<{
      id: string;
      data: Array<number | null>;
      lineStyle: { color: string };
    }>;
  };
}

describe("resultsCharts", () => {
  it("projects the exact accepted equity timeline without deciding authority", () => {
    const option = chartOption();

    expect(option.xAxis[0].data).toEqual(
      demoBacktestSnapshot.equity.map((sample) => sample.timestamp)
    );
    expect(option.series.map((series) => series.id)).toEqual([
      "equity",
      "drawdown"
    ]);
    expect(option.series[0].data).toEqual([
      100000,
      100000.88,
      99997.76,
      99998.64
    ]);
    expect(
      option.tooltip.axisPointer.label.formatter({
        value: demoBacktestSnapshot.equity[0].timestamp
      })
    ).toContain("13:30");
  });

  it("keeps nearby accepted ticks distinct", () => {
    const option = resultChartOption(
      {
        ...demoBacktestSnapshot,
        equity: [
          {
            timestamp: "2026-01-01T00:00:00Z",
            equity: "100000.00",
            drawdown: "0.00"
          },
          {
            timestamp: "2026-01-01T00:05:00Z",
            equity: "100000.88",
            drawdown: "0.00"
          }
        ]
      },
      "en",
      "light",
      "Equity",
      "Drawdown",
      null
    ) as {
      yAxis: Array<{
        axisLabel: { formatter: (value: number) => string };
      }>;
    };

    expect(option.yAxis[0].axisLabel.formatter(100000)).not.toBe(
      option.yAxis[0].axisLabel.formatter(100000.88)
    );
  });

  it("applies the frozen light and dark palettes", () => {
    expect(chartOption("light").series.map((series) => series.lineStyle.color)).toEqual([
      "#0e6b6f",
      "#d46a4c"
    ]);
    expect(chartOption("dark").series.map((series) => series.lineStyle.color)).toEqual([
      "#49b2ae",
      "#ef8a6b"
    ]);
  });
});
