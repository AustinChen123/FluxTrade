import type { EChartsCoreOption } from "echarts/core";

import { finiteDecimalNumber } from "../../shared/format/decimal";
import type { Locale } from "../../shared/i18n";
import type { Theme } from "../../shared/theme";
import { parseUtcTimestamp } from "../../shared/time/utc";
import type { BacktestResultSnapshot } from "./resultsModel";

function displayTimestamp(
  value: string,
  formatter: Intl.DateTimeFormat
): string {
  const timestamp = parseUtcTimestamp(value);
  return timestamp === null ? "—" : formatter.format(timestamp);
}

export function resultChartOption(
  snapshot: BacktestResultSnapshot,
  locale: Locale,
  theme: Theme,
  equityLabel: string,
  drawdownLabel: string,
  money: Intl.NumberFormat | null
): EChartsCoreOption {
  const dark = theme === "dark";
  const ink = dark ? "#e5ece9" : "#182128";
  const muted = dark ? "#94a29d" : "#66726f";
  const grid = dark ? "#3b4949" : "#cbd5d1";
  const axisTimestamp = new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC"
  });
  const tooltipTimestamp = new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC",
    timeZoneName: "short"
  });
  const axisNumber = new Intl.NumberFormat(locale, {
    maximumFractionDigits: 2,
    useGrouping: true
  });
  const timestamps = snapshot.equity.map((sample) => sample.timestamp);

  return {
    animation: false,
    textStyle: { color: ink },
    tooltip: {
      trigger: "axis",
      backgroundColor: dark ? "#182226" : "#f8faf9",
      borderColor: grid,
      textStyle: { color: ink },
      axisPointer: {
        label: {
          formatter: ({ value }: { value: unknown }) =>
            displayTimestamp(String(value), tooltipTimestamp)
        }
      },
      valueFormatter: (value: unknown) => {
        const parsed = typeof value === "number" ? value : Number(value);
        return Number.isFinite(parsed) && money ? money.format(parsed) : "—";
      }
    },
    legend: {
      top: 4,
      right: 12,
      textStyle: { color: muted },
      data: [equityLabel, drawdownLabel]
    },
    grid: [
      { left: 76, right: 24, top: 42, height: "52%" },
      { left: 76, right: 24, top: "72%", height: "17%" }
    ],
    xAxis: [
      {
        type: "category",
        data: timestamps,
        boundaryGap: false,
        axisLabel: { show: false },
        axisLine: { lineStyle: { color: grid } },
        splitLine: { show: false }
      },
      {
        type: "category",
        gridIndex: 1,
        data: timestamps,
        boundaryGap: false,
        axisLabel: {
          color: muted,
          hideOverlap: true,
          formatter: (value: string) => displayTimestamp(value, axisTimestamp)
        },
        axisLine: { lineStyle: { color: grid } }
      }
    ],
    yAxis: [
      {
        type: "value",
        scale: true,
        axisLabel: {
          color: muted,
          formatter: (value: number) => axisNumber.format(value)
        },
        splitLine: { lineStyle: { color: grid } }
      },
      {
        type: "value",
        gridIndex: 1,
        inverse: true,
        min: 0,
        axisLabel: {
          color: muted,
          formatter: (value: number) => money?.format(value) ?? "—"
        },
        splitLine: { lineStyle: { color: grid } }
      }
    ],
    series: [
      {
        id: "equity",
        name: equityLabel,
        type: "line",
        data: snapshot.equity.map((sample) =>
          finiteDecimalNumber(sample.equity)
        ),
        symbol: "none",
        lineStyle: { color: dark ? "#49b2ae" : "#0e6b6f", width: 2 },
        itemStyle: { color: dark ? "#49b2ae" : "#0e6b6f" },
        areaStyle: {
          color: dark ? "rgba(73,178,174,.12)" : "rgba(14,107,111,.09)"
        }
      },
      {
        id: "drawdown",
        name: drawdownLabel,
        type: "line",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: snapshot.equity.map((sample) =>
          finiteDecimalNumber(sample.drawdown)
        ),
        symbol: "none",
        lineStyle: { color: dark ? "#ef8a6b" : "#d46a4c", width: 1.5 },
        itemStyle: { color: dark ? "#ef8a6b" : "#d46a4c" },
        areaStyle: {
          color: dark ? "rgba(239,138,107,.26)" : "rgba(212,106,76,.2)"
        }
      }
    ]
  };
}
