import type { EChartsCoreOption } from "echarts/core";

import type { Theme } from "../../shared/theme";
import {
  formatPresentationTimestamp,
  PRESENTATION_TIME_ZONE
} from "../../shared/time/presentation";
import type {
  TradeChartModel,
  TradeEvent,
  TradeMarker,
  TradeSide
} from "./tradeModel";

export type TradeChartCopy = {
  price: string;
  entry: string;
  exit: string;
  longEntry: string;
  longExit: string;
  shortEntry: string;
  shortExit: string;
};

function markerSeries(
  model: TradeChartModel,
  side: TradeSide,
  event: TradeEvent,
  name: string,
  color: string
) {
  return {
    id: `${side.toLowerCase()}-${event}`,
    name,
    type: "scatter",
    data: model.markers.filter(
      (marker) => marker.side === side && marker.event === event
    ),
    symbol: event === "entry" ? "triangle" : "diamond",
    symbolRotate: side === "SHORT" && event === "entry" ? 180 : 0,
    symbolSize: event === "entry" ? 13 : 11,
    itemStyle: {
      color,
      borderColor: event === "exit" ? "#ffffff" : color,
      borderWidth: event === "exit" ? 1 : 0
    },
    emphasis: { scale: 1.45 },
    z: 5
  };
}

export function tradeChartOption(
  model: TradeChartModel,
  copy: TradeChartCopy,
  locale: string,
  theme: Theme
): EChartsCoreOption {
  const dark = theme === "dark";
  const ink = dark ? "#e5ece9" : "#182128";
  const muted = dark ? "#94a29d" : "#66726f";
  const grid = dark ? "#3b4949" : "#cbd5d1";
  const up = dark ? "#49b2ae" : "#0e6b6f";
  const down = dark ? "#ef8a6b" : "#d46a4c";
  const axisDate = new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: PRESENTATION_TIME_ZONE
  });
  const tooltipDate = new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: PRESENTATION_TIME_ZONE,
    timeZoneName: "short"
  });
  const number = new Intl.NumberFormat(locale, {
    maximumFractionDigits: 8
  });

  return {
    animation: false,
    textStyle: { color: ink },
    grid: { left: 66, right: 22, top: 58, bottom: 76 },
    legend: {
      type: "scroll",
      top: 14,
      left: 12,
      right: 12,
      data: [
        copy.longEntry,
        copy.longExit,
        copy.shortEntry,
        copy.shortExit
      ],
      textStyle: { color: muted },
      itemWidth: 12,
      itemHeight: 8,
      pageIconColor: ink,
      pageIconInactiveColor: grid,
      pageTextStyle: { color: muted }
    },
    tooltip: {
      trigger: "item",
      renderMode: "richText",
      formatter: (raw: unknown) => {
        const item = raw as {
          data?: TradeMarker | [number, number, number, number];
          dataIndex?: number;
          seriesType?: string;
          seriesName?: string;
        };
        if (
          item.seriesType === "candlestick" &&
          typeof item.dataIndex === "number"
        ) {
          const candle = model.candles[item.dataIndex];
          const timestamp = model.timestamps[item.dataIndex];
          if (!candle || !timestamp) {
            return "";
          }
          return [
            formatPresentationTimestamp(timestamp, tooltipDate),
            `O ${number.format(candle[0])}`,
            `H ${number.format(candle[3])}`,
            `L ${number.format(candle[2])}`,
            `C ${number.format(candle[1])}`
          ].join("\n");
        }
        const marker = item.data as TradeMarker | undefined;
        if (!marker?.tradeId) {
          return "";
        }
        return [
          `${marker.tradeId} · ${marker.side}`,
          `${marker.event === "entry" ? copy.entry : copy.exit} ${number.format(
            marker.value[1]
          )}`,
          formatPresentationTimestamp(marker.value[0], tooltipDate)
        ].join("\n");
      }
    },
    xAxis: {
      type: "category",
      data: model.timestamps,
      boundaryGap: true,
      axisLine: { lineStyle: { color: grid } },
      axisLabel: {
        color: muted,
        hideOverlap: true,
        formatter: (value: string) =>
          formatPresentationTimestamp(value, axisDate)
      }
    },
    yAxis: {
      type: "value",
      scale: true,
      name: copy.price,
      nameTextStyle: { color: muted },
      splitLine: { lineStyle: { color: grid, opacity: 0.42 } },
      axisLabel: {
        color: muted,
        formatter: (value: number) => number.format(value)
      }
    },
    dataZoom: [
      {
        type: "inside",
        start: model.timestamps.length > 48 ? 45 : 0,
        end: 100
      },
      {
        type: "slider",
        start: model.timestamps.length > 48 ? 45 : 0,
        end: 100,
        bottom: 20,
        height: 20,
        borderColor: grid,
        fillerColor: dark
          ? "rgba(73, 178, 174, 0.16)"
          : "rgba(14, 107, 111, 0.12)",
        dataBackground: {
          lineStyle: { color: muted },
          areaStyle: { color: grid }
        },
        textStyle: { color: muted }
      }
    ],
    series: [
      {
        id: "candles",
        name: copy.price,
        type: "candlestick",
        data: model.candles,
        itemStyle: {
          color: up,
          color0: down,
          borderColor: up,
          borderColor0: down
        },
        z: 2
      },
      markerSeries(model, "LONG", "entry", copy.longEntry, up),
      markerSeries(model, "LONG", "exit", copy.longExit, up),
      markerSeries(model, "SHORT", "entry", copy.shortEntry, down),
      markerSeries(model, "SHORT", "exit", copy.shortExit, down)
    ]
  };
}

export function selectedTradeOption(
  model: TradeChartModel,
  tradeId: string | null
): EChartsCoreOption {
  return {
    series: [
      {
        id: "selected-trade",
        type: "scatter",
        data:
          tradeId === null
            ? []
            : model.markers.filter((marker) => marker.tradeId === tradeId),
        symbol: "circle",
        symbolSize: 20,
        itemStyle: {
          color: "transparent",
          borderColor: "#f0b94d",
          borderWidth: 3
        },
        silent: true,
        z: 8
      }
    ]
  };
}
