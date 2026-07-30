import type { EChartsCoreOption } from "echarts/core";

import { finiteDecimalNumber } from "./decimal";
import type { Theme } from "./theme";
import { parseUtcTimestamp } from "./utc";

export type TradeSide = "LONG" | "SHORT";
export type TradeEvent = "entry" | "exit";

export type TradeCandle = {
  timestamp: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
};

export type ClosedTrade = {
  id: string;
  side: TradeSide;
  quantity: string;
  entryTime: string;
  entryPrice: string;
  exitTime: string;
  exitPrice: string;
  fee: string;
  pnl: string;
};

export type TradeChartSnapshot = {
  strategyId: string;
  productId: string;
  timeframe: string;
  candles: TradeCandle[];
  trades: ClosedTrade[];
};

export type TradeMarker = {
  value: [string, number];
  tradeId: string;
  event: TradeEvent;
  side: TradeSide;
  price: string;
};

export type TradeChartModel = {
  timestamps: string[];
  candles: [number, number, number, number][];
  markers: TradeMarker[];
  skippedCandles: number;
  skippedMarkers: number;
};

export type TradeChartCopy = {
  price: string;
  entry: string;
  exit: string;
  longEntry: string;
  longExit: string;
  shortEntry: string;
  shortExit: string;
};

export function buildTradeChartModel(
  snapshot: TradeChartSnapshot
): TradeChartModel {
  const timestamps: string[] = [];
  const candles: [number, number, number, number][] = [];
  const timestampByInstant = new Map<number, string>();
  let skippedCandles = 0;
  let previousTimestamp = Number.NEGATIVE_INFINITY;

  for (const candle of snapshot.candles) {
    const open = finiteDecimalNumber(candle.open);
    const high = finiteDecimalNumber(candle.high);
    const low = finiteDecimalNumber(candle.low);
    const close = finiteDecimalNumber(candle.close);
    const volume = finiteDecimalNumber(candle.volume);
    const timestamp = parseUtcTimestamp(candle.timestamp);
    const validTimestamp =
      timestamp !== null && timestamp > previousTimestamp;
    const validRange =
      open !== null &&
      high !== null &&
      low !== null &&
      close !== null &&
      volume !== null &&
      volume >= 0 &&
      high >= Math.max(open, close) &&
      low <= Math.min(open, close);
    if (!validTimestamp || !validRange) {
      skippedCandles += 1;
      continue;
    }
    previousTimestamp = timestamp;
    timestamps.push(candle.timestamp);
    timestampByInstant.set(timestamp, candle.timestamp);
    candles.push([open, close, low, high]);
  }

  const markers: TradeMarker[] = [];
  let skippedMarkers = 0;
  for (const trade of snapshot.trades) {
    const events: [TradeEvent, string, string][] = [
      ["entry", trade.entryTime, trade.entryPrice],
      ["exit", trade.exitTime, trade.exitPrice]
    ];
    for (const [event, timestamp, price] of events) {
      const numericPrice = finiteDecimalNumber(price);
      const instant = parseUtcTimestamp(timestamp);
      const candleTimestamp =
        instant === null ? undefined : timestampByInstant.get(instant);
      if (candleTimestamp === undefined || numericPrice === null) {
        skippedMarkers += 1;
        continue;
      }
      markers.push({
        value: [candleTimestamp, numericPrice],
        tradeId: trade.id,
        event,
        side: trade.side,
        price
      });
    }
  }

  return {
    timestamps,
    candles,
    markers,
    skippedCandles,
    skippedMarkers
  };
}

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
    timeZone: "UTC"
  });
  const tooltipDate = new Intl.DateTimeFormat(locale, {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC",
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
            tooltipDate.format(new Date(timestamp)),
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
          tooltipDate.format(new Date(marker.value[0]))
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
        formatter: (value: string) => axisDate.format(new Date(value))
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
        fillerColor: dark ? "rgba(73, 178, 174, 0.16)" : "rgba(14, 107, 111, 0.12)",
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

export function tradeIdFromChartData(data: unknown): string | null {
  if (!data || typeof data !== "object" || !("tradeId" in data)) {
    return null;
  }
  const tradeId = (data as { tradeId?: unknown }).tradeId;
  return typeof tradeId === "string" && tradeId.length > 0 ? tradeId : null;
}
