import { finiteDecimalNumber } from "../../decimal";
import type { ClosedTrade } from "../../shared/trading/closedTrade";
import { parseUtcTimestamp } from "../../utc";

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

export function tradeIdFromChartData(data: unknown): string | null {
  if (!data || typeof data !== "object" || !("tradeId" in data)) {
    return null;
  }
  const tradeId = (data as { tradeId?: unknown }).tradeId;
  return typeof tradeId === "string" && tradeId.length > 0 ? tradeId : null;
}
