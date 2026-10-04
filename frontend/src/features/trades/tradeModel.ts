import { finiteDecimalNumber } from "../../shared/format/decimal";
import type { BacktestResultsDetail } from "../../api";
import type { ClosedTrade } from "../../shared/trading/closedTrade";
import { projectClosedTradePage } from "../../shared/trading/closedTradePage";
import type { TradePage } from "../../shared/trading/closedTradePage";
import { parseUtcTimestamp } from "../../shared/time/utc";

const MAX_UTC_MILLISECONDS = 253_402_300_799_999;

type TradeDetailInput = Pick<
  BacktestResultsDetail,
  "job_id" | "strategy_id" | "product_id" | "timeframe" | "currency" |
  "started_at" | "ended_at" | "trade_page"
>;

export type BacktestTradeIdentity = {
  resultId: string;
  strategyId: string;
  productId: string;
  currency: string;
  timeframe: string;
  timeframeMs: number;
  coverageStart: number;
  coverageEnd: number;
  tradePage: TradePage;
};

export type TradeCandleWindow = { start: number; end: number };

function timeframeMilliseconds(timeframe: string): number | null {
  const match = /^([0-9]+)([smhd])$/.exec(timeframe);
  if (!match) return null;
  const amount = Number(match[1]);
  const unitMilliseconds = {
    s: 1_000,
    m: 60_000,
    h: 3_600_000,
    d: 86_400_000
  }[match[2] as "s" | "m" | "h" | "d"];
  const duration = amount * unitMilliseconds;
  return Number.isSafeInteger(amount) && amount > 0 &&
    Number.isSafeInteger(duration) && duration > 0
    ? duration
    : null;
}

export function projectBacktestTradeIdentity(
  detail: TradeDetailInput
): BacktestTradeIdentity | null {
  const timeframeMs = timeframeMilliseconds(detail.timeframe);
  const coverageStart = parseUtcTimestamp(detail.started_at);
  const coverageEndInclusive = parseUtcTimestamp(detail.ended_at);
  if (
    timeframeMs === null || coverageStart === null || coverageEndInclusive === null ||
    coverageStart < 0 || coverageStart > coverageEndInclusive
  ) {
    return null;
  }
  const coverageEnd = coverageEndInclusive + 1;
  if (
    !Number.isSafeInteger(coverageEnd) ||
    coverageEnd > MAX_UTC_MILLISECONDS
  ) {
    return null;
  }
  const tradePage = projectClosedTradePage(
    detail.trade_page.items,
    detail.trade_page.total_count,
    detail.trade_page.next_cursor,
    true
  );
  if (tradePage === null) return null;
  return {
    resultId: detail.job_id,
    strategyId: detail.strategy_id,
    productId: detail.product_id,
    currency: detail.currency,
    timeframe: detail.timeframe,
    timeframeMs,
    coverageStart,
    coverageEnd,
    tradePage
  };
}

export function tradeCandleWindow(
  identity: BacktestTradeIdentity,
  trade: ClosedTrade
): TradeCandleWindow | null {
  const entry = parseUtcTimestamp(trade.entryTime);
  const exit = parseUtcTimestamp(trade.exitTime);
  if (
    entry === null || exit === null || entry > exit ||
    entry < identity.coverageStart || exit < identity.coverageStart ||
    entry >= identity.coverageEnd || exit >= identity.coverageEnd
  ) {
    return null;
  }
  const timeframeMs = identity.timeframeMs;
  const rawStart = Math.floor(entry / timeframeMs) * timeframeMs - 2 * timeframeMs;
  const rawEnd = Math.floor(exit / timeframeMs) * timeframeMs + 3 * timeframeMs;
  if (!Number.isSafeInteger(rawStart) || !Number.isSafeInteger(rawEnd)) return null;
  const start = Math.max(identity.coverageStart, rawStart);
  const end = Math.min(identity.coverageEnd, rawEnd);
  return Number.isSafeInteger(start) && Number.isSafeInteger(end) && start < end
    ? { start, end }
    : null;
}

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
