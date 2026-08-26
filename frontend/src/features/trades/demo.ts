import type { ClosedTrade } from "../../shared/trading/closedTrade";
import type { TradeChartSnapshot } from "./tradeModel";

const START = Date.parse("2026-07-28T13:30:00Z");
const FIVE_MINUTES = 5 * 60 * 1_000;

function decimalFromCents(cents: number): string {
  const absolute = Math.abs(cents);
  const value = `${Math.floor(absolute / 100)}.${String(absolute % 100).padStart(2, "0")}`;
  return cents < 0 ? `-${value}` : value;
}

const candles = Array.from({ length: 84 }, (_, index) => {
  const trend = index * 9;
  const wave = ((index * 17) % 43) - 21;
  const open = 1_985_000 + trend + wave;
  const close = open + ((index * 29) % 35) - 17;
  const high = Math.max(open, close) + 14 + (index % 9);
  const low = Math.min(open, close) - 13 - ((index * 3) % 8);
  return {
    timestamp: new Date(START + index * FIVE_MINUTES).toISOString(),
    open: decimalFromCents(open),
    high: decimalFromCents(high),
    low: decimalFromCents(low),
    close: decimalFromCents(close),
    volume: String(820 + ((index * 137) % 1_900))
  };
});

function trade(
  id: string,
  side: ClosedTrade["side"],
  entryIndex: number,
  exitIndex: number,
  entryPrice: string,
  exitPrice: string,
  pnl: string
): ClosedTrade {
  return {
    id,
    side,
    quantity: "1",
    entryTime: candles[entryIndex].timestamp,
    entryPrice,
    exitTime: candles[exitIndex].timestamp,
    exitPrice,
    fee: "1.12",
    pnl
  };
}

export const demoTradeSnapshot: TradeChartSnapshot = {
  strategyId: "golden_cross_research",
  productId: "CME:MNQ-CONTINUOUS",
  timeframe: "5m",
  candles,
  trades: [
    trade("trade-000184", "LONG", 12, 24, "19851.25", "19852.25", "0.88"),
    trade("trade-000185", "SHORT", 34, 42, "19853.00", "19854.00", "-3.12"),
    trade("trade-000186", "LONG", 53, 67, "19855.00", "19856.00", "0.88")
  ]
};
