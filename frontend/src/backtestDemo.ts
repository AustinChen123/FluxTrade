import type { BacktestResultSnapshot } from "./BacktestResultsView";
import { demoTradeSnapshot } from "./tradeDemo";

export const demoBacktestSnapshot: BacktestResultSnapshot = {
  jobId: "job-research-0042",
  strategyId: demoTradeSnapshot.strategyId,
  productId: demoTradeSnapshot.productId,
  timeframe: demoTradeSnapshot.timeframe,
  startedAt: demoTradeSnapshot.candles[0].timestamp,
  endedAt: demoTradeSnapshot.candles.at(-1)!.timestamp,
  currency: "USD",
  metrics: {
    netPnl: "-1.36",
    returnPct: "-0.00136",
    maxDrawdown: "3.12",
    sharpe: null,
    sortino: null,
    calmar: null
  },
  equity: [
    {
      timestamp: demoTradeSnapshot.candles[0].timestamp,
      equity: "100000.00",
      drawdown: "0.00"
    },
    {
      timestamp: demoTradeSnapshot.trades[0].exitTime,
      equity: "100000.88",
      drawdown: "0.00"
    },
    {
      timestamp: demoTradeSnapshot.trades[1].exitTime,
      equity: "99997.76",
      drawdown: "3.12"
    },
    {
      timestamp: demoTradeSnapshot.trades[2].exitTime,
      equity: "99998.64",
      drawdown: "2.24"
    }
  ],
  monthlyReturns: [{ month: "2026-07", returnPct: "-0.00136" }],
  pnlDistribution: [
    { lower: null, upper: "0", count: 1 },
    { lower: "0", upper: null, count: 2 }
  ],
  tradePage: {
    items: demoTradeSnapshot.trades,
    totalCount: demoTradeSnapshot.trades.length,
    nextCursor: null
  }
};
