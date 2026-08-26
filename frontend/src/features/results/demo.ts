import type { ClosedTrade } from "../../shared/trading/closedTrade";
import type { BacktestResultSnapshot } from "./resultsModel";

const STARTED_AT = "2026-07-28T13:30:00.000Z";

function closedTrade(
  id: string,
  side: ClosedTrade["side"],
  entryTime: string,
  exitTime: string,
  entryPrice: string,
  exitPrice: string,
  pnl: string
): ClosedTrade {
  return {
    id,
    side,
    quantity: "1",
    entryTime,
    entryPrice,
    exitTime,
    exitPrice,
    fee: "1.12",
    pnl
  };
}

const trades: ClosedTrade[] = [
  closedTrade(
    "trade-000184",
    "LONG",
    "2026-07-28T14:30:00.000Z",
    "2026-07-28T15:30:00.000Z",
    "19851.25",
    "19852.25",
    "0.88"
  ),
  closedTrade(
    "trade-000185",
    "SHORT",
    "2026-07-28T16:20:00.000Z",
    "2026-07-28T17:00:00.000Z",
    "19853.00",
    "19854.00",
    "-3.12"
  ),
  closedTrade(
    "trade-000186",
    "LONG",
    "2026-07-28T17:55:00.000Z",
    "2026-07-28T19:05:00.000Z",
    "19855.00",
    "19856.00",
    "0.88"
  )
];

export const demoBacktestSnapshot: BacktestResultSnapshot = {
  jobId: "job-research-0042",
  strategyId: "golden_cross_research",
  productId: "CME:MNQ-CONTINUOUS",
  timeframe: "5m",
  startedAt: STARTED_AT,
  endedAt: "2026-07-28T20:25:00.000Z",
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
      timestamp: STARTED_AT,
      equity: "100000.00",
      drawdown: "0.00"
    },
    {
      timestamp: trades[0].exitTime,
      equity: "100000.88",
      drawdown: "0.00"
    },
    {
      timestamp: trades[1].exitTime,
      equity: "99997.76",
      drawdown: "3.12"
    },
    {
      timestamp: trades[2].exitTime,
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
    items: trades,
    totalCount: trades.length,
    nextCursor: null
  }
};
