import { finiteDecimalNumber } from "../../shared/format/decimal";
import type { TradePage } from "../../shared/trading/closedTradePage";
import { parseUtcTimestamp } from "../../shared/time/utc";

export {
  mergeTradeItems,
  validLoadedTradePage,
  validTradePage
} from "../../shared/trading/closedTradePage";
export type { TradePage } from "../../shared/trading/closedTradePage";

export type EquitySample = {
  timestamp: string;
  equity: string;
  drawdown: string;
};

export type MonthlyReturn = {
  month: string;
  returnPct: string;
};

export type DistributionBucket = {
  lower: string | null;
  upper: string | null;
  count: number;
};

export type BacktestResultSnapshot = {
  jobId: string;
  strategyId: string;
  productId: string;
  timeframe: string;
  startedAt: string;
  endedAt: string;
  currency: string;
  metrics: {
    netPnl: string | null;
    returnPct: string | null;
    maxDrawdown: string | null;
    sharpe: string | null;
    sortino: string | null;
    calmar: string | null;
  };
  equity: EquitySample[];
  monthlyReturns: MonthlyReturn[];
  pnlDistribution: DistributionBucket[];
  tradePage: TradePage;
};

export function validateEquitySamples(
  samples: EquitySample[]
): EquitySample[] | null {
  let previousTimestamp = Number.NEGATIVE_INFINITY;
  for (const sample of samples) {
    const timestamp = parseUtcTimestamp(sample.timestamp);
    if (
      timestamp === null ||
      timestamp <= previousTimestamp ||
      finiteDecimalNumber(sample.equity) === null ||
      finiteDecimalNumber(sample.drawdown) === null
    ) {
      return null;
    }
    previousTimestamp = timestamp;
  }
  return samples;
}

export function validDistributionBuckets(
  buckets: DistributionBucket[]
): boolean {
  return buckets.every(
    (bucket) => Number.isSafeInteger(bucket.count) && bucket.count >= 0
  );
}
