import type { BacktestResultsIndexItem } from "../../api";
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

export type BacktestResultIndexItem = {
  jobId: string;
  subjectId: string;
  datasetId: string;
  productId: string;
  timeframe: string;
  startedAt: string;
  endedAt: string;
  completedAt: string;
  resultDigest: string;
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

function validDigest(value: string): boolean {
  return /^[0-9a-f]{64}$/.test(value);
}

export function projectBacktestResultsIndexItem(
  item: BacktestResultsIndexItem
): BacktestResultIndexItem | null {
  const startedAt = parseUtcTimestamp(item.started_at);
  const endedAt = parseUtcTimestamp(item.ended_at);
  const completedAt = parseUtcTimestamp(item.completed_at);
  if (
    startedAt === null || endedAt === null || completedAt === null ||
    startedAt > endedAt ||
    !validDigest(item.result_digest)
  ) {
    return null;
  }
  return {
    jobId: item.job_id,
    subjectId: item.subject_id,
    datasetId: item.dataset_id,
    productId: item.product_id,
    timeframe: item.timeframe,
    startedAt: item.started_at,
    endedAt: item.ended_at,
    completedAt: item.completed_at,
    resultDigest: item.result_digest
  };
}
