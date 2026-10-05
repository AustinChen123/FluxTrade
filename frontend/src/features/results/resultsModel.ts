import type {
  BacktestResultsDetail,
  BacktestResultsIndexItem,
  BacktestResultsTradesPage
} from "../../api";
import { isDecimalString } from "../../shared/format/decimal";
import {
  projectClosedTradePage,
  validLoadedTradePage,
  validTradePage,
  type TradePage
} from "../../shared/trading/closedTradePage";
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

export type ReturnPctUnit = "ratio" | "percentage-points";

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
  returnPctUnit?: ReturnPctUnit;
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
      !isDecimalString(sample.equity) ||
      !isDecimalString(sample.drawdown)
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
  return buckets.every((bucket) =>
    Number.isSafeInteger(bucket.count) && bucket.count >= 0 &&
    (bucket.lower === null || isDecimalString(bucket.lower)) &&
    (bucket.upper === null || isDecimalString(bucket.upper))
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

export function projectBacktestTradesPage(
  page: BacktestResultsTradesPage,
  totalCount: number
): TradePage | null {
  return projectClosedTradePage(page.items, totalCount, page.next_cursor, false);
}

export function projectBacktestResultDetail(
  detail: BacktestResultsDetail
): BacktestResultSnapshot | null {
  const startedAt = parseUtcTimestamp(detail.started_at);
  const endedAt = parseUtcTimestamp(detail.ended_at);
  const metrics = [
    detail.metrics.net_pnl,
    detail.metrics.return_pct,
    detail.metrics.max_drawdown,
    detail.metrics.sharpe,
    detail.metrics.sortino,
    detail.metrics.calmar
  ];
  const equity = validateEquitySamples(detail.equity);
  const monthlyReturnsValid = detail.monthly_returns.every((month) =>
    /^\d{4}-(?:0[1-9]|1[0-2])$/.test(month.month) &&
    isDecimalString(month.return_pct)
  );
  const distribution = detail.pnl_distribution.map(({ lower, upper, count }) => ({
    lower, upper, count
  }));
  const tradePage = projectClosedTradePage(
    detail.trade_page.items,
    detail.trade_page.total_count,
    detail.trade_page.next_cursor,
    true
  );
  if (
    startedAt === null || endedAt === null || startedAt > endedAt ||
    !metrics.every(isDecimalString) || equity === null ||
    !monthlyReturnsValid || !validDistributionBuckets(distribution) ||
    !validDigest(detail.input_digest) || !validDigest(detail.result_digest) ||
    tradePage === null
  ) {
    return null;
  }
  return {
    jobId: detail.job_id,
    strategyId: detail.strategy_id,
    productId: detail.product_id,
    timeframe: detail.timeframe,
    startedAt: detail.started_at,
    endedAt: detail.ended_at,
    currency: detail.currency,
    returnPctUnit: "ratio",
    metrics: {
      netPnl: detail.metrics.net_pnl,
      returnPct: detail.metrics.return_pct,
      maxDrawdown: detail.metrics.max_drawdown,
      sharpe: detail.metrics.sharpe,
      sortino: detail.metrics.sortino,
      calmar: detail.metrics.calmar
    },
    equity,
    monthlyReturns: detail.monthly_returns.map(({ month, return_pct }) => ({
      month,
      returnPct: return_pct
    })),
    pnlDistribution: distribution,
    tradePage
  };
}
