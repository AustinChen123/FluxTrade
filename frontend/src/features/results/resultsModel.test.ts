import { describe, expect, it } from "vitest";

import type { ClosedTrade } from "../../shared/trading/closedTrade";
import {
  mergeTradeItems as sharedMergeTradeItems,
  validLoadedTradePage as sharedValidLoadedTradePage,
  validTradePage as sharedValidTradePage
} from "../../shared/trading/closedTradePage";
import type {
  BacktestResultsDetail,
  BacktestResultsIndexItem,
  BacktestResultsTradesPage
} from "../../api";
import {
  mergeTradeItems,
  projectBacktestResultDetail,
  projectBacktestResultsIndexItem,
  projectBacktestTradesPage,
  validDistributionBuckets,
  validLoadedTradePage,
  validTradePage,
  validateEquitySamples
} from "./resultsModel";

const trade: ClosedTrade = {
  id: "trade-1",
  side: "LONG",
  quantity: "1",
  entryTime: "2026-01-01T00:00:00Z",
  entryPrice: "100.00",
  exitTime: "2026-01-01T00:05:00Z",
  exitPrice: "101.00",
  fee: "0.25",
  pnl: "0.75"
};

const equitySample = {
  timestamp: "2026-01-01T00:00:00Z",
  equity: "100000.00",
  drawdown: "0.00"
};

const resultDigest = "1608d4e51f70afa1f3ba17bdfd588ea217686c71ee39615b3cc64486ed289bbd";
const indexItem: BacktestResultsIndexItem = {
  job_id: "job-c",
  subject_id: "summary-strategy:v1",
  dataset_id: "summary-dataset",
  product_id: "RITHMIC:MNQ-CONTINUOUS",
  timeframe: "1m",
  started_at: "2026-01-01T00:00:00.000Z",
  ended_at: "2026-01-01T00:01:00.000Z",
  completed_at: "2026-01-01T00:02:00.000Z",
  result_digest: resultDigest
};

const detail: BacktestResultsDetail = {
  job_id: "job-c",
  strategy_id: "summary-strategy:v1",
  subject_id: "summary-strategy:v1",
  dataset_id: "summary-dataset",
  product_id: "RITHMIC:MNQ-CONTINUOUS",
  timeframe: "1m",
  started_at: "2026-01-01T00:00:00.000Z",
  ended_at: "2026-01-01T00:01:00.000Z",
  currency: "USD",
  metrics: {
    net_pnl: "0.0000000000000000000001",
    return_pct: "0.0000000000000000000001",
    max_drawdown: "0",
    sharpe: "0.25",
    sortino: "0.5",
    calmar: "1"
  },
  equity: [{
    timestamp: "2026-01-01T00:00:00.000Z",
    equity: "100000.0000000000000000000001",
    drawdown: "0"
  }],
  monthly_returns: [{ month: "2026-01", return_pct: "0.0000000000000000000001" }],
  pnl_distribution: [{ lower: null, upper: "0", count: 0 }],
  trade_page: {
    items: [{
      id: "job-c:0",
      entry_time: "2026-01-01T00:00:00.000Z",
      exit_time: "2026-01-01T00:01:00.000Z",
      entry_price: "1.000000000000000001",
      exit_price: "1.000000000000000002",
      side: "LONG",
      quantity: "0.000000000000000001",
      pnl: "0.0000000000000000000001",
      fee: "0"
    }],
    total_count: 2,
    next_cursor: "opaque.cursor"
  },
  input_digest: resultDigest,
  result_digest: resultDigest,
  revision: 1
};

const supplementalTrades: BacktestResultsTradesPage = {
  items: detail.trade_page.items,
  next_cursor: null,
  revision: 1
};

describe("resultsModel", () => {
  it("keeps Results compatibility exports owned by shared closed-trade paging", () => {
    expect(mergeTradeItems).toBe(sharedMergeTradeItems);
    expect(validLoadedTradePage).toBe(sharedValidLoadedTradePage);
    expect(validTradePage).toBe(sharedValidTradePage);
  });

  it("preserves an exact valid equity sequence by identity", () => {
    const samples = [
      equitySample,
      {
        timestamp: "2026-01-01T00:05:00Z",
        equity: "100000.0000000000000000000001",
        drawdown: "0.0000000000000000000001"
      }
    ];

    expect(validateEquitySamples(samples)).toBe(samples);
  });

  it("projects an admissible index identity without changing timestamps or digest", () => {
    expect(projectBacktestResultsIndexItem(indexItem)).toEqual({
      jobId: "job-c",
      subjectId: "summary-strategy:v1",
      datasetId: "summary-dataset",
      productId: "RITHMIC:MNQ-CONTINUOUS",
      timeframe: "1m",
      startedAt: indexItem.started_at,
      endedAt: indexItem.ended_at,
      completedAt: indexItem.completed_at,
      resultDigest
    });
  });

  it("projects persisted detail with raw ratio and exact financial strings", () => {
    const snapshot = projectBacktestResultDetail(detail);
    expect(snapshot).not.toBeNull();
    expect(snapshot?.returnPctUnit).toBe("ratio");
    expect(snapshot?.metrics).toEqual({
      netPnl: detail.metrics.net_pnl,
      returnPct: detail.metrics.return_pct,
      maxDrawdown: detail.metrics.max_drawdown,
      sharpe: detail.metrics.sharpe,
      sortino: detail.metrics.sortino,
      calmar: detail.metrics.calmar
    });
    expect(snapshot?.equity[0].equity).toBe(detail.equity[0].equity);
    expect(snapshot?.monthlyReturns).toEqual([
      { month: "2026-01", returnPct: detail.monthly_returns[0].return_pct }
    ]);
    expect(snapshot?.pnlDistribution).toEqual(detail.pnl_distribution);
    expect(snapshot?.tradePage).toEqual({
      items: [{
        id: "job-c:0",
        side: "LONG",
        quantity: detail.trade_page.items[0].quantity,
        entryTime: detail.trade_page.items[0].entry_time,
        entryPrice: detail.trade_page.items[0].entry_price,
        exitTime: detail.trade_page.items[0].exit_time,
        exitPrice: detail.trade_page.items[0].exit_price,
        fee: "0",
        pnl: detail.trade_page.items[0].pnl
      }],
      totalCount: 2,
      nextCursor: "opaque.cursor"
    });
  });

  it("uses the detail count when projecting supplemental trade pages", () => {
    expect(projectBacktestTradesPage(supplementalTrades, detail.trade_page.total_count))
      .toEqual({
        items: projectBacktestResultDetail(detail)?.tradePage.items,
        totalCount: detail.trade_page.total_count,
        nextCursor: null
      });
    expect(projectBacktestTradesPage(supplementalTrades, 0)).toBeNull();
  });

  it.each([
    ["metric decimal", { metrics: { ...detail.metrics, net_pnl: "1e2" } }],
    ["equity UTC", { equity: [{ ...detail.equity[0], timestamp: "2026-02-30T00:00:00Z" }] }],
    ["equity decimal", { equity: [{ ...detail.equity[0], equity: "NaN" }] }],
    ["month", { monthly_returns: [{ month: "2026-13", return_pct: "0" }] }],
    ["monthly decimal", { monthly_returns: [{ month: "2026-01", return_pct: "Infinity" }] }],
    ["distribution decimal", { pnl_distribution: [{ lower: "1e2", upper: null, count: 0 }] }],
    ["distribution count", { pnl_distribution: [{ lower: null, upper: null, count: 0.5 }] }],
    ["trade side", { trade_page: { ...detail.trade_page, items: [{ ...detail.trade_page.items[0], side: "buy" }] } }],
    ["trade UTC", { trade_page: { ...detail.trade_page, items: [{ ...detail.trade_page.items[0], entry_time: "not-a-time" }] } }],
    ["trade order", { trade_page: { ...detail.trade_page, items: [{ ...detail.trade_page.items[0], entry_time: "2026-01-01T00:02:00.000Z" }] } }],
    ["trade decimal", { trade_page: { ...detail.trade_page, items: [{ ...detail.trade_page.items[0], pnl: "1e2" }] } }],
    ["trade page", { trade_page: { ...detail.trade_page, total_count: 0 } }],
    ["digest", { result_digest: "not-a-digest" }]
  ] satisfies Array<[string, Partial<BacktestResultsDetail>]>) (
    "rejects malformed persisted %s data",
    (_name, change) => {
      expect(projectBacktestResultDetail({ ...detail, ...change })).toBeNull();
    }
  );

  it("rejects malformed supplemental trade rows", () => {
    expect(projectBacktestTradesPage({
      ...supplementalTrades,
      items: [{ ...supplementalTrades.items[0], side: "buy" }]
    }, detail.trade_page.total_count)).toBeNull();
  });

  it("accepts exact decimal equity text even outside Number range", () => {
    const value = `1${"0".repeat(400)}`;
    const samples = [{ ...equitySample, equity: value }];
    expect(validateEquitySamples(samples)).toBe(samples);
    expect(projectBacktestResultDetail({
      ...detail,
      equity: [{ ...detail.equity[0], equity: value }]
    })?.equity[0].equity).toBe(value);
  });

  it.each(["started_at", "ended_at", "completed_at"])(
    "rejects an invalid index %s timestamp",
    (field) => {
      expect(projectBacktestResultsIndexItem({
        ...indexItem,
        [field]: "2026-02-30T00:00:00Z"
      })).toBeNull();
    }
  );

  it("rejects reversed index coverage and malformed result digests", () => {
    expect(projectBacktestResultsIndexItem({
      ...indexItem,
      started_at: "2026-01-01T00:02:00.000Z"
    })).toBeNull();
    expect(projectBacktestResultsIndexItem({
      ...indexItem,
      result_digest: "not-a-digest"
    })).toBeNull();
  });

  it("accepts a valid UTC timestamp at the Unix epoch", () => {
    expect(projectBacktestResultsIndexItem({
      ...indexItem,
      started_at: "1970-01-01T00:00:00.000Z"
    })).not.toBeNull();
  });

  it.each(["0x10", "1e2", "", "Infinity"])(
    "rejects non-decimal equity value %j",
    (equity) => {
      expect(validateEquitySamples([{ ...equitySample, equity }])).toBeNull();
    }
  );

  it.each([
    ["duplicate", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"],
    ["descending", "2026-01-01T00:05:00Z", "2026-01-01T00:00:00Z"]
  ])("rejects %s equity timestamps", (_name, first, second) => {
    expect(
      validateEquitySamples([
        { ...equitySample, timestamp: first },
        { ...equitySample, timestamp: second }
      ])
    ).toBeNull();
  });

  it.each([-1, 1.5, Number.NaN])(
    "rejects invalid distribution count %s",
    (count) => {
      expect(
        validDistributionBuckets([{ lower: null, upper: "0", count }])
      ).toBe(false);
    }
  );

  it("distinguishes a page shape from a complete loaded page", () => {
    const partialWithoutCursor = {
      items: [trade],
      totalCount: 2,
      nextCursor: null
    };

    expect(validTradePage(partialWithoutCursor)).toBe(true);
    expect(validLoadedTradePage(partialWithoutCursor)).toBe(false);
    expect(
      validLoadedTradePage({ items: [], totalCount: 0, nextCursor: null })
    ).toBe(true);
  });

  it("rejects malformed cursors, totals, IDs, and duplicate IDs", () => {
    expect(
      validTradePage({ items: [trade], totalCount: 1, nextCursor: " " })
    ).toBe(false);
    expect(
      validTradePage({ items: [trade], totalCount: 0, nextCursor: null })
    ).toBe(false);
    expect(
      validTradePage({
        items: [{ ...trade, id: "" }],
        totalCount: 1,
        nextCursor: null
      })
    ).toBe(false);
    expect(
      validTradePage({
        items: [trade, { ...trade }],
        totalCount: 2,
        nextCursor: null
      })
    ).toBe(false);
  });

  it.each([
    ["negative total", { items: [], totalCount: -1, nextCursor: null }],
    ["fractional total", { items: [], totalCount: 0.5, nextCursor: null }],
    ["blank cursor", { items: [], totalCount: 1, nextCursor: " " }],
    [
      "complete page with a cursor",
      { items: [trade], totalCount: 1, nextCursor: "cursor-1" }
    ],
    [
      "incomplete page without a cursor",
      { items: [trade], totalCount: 2, nextCursor: null }
    ]
  ] satisfies Array<
    [
      string,
      {
        items: ClosedTrade[];
        totalCount: number;
        nextCursor: string | null;
      }
    ]
  >)(
    "rejects an invalid loaded-page state: %s",
    (_name, page) => {
      expect(validLoadedTradePage(page)).toBe(false);
    }
  );

  it("merges new and identical trades but rejects a conflicting duplicate", () => {
    const second = { ...trade, id: "trade-2" };

    const merged = mergeTradeItems([trade], [{ ...trade }, second]);
    expect(merged).toEqual([trade, second]);
    expect(merged?.[0]).toBe(trade);
    expect(mergeTradeItems([trade], [{ ...trade, pnl: "999.00" }])).toBeNull();
  });

  it.each([
    ["side", { side: "SHORT" }],
    ["quantity", { quantity: "2" }],
    ["entryTime", { entryTime: "2026-01-01T00:01:00Z" }],
    ["entryPrice", { entryPrice: "100.0" }],
    ["exitTime", { exitTime: "2026-01-01T00:06:00Z" }],
    ["exitPrice", { exitPrice: "101.0" }],
    ["fee", { fee: "0.250" }],
    ["pnl", { pnl: "0.750" }]
  ] satisfies Array<[string, Partial<ClosedTrade>]>)(
    "rejects a duplicate ID whose %s differs by strict value",
    (_field, change) => {
      expect(mergeTradeItems([trade], [{ ...trade, ...change }])).toBeNull();
    }
  );

  it("rejects duplicate IDs within one incoming page before merging", () => {
    expect(
      validTradePage({
        items: [trade, trade],
        totalCount: 2,
        nextCursor: null
      })
    ).toBe(false);
    expect(
      validTradePage({
        items: [trade, { ...trade }],
        totalCount: 2,
        nextCursor: null
      })
    ).toBe(false);
    expect(
      validTradePage({
        items: [trade, { ...trade, fee: "0.250" }],
        totalCount: 2,
        nextCursor: null
      })
    ).toBe(false);
  });
});
