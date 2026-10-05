import type {
  BacktestResultsCandlesPage,
  BacktestResultsDetail,
  BacktestResultsIndexItem,
  BacktestResultsIndexPage,
  BacktestResultsTradesPage
} from "../src/api";

export const BROWSER_NOW = "2026-08-23T12:00:00Z";
export const BROWSER_LOCALE = "de-DE";
export const BROWSER_TIME_ZONE = "UTC";

export const SCENARIO_IDS = [
  "direct-navigation",
  "research-cache",
  "navigation-serialization",
  "demo-dev",
  "demo-production-denied",
  "strategy-command",
  "locale-theme-reload",
  "lazy-chunk-inventory",
  "responsive-overflow",
  "berlin-presentation-time",
  "production-results-flow",
  "production-results-states",
  "production-results-candle-states",
  "production-results-stale-detail",
  "production-results-stale-trades",
  "production-results-stale-candles"
] as const;

export type ScenarioId = (typeof SCENARIO_IDS)[number];
export type ServerId = "dev" | "production";

export const CASE_IDS = {
  "direct-navigation": ["results", "strategies", "trades"],
  "research-cache": ["main"],
  "navigation-serialization": ["main"],
  "demo-dev": ["main"],
  "demo-production-denied": ["results", "trades"],
  "strategy-command": ["main"],
  "locale-theme-reload": ["main"],
  "lazy-chunk-inventory": ["main"],
  "responsive-overflow": ["research", "results", "strategies", "trades"],
  "berlin-presentation-time": ["research-api", "demo-features"],
  "production-results-flow": ["main"],
  "production-results-states": ["main"],
  "production-results-candle-states": ["main"],
  "production-results-stale-detail": ["main"],
  "production-results-stale-trades": ["main"],
  "production-results-stale-candles": ["main"]
} as const satisfies Record<ScenarioId, readonly string[]>;

export const BROWSER_SESSION = {
  actor: "frontend-smoke@example.invalid",
  capabilities: [],
  permissions: { can_mutate: true, can_step_up: true },
  csrf_token: "csrf-browser-smoke",
  expires_at: "2026-08-24T12:00:00Z",
  step_up_expires_at: null
} as const;

// Fixed synthetic wire DTOs for browser-contract checks; these are not economic evidence.
export const BACKTEST_RESULT_ID = "job-browser-001";
export const BACKTEST_TRADE_ID = `${BACKTEST_RESULT_ID}:0`;
export const BACKTEST_RESULT_INDEX: BacktestResultsIndexPage = {
  items: [],
  next_cursor: null,
  revision: 1
};
export const BACKTEST_RESULT_DETAIL: BacktestResultsDetail = {
  job_id: BACKTEST_RESULT_ID,
  strategy_id: "strategy-browser-001",
  subject_id: "subject-browser-001",
  dataset_id: "dataset-browser-001",
  product_id: "MNQ-USD",
  timeframe: "5m",
  started_at: "2026-01-15T12:00:00.000Z",
  ended_at: "2026-01-15T13:00:00.000Z",
  currency: "USD",
  metrics: {
    net_pnl: "12.50",
    return_pct: "0.00125",
    max_drawdown: "4.25",
    sharpe: "1.25",
    sortino: "1.50",
    calmar: "2.00"
  },
  equity: [{ timestamp: "2026-01-15T12:00:00.000Z", equity: "10012.50", drawdown: "0" }],
  monthly_returns: [{ month: "2026-01", return_pct: "0.00125" }],
  pnl_distribution: [{ lower: null, upper: null, count: 1 }],
  trade_page: {
    items: [{
      id: BACKTEST_TRADE_ID,
      entry_time: "2026-01-15T12:10:00.000Z",
      exit_time: "2026-01-15T12:15:00.000Z",
      entry_price: "100.00",
      exit_price: "101.00",
      side: "LONG",
      quantity: "1",
      pnl: "1.00",
      fee: "0.25"
    }],
    total_count: 1,
    next_cursor: null
  },
  input_digest: "1".repeat(64),
  result_digest: "2".repeat(64),
  revision: 1
};
export const BACKTEST_CANDLES: BacktestResultsCandlesPage = {
  items: [{
    timestamp: "2026-01-15T12:10:00.000Z",
    open: "100.00",
    high: "102.00",
    low: "99.00",
    close: "101.00",
    volume: "10"
  }],
  next_cursor: null,
  revision: 1
};

export const FLOW_RESULT_ID = "job-browser-002";
export const FLOW_TRADE_ID = `${FLOW_RESULT_ID}:1`;
export const FLOW_INDEX_CURSOR = "signed.index/opaque+token==";
export const FLOW_TRADE_CURSOR = "signed.trades/opaque+token==";
export const FLOW_CANDLE_CURSOR = "signed.candles/opaque+token==";
export const FLOW_INDEX_CURSOR_QUERY = "?cursor=signed.index%2Fopaque%2Btoken%3D%3D";
export const FLOW_TRADE_CURSOR_QUERY = "?cursor=signed.trades%2Fopaque%2Btoken%3D%3D";
export const FLOW_CANDLE_CURSOR_QUERY =
  "?start=1768478700000&end=1768479900000&cursor=signed.candles%2Fopaque%2Btoken%3D%3D";
export const F2B_ZERO_RESULT_ID = "job-browser-zero";
export const F2B_FORBIDDEN_RESULT_ID = "job-browser-403";
export const F2B_MISSING_RESULT_ID = "job-browser-404";
export const F2B_ERROR_RESULT_ID = "job-browser-503";
export const F2B_INVALID_RESULT_ID = "job-browser-invalid";
export const F2B_REVERSED_RESULT_ID = "job-browser-reversed";
export const F2B_CANDLE_FIRST_422_ID = "job-browser-candle-first-422";
export const F2B_CANDLE_NEXT_422_ID = "job-browser-candle-next-422";
export const F2B_STALE_DETAIL_OLD_ID = "job-browser-stale-detail-old";
export const F2B_STALE_DETAIL_NEW_ID = "job-browser-stale-detail-new";
export const F2B_STALE_TRADES_OLD_ID = "job-browser-stale-trades-old";
export const F2B_STALE_TRADES_NEW_ID = "job-browser-stale-trades-new";
export const F2B_STALE_CANDLES_OLD_ID = "job-browser-stale-candles-old";
export const F2B_STALE_CANDLES_NEW_ID = "job-browser-stale-candles-new";
export const F2B_STALE_TRADES_CURSOR = "signed.stale.trades.cursor";
export const F2B_STALE_TRADES_CURSOR_QUERY = "?cursor=signed.stale.trades.cursor";
export const F2B_STALE_CANDLES_CURSOR = "signed.stale.candles.cursor";
export const F2B_STALE_CANDLES_UNEXPECTED_CURSOR = "signed.stale.candles.unexpected.cursor";
export const F2B_STALE_CANDLES_CURSOR_QUERY =
  "?start=1768478400000&end=1768480200000&cursor=signed.stale.candles.cursor";
export const F2B_CANDLE_NEXT_422_CURSOR = "signed.candle-window.next.cursor";
export const F2B_CANDLE_NEXT_422_QUERY =
  "?start=1768478400000&end=1768480200000&cursor=signed.candle-window.next.cursor";

const flowFirstResult: BacktestResultsIndexItem = {
  job_id: BACKTEST_RESULT_ID,
  subject_id: "subject-browser-001",
  dataset_id: "dataset-browser-001",
  product_id: "BTC-USDT",
  timeframe: "5m",
  started_at: "2026-01-15T12:00:00.000Z",
  ended_at: "2026-01-15T13:00:00.000Z",
  completed_at: "2026-01-15T13:01:00.000Z",
  result_digest: "3".repeat(64)
};
function f2bIndexItem(jobId: string): BacktestResultsIndexItem {
  return {
    ...flowFirstResult,
    job_id: jobId,
    subject_id: `subject-${jobId}`,
    dataset_id: `dataset-${jobId}`,
    result_digest: "6".repeat(64)
  };
}

function f2bTrade(jobId: string, entry: string, exit: string) {
  return {
    ...BACKTEST_RESULT_DETAIL.trade_page.items[0],
    id: `${jobId}:0`,
    entry_time: entry,
    exit_time: exit
  };
}

function f2bDetail(
  jobId: string,
  options: { entry?: string; exit?: string; nextCursor?: string | null } = {}
): BacktestResultsDetail {
  return {
    ...BACKTEST_RESULT_DETAIL,
    job_id: jobId,
    subject_id: `subject-${jobId}`,
    dataset_id: `dataset-${jobId}`,
    result_digest: "7".repeat(64),
    trade_page: {
      items: [f2bTrade(
        jobId,
        options.entry ?? "2026-01-15T12:10:00.000Z",
        options.exit ?? "2026-01-15T12:15:00.000Z"
      )],
      total_count: options.nextCursor === undefined ? 1 : 2,
      next_cursor: options.nextCursor ?? null
    }
  };
}

export const F2B_ZERO_DETAIL: BacktestResultsDetail = {
  ...f2bDetail(F2B_ZERO_RESULT_ID),
  metrics: { ...BACKTEST_RESULT_DETAIL.metrics, net_pnl: "0", return_pct: "0", max_drawdown: "0" },
  equity: [{ timestamp: "2026-01-15T12:00:00.000Z", equity: "10000.00", drawdown: "0" }],
  monthly_returns: [],
  pnl_distribution: [],
  trade_page: { items: [], total_count: 0, next_cursor: null }
};
export const F2B_REVERSED_DETAIL = f2bDetail(F2B_REVERSED_RESULT_ID, {
  entry: "2026-01-15T12:15:00.000Z",
  exit: "2026-01-15T12:10:00.000Z"
});
export const F2B_CANDLE_FIRST_422_DETAIL = f2bDetail(F2B_CANDLE_FIRST_422_ID);
export const F2B_CANDLE_NEXT_422_DETAIL = f2bDetail(F2B_CANDLE_NEXT_422_ID);
export const F2B_STALE_DETAIL_OLD_DETAIL = {
  ...f2bDetail(F2B_STALE_DETAIL_OLD_ID, {
    nextCursor: "signed.stale.detail.cursor"
  }),
  metrics: { ...BACKTEST_RESULT_DETAIL.metrics, net_pnl: "11.00" }
};
export const F2B_STALE_DETAIL_NEW_DETAIL = {
  ...f2bDetail(F2B_STALE_DETAIL_NEW_ID),
  metrics: { ...BACKTEST_RESULT_DETAIL.metrics, net_pnl: "22.00" }
};
export const F2B_STALE_TRADES_OLD_DETAIL = {
  ...f2bDetail(F2B_STALE_TRADES_OLD_ID, {
    nextCursor: F2B_STALE_TRADES_CURSOR
  }),
  metrics: { ...BACKTEST_RESULT_DETAIL.metrics, net_pnl: "11.00" }
};
export const F2B_STALE_TRADES_NEW_DETAIL = {
  ...f2bDetail(F2B_STALE_TRADES_NEW_ID),
  metrics: { ...BACKTEST_RESULT_DETAIL.metrics, net_pnl: "22.00" }
};
export const F2B_STALE_CANDLES_OLD_DETAIL = {
  ...f2bDetail(F2B_STALE_CANDLES_OLD_ID),
  metrics: { ...BACKTEST_RESULT_DETAIL.metrics, net_pnl: "11.00" }
};
export const F2B_STALE_CANDLES_NEW_DETAIL = {
  ...f2bDetail(F2B_STALE_CANDLES_NEW_ID),
  metrics: { ...BACKTEST_RESULT_DETAIL.metrics, net_pnl: "22.00" }
};
export const F2B_CANDLE_NEXT_422_FIRST: BacktestResultsCandlesPage = {
  items: [BACKTEST_CANDLES.items[0]],
  next_cursor: F2B_CANDLE_NEXT_422_CURSOR,
  revision: 1
};
export const F2B_STALE_CANDLES_FIRST: BacktestResultsCandlesPage = {
  items: [BACKTEST_CANDLES.items[0]],
  next_cursor: F2B_STALE_CANDLES_CURSOR,
  revision: 1
};
export const F2B_STALE_CANDLES_FINAL: BacktestResultsCandlesPage = {
  items: [{ ...BACKTEST_CANDLES.items[0], timestamp: "2026-01-15T12:15:00.000Z" }],
  next_cursor: F2B_STALE_CANDLES_UNEXPECTED_CURSOR,
  revision: 1
};

export const F2B_STALE_DETAIL_INDEX: BacktestResultsIndexPage = {
  items: [F2B_STALE_DETAIL_OLD_ID, F2B_STALE_DETAIL_NEW_ID].map(f2bIndexItem),
  next_cursor: null,
  revision: 1
};
export const F2B_STALE_TRADES_INDEX: BacktestResultsIndexPage = {
  items: [F2B_STALE_TRADES_OLD_ID, F2B_STALE_TRADES_NEW_ID].map(f2bIndexItem),
  next_cursor: null,
  revision: 1
};
export const F2B_STALE_CANDLES_INDEX: BacktestResultsIndexPage = {
  items: [F2B_STALE_CANDLES_OLD_ID, F2B_STALE_CANDLES_NEW_ID].map(f2bIndexItem),
  next_cursor: null,
  revision: 1
};
export const F2B_STATES_INDEX: BacktestResultsIndexPage = {
  items: [F2B_ZERO_RESULT_ID, F2B_FORBIDDEN_RESULT_ID, F2B_MISSING_RESULT_ID,
    F2B_ERROR_RESULT_ID, F2B_INVALID_RESULT_ID].map(f2bIndexItem),
  next_cursor: null,
  revision: 1
};
export const F2B_CANDLE_STATES_INDEX: BacktestResultsIndexPage = {
  items: [F2B_REVERSED_RESULT_ID, F2B_CANDLE_FIRST_422_ID, F2B_CANDLE_NEXT_422_ID]
    .map(f2bIndexItem),
  next_cursor: null,
  revision: 1
};
const flowSelectedResult: BacktestResultsIndexItem = {
  ...flowFirstResult,
  job_id: FLOW_RESULT_ID,
  subject_id: "subject-browser-002",
  dataset_id: "dataset-browser-002",
  ended_at: "2026-01-15T12:24:59.999Z",
  completed_at: "2026-01-15T13:02:00.000Z",
  result_digest: "4".repeat(64)
};
export const FLOW_RESULT_INDEX_FIRST: BacktestResultsIndexPage = {
  items: [flowFirstResult],
  next_cursor: FLOW_INDEX_CURSOR,
  revision: 1
};
export const FLOW_RESULT_INDEX_SECOND: BacktestResultsIndexPage = {
  items: [flowSelectedResult],
  next_cursor: null,
  revision: 1
};

const flowFirstTrade = {
  id: `${FLOW_RESULT_ID}:0`,
  entry_time: "2026-01-15T12:10:00.000Z",
  exit_time: "2026-01-15T12:15:00.000Z",
  entry_price: "9007199254740993.12",
  exit_price: "9007199254740994.12",
  side: "LONG",
  quantity: "1.25",
  pnl: "9007199254740993.12",
  fee: "0.25"
} as const;
const flowSelectedTrade = {
  id: FLOW_TRADE_ID,
  entry_time: "2026-01-15T12:15:00.000Z",
  exit_time: "2026-01-15T12:20:00.000Z",
  entry_price: "9007199254740994.12",
  exit_price: "9007199254740993.12",
  side: "SHORT",
  quantity: "2.50",
  pnl: "-9007199254740993.12",
  fee: "0.50"
} as const;
export const FLOW_RESULT_DETAIL: BacktestResultsDetail = {
  ...BACKTEST_RESULT_DETAIL,
  job_id: FLOW_RESULT_ID,
  ended_at: "2026-01-15T12:24:59.999Z",
  subject_id: "subject-browser-002",
  dataset_id: "dataset-browser-002",
  product_id: "BTC-USDT",
  currency: "USDT",
  metrics: {
    ...BACKTEST_RESULT_DETAIL.metrics,
    net_pnl: "9007199254740993.12",
    return_pct: "0.0125",
    max_drawdown: "123.45"
  },
  trade_page: {
    items: [flowFirstTrade],
    total_count: 2,
    next_cursor: FLOW_TRADE_CURSOR
  },
  input_digest: "5".repeat(64),
  result_digest: "4".repeat(64)
};
export const FLOW_RESULT_TRADES_SECOND: BacktestResultsTradesPage = {
  items: [flowSelectedTrade],
  next_cursor: null,
  revision: 1
};
export const FLOW_RESULT_CANDLES_FIRST: BacktestResultsCandlesPage = {
  items: [
    { timestamp: "2026-01-15T12:05:00.000Z", open: "100.00", high: "102.00", low: "99.00", close: "101.00", volume: "10" },
    { timestamp: "2026-01-15T12:10:00.000Z", open: "101.00", high: "103.00", low: "100.00", close: "102.00", volume: "11" },
    { timestamp: "2026-01-15T12:15:00.000Z", open: "102.00", high: "104.00", low: "101.00", close: "103.00", volume: "12" }
  ],
  next_cursor: FLOW_CANDLE_CURSOR,
  revision: 1
};
export const FLOW_RESULT_CANDLES_SECOND: BacktestResultsCandlesPage = {
  items: [
    { timestamp: "2026-01-15T12:20:00.000Z", open: "103.00", high: "104.00", low: "101.00", close: "102.00", volume: "13" }
  ],
  next_cursor: null,
  revision: 1
};

export const EPOCH_A = {
  id: "epoch-a",
  strategy_id: "strategy-epoch-a",
  started_at: "2026-01-15T12:34:00Z",
  finished_at: "2026-01-15T13:34:00Z",
  pop_size: 2,
  max_generations: 1,
  generations_run: 1,
  best_score: "9007199254740993.00",
  seed: 1,
  config_json: { objective: "maximize_score" },
  status: "completed",
  eval_pair: "RITHMIC:MNQ_ROLL-PERP",
  eval_start_date: "2026-01-01",
  eval_end_date: "2026-01-15",
  eval_timeframe: "5m"
} as const;

export const EPOCH_B = {
  id: "epoch-b",
  strategy_id: "strategy-epoch-b",
  started_at: "2026-07-15T12:34:00Z",
  finished_at: "2026-07-15T13:34:00Z",
  pop_size: 2,
  max_generations: 1,
  generations_run: 1,
  best_score: "2.00",
  seed: 2,
  config_json: { objective: "maximize_score" },
  status: "completed",
  eval_pair: "RITHMIC:MNQ_ROLL-PERP",
  eval_start_date: "2026-01-01",
  eval_end_date: "2026-01-15",
  eval_timeframe: "5m"
} as const;

export const GENERATION_A = {
  generation_index: 0,
  candidate_count: 1,
  score_min: "9007199254740993.00",
  score_max: "9007199254740993.00",
  drawdown_min: "0.1000",
  drawdown_max: "0.1000"
} as const;

export const GENERATION_B = {
  generation_index: 0,
  candidate_count: 1,
  score_min: "2.00",
  score_max: "2.00",
  drawdown_min: "0.1000",
  drawdown_max: "0.1000"
} as const;

export const GENE_A = {
  id: 1,
  strategy_id: "strategy-epoch-a",
  role: "challenger",
  param_pack: { fast: 5, slow: 20 },
  score_total: "9007199254740993.00",
  score_breakdown: {},
  max_drawdown: "0.1000",
  generation_index: 0,
  candidate_id: "candidate-a",
  epoch_id: "epoch-a",
  created_at: "2026-01-15T13:34:00Z"
} as const;

export const GENE_B = {
  id: 2,
  strategy_id: "strategy-epoch-b",
  role: "challenger",
  param_pack: { fast: 6, slow: 20 },
  score_total: "2.00",
  score_breakdown: {},
  max_drawdown: "0.1000",
  generation_index: 0,
  candidate_id: "candidate-b",
  epoch_id: "epoch-b",
  created_at: "2026-07-15T13:34:00Z"
} as const;

export const STRATEGY_PAGE = {
  total: 1,
  limit: 500,
  offset: 0,
  states: [
    {
      strategy_id: "active-strategy",
      status: "ACTIVE",
      config: {},
      performance: {},
      last_heartbeat: 1768480440000,
      uptime_start: 1768476840000,
      last_error_message: null,
      entered_error_at: null,
      recovered_at: null,
      stopped_at: null,
      version: 3,
      available_commands: ["STOP"]
    }
  ]
} as const;

export const STOPPED_STRATEGY_PAGE = {
  total: 1,
  limit: 500,
  offset: 0,
  states: [
    {
      ...STRATEGY_PAGE.states[0],
      status: "STOPPED",
      version: 4,
      available_commands: ["RESUME"]
    }
  ]
} as const;

export type RouteCounts = Readonly<{
  S: number;
  P: number;
  E: number;
  A: number;
  B: number;
  a: number;
  b: number;
  T: number;
  C: number;
  RI: number;
  RD: number;
  RT: number;
  RC: number;
  I: number;
}>;

export const EXPECTED_REQUEST_COUNTS = {
  "direct-navigation:dev:results": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "direct-navigation:dev:strategies": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "direct-navigation:dev:trades": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "direct-navigation:production:results": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 1, RD: 0, RT: 0, RC: 0, I: 0 },
  "direct-navigation:production:strategies": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 1, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "direct-navigation:production:trades": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 0, RD: 1, RT: 0, RC: 1, I: 0 },
  "research-cache:dev:main": { S: 4, P: 0, E: 2, A: 1, B: 0, a: 1, b: 0, T: 2, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 2 },
  "research-cache:production:main": { S: 2, P: 0, E: 1, A: 1, B: 0, a: 1, b: 0, T: 1, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 2 },
  "navigation-serialization:dev:main": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "demo-dev:dev:main": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "demo-production-denied:production:results": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 1, RD: 0, RT: 0, RC: 0, I: 0 },
  "demo-production-denied:production:trades": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 0, RD: 1, RT: 0, RC: 1, I: 0 },
  "strategy-command:dev:main": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 3, C: 1, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "strategy-command:production:main": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 1, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "locale-theme-reload:dev:main": { S: 4, P: 0, E: 4, A: 2, B: 0, a: 2, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 2 },
  "locale-theme-reload:production:main": { S: 2, P: 0, E: 2, A: 2, B: 0, a: 2, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 2 },
  "lazy-chunk-inventory:dev:main": { S: 4, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 4, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "lazy-chunk-inventory:production:main": { S: 5, P: 0, E: 1, A: 1, B: 0, a: 1, b: 0, T: 2, C: 0, RI: 2, RD: 0, RT: 0, RC: 0, I: 5 },
  "responsive-overflow:dev:research": { S: 2, P: 0, E: 2, A: 1, B: 0, a: 1, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 1 },
  "responsive-overflow:dev:results": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "responsive-overflow:dev:strategies": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "responsive-overflow:dev:trades": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "responsive-overflow:production:research": { S: 1, P: 0, E: 1, A: 1, B: 0, a: 1, b: 0, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 1 },
  "responsive-overflow:production:results": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 1, RD: 0, RT: 0, RC: 0, I: 0 },
  "responsive-overflow:production:strategies": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 1, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "responsive-overflow:production:trades": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 0, RD: 1, RT: 0, RC: 1, I: 0 },
  "berlin-presentation-time:dev:research-api": { S: 2, P: 0, E: 2, A: 1, B: 1, a: 1, b: 1, T: 0, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 1 },
  "berlin-presentation-time:dev:demo-features": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 0, RI: 0, RD: 0, RT: 0, RC: 0, I: 0 },
  "production-results-flow:production:main": { S: 8, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 2, RD: 2, RT: 2, RC: 2, I: 0 },
  "production-results-states:production:main": { S: 21, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 6, RD: 15, RT: 0, RC: 0, I: 0 },
  "production-results-candle-states:production:main": { S: 12, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0, RI: 2, RD: 7, RT: 0, RC: 3, I: 0 },
  "production-results-stale-detail:production:main": { S: 5, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 1, C: 0, RI: 2, RD: 2, RT: 0, RC: 0, I: 0 },
  "production-results-stale-trades:production:main": { S: 6, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 1, C: 0, RI: 2, RD: 2, RT: 1, RC: 0, I: 0 },
  "production-results-stale-candles:production:main": { S: 8, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 1, C: 0, RI: 2, RD: 3, RT: 0, RC: 2, I: 0 }
} as const satisfies Readonly<Record<string, RouteCounts>>;

export const EXPECTED_DOCUMENT_COUNTS = {
  "direct-navigation:dev:results": 1,
  "direct-navigation:dev:strategies": 1,
  "direct-navigation:dev:trades": 1,
  "direct-navigation:production:results": 1,
  "direct-navigation:production:strategies": 1,
  "direct-navigation:production:trades": 1,
  "research-cache:dev:main": 1,
  "research-cache:production:main": 1,
  "navigation-serialization:dev:main": 1,
  "demo-dev:dev:main": 1,
  "demo-production-denied:production:results": 1,
  "demo-production-denied:production:trades": 1,
  "strategy-command:dev:main": 1,
  "strategy-command:production:main": 1,
  "locale-theme-reload:dev:main": 2,
  "locale-theme-reload:production:main": 2,
  "lazy-chunk-inventory:dev:main": 1,
  "lazy-chunk-inventory:production:main": 1,
  "responsive-overflow:dev:research": 1,
  "responsive-overflow:dev:results": 1,
  "responsive-overflow:dev:strategies": 1,
  "responsive-overflow:dev:trades": 1,
  "responsive-overflow:production:research": 1,
  "responsive-overflow:production:results": 1,
  "responsive-overflow:production:strategies": 1,
  "responsive-overflow:production:trades": 1,
  "berlin-presentation-time:dev:research-api": 1,
  "berlin-presentation-time:dev:demo-features": 1,
  "production-results-flow:production:main": 1,
  "production-results-states:production:main": 1,
  "production-results-candle-states:production:main": 1,
  "production-results-stale-detail:production:main": 1,
  "production-results-stale-trades:production:main": 1,
  "production-results-stale-candles:production:main": 1
} as const satisfies Readonly<
  Record<keyof typeof EXPECTED_REQUEST_COUNTS, number>
>;
