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
  "responsive-overflow"
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
  "responsive-overflow": ["research", "results", "strategies", "trades"]
} as const satisfies Record<ScenarioId, readonly string[]>;

export const BROWSER_SESSION = {
  actor: "frontend-smoke@example.invalid",
  capabilities: [],
  csrf_token: "csrf-browser-smoke",
  expires_at: "2026-08-24T12:00:00Z",
  step_up_expires_at: null
} as const;

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
}>;

export const EXPECTED_REQUEST_COUNTS = {
  "direct-navigation:dev:results": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "direct-navigation:dev:strategies": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 0 },
  "direct-navigation:dev:trades": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "direct-navigation:production:results": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "direct-navigation:production:strategies": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 1, C: 0 },
  "direct-navigation:production:trades": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "research-cache:dev:main": { S: 4, P: 0, E: 2, A: 1, B: 0, a: 1, b: 0, T: 2, C: 0 },
  "research-cache:production:main": { S: 2, P: 0, E: 1, A: 1, B: 0, a: 1, b: 0, T: 1, C: 0 },
  "navigation-serialization:dev:main": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 0 },
  "demo-dev:dev:main": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "demo-production-denied:production:results": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "demo-production-denied:production:trades": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "strategy-command:dev:main": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 3, C: 1 },
  "strategy-command:production:main": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 1 },
  "locale-theme-reload:dev:main": { S: 4, P: 0, E: 4, A: 2, B: 0, a: 2, b: 0, T: 0, C: 0 },
  "locale-theme-reload:production:main": { S: 2, P: 0, E: 2, A: 2, B: 0, a: 2, b: 0, T: 0, C: 0 },
  "lazy-chunk-inventory:dev:main": { S: 4, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 4, C: 0 },
  "lazy-chunk-inventory:production:main": { S: 3, P: 0, E: 1, A: 1, B: 0, a: 1, b: 0, T: 2, C: 0 },
  "responsive-overflow:dev:research": { S: 2, P: 0, E: 2, A: 1, B: 0, a: 1, b: 0, T: 0, C: 0 },
  "responsive-overflow:dev:results": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "responsive-overflow:dev:strategies": { S: 2, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 2, C: 0 },
  "responsive-overflow:dev:trades": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "responsive-overflow:production:research": { S: 1, P: 0, E: 1, A: 1, B: 0, a: 1, b: 0, T: 0, C: 0 },
  "responsive-overflow:production:results": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 },
  "responsive-overflow:production:strategies": { S: 1, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 1, C: 0 },
  "responsive-overflow:production:trades": { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 }
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
  "responsive-overflow:production:trades": 1
} as const satisfies Readonly<
  Record<keyof typeof EXPECTED_REQUEST_COUNTS, number>
>;
