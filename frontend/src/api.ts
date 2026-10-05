export type Epoch = {
  id: string;
  strategy_id: string;
  started_at: string;
  finished_at: string | null;
  pop_size: number;
  max_generations: number;
  generations_run: number | null;
  best_score: string | null;
  seed: number;
  config_json: Record<string, unknown>;
  status: "running" | "completed" | "aborted";
  eval_pair: string;
  eval_start_date: string;
  eval_end_date: string;
  eval_timeframe: string;
};

export type Gene = {
  id: number;
  strategy_id: string;
  role: "challenger" | "champion" | "retired";
  param_pack: Record<string, unknown>;
  score_total: string;
  score_breakdown: Record<string, unknown>;
  max_drawdown: string;
  generation_index: number;
  candidate_id: string;
  epoch_id: string;
  created_at: string;
};

export type GenerationSummary = {
  generation_index: number;
  candidate_count: number;
  score_min: string;
  score_max: string;
  drawdown_min: string;
  drawdown_max: string;
};

export type BrowserSession = {
  actor: string;
  capabilities: string[];
  permissions: { can_mutate: boolean; can_step_up: boolean };
  csrf_token: string;
  expires_at: string;
  step_up_expires_at: string | null;
};

export type GaProfileField = Readonly<Record<string, unknown>>;
export type GaProfile = {
  parameter_search_profile_id: "golden_cross_research_v1";
  profile_revision: string;
  strategy_subject: "builtin:golden_cross";
  strategy_version: string;
  fitness_profile_id: string;
  cost_profile_id: string;
  accepted_fields: Record<string, GaProfileField>;
  [key: string]: unknown;
};

export type GaJob = {
  id: string;
  kind: "ga";
  status: "QUEUED" | "RUNNING" | "PAUSING" | "PAUSED" | "CANCELLING" | "CANCELLED" | "SUCCEEDED" | "FAILED";
  version: number;
  epoch_id: string | null;
  completed_generation: number;
  checkpoint: { epoch_id: string; completed_generation: number } | null;
  retry_of_job_id: string | null;
  request: Record<string, unknown>;
  error: "control_plane_interrupted" | "ga_execution_failed" | null;
};

export type GaJobsPage = {
  schema_version: 1;
  items: GaJob[];
  total_count: number;
  limit: number;
  offset: number;
};

export type GaJobEnvelope = { schema_version: 1; job: GaJob };
export type GaJobOperation = "submit" | "pause" | "resume" | "cancel" | "retry";
export type ResearchCandidatePromotion = {
  scope: "RESEARCH_CANDIDATE_ONLY";
  gene: {
    gene_id: number;
    strategy_id: string;
    role: "champion";
    activated_at: string | null;
    retired_gene_ids: number[];
  };
};

export type GeneLifecycleEvent = {
  id: number;
  event_type: "gene_promote" | "gene_retire";
  event_subtype: string | null;
  related_strategy_id: string | null;
  related_order_id: string | null;
  related_gene_id: number | null;
  payload: Record<string, unknown>;
  created_at: string | null;
};

export type StrategyStatus =
  | "DISCOVERED"
  | "READY"
  | "WARNING"
  | "ACTIVE"
  | "STOPPED"
  | "ERROR";

export type StrategyCommand = "START" | "STOP" | "RESUME" | "FORCE_RECOVER";

export type StrategyState = {
  strategy_id: string;
  status: StrategyStatus;
  config: Record<string, unknown> | null;
  performance: Record<string, unknown> | null;
  last_heartbeat: number | null;
  uptime_start: number | null;
  last_error_message: string | null;
  entered_error_at: string | null;
  recovered_at: string | null;
  stopped_at: string | null;
  version: number;
  available_commands: StrategyCommand[];
};

export type KillSwitchStatus = {
  state: "LOCKDOWN" | "OK" | "UNKNOWN";
  redis_state: "LOCKDOWN" | "OK" | null;
  durable_state: "LOCKDOWN" | "OK" | null;
  listener_available: boolean;
};

export type BacktestResultsIndexItem = {
  job_id: string;
  subject_id: string;
  dataset_id: string;
  product_id: string;
  timeframe: string;
  started_at: string;
  ended_at: string;
  completed_at: string;
  result_digest: string;
};

export type BacktestResultsIndexPage = {
  items: BacktestResultsIndexItem[];
  next_cursor: string | null;
  revision: 1;
};

export type BacktestResultsTrade = {
  id: string;
  entry_time: string;
  exit_time: string;
  entry_price: string;
  exit_price: string;
  side: string;
  quantity: string;
  pnl: string;
  fee: string;
};

export type BacktestResultsTradesPage = {
  items: BacktestResultsTrade[];
  next_cursor: string | null;
  revision: 1;
};

export type BacktestResultsCandle = {
  timestamp: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
};

export type BacktestResultsCandlesPage = {
  items: BacktestResultsCandle[];
  next_cursor: string | null;
  revision: 1;
};

export type BacktestResultsDetailMetrics = {
  net_pnl: string;
  return_pct: string;
  max_drawdown: string;
  sharpe: string;
  sortino: string;
  calmar: string;
};

export type BacktestResultsEquityItem = {
  timestamp: string;
  equity: string;
  drawdown: string;
};

export type BacktestResultsMonthlyReturn = {
  month: string;
  return_pct: string;
};

export type BacktestResultsDistributionItem = {
  lower: string | null;
  upper: string | null;
  count: number;
};

export type BacktestResultsDetail = {
  job_id: string;
  strategy_id: string;
  subject_id: string;
  dataset_id: string;
  product_id: string;
  timeframe: string;
  started_at: string;
  ended_at: string;
  currency: string;
  metrics: BacktestResultsDetailMetrics;
  equity: BacktestResultsEquityItem[];
  monthly_returns: BacktestResultsMonthlyReturn[];
  pnl_distribution: BacktestResultsDistributionItem[];
  trade_page: {
    items: BacktestResultsTrade[];
    total_count: number;
    next_cursor: string | null;
  };
  input_digest: string;
  result_digest: string;
  revision: 1;
};

export type BacktestResultsPageOptions = {
  limit?: number;
  cursor?: string;
};

export type BacktestResultsCandleOptions = BacktestResultsPageOptions & {
  start: number;
  end: number;
};

type Page<TName extends string, T> = {
  total: number;
  limit: number;
  offset: number;
} & Record<TName, T[]>;

const PAGE_SIZE = 10_000;
const STRATEGY_PAGE_SIZE = 500;

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number
  ) {
    super(message);
  }
}

async function request<T>(
  path: string,
  init?: RequestInit,
  expectedStatus?: number
): Promise<T> {
  const response = await fetch(path, {
    credentials: "include",
    ...init,
    headers: {
      Accept: "application/json",
      ...init?.headers
    }
  });
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    if (init?.method === "POST") throw new Error("invalid_response");
    throw new ApiError("invalid_response", response.status);
  }
  if (!response.ok) {
    if (init?.method === "POST" && (body === null || typeof body !== "object")) {
      throw new Error("invalid_response");
    }
    const reason =
      body !== null && typeof body === "object" && "error" in body
        ? String((body as { error?: string }).error)
        : response.statusText;
    throw new ApiError(reason, response.status);
  }
  if (expectedStatus !== undefined && response.status !== expectedStatus) {
    throw new ApiError("invalid_response", response.status);
  }
  if (path === "/ops/kill-switch/clear" && response.status !== 202) {
    throw new ApiError("invalid_response", response.status);
  }
  if (path.startsWith("/strategy-states?") && !validPage(body, "states", validStrategy)) {
    throw new Error("invalid_response");
  }
  if (path === "/ops/kill-switch" && !validKillSwitchStatus(body)) {
    throw new ApiError("invalid_response", response.status);
  }
  if (path.startsWith("/evolution-epochs?") && !validPage(body, "epochs", validEpoch)) {
    throw new Error("invalid_response");
  }
  if (path.endsWith("/generations") &&
      (!record(body) || !rows(body.generations, validGeneration))) {
    throw new Error("invalid_response");
  }
  if (path.startsWith("/genes?") && !validPage(body, "genes", validGene)) {
    throw new Error("invalid_response");
  }
  return body as T;
}

function validKillSwitchStatus(value: unknown): value is KillSwitchStatus {
  return record(value) && ["LOCKDOWN", "OK", "UNKNOWN"].includes(value.state as string) &&
    [null, "LOCKDOWN", "OK"].includes(value.redis_state as string | null) &&
    [null, "LOCKDOWN", "OK"].includes(value.durable_state as string | null) &&
    typeof value.listener_available === "boolean";
}

export function loadKillSwitchStatus(): Promise<KillSwitchStatus> {
  return request<KillSwitchStatus>("/ops/kill-switch", { cache: "no-store" });
}

export async function clearKillSwitch(csrfToken: string): Promise<void> {
  const response = await request<{ status: string }>("/ops/kill-switch/clear", {
    method: "POST", headers: { "Content-Type": "application/json", "X-CSRF-Token": csrfToken },
    body: JSON.stringify({ confirm: true })
  });
  if (response.status !== "accepted") throw new Error("invalid_response");
}

export async function ensureBrowserSession(): Promise<BrowserSession | null> {
  try {
    return await request<BrowserSession>("/api/v1/auth/session");
  } catch (error) {
    if (!(error instanceof ApiError) || error.status !== 401) {
      if (error instanceof ApiError && error.status === 404) {
        return null;
      }
      throw error;
    }
    return request<BrowserSession>("/api/v1/auth/session", { method: "POST" });
  }
}

export async function renewBrowserSession(): Promise<BrowserSession> {
  return request<BrowserSession>("/api/v1/auth/session", { method: "POST", cache: "no-store" });
}

export async function loadBacktestResultsIndex(
  options: BacktestResultsPageOptions = {}
): Promise<BacktestResultsIndexPage> {
  const query = new URLSearchParams();
  if (options.limit !== undefined) query.set("limit", String(options.limit));
  if (options.cursor !== undefined) query.set("cursor", options.cursor);
  const suffix = query.toString();
  const value = await request<unknown>(
    `/api/v1/backtest-results${suffix ? `?${suffix}` : ""}`,
    { cache: "no-store" }
  );
  if (!validBacktestResultsIndexPage(value)) throw new Error("invalid_response");
  return value;
}

export async function loadBacktestResult(
  jobId: string
): Promise<BacktestResultsDetail> {
  const value = await request<unknown>(
    `/api/v1/backtest-results/${encodeURIComponent(jobId)}`,
    { cache: "no-store" }
  );
  if (!validBacktestResultsDetail(value)) throw new Error("invalid_response");
  return value;
}

export async function loadBacktestResultTrades(
  jobId: string,
  options: BacktestResultsPageOptions = {}
): Promise<BacktestResultsTradesPage> {
  const query = new URLSearchParams();
  if (options.limit !== undefined) query.set("limit", String(options.limit));
  if (options.cursor !== undefined) query.set("cursor", options.cursor);
  const suffix = query.toString();
  const value = await request<unknown>(
    `/api/v1/backtest-results/${encodeURIComponent(jobId)}/trades${suffix ? `?${suffix}` : ""}`,
    { cache: "no-store" }
  );
  if (!validBacktestResultsTradesPage(value)) throw new Error("invalid_response");
  return value;
}

export async function loadBacktestResultCandles(
  jobId: string,
  options: BacktestResultsCandleOptions
): Promise<BacktestResultsCandlesPage> {
  const query = new URLSearchParams();
  query.set("start", String(options.start));
  query.set("end", String(options.end));
  if (options.limit !== undefined) query.set("limit", String(options.limit));
  if (options.cursor !== undefined) query.set("cursor", options.cursor);
  const value = await request<unknown>(
    `/api/v1/backtest-results/${encodeURIComponent(jobId)}/candles?${query}`,
    { cache: "no-store" }
  );
  if (!validBacktestResultsCandlesPage(value)) throw new Error("invalid_response");
  return value;
}

export async function loadStrategyStates(): Promise<StrategyState[]> {
  const query = (offset: number) =>
    `/strategy-states?limit=${STRATEGY_PAGE_SIZE}&offset=${offset}`;
  const first = await request<Page<"states", StrategyState>>(query(0));
  const offsets = Array.from(
    { length: Math.ceil(first.total / STRATEGY_PAGE_SIZE) - 1 },
    (_, index) => (index + 1) * STRATEGY_PAGE_SIZE
  );
  const remaining = await Promise.all(
    offsets.map((offset) =>
      request<Page<"states", StrategyState>>(query(offset))
    )
  );
  return [first, ...remaining].flatMap((page) => page.states);
}

export async function sendStrategyCommand(
  strategyId: string,
  command: StrategyCommand,
  expectedVersion: number,
  idempotencyKey: string,
  csrfToken?: string
): Promise<void> {
  await request(`/strategies/${encodeURIComponent(strategyId)}/commands`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
      ...(csrfToken ? { "X-CSRF-Token": csrfToken } : {})
    },
    body: JSON.stringify({ command, expected_version: expectedVersion })
  });
}

export async function loadEpochs(): Promise<Epoch[]> {
  const page = await request<Page<"epochs", Epoch>>(
    "/evolution-epochs?limit=100&offset=0"
  );
  return page.epochs;
}

export async function loadEpochById(epochId: string): Promise<Epoch> {
  const value = await request<unknown>(`/evolution-epochs/${encodeURIComponent(epochId)}`, {
    cache: "no-store"
  });
  if (!record(value) || !validEpoch(value.epoch) || value.epoch.id !== epochId) {
    throw new Error("invalid_response");
  }
  return value.epoch;
}

export async function loadGene(geneId: number): Promise<Gene> {
  const value = await request<unknown>(`/genes/${geneId}`, { cache: "no-store" });
  if (!record(value) || !validGene(value.gene) || value.gene.id !== geneId) {
    throw new Error("invalid_response");
  }
  return value.gene;
}

export async function loadGeneLifecycleEvents(
  geneId: number,
  strategyId: string,
  eventType: "gene_promote" | "gene_retire"
): Promise<GeneLifecycleEvent[]> {
  const query = new URLSearchParams({
    event_type: eventType,
    related_gene_id: String(geneId),
    strategy_id: strategyId,
    limit: "100",
    offset: "0"
  });
  const value = await request<unknown>(`/system-events?${query}`, { cache: "no-store" });
  if (!validPage(value, "events", validGeneLifecycleEvent)) throw new Error("invalid_response");
  return (value as Page<"events", GeneLifecycleEvent>).events;
}

export async function loadGenerationSummaries(
  epochId: string
): Promise<GenerationSummary[]> {
  const body = await request<{ generations: GenerationSummary[] }>(
    `/evolution-epochs/${encodeURIComponent(epochId)}/generations`
  );
  return body.generations;
}

export async function loadGenerationGenes(
  epochId: string,
  generationIndex: number
): Promise<Gene[]> {
  const query = (offset: number) =>
    `/genes?epoch_id=${encodeURIComponent(epochId)}` +
    `&generation_index=${generationIndex}&limit=${PAGE_SIZE}&offset=${offset}`;
  const first = await request<Page<"genes", Gene>>(query(0));
  const offsets = Array.from(
    { length: Math.ceil(first.total / PAGE_SIZE) - 1 },
    (_, index) => (index + 1) * PAGE_SIZE
  );
  const remaining = await Promise.all(
    offsets.map((offset) => request<Page<"genes", Gene>>(query(offset)))
  );
  return [first, ...remaining].flatMap((page) => page.genes);
}

// Validate read-only payload shapes before they enter React state.
type RecordValue = Record<string, unknown>;
const record = (value: unknown): value is RecordValue =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const text = (value: unknown) => typeof value === "string";
const integer = (value: unknown) => Number.isSafeInteger(value) && (value as number) >= 0;
const nullableText = (value: unknown) => value === null || text(value);
const nullableNumber = (value: unknown) => value === null ||
  (typeof value === "number" && Number.isFinite(value));
const rows = (value: unknown, valid: (row: unknown) => boolean) =>
  Array.isArray(value) && value.every(valid);

function textFields(value: unknown, ...fields: string[]): boolean {
  return record(value) && fields.every((key) => text(value[key]));
}

function validBacktestResultsIndexItem(value: unknown): value is BacktestResultsIndexItem {
  return textFields(
    value,
    "job_id",
    "subject_id",
    "dataset_id",
    "product_id",
    "timeframe",
    "started_at",
    "ended_at",
    "completed_at",
    "result_digest"
  );
}

function validBacktestResultsIndexPage(
  value: unknown
): value is BacktestResultsIndexPage {
  return record(value) &&
    rows(value.items, validBacktestResultsIndexItem) &&
    nullableText(value.next_cursor) &&
    value.revision === 1;
}

function validBacktestResultsTradesPage(
  value: unknown
): value is BacktestResultsTradesPage {
  return record(value) && rows(value.items, validBacktestResultsTrade) &&
    nullableText(value.next_cursor) && value.revision === 1;
}

function validBacktestResultsTrade(value: unknown): value is BacktestResultsTrade {
  return textFields(
    value,
    "id",
    "entry_time",
    "exit_time",
    "entry_price",
    "exit_price",
    "side",
    "quantity",
    "pnl",
    "fee"
  );
}

function validBacktestResultsCandle(value: unknown): value is BacktestResultsCandle {
  return textFields(value, "timestamp", "open", "high", "low", "close", "volume");
}

function validBacktestResultsCandlesPage(
  value: unknown
): value is BacktestResultsCandlesPage {
  return record(value) && rows(value.items, validBacktestResultsCandle) &&
    nullableText(value.next_cursor) && value.revision === 1;
}

function validBacktestResultsDetailMetrics(
  value: unknown
): value is BacktestResultsDetailMetrics {
  return textFields(
    value,
    "net_pnl",
    "return_pct",
    "max_drawdown",
    "sharpe",
    "sortino",
    "calmar"
  );
}

function validBacktestResultsEquityItem(
  value: unknown
): value is BacktestResultsEquityItem {
  return textFields(value, "timestamp", "equity", "drawdown");
}

function validBacktestResultsMonthlyReturn(
  value: unknown
): value is BacktestResultsMonthlyReturn {
  return textFields(value, "month", "return_pct");
}

function validBacktestResultsDistributionItem(
  value: unknown
): value is BacktestResultsDistributionItem {
  return record(value) && nullableText(value.lower) &&
    nullableText(value.upper) && integer(value.count);
}

function validBacktestResultsDetail(value: unknown): value is BacktestResultsDetail {
  if (!record(value) || !record(value.trade_page)) return false;
  return textFields(
    value,
    "job_id",
    "strategy_id",
    "subject_id",
    "dataset_id",
    "product_id",
    "timeframe",
    "started_at",
    "ended_at",
    "currency",
    "input_digest",
    "result_digest"
  ) &&
    validBacktestResultsDetailMetrics(value.metrics) &&
    rows(value.equity, validBacktestResultsEquityItem) &&
    rows(value.monthly_returns, validBacktestResultsMonthlyReturn) &&
    rows(value.pnl_distribution, validBacktestResultsDistributionItem) &&
    rows(value.trade_page.items, validBacktestResultsTrade) &&
    integer(value.trade_page.total_count) &&
    nullableText(value.trade_page.next_cursor) &&
    value.revision === 1;
}

function validPage(value: unknown, key: string, valid: (row: unknown) => boolean) {
  return record(value) && integer(value.total) && integer(value.offset) &&
    integer(value.limit) && (value.limit as number) > 0 && rows(value[key], valid);
}
function validStrategy(value: unknown): boolean {
  return record(value) && text(value.strategy_id) && integer(value.version) &&
    ["DISCOVERED", "READY", "WARNING", "ACTIVE", "STOPPED", "ERROR"].includes(value.status as string) &&
    rows(value.available_commands, (command) =>
      ["START", "STOP", "RESUME", "FORCE_RECOVER"].includes(command as string)) &&
    nullableNumber(value.last_heartbeat) && nullableNumber(value.uptime_start) &&
    nullableText(value.last_error_message) &&
    ["entered_error_at", "recovered_at", "stopped_at"].every((key) => nullableText(value[key])) &&
    ["config", "performance"].every((key) => value[key] === null || record(value[key]));
}
function validEpoch(value: unknown): value is Epoch {
  return record(value) &&
    ["id", "strategy_id", "started_at", "eval_pair", "eval_timeframe", "eval_start_date", "eval_end_date"].every((key) => text(value[key])) &&
    ["pop_size", "max_generations"].every((key) => integer(value[key])) &&
    (value.generations_run === null || integer(value.generations_run)) &&
    nullableText(value.finished_at) && nullableText(value.best_score) &&
    Number.isInteger(value.seed) && record(value.config_json) &&
    ["running", "completed", "aborted"].includes(value.status as string);
}
function validGeneration(value: unknown): boolean {
  return record(value) && integer(value.generation_index) && integer(value.candidate_count) &&
    ["score_min", "score_max", "drawdown_min", "drawdown_max"].every((key) => text(value[key]));
}
function validGene(value: unknown): value is Gene {
  return record(value) && integer(value.id) && integer(value.generation_index) &&
    ["strategy_id", "candidate_id", "epoch_id", "created_at", "score_total", "max_drawdown"].every((key) => text(value[key])) &&
    ["challenger", "champion", "retired"].includes(value.role as string) &&
    record(value.param_pack) && record(value.score_breakdown);
}

function validGaJob(value: unknown): value is GaJob {
  return record(value) && text(value.id) && value.kind === "ga" &&
    ["QUEUED", "RUNNING", "PAUSING", "PAUSED", "CANCELLING", "CANCELLED", "SUCCEEDED", "FAILED"].includes(value.status as string) &&
    Number.isSafeInteger(value.version) && (value.version as number) > 0 &&
    nullableText(value.epoch_id) && Number.isSafeInteger(value.completed_generation) &&
    (value.completed_generation as number) >= -1 &&
    (value.checkpoint === null || (record(value.checkpoint) && text(value.checkpoint.epoch_id) &&
      Number.isSafeInteger(value.checkpoint.completed_generation) && (value.checkpoint.completed_generation as number) >= 0)) &&
    nullableText(value.retry_of_job_id) && record(value.request) &&
    (value.error === null || value.error === "control_plane_interrupted" || value.error === "ga_execution_failed");
}

function validGaJobEnvelope(value: unknown): value is GaJobEnvelope {
  return record(value) && value.schema_version === 1 && validGaJob(value.job);
}

export async function loadGaProfile(): Promise<{ schema_version: 1; profile: GaProfile }> {
  const value = await request<unknown>(
    "/api/v1/ga-profiles/golden_cross_research_v1",
    { cache: "no-store" }
  );
  if (!record(value) || value.schema_version !== 1 || !record(value.profile) ||
      value.profile.parameter_search_profile_id !== "golden_cross_research_v1" ||
      value.profile.strategy_subject !== "builtin:golden_cross" ||
      !text(value.profile.profile_revision) || !text(value.profile.strategy_version) ||
      !text(value.profile.fitness_profile_id) || !text(value.profile.cost_profile_id) ||
      !record(value.profile.accepted_fields)) {
    throw new Error("invalid_response");
  }
  return value as { schema_version: 1; profile: GaProfile };
}

export async function loadGaJobs(limit = 50, offset = 0): Promise<GaJobsPage> {
  const value = await request<unknown>(
    `/api/v1/ga-jobs?limit=${limit}&offset=${offset}`,
    { cache: "no-store" }
  );
  if (!record(value) || value.schema_version !== 1 || !rows(value.items, validGaJob) ||
      !integer(value.total_count) || !integer(value.limit) || value.limit !== limit ||
      !integer(value.offset) || value.offset !== offset) {
    throw new Error("invalid_response");
  }
  return value as GaJobsPage;
}

export async function loadGaJob(jobId: string): Promise<GaJobEnvelope> {
  const value = await request<unknown>(`/api/v1/ga-jobs/${encodeURIComponent(jobId)}`, {
    cache: "no-store"
  });
  if (!validGaJobEnvelope(value) || value.job.id !== jobId) throw new Error("invalid_response");
  return value;
}

export async function sendGaCommand(
  operation: GaJobOperation,
  jobId: string | null,
  idempotencyKey: string,
  body: Record<string, unknown>,
  csrfToken: string
): Promise<GaJobEnvelope> {
  const path = operation === "submit"
    ? "/api/v1/ga-jobs"
    : `/api/v1/ga-jobs/${encodeURIComponent(jobId ?? "")}/${operation}`;
  const value = await request<unknown>(path, {
    method: "POST",
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
      "X-CSRF-Token": csrfToken
    },
    body: JSON.stringify(body)
  }, operation === "submit" || operation === "retry" ? 202 : 200);
  if (!validGaJobEnvelope(value) ||
      (operation === "submit" || operation === "retry" ? value.job.status !== "QUEUED" : false)) {
    throw new Error("invalid_response");
  }
  return value;
}

export async function promoteResearchCandidate(
  geneId: number,
  reason: string | null,
  csrfToken: string
): Promise<ResearchCandidatePromotion> {
  const value = await request<unknown>(`/genes/${geneId}/promote`, {
    method: "POST",
    cache: "no-store",
    headers: {
      "Content-Type": "application/json",
      "X-CSRF-Token": csrfToken
    },
    body: JSON.stringify({ reason })
  }, 200);
  if (!record(value) || value.scope !== "RESEARCH_CANDIDATE_ONLY" || !record(value.gene) ||
      value.gene.gene_id !== geneId || !text(value.gene.strategy_id) ||
      value.gene.role !== "champion" ||
      !nullableText(value.gene.activated_at) ||
      !Array.isArray(value.gene.retired_gene_ids) ||
      !value.gene.retired_gene_ids.every((id) => Number.isSafeInteger(id) && (id as number) >= 0)) {
    throw new Error("invalid_response");
  }
  return value as ResearchCandidatePromotion;
}

function validGeneLifecycleEvent(value: unknown): value is GeneLifecycleEvent {
  return record(value) && integer(value.id) &&
    ["gene_promote", "gene_retire"].includes(value.event_type as string) &&
    nullableText(value.event_subtype) && nullableText(value.related_strategy_id) &&
    nullableText(value.related_order_id) &&
    (value.related_gene_id === null || integer(value.related_gene_id)) &&
    record(value.payload) && nullableText(value.created_at);
}
