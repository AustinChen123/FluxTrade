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

export type BacktestResultsPageOptions = {
  limit?: number;
  cursor?: string;
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

async function request<T>(path: string, init?: RequestInit): Promise<T> {
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
function validEpoch(value: unknown): boolean {
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
function validGene(value: unknown): boolean {
  return record(value) && integer(value.id) && integer(value.generation_index) &&
    ["strategy_id", "candidate_id", "epoch_id", "created_at", "score_total", "max_drawdown"].every((key) => text(value[key])) &&
    ["challenger", "champion", "retired"].includes(value.role as string) &&
    record(value.param_pack) && record(value.score_breakdown);
}
