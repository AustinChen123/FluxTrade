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

type Page<TName extends string, T> = {
  total: number;
  limit: number;
  offset: number;
} & Record<TName, T[]>;

const PAGE_SIZE = 10_000;

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
  const body = (await response.json()) as T | { error?: string };
  if (!response.ok) {
    const reason =
      "error" in (body as object)
        ? String((body as { error?: string }).error)
        : response.statusText;
    throw new ApiError(reason, response.status);
  }
  return body as T;
}

export async function ensureBrowserSession(): Promise<void> {
  try {
    await request("/api/v1/auth/session");
  } catch (error) {
    if (!(error instanceof ApiError) || error.status !== 401) {
      if (error instanceof ApiError && error.status === 404) {
        return;
      }
      throw error;
    }
    await request("/api/v1/auth/session", { method: "POST" });
  }
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
