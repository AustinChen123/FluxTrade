// @vitest-environment jsdom

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError, type BrowserSession, type Epoch, type GaJob, type GaProfile, type Gene, type GenerationSummary, type GeneLifecycleEvent } from "../../api";
import i18n from "../../shared/i18n";
import { supportsGaProfile } from "./gaOperationsModel";
import { ResearchRoute } from "./ResearchRoute";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadEpochById: vi.fn(),
  loadEpochs: vi.fn(),
  loadGenerationGenes: vi.fn(),
  loadGenerationSummaries: vi.fn(),
  loadGaProfile: vi.fn(),
  loadGaJobs: vi.fn(),
  loadGaJob: vi.fn(),
  sendGaCommand: vi.fn(),
  loadGene: vi.fn(),
  loadGeneLifecycleEvents: vi.fn(),
  promoteResearchCandidate: vi.fn(),
  renewBrowserSession: vi.fn()
}));
const charts = vi.hoisted(() => ({
  render: vi.fn()
}));

vi.mock("../../api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api")>()),
  ...api
}));
vi.mock("../../shared/charts/EChart", () => ({
  EChart: (props: {
    ariaLabel: string;
    option: unknown;
    updateOption?: unknown;
  }) => {
    charts.render(props);
    return <div aria-label={props.ariaLabel} />;
  }
}));
vi.mock("./FitnessSurface3D", () => ({
  FitnessSurface3D: ({
    ariaLabel,
    observationLabel,
    onDataClick
  }: {
    ariaLabel: string;
    observationLabel: string;
    onDataClick: (data: unknown) => void;
  }) => (
    <button
      type="button"
      aria-label={ariaLabel}
      data-observation-label={observationLabel}
      onClick={() => onDataClick([5, 20, 1, 2, "candidate-b"])}
    />
  )
}));

const epoch: Epoch = {
  id: "epoch",
  strategy_id: "strategy",
  started_at: "2026-07-28T00:00:00Z",
  finished_at: "2026-07-28T01:00:00Z",
  pop_size: 1,
  max_generations: 1,
  generations_run: 1,
  best_score: "1",
  seed: 1,
  config_json: { objective: "maximize_score" },
  status: "completed",
  eval_pair: "RITHMIC:MNQ_ROLL-PERP",
  eval_start_date: "2026-01-01",
  eval_end_date: "2026-07-28",
  eval_timeframe: "5m"
};
const summary: GenerationSummary = {
  generation_index: 0,
  candidate_count: 1,
  score_min: "1",
  score_max: "1",
  drawdown_min: "0.1",
  drawdown_max: "0.1"
};
function gene(epochId = "epoch", id = 1): Gene {
  return {
    id,
    strategy_id: `strategy-${epochId}`,
    role: "challenger",
    param_pack: { fast: 5, slow: 20 },
    score_total: String(id),
    score_breakdown: {},
    max_drawdown: "0.1",
    generation_index: 0,
    candidate_id: epochId === "epoch" ? "candidate" : `candidate-${epochId}`,
    epoch_id: epochId,
    created_at: "2026-07-28T01:00:00Z"
  };
}

const gaProfile = {
  parameter_search_profile_id: "golden_cross_research_v1",
  profile_revision: "a".repeat(64), strategy_subject: "builtin:golden_cross",
  strategy_version: "b".repeat(64), fitness_profile_id: "mark_to_market_pnl_v1",
  cost_profile_id: "explicit_accounting_v1",
  accepted_fields: {
    parameter_search_profile_id: { required: true, type: "string", const: "golden_cross_research_v1", immutable: "server_profile" },
    strategy_subject: { required: true, type: "string", const: "builtin:golden_cross", immutable: "server_profile" },
    fitness_profile_id: { required: true, type: "string", const: "mark_to_market_pnl_v1", immutable: "server_profile" },
    cost_profile_id: { required: true, type: "string", const: "explicit_accounting_v1", immutable: "server_profile" },
    profile_revision: { required: true, type: "sha256", immutable: "server_profile" },
    strategy_version: { required: true, type: "sha256", immutable: "imported_builtin_source" },
    dataset_id: { required: true, type: "string", source: "sealed_metadata.id", immutable: "compiled_binding" },
    start_time: { required: true, type: "strict_integer_utc_ms", minimum: 0, maximum: 253402300799999, constraint: "within sealed coverage and <= end_time", immutable: "compiled_binding" },
    end_time: { required: true, type: "strict_integer_utc_ms", minimum: 0, maximum: 253402300799999, constraint: "inclusive; within sealed coverage and >= start_time", immutable: "compiled_binding" },
    initial_balance: { required: true, type: "finite_decimal_or_decimal_string", exclusive_minimum: "0", immutable: "compiled_binding" },
    fees: { required: true, type: "object", immutable: "compiled_binding", fields: {
      maker: { required: true, type: "finite_decimal_or_decimal_string", minimum: "0", immutable: "compiled_binding" },
      taker: { required: true, type: "finite_decimal_or_decimal_string", minimum: "0", immutable: "compiled_binding" }
    } },
    instrument: { required: true, type: "object", immutable: "compiled_binding", constraint: "dated_future products require quantity_step and price_tick", fields: {
      multiplier: { required: false, type: "finite_decimal_or_decimal_string", default: "1", exclusive_minimum: "0", immutable: "compiled_binding" },
      quantity_step: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "required for dated_future products", immutable: "compiled_binding" },
      price_tick: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "required for dated_future products", immutable: "compiled_binding" },
      fee_model: { required: false, type: "string_enum", default: "percentage_notional", choices: ["per_contract", "percentage_notional"], immutable: "compiled_binding" },
      capital_model: { required: false, type: "string_enum", default: "notional", choices: ["notional", "per_contract"], immutable: "compiled_binding" },
      capital_per_contract: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "positive and required only when capital_model is per_contract", immutable: "compiled_binding" }
    } },
    parameters: { fields: {
      short_window: { required: false, type: "integer_range", default: { min: 5, max: 50, step: 5 }, constraint: "min <= max; step > 0", immutable: "compiled_binding", fields: {
        min: { required: true, type: "strict_integer", minimum: 1, maximum: 10000, immutable: "compiled_binding" },
        max: { required: true, type: "strict_integer", minimum: 1, maximum: 10000, immutable: "compiled_binding" },
        step: { required: true, type: "strict_integer", exclusive_minimum: 0, immutable: "compiled_binding" }
      } },
      long_window: { required: false, type: "integer_range", default: { min: 60, max: 200, step: 10 }, constraint: "min <= max; step > 0", immutable: "compiled_binding", fields: {
        min: { required: true, type: "strict_integer", minimum: 2, maximum: 10000, immutable: "compiled_binding" },
        max: { required: true, type: "strict_integer", minimum: 2, maximum: 10000, immutable: "compiled_binding" },
        step: { required: true, type: "strict_integer", exclusive_minimum: 0, immutable: "compiled_binding" }
      } },
      quantity: { required: false, type: "finite_decimal_or_decimal_string", default: "0.01", exclusive_minimum: "0", immutable: "compiled_binding" }
    }, required: false, type: "object", dependency: "short_window.max < long_window.min", quantity_constraint: "must align to instrument.quantity_step when configured" },
    population_size: { required: false, type: "strict_integer", default: 32, minimum: 2, maximum: 256, constraint: "must not exceed parameter Cartesian cardinality", immutable: "compiled_binding" },
    max_generations: { required: false, type: "strict_integer", default: 10, minimum: 1, maximum: 100, immutable: "compiled_binding" },
    seed: { required: false, type: "strict_integer", default: 0, minimum: 0, maximum: 2147483647, immutable: "compiled_binding" }
  },
  compiled_fields: { kind: "parameter_search", strategy_type: "golden_cross", strategy_id: "golden_cross", objective: "maximize_score", market_data: "sealed_dataset", write_reports: false, capital_allocation: null, evaluation_set: null, fitness: null },
  evolution_defaults: { tournament_size: 2, elite_count: 1, crossover_probability: "0.9", mutation_probability: "0.1", mutation_sigma_steps: "1" }
} as unknown as GaProfile;

const gaJob: GaJob = {
  id: "ga-route-job", kind: "ga", status: "QUEUED", version: 1,
  epoch_id: null, completed_generation: -1, checkpoint: null,
  retry_of_job_id: null, request: { ga_binding: { input_digest: "c".repeat(64) } }, error: null
};
const gaJob2: GaJob = {
  ...gaJob, id: "ga-route-job-2", status: "FAILED", version: 9, epoch_id: "epoch-2",
  completed_generation: 0, checkpoint: { epoch_id: "epoch-2", completed_generation: 0 },
  error: "ga_execution_failed"
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("ResearchRoute", () => {
  beforeEach(async () => {
    vi.clearAllMocks();
    await i18n.changeLanguage("zh-TW");
    api.ensureBrowserSession.mockResolvedValue(undefined);
    api.loadEpochById.mockResolvedValue(epoch);
    api.loadEpochs.mockResolvedValue([epoch]);
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene()]);
    api.loadGaProfile.mockResolvedValue({ schema_version: 1, profile: gaProfile });
    api.loadGaJobs.mockResolvedValue({ schema_version: 1, items: [gaJob], total_count: 1, limit: 50, offset: 0 });
    api.loadGaJob.mockResolvedValue({ schema_version: 1, job: gaJob });
    api.sendGaCommand.mockResolvedValue({ schema_version: 1, job: gaJob });
    api.loadGene.mockImplementation(async (id: number) => gene("epoch", id));
    api.loadGeneLifecycleEvents.mockResolvedValue([]);
    api.promoteResearchCandidate.mockResolvedValue({
      scope: "RESEARCH_CANDIDATE_ONLY", gene: {
        gene_id: 1, strategy_id: "strategy-epoch", role: "champion",
        activated_at: "2026-10-05T12:00:00Z", retired_gene_ids: [2]
      }
    });
    api.renewBrowserSession.mockResolvedValue(undefined);
  });

  afterEach(cleanup);

  it("shows the exact submitted profile values and financial text before confirmation", async () => {
    expect(supportsGaProfile(gaProfile)).toBe(true);
    api.ensureBrowserSession.mockResolvedValue({
      actor: "operator@example.test", capabilities: [], permissions: { can_mutate: true, can_step_up: false },
      csrf_token: "csrf", expires_at: "2026-10-05T12:00:00Z", step_up_expires_at: null
    });
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ content }) => content}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");
    await screen.findByRole("button", { name: "檢視研究提交內容" });
    const set = (label: string, value: string) => fireEvent.change(screen.getByLabelText(new RegExp(label)), { target: { value } });
    set("封存資料集 ID", "sealed-dataset-1");
    set("包含起點・UTC 毫秒", "1767225600000");
    set("包含終點・UTC 毫秒", "1767311999999");
    set("初始餘額", "123456789012345678.000000000000000001");
    set("Maker 費率", "0.000000000000000001");
    set("Taker 費率", "0.000000000000000002");
    set("商品乘數", "1.000000000000000001");
    set("數量步進", "0.0001");
    set("價格跳動", "0.25");
    fireEvent.change(screen.getByLabelText("資本模型"), { target: { value: "per_contract" } });
    set("每口資本", "40.000000000000000001");
    set("固定數量", "0.000000000000000013");
    fireEvent.click(screen.getByRole("button", { name: "檢視研究提交內容" }));

    const preview = document.querySelector(".ga-confirm-facts")?.textContent ?? "";
    for (const value of [
      "123456789012345678.000000000000000001", "0.000000000000000001", "0.000000000000000002",
      "1.000000000000000001", "0.0001", "0.25", "percentage_notional", "per_contract",
      "40.000000000000000001", "5", "50", "60", "200", "0.000000000000000013", "32", "10", "0"
    ]) expect(preview).toContain(value);
    expect(api.sendGaCommand).not.toHaveBeenCalled();
  });

  it("offers a manual full refresh for visible research", async () => {
    api.ensureBrowserSession.mockResolvedValue({
      actor: "operator@example.test", capabilities: [], permissions: { can_mutate: true, can_step_up: false },
      csrf_token: "csrf", expires_at: "2026-10-05T12:00:00Z", step_up_expires_at: null
    });
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");
    await waitFor(() => expect(api.loadGaJobs).toHaveBeenCalledTimes(1));
    expect(api.loadEpochs).toHaveBeenCalledTimes(1);
    expect(api.loadGaJobs).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole("button", { name: "重新整理研究資料" }));

    await waitFor(() => expect(api.loadEpochs).toHaveBeenCalledTimes(2));
    expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(2);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(2);
    expect(api.loadGaJobs).toHaveBeenCalledTimes(2);
  });

  it("shows the prior GA detail as stale and disables its actions after refresh fails", async () => {
    const running: GaJob = { ...gaJob, status: "RUNNING", version: 4 };
    api.ensureBrowserSession.mockResolvedValue({
      actor: "operator@example.test", capabilities: [], permissions: { can_mutate: true, can_step_up: false },
      csrf_token: "csrf", expires_at: "2026-10-05T12:00:00Z", step_up_expires_at: null
    });
    api.loadGaJobs.mockResolvedValueOnce({ schema_version: 1, items: [running], total_count: 1, limit: 50, offset: 0 })
      .mockRejectedValueOnce(new ApiError("unavailable", 503));
    api.loadGaJob.mockResolvedValue({ schema_version: 1, job: running });
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await screen.findByRole("button", { name: /ga-route-job/ });
    expect(await screen.findAllByText("執行中")).toHaveLength(2);

    fireEvent.click(screen.getByRole("button", { name: "重新整理 GA 狀態" }));

    await screen.findByText("工作詳情已過期；重新整理成功後才能執行動作。");
    expect(screen.getAllByText("ga-route-job")).toHaveLength(2);
    expect((screen.getByRole("button", { name: "暫停" }) as HTMLButtonElement).disabled).toBe(true);
    expect(api.sendGaCommand).not.toHaveBeenCalled();
  });

  it("requires an explicit step-up renewal and confirms a research-only candidate receipt", async () => {
    const baseSession: BrowserSession = {
      actor: "operator@example.test", capabilities: [],
      permissions: { can_mutate: true, can_step_up: false }, csrf_token: "csrf",
      expires_at: "2026-10-05T12:00:00Z", step_up_expires_at: null
    };
    const steppedUp: BrowserSession = {
      ...baseSession, permissions: { can_mutate: true, can_step_up: true },
      step_up_expires_at: "2026-10-05T12:30:00Z"
    };
    api.ensureBrowserSession.mockResolvedValue(baseSession);
    api.renewBrowserSession.mockResolvedValue(steppedUp);
    const event: GeneLifecycleEvent = {
      id: 12, event_type: "gene_promote", event_subtype: null,
      related_strategy_id: "strategy-epoch", related_order_id: null,
      related_gene_id: 1, payload: {}, created_at: "2026-10-05T12:00:00Z"
    };
    api.loadGeneLifecycleEvents.mockImplementation(async (geneId: number, _strategyId: string, eventType: "gene_promote" | "gene_retire") => {
      if (geneId === 1 && eventType === "gene_promote") return [event];
      if (geneId === 2 && eventType === "gene_retire") {
        return [{ ...event, id: 13, related_gene_id: 2, event_type: "gene_retire" }];
      }
      return [];
    });
    api.promoteResearchCandidate.mockRejectedValueOnce(new ApiError("ga_backend_unavailable", 503));
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ content }) => content}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");
    const prepare = await screen.findByRole("button", { name: "準備晉升" });
    expect((prepare as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "更新操作員加強驗證" }));
    await waitFor(() => expect(api.renewBrowserSession).toHaveBeenCalledTimes(1));
    await waitFor(() => expect((screen.getByRole("button", { name: "準備晉升" }) as HTMLButtonElement).disabled).toBe(false));
    fireEvent.click(screen.getByRole("button", { name: "準備晉升" }));
    fireEvent.change(screen.getByLabelText("晉升原因（選填）"), { target: { value: "reviewed research candidate" } });
    fireEvent.click(screen.getByRole("button", { name: "檢視候選晉升" }));
    expect(screen.getByText(/不會部署策略或啟用實盤交易/)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "確認僅限研究的晉升" }));

    expect(await screen.findByText(/晉升回應不明確/)).toBeTruthy();
    expect(api.promoteResearchCandidate).toHaveBeenCalledTimes(1);
    expect(screen.getByRole("button", { name: "檢視候選晉升" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: "確認僅限研究的晉升" })).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "檢視候選晉升" }));
    fireEvent.click(screen.getByRole("button", { name: "確認僅限研究的晉升" }));

    await waitFor(() => expect(api.promoteResearchCandidate).toHaveBeenCalledWith(1, "reviewed research candidate", "csrf"));
    expect(api.promoteResearchCandidate).toHaveBeenCalledTimes(2);
    expect(await screen.findByText(/範圍：僅限研究候選/)).toBeTruthy();
    expect(screen.getAllByText("gene_promote").length).toBeGreaterThan(0);
    expect(screen.getByText("gene_retire")).toBeTruthy();
  });

  it("loads the next persisted-job page, opens its exact epoch, and retries the selected version", async () => {
    const page = (items: GaJob[]) => ({ schema_version: 1, items, total_count: 2, limit: 50, offset: 0 });
    const retried: GaJob = { ...gaJob, id: "ga-retry-job", retry_of_job_id: gaJob2.id };
    api.ensureBrowserSession.mockResolvedValue({
      actor: "operator@example.test", capabilities: [], permissions: { can_mutate: true, can_step_up: false },
      csrf_token: "csrf", expires_at: "2026-10-05T12:00:00Z", step_up_expires_at: null
    });
    api.loadGaJobs.mockResolvedValueOnce(page([gaJob])).mockResolvedValueOnce(page([gaJob2])).mockResolvedValue(page([gaJob]));
    api.loadGaJob.mockImplementation(async (id: string) => ({
      schema_version: 1, job: id === gaJob2.id ? gaJob2 : id === retried.id ? retried : gaJob
    }));
    api.sendGaCommand.mockResolvedValue({ schema_version: 1, job: retried });
    api.loadEpochById.mockResolvedValue({ ...epoch, id: "epoch-2" });
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ content }) => content}
      </ResearchRoute>
    );
    await screen.findByRole("button", { name: /ga-route-job/ });
    fireEvent.click(screen.getByRole("button", { name: "載入下一個 ID 順序頁面" }));
    await screen.findByRole("button", { name: /ga-route-job-2/ });
    expect(api.loadGaJobs).toHaveBeenCalledWith(50, 1);

    fireEvent.click(screen.getByRole("button", { name: /ga-route-job-2/ }));
    expect(await screen.findByText("epoch-2 · 0")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "開啟此作業已提交的世代批次 epoch-2" }));
    await waitFor(() => expect(api.loadEpochById).toHaveBeenCalledWith("epoch-2"));

    fireEvent.click(screen.getByRole("button", { name: "重試" }));
    fireEvent.click(screen.getByRole("button", { name: "確認建立新重試作業" }));
    await waitFor(() => expect(api.sendGaCommand).toHaveBeenCalledWith(
      "retry", gaJob2.id, expect.any(String), { expected_version: 9 }, "csrf"
    ));
    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
  });

  it("keeps the request owner mounted while both hidden slots are null", async () => {
    const epochs = deferred<Epoch[]>();
    api.loadEpochs.mockReturnValue(epochs.promise);
    const view = render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => (
          <div>
            <div data-testid="toolbar-slot">{toolbar}</div>
            <div data-testid="content-slot">{content}</div>
          </div>
        )}
      </ResearchRoute>
    );
    await waitFor(() => expect(api.loadEpochs).toHaveBeenCalledTimes(1));

    view.rerender(
      <ResearchRoute visible={false} demoMode={false} theme="light">
        {({ toolbar, content }) => (
          <div>
            <div data-testid="toolbar-slot">{toolbar}</div>
            <div data-testid="content-slot">{content}</div>
          </div>
        )}
      </ResearchRoute>
    );
    expect(screen.getByTestId("toolbar-slot").childElementCount).toBe(0);
    expect(screen.getByTestId("content-slot").childElementCount).toBe(0);

    epochs.resolve([epoch]);
    await waitFor(() =>
      expect(api.loadGenerationGenes).toHaveBeenCalledWith("epoch", 0)
    );
    view.rerender(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => (
          <div>
            <div data-testid="toolbar-slot">{toolbar}</div>
            <div data-testid="content-slot">{content}</div>
          </div>
        )}
      </ResearchRoute>
    );

    expect(await screen.findAllByText("candidate")).toHaveLength(2);
    expect(screen.getByRole("combobox", { name: "演化批次" })).toBeTruthy();
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(1);
    expect(api.loadEpochs).toHaveBeenCalledTimes(1);
  });

  it("renders canonical epoch instants in the Berlin calendar and rejects malformed direct fields", async () => {
    api.loadEpochs.mockResolvedValue([
      { ...epoch, id: "winter", started_at: "2026-01-15T23:30:00Z" },
      { ...epoch, id: "summer", started_at: "2026-07-15T12:34:00Z" },
      { ...epoch, id: "invalid", started_at: "not-a-timestamp" }
    ]);
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar }) => toolbar}
      </ResearchRoute>
    );

    const options = await screen.findAllByRole("option");
    expect(options.map((option) => option.textContent)).toEqual([
      expect.stringContaining("2026/01/16"),
      expect.stringContaining("2026/07/15"),
      "strategy · —"
    ]);
  });

  it("keeps a pending summary request and its dependent cache across hiding", async () => {
    const summaries = deferred<GenerationSummary[]>();
    api.loadGenerationSummaries.mockReturnValue(summaries.promise);
    const view = render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await waitFor(() =>
      expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(1)
    );

    view.rerender(
      <ResearchRoute visible={false} demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    summaries.resolve([summary]);
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1));

    view.rerender(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    expect(await screen.findAllByText("candidate")).toHaveLength(2);
    expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);
  });

  it("keeps a pending gene request and exact selection across hiding", async () => {
    const genes = deferred<Gene[]>();
    api.loadGenerationGenes.mockReturnValue(genes.promise);
    const view = render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1));

    view.rerender(
      <ResearchRoute visible={false} demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    genes.resolve([gene()]);
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1));

    view.rerender(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    expect(await screen.findAllByText("candidate")).toHaveLength(2);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);
  });

  it("clears the previous epoch while the next epoch is loading", async () => {
    const epochB = { ...epoch, id: "b", strategy_id: "strategy-b" };
    const epochBSummaries = deferred<GenerationSummary[]>();
    api.loadEpochs.mockResolvedValue([epoch, epochB]);
    api.loadGenerationSummaries.mockImplementation((epochId: string) =>
      epochId === epoch.id ? Promise.resolve([summary]) : epochBSummaries.promise
    );
    api.loadGenerationGenes.mockImplementation((epochId: string) =>
      Promise.resolve([gene(epochId, epochId === epoch.id ? 1 : 2)])
    );
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");

    fireEvent.change(screen.getByRole("combobox", { name: "演化批次" }), {
      target: { value: "b" }
    });

    expect(screen.queryByText("candidate")).toBeNull();
    await waitFor(() =>
      expect(api.loadGenerationSummaries).toHaveBeenCalledWith("b")
    );
    epochBSummaries.resolve([summary]);
    await screen.findAllByText("candidate-b");

    const transitionalSurfaces = charts.render.mock.calls
      .map(([props]) => props as {
        ariaLabel: string;
        option: unknown;
        updateOption?: unknown;
      })
      .filter(
        ({ ariaLabel, option }) =>
          ariaLabel === "兩個策略參數與候選適應度的觀測地形圖" &&
          JSON.stringify(option) === "{}"
      );
    expect(transitionalSurfaces.length).toBeGreaterThan(0);
    expect(
      transitionalSurfaces.every(
        ({ updateOption }) => JSON.stringify(updateOption) === "{}"
      )
    ).toBe(true);
  });

  it("retries downstream data without reloading successful epochs", async () => {
    api.loadGenerationSummaries
      .mockRejectedValueOnce(new Error("summary unavailable"))
      .mockResolvedValueOnce([summary]);
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );

    expect(await screen.findByText("研究資料未載入")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "重新讀取" }));

    await waitFor(() => {
      expect(api.loadEpochs).toHaveBeenCalledTimes(1);
      expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(2);
      expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);
    });
  });

  it("keeps the two surface axes distinct", async () => {
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");

    const xAxis = screen.getByLabelText("X") as HTMLSelectElement;
    const yAxis = screen.getByLabelText("Y") as HTMLSelectElement;
    await waitFor(() => {
      expect(xAxis.value).toBe("fast");
      expect(yAxis.value).toBe("slow");
    });
    expect(
      Array.from(xAxis.options).find((option) => option.value === "slow")
        ?.disabled
    ).toBe(true);
    expect(
      Array.from(yAxis.options).find((option) => option.value === "fast")
        ?.disabled
    ).toBe(true);
  });

  it("owns the complete 2D/3D selection transition", async () => {
    api.loadGenerationGenes.mockResolvedValue([gene(), gene("b", 2)]);
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");

    fireEvent.click(screen.getByRole("button", { name: "3D 地形" }));
    const surface3d = await screen.findByRole("button", {
      name: "兩個策略參數與候選適應度的互動式三維插值地形圖"
    });
    expect(surface3d.dataset.observationLabel).toBe("觀測候選");
    fireEvent.click(surface3d);
    expect(screen.getAllByText("candidate-b")).toHaveLength(2);
    fireEvent.click(screen.getByRole("button", { name: "2D 色階" }));
    expect(
      screen.getByLabelText("兩個策略參數與候選適應度的觀測地形圖")
    ).toBeTruthy();
  });

  it("fails closed when the epoch objective is unsupported", async () => {
    api.loadEpochs.mockResolvedValue([
      { ...epoch, config_json: { objective: "maximize_magic" } }
    ]);
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );

    expect(
      await screen.findByText(
        "無法辨識這個批次的最佳化目標，已停止繪製候選地形。"
      )
    ).toBeTruthy();
    expect(screen.getByText("不支援的最佳化目標")).toBeTruthy();
    expect(
      screen.queryByLabelText("兩個策略參數與候選適應度的觀測地形圖")
    ).toBeNull();
  });
});
