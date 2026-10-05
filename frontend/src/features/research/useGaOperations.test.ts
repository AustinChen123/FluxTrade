// @vitest-environment jsdom

import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ApiError, type BrowserSession, type GaJob, type GaProfile } from "../../api";
import { GA_AWAITING_STORAGE_KEY } from "./gaOperationsModel";
import { useGaOperations } from "./useGaOperations";
import type { ResearchWorkspace } from "./useResearchWorkspace";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(), loadGaProfile: vi.fn(), loadGaJobs: vi.fn(),
  loadGaJob: vi.fn(), sendGaCommand: vi.fn(), loadGene: vi.fn(),
  loadGeneLifecycleEvents: vi.fn(), promoteResearchCandidate: vi.fn(), renewBrowserSession: vi.fn()
}));
vi.mock("../../api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api")>()), ...api
}));

const session: BrowserSession = {
  actor: "operator@example.test", capabilities: [], permissions: { can_mutate: true, can_step_up: false },
  csrf_token: "csrf", expires_at: "2026-10-05T12:00:00Z", step_up_expires_at: null
};
const job: GaJob = {
  id: "ga-1", kind: "ga", status: "RUNNING", version: 4, epoch_id: "epoch-1",
  completed_generation: 2, checkpoint: { epoch_id: "epoch-1", completed_generation: 2 },
  retry_of_job_id: null, request: { ga_binding: { input_digest: "f".repeat(64) } }, error: null
};
const profile = {
  parameter_search_profile_id: "golden_cross_research_v1", profile_revision: "a".repeat(64),
  strategy_subject: "builtin:golden_cross", strategy_version: "b".repeat(64),
  fitness_profile_id: "mark_to_market_pnl_v1", cost_profile_id: "explicit_accounting_v1",
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
    parameters: { required: false, type: "object", dependency: "short_window.max < long_window.min", quantity_constraint: "must align to instrument.quantity_step when configured", fields: {
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
    } },
    population_size: { required: false, type: "strict_integer", default: 32, minimum: 2, maximum: 256, constraint: "must not exceed parameter Cartesian cardinality", immutable: "compiled_binding" },
    max_generations: { required: false, type: "strict_integer", default: 10, minimum: 1, maximum: 100, immutable: "compiled_binding" },
    seed: { required: false, type: "strict_integer", default: 0, minimum: 0, maximum: 2147483647, immutable: "compiled_binding" }
  },
  compiled_fields: { kind: "parameter_search", strategy_type: "golden_cross", strategy_id: "golden_cross", objective: "maximize_score", market_data: "sealed_dataset", write_reports: false, capital_allocation: null, evaluation_set: null, fitness: null },
  evolution_defaults: { tournament_size: 2, elite_count: 1, crossover_probability: "0.9", mutation_probability: "0.1", mutation_sigma_steps: "1" }
} as unknown as GaProfile;
const workspace = {
  session, sessionResolved: true,
  genes: [], selectedGeneId: null, refresh: vi.fn().mockResolvedValue(undefined)
} as unknown as ResearchWorkspace;

function page(items: GaJob[] = [job]) {
  return { schema_version: 1 as const, items, total_count: items.length, limit: 50, offset: 0 };
}
function envelope(value: GaJob = job) {
  return { schema_version: 1 as const, job: value };
}
function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((done, fail) => { resolve = done; reject = fail; });
  return { promise, resolve, reject };
}

describe("Research GA intent owner", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    window.sessionStorage.clear();
    api.ensureBrowserSession.mockResolvedValue(session);
    api.loadGaProfile.mockResolvedValue({ schema_version: 1, profile });
    api.loadGaJobs.mockResolvedValue(page());
    api.loadGaJob.mockResolvedValue(envelope());
    api.sendGaCommand.mockResolvedValue(envelope({ ...job, version: 5, status: "PAUSING" }));
  });
  afterEach(() => vi.restoreAllMocks());

  it.each([
    { status: 401, code: "unauthorized" },
    { status: 403, code: "forbidden" },
    { status: 404, code: "not_found" },
    { status: 404, code: "dataset_not_found" },
    { status: 422, code: "validation_error" },
    { status: 409, code: "idempotency_conflict" },
    { status: 409, code: "job_version_conflict" }
  ])("persists the captured version/key and retains failed manual resolution $status/$code", async ({ status, code }) => {
    const first = deferred<ReturnType<typeof envelope>>();
    api.sendGaCommand.mockImplementationOnce((operation, jobId, key, body, csrf) => {
      const stored = JSON.parse(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY) ?? "[]")[0];
      expect(stored).toMatchObject({ operation, jobId, idempotencyKey: key, body, actor: session.actor, awaiting: false });
      expect(csrf).toBe("csrf");
      return first.promise;
    }).mockRejectedValueOnce(new ApiError(code, status))
      .mockResolvedValueOnce(envelope({ ...job, version: 5, status: "PAUSING" }));
    const mounted = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(mounted.result.current.selectedJob?.id).toBe("ga-1"));

    let command!: Promise<void>;
    act(() => { command = mounted.result.current.act("pause"); });
    await waitFor(() => expect(api.sendGaCommand).toHaveBeenCalledTimes(1));
    const original = JSON.parse(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY) ?? "[]")[0];
    expect(original.expectedVersion).toBe(4);
    first.reject(new Error("connection lost"));
    await act(async () => command);
    expect(mounted.result.current.intents[0]?.awaiting).toBe(true);

    mounted.unmount();
    const restored = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(restored.result.current.intents).toHaveLength(1));
    expect(restored.result.current.intents[0]).toMatchObject({
      idempotencyKey: original.idempotencyKey, actor: session.actor, body: { expected_version: 4 }, awaiting: true
    });
    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);

    await act(async () => restored.result.current.resolveIntent(restored.result.current.intents[0]));
    expect(restored.result.current.intents[0]?.idempotencyKey).toBe(original.idempotencyKey);
    expect(restored.result.current.error).toMatchObject({ type: "uncertain", status, code });
    restored.unmount();
    const afterRejectedResolutionReload = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(afterRejectedResolutionReload.result.current.intents).toHaveLength(1));
    expect(afterRejectedResolutionReload.result.current.intents[0]).toMatchObject({
      ...original, awaiting: true
    });
    expect(api.sendGaCommand).toHaveBeenCalledTimes(2);
    await act(async () => afterRejectedResolutionReload.result.current.resolveIntent(afterRejectedResolutionReload.result.current.intents[0]));
    await waitFor(() => expect(afterRejectedResolutionReload.result.current.intents).toHaveLength(0));
    expect(api.sendGaCommand.mock.calls.map((call) => call[2])).toEqual([
      original.idempotencyKey, original.idempotencyKey, original.idempotencyKey
    ]);
    expect(api.sendGaCommand.mock.calls.map((call) => call[3])).toEqual([
      { expected_version: 4 }, { expected_version: 4 }, { expected_version: 4 }
    ]);
    expect(window.sessionStorage.getItem("fluxtrade.ga.awaiting")).toBeNull();
  });

  it.each([
    { status: 401, code: "unauthorized" },
    { status: 403, code: "forbidden" },
    { status: 404, code: "not_found" },
    { status: 404, code: "dataset_not_found" },
    { status: 422, code: "validation_error" },
    { status: 409, code: "idempotency_conflict" },
    { status: 409, code: "job_version_conflict" }
  ])("retains a pending manual resolution across actor change after $status/$code", async ({ status, code }) => {
    const originalIntent = {
      operation: "pause", jobId: job.id, expectedVersion: 4,
      idempotencyKey: "captured-resolution-key", actor: session.actor,
      body: { expected_version: 4 }, awaiting: true
    };
    window.sessionStorage.setItem(GA_AWAITING_STORAGE_KEY, JSON.stringify([originalIntent]));
    const response = deferred<ReturnType<typeof envelope>>();
    api.sendGaCommand.mockReturnValueOnce(response.promise);
    const otherActor = {
      ...workspace,
      session: { ...session, actor: "other-operator@example.test", csrf_token: "other-csrf" }
    } as ResearchWorkspace;
    const { result, rerender } = renderHook(
      ({ current }: { current: ResearchWorkspace }) => useGaOperations(true, current),
      { initialProps: { current: workspace } }
    );
    await waitFor(() => expect(result.current.intents).toHaveLength(1));
    let resolving!: Promise<void>;
    act(() => { resolving = result.current.resolveIntent(result.current.intents[0]); });
    await waitFor(() => expect(api.sendGaCommand).toHaveBeenCalledTimes(1));

    rerender({ current: otherActor });
    await waitFor(() => expect(result.current.session?.actor).toBe(otherActor.session?.actor));
    response.reject(new ApiError(code, status));
    await act(async () => resolving);

    expect(result.current.intents).toEqual([originalIntent]);
    expect(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY)).toContain("captured-resolution-key");
    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);

    rerender({ current: workspace });
    await waitFor(() => expect(result.current.session?.actor).toBe(session.actor));
    await waitFor(() => expect(result.current.intents).toEqual([originalIntent]));
    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
    await act(async () => result.current.resolveIntent(result.current.intents[0]));
    expect(api.sendGaCommand).toHaveBeenCalledTimes(2);
    expect(api.sendGaCommand.mock.calls[1]).toEqual([
      "pause", job.id, originalIntent.idempotencyKey, originalIntent.body, session.csrf_token
    ]);
  });

  it("does not read or mutate GA when the session lacks operator capability", async () => {
    const deniedWorkspace = {
      ...workspace,
      session: { ...session, permissions: { ...session.permissions, can_mutate: false } }
    } as ResearchWorkspace;
    const { result } = renderHook(() => useGaOperations(true, deniedWorkspace));
    await waitFor(() => expect(result.current.error).toMatchObject({ type: "permission" }));
    expect(api.loadGaProfile).not.toHaveBeenCalled();
    expect(api.loadGaJobs).not.toHaveBeenCalled();
    await act(async () => result.current.act("pause"));
    expect(api.sendGaCommand).not.toHaveBeenCalled();
  });

  it("clears a definite first rejection but retains an idempotency conflict", async () => {
    api.sendGaCommand.mockRejectedValueOnce(new ApiError("validation_error", 422))
      .mockRejectedValueOnce(new ApiError("idempotency_conflict", 409));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));

    await act(async () => result.current.act("pause"));
    expect(result.current.intents).toHaveLength(0);
    expect(result.current.error).toMatchObject({ type: "rejected", status: 422, code: "validation_error" });

    await act(async () => result.current.act("pause"));
    expect(result.current.intents).toHaveLength(1);
    expect(result.current.intents[0]).toMatchObject({ operation: "pause", awaiting: true });
    expect(result.current.error).toMatchObject({ type: "uncertain", status: 409, code: "idempotency_conflict" });
  });

  it("keeps an in-memory target lock and reports the storage limit when persistence fails", async () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("storage blocked", "SecurityError");
    });
    api.sendGaCommand.mockRejectedValueOnce(new Error("connection lost"));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));

    await act(async () => result.current.act("pause"));
    expect(result.current.storageLimited).toBe(true);
    expect(result.current.intents).toHaveLength(1);
    await act(async () => result.current.act("pause"));
    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
    expect(result.current.intents).toHaveLength(1);
  });

  it("does not start GA reads before session resolution or restore stale reads after demo disable", async () => {
    const profileRequest = deferred<{ schema_version: 1; profile: GaProfile }>();
    const pageRequest = deferred<ReturnType<typeof page>>();
    api.loadGaProfile.mockReturnValueOnce(profileRequest.promise);
    api.loadGaJobs.mockReturnValueOnce(pageRequest.promise);
    const unresolved = { ...workspace, session: null, sessionResolved: false } as unknown as ResearchWorkspace;
    const { result, rerender } = renderHook(
      ({ enabled, current }: { enabled: boolean; current: ResearchWorkspace }) => useGaOperations(enabled, current),
      { initialProps: { enabled: true, current: unresolved } }
    );
    expect(api.loadGaProfile).not.toHaveBeenCalled();
    rerender({ enabled: true, current: workspace });
    await waitFor(() => expect(api.loadGaProfile).toHaveBeenCalledTimes(1));
    rerender({ enabled: false, current: workspace });
    await act(async () => {
      profileRequest.resolve({ schema_version: 1, profile });
      pageRequest.resolve(page([job]));
      await Promise.resolve();
    });
    expect(result.current.profile).toBeNull();
    expect(result.current.selectedJob).toBeNull();
    expect(result.current.error).toBeNull();
    expect(api.loadGaJob).not.toHaveBeenCalled();
  });

  it("ignores pending initial GA reads after unmount", async () => {
    const profileRequest = deferred<{ schema_version: 1; profile: GaProfile }>();
    const jobsRequest = deferred<ReturnType<typeof page>>();
    api.loadGaProfile.mockReturnValueOnce(profileRequest.promise);
    api.loadGaJobs.mockReturnValueOnce(jobsRequest.promise);
    const { unmount } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(api.loadGaProfile).toHaveBeenCalledTimes(1));
    unmount();
    await act(async () => {
      profileRequest.resolve({ schema_version: 1, profile });
      jobsRequest.resolve(page([job]));
      await Promise.resolve();
    });
    expect(api.loadGaJob).not.toHaveBeenCalled();
  });

  it("fences stale successful and failed reads when the authenticated actor changes", async () => {
    const oldProfile = deferred<{ schema_version: 1; profile: GaProfile }>();
    const newProfile = deferred<{ schema_version: 1; profile: GaProfile }>();
    const oldPage = deferred<ReturnType<typeof page>>();
    const newPage = deferred<ReturnType<typeof page>>();
    api.loadGaProfile.mockReturnValueOnce(oldProfile.promise).mockReturnValueOnce(newProfile.promise);
    api.loadGaJobs.mockReturnValueOnce(oldPage.promise).mockReturnValueOnce(newPage.promise);
    const newJob: GaJob = { ...job, id: "ga-new-actor" };
    api.loadGaJob.mockImplementation(async (id: string) => envelope(id === newJob.id ? newJob : job));
    const original = workspace;
    const nextActor = {
      ...workspace, session: { ...session, actor: "next-operator@example.test" }
    } as ResearchWorkspace;
    const { result, rerender } = renderHook(
      ({ current }: { current: ResearchWorkspace }) => useGaOperations(true, current),
      { initialProps: { current: original } }
    );
    await waitFor(() => expect(api.loadGaProfile).toHaveBeenCalledTimes(1));
    rerender({ current: nextActor });
    await waitFor(() => expect(api.loadGaProfile).toHaveBeenCalledTimes(2));

    await act(async () => {
      newProfile.resolve({ schema_version: 1, profile });
      newPage.resolve(page([newJob]));
    });
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(newJob.id));
    expect(result.current.loading).toBe(false);

    await act(async () => {
      oldProfile.resolve({ schema_version: 1, profile });
      oldPage.reject(new ApiError("unavailable", 503));
      await Promise.resolve();
    });
    expect(result.current.selectedJob?.id).toBe(newJob.id);
    expect(result.current.error).toBeNull();
    expect(result.current.loading).toBe(false);
    expect(api.loadGaJob).toHaveBeenCalledTimes(1);
  });

  it.each([
    { source: "initial", selected: "success", stale: "success" },
    { source: "initial", selected: "failure", stale: "failure" },
    { source: "refresh", selected: "success", stale: "failure" },
    { source: "refresh", selected: "failure", stale: "success" }
  ] as const)("retires superseded $source loading without losing selected $selected state", async ({ source, selected, stale }) => {
    const secondJob: GaJob = { ...job, id: "ga-2", status: "PAUSED", version: 8 };
    const oldProfile = deferred<{ schema_version: 1; profile: GaProfile }>();
    const oldPage = deferred<ReturnType<typeof page>>();
    const selectedDetail = deferred<ReturnType<typeof envelope>>();
    if (source === "initial") {
      api.loadGaProfile.mockReturnValueOnce(oldProfile.promise);
      api.loadGaJobs.mockReturnValueOnce(oldPage.promise);
    }
    api.loadGaJob.mockImplementation(async (id: string) =>
      id === secondJob.id ? selectedDetail.promise : envelope(job)
    );
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(api.loadGaProfile).toHaveBeenCalled());
    if (source === "refresh") {
      await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));
      api.loadGaProfile.mockReturnValueOnce(oldProfile.promise);
      api.loadGaJobs.mockReturnValueOnce(oldPage.promise);
      void result.current.refresh();
      await waitFor(() => expect(api.loadGaJobs).toHaveBeenCalledTimes(2));
    }

    act(() => result.current.selectJob(secondJob.id));
    await waitFor(() => expect(api.loadGaJob).toHaveBeenCalledWith(secondJob.id));
    if (selected === "success") {
      selectedDetail.resolve(envelope(secondJob));
    } else {
      selectedDetail.reject(new ApiError("unavailable", 503));
    }
    await waitFor(() => {
      expect(result.current.loadingDetail).toBe(false);
      expect(result.current.error?.type ?? null).toBe(selected === "success" ? null : "read");
    });
    expect(result.current.loading).toBe(false);
    expect(result.current.selectedJobId).toBe(secondJob.id);
    expect(result.current.selectedJob?.id ?? null).toBe(selected === "success" ? secondJob.id : null);

    if (stale === "success") {
      oldProfile.resolve({ schema_version: 1, profile });
      oldPage.resolve(page([job]));
    } else {
      oldProfile.reject(new ApiError("unavailable", 503));
      oldPage.resolve(page([job]));
    }
    await act(async () => { await Promise.resolve(); });
    expect(result.current.loading).toBe(false);
    expect(result.current.selectedJobId).toBe(secondJob.id);
    expect(result.current.selectedJob?.id ?? null).toBe(selected === "success" ? secondJob.id : null);
    expect(result.current.error?.type ?? null).toBe(selected === "success" ? null : "read");
  });

  it("keeps the stream invalidation callback stable as the shared session resolves", async () => {
    const unresolved = { ...workspace, session: null, sessionResolved: false } as unknown as ResearchWorkspace;
    const { result, rerender } = renderHook(
      ({ current }: { current: ResearchWorkspace }) => useGaOperations(true, current),
      { initialProps: { current: unresolved } }
    );
    const initialRefresh = result.current.refresh;
    rerender({ current: workspace });
    await waitFor(() => expect(result.current.profileSupported).toBe(true));
    expect(result.current.refresh).toBe(initialRefresh);
  });

  it("stores one exact financial-text submit intent before the accepted request", async () => {
    const newJob: GaJob = { ...job, id: "ga-new", status: "QUEUED", version: 1, epoch_id: null, completed_generation: -1, checkpoint: null };
    api.loadGaJob
      .mockResolvedValueOnce(envelope(job))
      .mockResolvedValueOnce(envelope(newJob))
      .mockResolvedValueOnce(envelope(newJob));
    api.sendGaCommand.mockImplementationOnce(async (operation, jobId, key, body, csrf) => {
      const stored = JSON.parse(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY) ?? "[]")[0];
      expect(stored).toMatchObject({ operation: "submit", jobId: null, idempotencyKey: key, actor: session.actor, awaiting: false });
      expect(csrf).toBe(session.csrf_token);
      return envelope(newJob);
    });
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.profileSupported).toBe(true));
    const form = result.current.form!;
    act(() => result.current.setForm({
      ...form, dataset_id: "sealed-dataset", start_time: "1767225600000", end_time: "1767311999999",
      initial_balance: "100000000000000000.000000000000000001",
      fees: { maker: "0.0000000000000000001", taker: "0.0000000000000000002" },
      instrument: { ...form.instrument, quantity_step: "0.001", price_tick: "0.25" },
      parameters: {
        short_window: { min: "5", max: "20", step: "5" },
        long_window: { min: "30", max: "60", step: "10" }, quantity: "0.001"
      }, population_size: "12", max_generations: "4", seed: "9"
    }));

    await act(async () => result.current.submit());

    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
    expect(api.sendGaCommand.mock.calls[0]?.slice(0, 3)).toEqual(["submit", null, expect.any(String)]);
    const sent = api.sendGaCommand.mock.calls[0]?.[3] as Record<string, unknown>;
    expect(sent.initial_balance).toBe("100000000000000000.000000000000000001");
    expect(sent.fees).toEqual({ maker: "0.0000000000000000001", taker: "0.0000000000000000002" });
    expect(sent.dataset_id).toBe("sealed-dataset");
    expect(api.loadGaJobs).toHaveBeenCalledTimes(2);
    expect(api.loadGaJob).toHaveBeenCalledTimes(3);
    expect(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY)).toBeNull();
  });

  for (const operation of ["submit", "retry"] as const) {
    it(`restores an uncertain ${operation} after same-tab remount without automatic resend`, async () => {
      const source = operation === "retry" ? { ...job, status: "FAILED" as const } : job;
      const accepted: GaJob = {
        ...job, id: operation === "submit" ? "ga-remount-new" : "ga-remount-retry",
        status: "QUEUED", version: 1, epoch_id: null, completed_generation: -1,
        checkpoint: null, retry_of_job_id: operation === "retry" ? source.id : null
      };
      api.loadGaJobs.mockResolvedValue(page([source]));
      api.loadGaJob.mockImplementation(async (id: string) => envelope(id === accepted.id ? accepted : source));
      api.sendGaCommand.mockRejectedValueOnce(new Error("connection lost"))
        .mockResolvedValueOnce(envelope(accepted));
      const first = renderHook(() => useGaOperations(true, workspace));
      await waitFor(() => expect(first.result.current.profileSupported).toBe(true));

      if (operation === "submit") {
        const form = first.result.current.form!;
        act(() => first.result.current.setForm({
          ...form, dataset_id: "sealed-dataset", start_time: "1767225600000", end_time: "1767311999999",
          initial_balance: "100.000000000000000001", fees: { maker: "0", taker: "0" },
          instrument: { ...form.instrument, multiplier: "1", fee_model: "percentage_notional", capital_model: "notional" },
          parameters: {
            short_window: { min: "2", max: "2", step: "1" },
            long_window: { min: "3", max: "3", step: "1" }, quantity: "1"
          }, population_size: "1", max_generations: "1", seed: "1"
        }));
        await act(async () => first.result.current.submit());
      } else {
        await waitFor(() => expect(first.result.current.selectedJob?.status).toBe("FAILED"));
        await act(async () => first.result.current.act("retry"));
      }

      expect(first.result.current.intents).toHaveLength(1);
      const original = first.result.current.intents[0];
      expect(original).toMatchObject({ operation, actor: session.actor, awaiting: true });
      const firstCall = api.sendGaCommand.mock.calls[0];
      expect(firstCall).toEqual([operation, operation === "submit" ? null : source.id,
        original.idempotencyKey, original.body, session.csrf_token]);
      first.unmount();

      const restored = renderHook(() => useGaOperations(true, workspace));
      await waitFor(() => expect(restored.result.current.intents).toHaveLength(1));
      expect(restored.result.current.intents[0]).toEqual(original);
      expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
      await act(async () => restored.result.current.resolveIntent(restored.result.current.intents[0]));
      expect(api.sendGaCommand).toHaveBeenCalledTimes(2);
      expect(api.sendGaCommand.mock.calls[1]).toEqual(firstCall);
      await waitFor(() => expect(restored.result.current.intents).toHaveLength(0));
    });
  }

  it("keeps an intent blocked when another actor restores the Research route", async () => {
    window.sessionStorage.setItem("fluxtrade.ga.awaiting", JSON.stringify({
      operation: "pause", jobId: "ga-1", expectedVersion: 4, idempotencyKey: "same-key",
      actor: "original@example.test", body: { expected_version: 4 }, awaiting: true
    }));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.intents).toHaveLength(1));
    await act(async () => result.current.resolveIntent(result.current.intents[0]));
    expect(api.sendGaCommand).not.toHaveBeenCalled();
    expect(window.sessionStorage.getItem("fluxtrade.ga.awaiting")).toContain("same-key");
  });

  it("keeps an uncertain source-job intent while an unrelated selected job remains actionable", async () => {
    const secondJob: GaJob = { ...job, id: "ga-2", version: 7, status: "RUNNING" };
    const sourcePage = page([job, secondJob]);
    api.loadGaJobs.mockResolvedValue(sourcePage);
    api.loadGaJob.mockImplementation(async (id: string) => envelope({
      ...(id === "ga-2" ? secondJob : job), id
    }));
    const first = deferred<ReturnType<typeof envelope>>();
    api.sendGaCommand.mockImplementationOnce(() => first.promise)
      .mockResolvedValueOnce(envelope({ ...secondJob, status: "CANCELLING", version: 8 }))
      .mockResolvedValueOnce(envelope({ ...job, status: "PAUSING", version: 5 }));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe("ga-1"));

    let firstCommand!: Promise<void>;
    act(() => { firstCommand = result.current.act("pause"); });
    await waitFor(() => expect(api.sendGaCommand).toHaveBeenCalledTimes(1));
    const sourceIntent = JSON.parse(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY) ?? "[]")[0];
    first.reject(new Error("response lost"));
    await act(async () => firstCommand);
    expect(result.current.intents).toHaveLength(1);
    expect(result.current.intents[0]?.jobId).toBe("ga-1");

    act(() => result.current.selectJob("ga-2"));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe("ga-2"));
    await act(async () => result.current.act("cancel"));
    expect(api.sendGaCommand).toHaveBeenCalledTimes(2);
    expect(api.sendGaCommand.mock.calls[1]?.[0]).toBe("cancel");
    expect(api.sendGaCommand.mock.calls[1]?.[1]).toBe("ga-2");
    expect(result.current.intents).toHaveLength(1);
    expect(result.current.intents[0]?.idempotencyKey).toBe(sourceIntent.idempotencyKey);

    await act(async () => result.current.resolveIntent(result.current.intents[0]));
    expect(api.sendGaCommand).toHaveBeenCalledTimes(3);
    expect(api.sendGaCommand.mock.calls[2]?.[1]).toBe("ga-1");
    expect(api.sendGaCommand.mock.calls[2]?.[2]).toBe(sourceIntent.idempotencyKey);
    await waitFor(() => expect(result.current.intents).toHaveLength(0));
  });

  it("keeps a late successful receipt from replacing a newer selected job", async () => {
    const secondJob: GaJob = { ...job, id: "ga-2", version: 7, status: "RUNNING" };
    api.loadGaJobs.mockResolvedValue(page([job, secondJob]));
    api.loadGaJob.mockImplementation(async (id: string) => envelope({ ...(id === "ga-2" ? secondJob : job), id }));
    const response = deferred<ReturnType<typeof envelope>>();
    api.sendGaCommand.mockImplementationOnce(() => response.promise);
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe("ga-1"));

    let command!: Promise<void>;
    act(() => { command = result.current.act("pause"); });
    await waitFor(() => expect(api.sendGaCommand).toHaveBeenCalledTimes(1));
    act(() => result.current.selectJob("ga-2"));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe("ga-2"));
    response.resolve(envelope({ ...job, status: "PAUSING", version: 5 }));
    await act(async () => command);

    expect(result.current.selectedJobId).toBe("ga-2");
    expect(result.current.selectedJob?.id).toBe("ga-2");
    expect(result.current.intents).toHaveLength(0);
  });

  it("retains the original intent when accepted POST is followed by failed detail read", async () => {
    api.sendGaCommand.mockResolvedValueOnce(envelope({ ...job, status: "PAUSING", version: 5 }))
      .mockResolvedValueOnce(envelope({ ...job, status: "PAUSING", version: 5 }));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe("ga-1"));
    api.loadGaJob.mockRejectedValueOnce(new ApiError("not_found", 404));

    await act(async () => result.current.act("pause"));
    expect(result.current.intents).toHaveLength(1);
    const original = result.current.intents[0];
    expect(original.awaiting).toBe(true);
    expect(result.current.error).toMatchObject({ type: "uncertain", status: 404 });

    await act(async () => result.current.resolveIntent(original));
    expect(api.sendGaCommand.mock.calls.map((call) => call[2])).toEqual([
      original.idempotencyKey, original.idempotencyKey
    ]);
    await waitFor(() => expect(result.current.intents).toHaveLength(0));
  });

  it("retains an accepted intent when the follow-up list refresh fails", async () => {
    api.loadGaJobs.mockResolvedValueOnce(page()).mockRejectedValueOnce(new ApiError("unavailable", 503));
    api.sendGaCommand.mockResolvedValueOnce(envelope({ ...job, status: "PAUSING", version: 5 }));
    api.loadGaJob.mockResolvedValueOnce(envelope(job)).mockResolvedValueOnce(envelope({ ...job, status: "PAUSING", version: 5 }));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe("ga-1"));

    await act(async () => result.current.act("pause"));

    expect(result.current.intents).toHaveLength(1);
    expect(result.current.intents[0]).toMatchObject({
      operation: "pause", jobId: "ga-1", expectedVersion: 4, awaiting: true,
      body: { expected_version: 4 }
    });
    expect(result.current.error).toMatchObject({ type: "read", status: 503 });
    expect(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY)).toContain(result.current.intents[0]?.idempotencyKey);
  });

  it("does not continue command reads after the feature is disabled during an accepted request", async () => {
    const response = deferred<ReturnType<typeof envelope>>();
    api.sendGaCommand.mockReturnValueOnce(response.promise);
    const { result, rerender } = renderHook(
      ({ enabled }: { enabled: boolean }) => useGaOperations(enabled, workspace),
      { initialProps: { enabled: true } }
    );
    await waitFor(() => expect(result.current.selectedJob?.id).toBe("ga-1"));
    const detailCalls = api.loadGaJob.mock.calls.length;

    let command!: Promise<void>;
    act(() => { command = result.current.act("pause"); });
    await waitFor(() => expect(api.sendGaCommand).toHaveBeenCalledTimes(1));
    rerender({ enabled: false });
    response.resolve(envelope({ ...job, status: "PAUSING", version: 5 }));
    await act(async () => command);

    expect(api.loadGaJob).toHaveBeenCalledTimes(detailCalls);
    expect(api.loadGaJobs).toHaveBeenCalledTimes(1);
    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
    expect(JSON.parse(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY) ?? "[]")[0]).toMatchObject({
      operation: "pause", jobId: "ga-1", expectedVersion: 4, awaiting: true
    });
    rerender({ enabled: true });
    await waitFor(() => expect(result.current.intents[0]?.awaiting).toBe(true));
    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
  });

  it.each([
    { change: "disabled", outcome: "success" }, { change: "disabled", outcome: "error" },
    { change: "actor-change", outcome: "success" }, { change: "actor-change", outcome: "error" },
    { change: "unmount", outcome: "success" }, { change: "unmount", outcome: "error" }
  ] as const)(
    "retains a sent command for same-actor resolution after $change/$outcome", async ({ change, outcome }) => {
      const response = deferred<ReturnType<typeof envelope>>();
      api.sendGaCommand.mockReturnValueOnce(response.promise);
      const nextActor = {
        ...workspace,
        session: { ...session, actor: "next-operator@example.test", csrf_token: "next-csrf" }
      } as ResearchWorkspace;
      const { result, rerender, unmount } = renderHook(
        ({ enabled, current }: { enabled: boolean; current: ResearchWorkspace }) => useGaOperations(enabled, current),
        { initialProps: { enabled: true, current: workspace } }
      );
      await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));
      let command!: Promise<void>;
      act(() => { command = result.current.act("pause"); });
      await waitFor(() => expect(api.sendGaCommand).toHaveBeenCalledTimes(1));
      const intent = JSON.parse(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY) ?? "[]")[0];
      const readsBeforeContextChange = {
        jobs: api.loadGaJobs.mock.calls.length,
        details: api.loadGaJob.mock.calls.length
      };
      if (change === "disabled") rerender({ enabled: false, current: workspace });
      if (change === "actor-change") {
        rerender({ enabled: true, current: nextActor });
        await waitFor(() => expect(result.current.session?.actor).toBe("next-operator@example.test"));
      }
      if (change === "unmount") unmount();

      if (outcome === "success") response.resolve(envelope({ ...job, version: 5, status: "PAUSING" }));
      else response.reject(new Error("response lost"));
      await act(async () => command);
      const stored = JSON.parse(window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY) ?? "[]")[0];
      expect(stored).toMatchObject({
        actor: session.actor, idempotencyKey: intent.idempotencyKey,
        body: { expected_version: 4 }, awaiting: true
      });
      expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
      if (change !== "actor-change") {
        expect(api.loadGaJobs).toHaveBeenCalledTimes(readsBeforeContextChange.jobs);
        expect(api.loadGaJob).toHaveBeenCalledTimes(readsBeforeContextChange.details);
      }
    }
  );

  it.each([
    { context: "actor-change", outcome: "success" },
    { context: "actor-change", outcome: "error" },
    { context: "disable-and-return", outcome: "success" },
    { context: "disable-and-return", outcome: "error" }
  ] as const)("does not publish a pending page after $context/$outcome", async ({ context, outcome }) => {
    const oldPage = deferred<ReturnType<typeof page>>();
    const oldExtra: GaJob = { ...job, id: "ga-old-context-page" };
    const newJob: GaJob = { ...job, id: "ga-new-context" };
    api.loadGaJobs.mockResolvedValueOnce(page([job])).mockReturnValueOnce(oldPage.promise);
    if (context === "actor-change") api.loadGaJobs.mockResolvedValueOnce(page([newJob]));
    api.loadGaJob.mockImplementation(async (id: string) => envelope(id === newJob.id ? newJob : job));
    const otherActor = {
      ...workspace,
      session: { ...session, actor: "new-operator@example.test", csrf_token: "new-csrf" }
    } as ResearchWorkspace;
    const { result, rerender } = renderHook(
      ({ enabled, current }: { enabled: boolean; current: ResearchWorkspace }) => useGaOperations(enabled, current),
      { initialProps: { enabled: true, current: workspace } }
    );
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));
    let loadingPage!: Promise<void>;
    act(() => { loadingPage = result.current.loadMore(); });
    await waitFor(() => expect(api.loadGaJobs).toHaveBeenCalledTimes(2));

    if (context === "actor-change") {
      rerender({ enabled: true, current: otherActor });
      await waitFor(() => expect(result.current.selectedJob?.id).toBe(newJob.id));
    } else {
      rerender({ enabled: false, current: workspace });
      expect(result.current.jobs).toEqual([]);
      rerender({ enabled: true, current: workspace });
      await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));
    }

    if (outcome === "success") oldPage.resolve(page([oldExtra]));
    else oldPage.reject(new ApiError("obsolete-page-failure", 503));
    await act(async () => loadingPage);

    expect(result.current.jobs.map((item) => item.id)).toEqual([context === "actor-change" ? newJob.id : job.id]);
    expect(result.current.error).toBeNull();
  });

  it("resolves an explicit job identity even when it is outside the first fifty rows", async () => {
    const exactJob: GaJob = { ...job, id: "ga-job-outside-first-page", status: "PAUSED" };
    api.loadGaJobs.mockResolvedValue(page([job]));
    api.loadGaJob.mockImplementation(async (id: string) => envelope(id === exactJob.id ? exactJob : job));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));

    act(() => result.current.selectJob(exactJob.id));

    await waitFor(() => expect(result.current.selectedJob?.id).toBe(exactJob.id));
    expect(api.loadGaJob).toHaveBeenLastCalledWith(exactJob.id);
    expect(result.current.jobs).toEqual([job]);
  });

  it("does not publish promotion, audit, or session-renewal completions after disable", async () => {
    const steppedUp = { ...session, permissions: { can_mutate: true, can_step_up: true } };
    const candidate = {
      id: 12, strategy_id: "builtin:golden_cross", role: "challenger" as const,
      param_pack: {}, score_total: "1.25", score_breakdown: {}, max_drawdown: "0.01",
      generation_index: 3, candidate_id: "candidate-12", epoch_id: "epoch-1", created_at: "2026-10-05T10:00:00Z"
    };
    const currentWorkspace = { ...workspace, session: steppedUp, genes: [candidate], selectedGeneId: candidate.id } as unknown as ResearchWorkspace;
    const promotion = deferred<{
      scope: "RESEARCH_CANDIDATE_ONLY";
      gene: { gene_id: number; strategy_id: string; role: "champion"; activated_at: string; retired_gene_ids: number[] };
    }>();
    api.promoteResearchCandidate.mockReturnValueOnce(promotion.promise);
    const hook = renderHook(({ enabled }: { enabled: boolean }) => useGaOperations(enabled, currentWorkspace), {
      initialProps: { enabled: true }
    });
    await waitFor(() => expect(hook.result.current.profileSupported).toBe(true));
    act(() => hook.result.current.choosePromotion(candidate));
    let pendingPromotion!: Promise<void>;
    act(() => { pendingPromotion = hook.result.current.promote("reviewed"); });
    await waitFor(() => expect(api.promoteResearchCandidate).toHaveBeenCalledTimes(1));
    hook.rerender({ enabled: false });
    promotion.resolve({ scope: "RESEARCH_CANDIDATE_ONLY", gene: {
      gene_id: candidate.id, strategy_id: candidate.strategy_id, role: "champion",
      activated_at: "2026-10-05T10:01:00Z", retired_gene_ids: []
    } });
    await act(async () => pendingPromotion);
    expect(api.loadGeneLifecycleEvents).not.toHaveBeenCalled();
    expect(currentWorkspace.refresh).not.toHaveBeenCalled();
    expect(hook.result.current.promotionReceipt).toBeNull();
    hook.unmount();

    const audit = deferred<ReturnType<typeof api.loadGeneLifecycleEvents>>();
    api.loadGeneLifecycleEvents.mockReturnValueOnce(audit.promise);
    const refreshHook = renderHook(({ enabled }: { enabled: boolean }) => useGaOperations(enabled, currentWorkspace), {
      initialProps: { enabled: true }
    });
    await waitFor(() => expect(refreshHook.result.current.profileSupported).toBe(true));
    act(() => refreshHook.result.current.choosePromotion(candidate));
    let pendingAudit!: Promise<void>;
    act(() => { pendingAudit = refreshHook.result.current.refreshPromotion(); });
    await waitFor(() => expect(api.loadGeneLifecycleEvents).toHaveBeenCalled());
    refreshHook.rerender({ enabled: false });
    audit.resolve([]);
    await act(async () => pendingAudit);
    expect(refreshHook.result.current.promotionEvents).toEqual([]);
    expect(currentWorkspace.refresh).not.toHaveBeenCalled();
    refreshHook.unmount();

    const renewed = deferred<BrowserSession>();
    api.renewBrowserSession.mockReturnValueOnce(renewed.promise);
    const renewalHook = renderHook(({ enabled }: { enabled: boolean }) => useGaOperations(enabled, currentWorkspace), {
      initialProps: { enabled: true }
    });
    await waitFor(() => expect(renewalHook.result.current.profileSupported).toBe(true));
    let pendingRenewal!: Promise<void>;
    act(() => { pendingRenewal = renewalHook.result.current.renewStepUp(); });
    await waitFor(() => expect(api.renewBrowserSession).toHaveBeenCalledTimes(1));
    renewalHook.rerender({ enabled: false });
    renewed.resolve({ ...steppedUp, actor: "stale-renewed-actor" });
    await act(async () => pendingRenewal);
    expect(renewalHook.result.current.session).toBeNull();
    expect(renewalHook.result.current.error).toBeNull();
    renewalHook.unmount();
  });

  it("uses the explicitly renewed session for later operator commands", async () => {
    const renewed = { ...session, csrf_token: "renewed-csrf", permissions: { can_mutate: true, can_step_up: true } };
    api.renewBrowserSession.mockResolvedValueOnce(renewed);
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));
    await act(async () => result.current.renewStepUp());
    await waitFor(() => expect(result.current.session?.csrf_token).toBe("renewed-csrf"));

    await act(async () => result.current.act("pause"));

    expect(api.sendGaCommand).toHaveBeenCalledWith(
      "pause", job.id, expect.any(String), { expected_version: job.version }, "renewed-csrf"
    );
  });

  it("keeps selected detail independent from a concurrent next-page read", async () => {
    const secondJob: GaJob = { ...job, id: "ga-2", status: "PAUSED", version: 8 };
    api.loadGaJobs.mockResolvedValueOnce(page()).mockResolvedValueOnce(page([job, secondJob]));
    api.loadGaJob.mockImplementation(async (id: string) => envelope(id === secondJob.id ? secondJob : job));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe("ga-1"));
    const detail = deferred<ReturnType<typeof envelope>>();
    api.loadGaJob.mockReturnValueOnce(detail.promise);

    act(() => result.current.selectJob(secondJob.id));
    await waitFor(() => expect(result.current.loadingDetail).toBe(true));
    await act(async () => result.current.loadMore());
    detail.resolve(envelope(secondJob));

    await waitFor(() => expect(result.current.selectedJob?.id).toBe(secondJob.id));
    expect(result.current.loadingDetail).toBe(false);
    expect(result.current.jobs.map((item) => item.id)).toEqual([job.id, secondJob.id]);
  });

  it("refreshes the exact source detail after a version conflict before another action", async () => {
    const currentJob: GaJob = { ...job, version: 5, status: "RUNNING" };
    api.loadGaJob.mockResolvedValueOnce(envelope(job)).mockResolvedValueOnce(envelope(currentJob));
    api.sendGaCommand.mockRejectedValueOnce(new ApiError("job_version_conflict", 409));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.version).toBe(4));

    await act(async () => result.current.act("pause"));

    await waitFor(() => expect(result.current.selectedJob?.version).toBe(5));
    expect(api.loadGaJob.mock.calls.map(([id]) => id)).toEqual([job.id, job.id]);
    expect(result.current.intents).toHaveLength(0);
    expect(result.current.error).toMatchObject({ type: "rejected", status: 409, code: "job_version_conflict" });
  });

  it("keeps the same selected snapshot visibly stale and non-commandable after refresh failure", async () => {
    api.loadGaJobs.mockResolvedValueOnce(page()).mockRejectedValueOnce(new ApiError("unavailable", 503));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));

    let refreshing!: Promise<boolean>;
    act(() => { refreshing = result.current.refresh(); });
    expect(result.current.selectedJob?.id).toBe(job.id);
    expect(result.current.selectedJobStale).toBe(true);
    await act(async () => { await refreshing; });
    expect(result.current.selectedJob?.id).toBe(job.id);
    expect(result.current.selectedJobStale).toBe(true);
    expect(result.current.error).toMatchObject({ type: "read", status: 503 });
    await act(async () => result.current.act("pause"));
    expect(api.sendGaCommand).not.toHaveBeenCalled();
  });

  it("refreshes a selected job by exact ID even when it is absent from page one", async () => {
    const exactJob: GaJob = { ...job, id: "ga-job-outside-first-page", status: "RUNNING", version: 17 };
    api.loadGaJobs.mockResolvedValueOnce(page()).mockResolvedValueOnce(page());
    api.loadGaJob.mockImplementation(async (id: string) => envelope(id === exactJob.id ? exactJob : job));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(job.id));
    act(() => result.current.selectJob(exactJob.id));
    await waitFor(() => expect(result.current.selectedJob?.id).toBe(exactJob.id));

    await act(async () => { await result.current.refresh(); });

    expect(result.current.selectedJobId).toBe(exactJob.id);
    expect(result.current.selectedJob).toEqual(exactJob);
    expect(result.current.selectedJobStale).toBe(false);
    expect(api.loadGaJob).toHaveBeenLastCalledWith(exactJob.id);
    expect(result.current.jobs).toEqual([job]);
  });

  it("keeps a version-conflicted detail stale and locked when its authoritative read fails", async () => {
    api.loadGaJob.mockResolvedValueOnce(envelope(job)).mockRejectedValueOnce(new ApiError("unavailable", 503));
    api.sendGaCommand.mockRejectedValueOnce(new ApiError("job_version_conflict", 409));
    const { result } = renderHook(() => useGaOperations(true, workspace));
    await waitFor(() => expect(result.current.selectedJob).toEqual(job));

    await act(async () => result.current.act("pause"));

    expect(result.current.selectedJob).toEqual(job);
    expect(result.current.selectedJobStale).toBe(true);
    expect(result.current.error).toMatchObject({ type: "rejected", status: 409, code: "job_version_conflict" });
    await act(async () => result.current.act("pause"));
    expect(api.sendGaCommand).toHaveBeenCalledTimes(1);
  });

  it("keeps candidate and retired-gene audit events when filtered reads finish out of order", async () => {
    const steppedUp = {
      ...session,
      permissions: { can_mutate: true, can_step_up: true }
    };
    const candidate = {
      id: 12, strategy_id: "builtin:golden_cross", role: "challenger" as const,
      param_pack: {}, score_total: "1.25", score_breakdown: {}, max_drawdown: "0.01",
      generation_index: 3, candidate_id: "candidate-12", epoch_id: "epoch-1",
      created_at: "2026-10-05T10:00:00Z"
    };
    const retired = {
      ...candidate, id: 7, role: "retired" as const, candidate_id: "candidate-7"
    };
    const promoteEvent = {
      id: 31, event_type: "gene_promote" as const, event_subtype: null,
      related_strategy_id: candidate.strategy_id, related_order_id: null,
      related_gene_id: candidate.id, payload: {}, created_at: "2026-10-05T10:01:00Z"
    };
    const retireEvent = {
      ...promoteEvent, id: 32, event_type: "gene_retire" as const,
      related_gene_id: retired.id
    };
    const candidatePromote = deferred<typeof promoteEvent[]>();
    const candidateWorkspace = {
      ...workspace, session: steppedUp, genes: [candidate], selectedGeneId: candidate.id
    } as unknown as ResearchWorkspace;
    api.loadGene.mockImplementation(async (id: number) => ({
      ...(id === retired.id ? retired : candidate), id
    }));
    api.loadGeneLifecycleEvents.mockImplementation(async (id: number, _strategy: string, type: string) => {
      if (id === candidate.id && type === "gene_promote") return candidatePromote.promise;
      if (id === retired.id && type === "gene_retire") return [retireEvent];
      return [];
    });
    api.promoteResearchCandidate.mockResolvedValue({
      scope: "RESEARCH_CANDIDATE_ONLY",
      gene: {
        gene_id: candidate.id, strategy_id: candidate.strategy_id,
        role: "champion", activated_at: "2026-10-05T10:01:00Z",
        retired_gene_ids: [retired.id]
      }
    });
    const { result } = renderHook(() => useGaOperations(true, candidateWorkspace));
    await waitFor(() => expect(result.current.profileSupported).toBe(true));
    act(() => result.current.choosePromotion(candidate));

    let promotion!: Promise<void>;
    act(() => { promotion = result.current.promote("reviewed candidate"); });
    await waitFor(() => expect(api.loadGeneLifecycleEvents).toHaveBeenCalledTimes(4));
    expect(result.current.promotionEvents).toEqual([]);

    await act(async () => {
      candidatePromote.resolve([promoteEvent]);
      await promotion;
    });
    await waitFor(() => expect(result.current.promotionEvents).toContainEqual(promoteEvent));
    expect(result.current.promotionEvents).toEqual(
      expect.arrayContaining([promoteEvent, retireEvent])
    );
  });
});
