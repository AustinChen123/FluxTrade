// @vitest-environment jsdom

import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadBacktestResultsIndex: vi.fn(),
  loadBacktestResult: vi.fn(),
  loadBacktestResultTrades: vi.fn(),
  ApiError: class ApiError extends Error {
    constructor(message: string, readonly status: number) {
      super(message);
    }
  }
}));

vi.mock("../../api", () => api);

import type {
  BacktestResultsDetail,
  BacktestResultsIndexPage,
  BacktestResultsTradesPage,
  BrowserSession
} from "../../api";
import { useBacktestResults } from "./useBacktestResults";

const session: BrowserSession = {
  actor: "operator",
  capabilities: ["backtest_results:read"],
  permissions: { can_mutate: false, can_step_up: false },
  csrf_token: "",
  expires_at: "2030-01-01T00:00:00Z",
  step_up_expires_at: null
};
const indexPage: BacktestResultsIndexPage = {
  items: [{
    job_id: "job-A",
    subject_id: "summary-strategy:v1",
    dataset_id: "dataset-1",
    product_id: "RITHMIC:MNQ-CONTINUOUS",
    timeframe: "1m",
    started_at: "2026-01-01T00:00:00.000Z",
    ended_at: "2026-01-01T00:01:00.000Z",
    completed_at: "2026-01-01T00:01:00.000Z",
    result_digest: "a".repeat(64)
  }],
  next_cursor: "signed.index+cursor",
  revision: 1
};
const detail: BacktestResultsDetail = {
  job_id: "job-A",
  strategy_id: "summary-strategy:v1",
  subject_id: "summary-strategy:v1",
  dataset_id: "dataset-1",
  product_id: "RITHMIC:MNQ-CONTINUOUS",
  timeframe: "1m",
  started_at: "2026-01-01T00:00:00.000Z",
  ended_at: "2026-01-01T00:01:00.000Z",
  currency: "USD",
  metrics: {
    net_pnl: "0.0000000000000000001",
    return_pct: "0.000000000000000001",
    max_drawdown: "0",
    sharpe: "1",
    sortino: "1",
    calmar: "1"
  },
  equity: [{
    timestamp: "2026-01-01T00:00:00.000Z",
    equity: "100.000000000000000001",
    drawdown: "0"
  }],
  monthly_returns: [{ month: "2026-01", return_pct: "0.000000000000000001" }],
  pnl_distribution: [{ lower: null, upper: null, count: 1 }],
  trade_page: {
    items: [{
      id: "job-A:0",
      entry_time: "2026-01-01T00:00:00.000Z",
      exit_time: "2026-01-01T00:01:00.000Z",
      entry_price: "10.000000000000000001",
      exit_price: "11.000000000000000001",
      side: "LONG",
      quantity: "0.000000000000000001",
      pnl: "0.0000000000000000001",
      fee: "0"
    }],
    total_count: 2,
    next_cursor: "signed.cursor+A"
  },
  input_digest: "a".repeat(64),
  result_digest: "a".repeat(64),
  revision: 1
};
const tradesPage: BacktestResultsTradesPage = {
  items: [{
    id: "job-A:1",
    entry_time: "2026-01-01T00:01:00.000Z",
    exit_time: "2026-01-01T00:02:00.000Z",
    entry_price: "11.000000000000000001",
    exit_price: "12.000000000000000001",
    side: "SHORT",
    quantity: "0.000000000000000001",
    pnl: "0.0000000000000000002",
    fee: "0.00000000000000000001"
  }],
  next_cursor: null,
  revision: 1
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => { resolve = res; });
  return { promise, resolve };
}

describe("useBacktestResults index read", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    api.ensureBrowserSession.mockResolvedValue(session);
    api.loadBacktestResultsIndex.mockResolvedValue(indexPage);
    api.loadBacktestResult.mockResolvedValue(detail);
    api.loadBacktestResultTrades.mockResolvedValue(tradesPage);
  });

  it("reads only the signed index after session setup and preserves opaque cursors", async () => {
    const { result } = renderHook(() => useBacktestResults({ selectedResultId: null }));
    await waitFor(() => expect(result.current.state.status).toBe("index"));
    expect(api.ensureBrowserSession.mock.invocationCallOrder[0])
      .toBeLessThan(api.loadBacktestResultsIndex.mock.invocationCallOrder[0]);
    expect(result.current.state).toMatchObject({
      status: "index",
      items: [{ jobId: "job-A", resultDigest: "a".repeat(64) }],
      nextCursor: "signed.index+cursor"
    });

    const nextPage = deferred<BacktestResultsIndexPage>();
    api.loadBacktestResultsIndex.mockReturnValueOnce(nextPage.promise);
    let loadMore!: Promise<void>;
    act(() => {
      loadMore = result.current.loadMoreIndex();
      void result.current.loadMoreIndex();
    });
    await waitFor(() => expect(api.loadBacktestResultsIndex).toHaveBeenCalledTimes(2));
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(2);
    expect(api.ensureBrowserSession.mock.invocationCallOrder[1])
      .toBeLessThan(api.loadBacktestResultsIndex.mock.invocationCallOrder[1]);
    expect(api.loadBacktestResultsIndex).toHaveBeenLastCalledWith({
      cursor: "signed.index+cursor"
    });
    nextPage.resolve({ ...indexPage, items: [], next_cursor: null });
    await act(async () => loadMore);
    expect(result.current.state).toMatchObject({
      status: "index", items: [{ jobId: "job-A" }], nextCursor: null
    });
  });

  it.each([
    [401, "permission-unavailable"], [403, "permission-unavailable"],
    [404, "unavailable"], [409, "unavailable"], [422, "error"], [429, "error"]
  ] as const)("maps failed HTTP %i to %s before inspecting error text", async (status, expected) => {
    api.loadBacktestResultsIndex.mockRejectedValue(new api.ApiError("invalid_response", status));
    const { result } = renderHook(() => useBacktestResults({ selectedResultId: null }));
    await waitFor(() => expect(result.current.state.status).toBe(expected));
    expect(JSON.stringify(result.current.state)).not.toContain("invalid_response");
  });

  it.each([
    new api.ApiError("invalid_response", 200),
    new Error("invalid_response")
  ])("maps successful invalid responses to invalid-data", async (error) => {
    api.loadBacktestResultsIndex.mockRejectedValue(error);
    const { result } = renderHook(() => useBacktestResults({ selectedResultId: null }));
    await waitFor(() => expect(result.current.state.status).toBe("invalid-data"));
  });

  it("does not read results without a browser session", async () => {
    api.ensureBrowserSession.mockResolvedValue(null);
    const { result } = renderHook(() => useBacktestResults({ selectedResultId: null }));
    await waitFor(() => expect(result.current.state.status).toBe("unavailable"));
    expect(api.loadBacktestResultsIndex).not.toHaveBeenCalled();
  });

  it("rejects a successfully returned invalid projection and distinguishes an empty index", async () => {
    api.loadBacktestResultsIndex.mockResolvedValueOnce({
      ...indexPage,
      items: [{ ...indexPage.items[0], result_digest: "invalid" }]
    });
    const invalid = renderHook(() => useBacktestResults({ selectedResultId: null }));
    await waitFor(() => expect(invalid.result.current.state.status).toBe("invalid-data"));
    invalid.unmount();

    api.loadBacktestResultsIndex.mockResolvedValueOnce({
      items: [], next_cursor: null, revision: 1
    });
    const empty = renderHook(() => useBacktestResults({ selectedResultId: null }));
    await waitFor(() => expect(empty.result.current.state.status).toBe("empty"));
    expect(empty.result.current.state).toMatchObject({ items: [] });
  });

  it("does not begin a cursor read after its request generation is invalidated", async () => {
    const sessionResponse = deferred<BrowserSession | null>();
    const hook = renderHook(() => useBacktestResults({ selectedResultId: null }));
    await waitFor(() => expect(hook.result.current.state.status).toBe("index"));
    api.ensureBrowserSession.mockReturnValueOnce(sessionResponse.promise);
    let loadMore!: Promise<void>;
    act(() => { loadMore = hook.result.current.loadMoreIndex(); });
    hook.unmount();
    sessionResponse.resolve(session);
    await act(async () => loadMore);
    expect(api.loadBacktestResultsIndex).toHaveBeenCalledOnce();
  });

  it("loads an explicit result directly and binds the trade page to its detail count", async () => {
    const { result } = renderHook(() =>
      useBacktestResults({ selectedResultId: "job-A" })
    );
    await waitFor(() => expect(result.current.state.status).toBe("detail"));
    expect(api.loadBacktestResultsIndex).not.toHaveBeenCalled();
    expect(api.ensureBrowserSession.mock.invocationCallOrder[0])
      .toBeLessThan(api.loadBacktestResult.mock.invocationCallOrder[0]);
    expect(api.loadBacktestResult).toHaveBeenCalledWith("job-A");
    expect(result.current.state).toMatchObject({
      status: "detail",
      resultId: "job-A",
      snapshot: {
        jobId: "job-A",
        returnPctUnit: "ratio",
        metrics: { netPnl: "0.0000000000000000001" }
      }
    });

    const page = await result.current.loadMoreTrades("signed.cursor+A");
    expect(api.ensureBrowserSession.mock.invocationCallOrder[1])
      .toBeLessThan(api.loadBacktestResultTrades.mock.invocationCallOrder[0]);
    expect(api.loadBacktestResultTrades).toHaveBeenCalledWith("job-A", {
      cursor: "signed.cursor+A"
    });
    expect(page).toMatchObject({
      totalCount: 2,
      items: [{ pnl: "0.0000000000000000002", fee: "0.00000000000000000001" }],
      nextCursor: null
    });
  });

  it("rejects detail whose projected identity differs from the requested result", async () => {
    api.loadBacktestResult.mockResolvedValue({ ...detail, job_id: "other-job" });
    const { result } = renderHook(() =>
      useBacktestResults({ selectedResultId: "job-A" })
    );
    await waitFor(() => expect(result.current.state.status).toBe("invalid-data"));
  });

  it("masks the prior snapshot and ignores obsolete detail responses on selection change", async () => {
    const nextResponse = deferred<BacktestResultsDetail>();
    const observed: Array<ReturnType<typeof useBacktestResults>["state"]> = [];
    const hook = renderHook(
      ({ id }: { id: string }) => {
        const value = useBacktestResults({ selectedResultId: id });
        observed.push(value.state);
        return value;
      },
      { initialProps: { id: "job-A" } }
    );
    await waitFor(() => expect(hook.result.current.state.status).toBe("detail"));
    api.loadBacktestResult.mockImplementation((jobId: string) =>
      jobId === "job-B" ? nextResponse.promise : Promise.resolve({ ...detail, job_id: jobId })
    );
    const start = observed.length;
    hook.rerender({ id: "job-B" });
    expect(hook.result.current.state).toEqual({ status: "pending", selection: "job-B" });
    expect(observed.slice(start)[0]).toEqual({ status: "pending", selection: "job-B" });
    await waitFor(() => expect(api.loadBacktestResult).toHaveBeenLastCalledWith("job-B"));
    hook.rerender({ id: "job-C" });
    await waitFor(() => expect(hook.result.current.state).toMatchObject({
      status: "detail", resultId: "job-C"
    }));
    nextResponse.resolve({ ...detail, job_id: "job-B" });
    await act(async () => nextResponse.promise);
    expect(hook.result.current.state).toMatchObject({ status: "detail", resultId: "job-C" });
  });

  it("rejects retained index and trade callbacks as soon as identity changes", async () => {
    api.loadBacktestResult.mockImplementation((jobId: string) =>
      Promise.resolve({ ...detail, job_id: jobId })
    );
    const hook = renderHook(
      ({ id }: { id: string | null }) => useBacktestResults({ selectedResultId: id }),
      { initialProps: { id: null as string | null } }
    );
    await waitFor(() => expect(hook.result.current.state.status).toBe("index"));
    const oldIndexLoader = hook.result.current.loadMoreIndex;
    hook.rerender({ id: "job-A" });
    expect(hook.result.current.state).toEqual({ status: "pending", selection: "job-A" });
    await act(() => oldIndexLoader());
    expect(api.loadBacktestResultsIndex).toHaveBeenCalledOnce();
    await waitFor(() => expect(hook.result.current.state.status).toBe("detail"));

    const oldTradeLoader = hook.result.current.loadMoreTrades;
    hook.rerender({ id: "job-B" });
    expect(hook.result.current.state).toEqual({ status: "pending", selection: "job-B" });
    await expect(oldTradeLoader("signed.cursor+A")).rejects.toThrow("result_unavailable");
    expect(api.loadBacktestResultTrades).not.toHaveBeenCalled();
    await waitFor(() => expect(hook.result.current.state).toMatchObject({
      status: "detail", resultId: "job-B"
    }));
  });

  it("discards an obsolete supplemental-page response after result selection changes", async () => {
    const oldPage = deferred<BacktestResultsTradesPage>();
    api.loadBacktestResultTrades.mockReturnValue(oldPage.promise);
    api.loadBacktestResult.mockImplementation((jobId: string) =>
      Promise.resolve({ ...detail, job_id: jobId })
    );
    const hook = renderHook(
      ({ id }: { id: string }) => useBacktestResults({ selectedResultId: id }),
      { initialProps: { id: "job-A" } }
    );
    await waitFor(() => expect(hook.result.current.state.status).toBe("detail"));
    const loadMore = hook.result.current.loadMoreTrades("signed.cursor+A");
    await waitFor(() => expect(api.loadBacktestResultTrades).toHaveBeenCalledOnce());
    hook.rerender({ id: "job-B" });
    expect(hook.result.current.state).toEqual({ status: "pending", selection: "job-B" });
    await waitFor(() => expect(hook.result.current.state.status).toBe("detail"));
    oldPage.resolve(tradesPage);
    await expect(loadMore).rejects.toThrow("stale_result_request");
    expect(hook.result.current.state).toMatchObject({ status: "detail", resultId: "job-B" });
  });

  it("keeps disabled mode inert and masks a previously loaded snapshot", async () => {
    const observed: Array<ReturnType<typeof useBacktestResults>["state"]> = [];
    const hook = renderHook(
      ({ enabled }: { enabled: boolean }) => {
        const value = useBacktestResults({ selectedResultId: "job-A", enabled });
        observed.push(value.state);
        return value;
      },
      { initialProps: { enabled: false } }
    );
    expect(hook.result.current.state).toEqual({ status: "pending", selection: "job-A" });
    expect(api.ensureBrowserSession).not.toHaveBeenCalled();
    expect(api.loadBacktestResult).not.toHaveBeenCalled();

    hook.rerender({ enabled: true });
    await waitFor(() => expect(hook.result.current.state.status).toBe("detail"));
    const start = observed.length;
    hook.rerender({ enabled: false });
    expect(hook.result.current.state).toEqual({ status: "pending", selection: "job-A" });
    expect(observed.slice(start)[0]).toEqual({ status: "pending", selection: "job-A" });
    expect(api.loadBacktestResult).toHaveBeenCalledOnce();

    hook.unmount();
    const lateDetail = deferred<BacktestResultsDetail>();
    api.loadBacktestResult.mockReturnValueOnce(lateDetail.promise);
    const delayed = renderHook(
      ({ enabled }: { enabled: boolean }) => useBacktestResults({
        selectedResultId: "job-A", enabled
      }),
      { initialProps: { enabled: true } }
    );
    await waitFor(() => expect(api.loadBacktestResult).toHaveBeenCalledTimes(2));
    delayed.rerender({ enabled: false });
    lateDetail.resolve(detail);
    await act(async () => lateDetail.promise);
    expect(delayed.result.current.state).toEqual({ status: "pending", selection: "job-A" });
  });
});
