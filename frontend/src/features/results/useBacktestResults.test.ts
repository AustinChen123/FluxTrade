// @vitest-environment jsdom

import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadBacktestResultsIndex: vi.fn(),
  ApiError: class ApiError extends Error {
    constructor(message: string, readonly status: number) {
      super(message);
    }
  }
}));

vi.mock("../../api", () => api);

import type {
  BacktestResultsIndexPage,
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
  });

  it("reads only the signed index after session setup and preserves opaque cursors", async () => {
    const { result } = renderHook(() => useBacktestResults());
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
    const { result } = renderHook(() => useBacktestResults());
    await waitFor(() => expect(result.current.state.status).toBe(expected));
    expect(JSON.stringify(result.current.state)).not.toContain("invalid_response");
  });

  it.each([
    new api.ApiError("invalid_response", 200),
    new Error("invalid_response")
  ])("maps successful invalid responses to invalid-data", async (error) => {
    api.loadBacktestResultsIndex.mockRejectedValue(error);
    const { result } = renderHook(() => useBacktestResults());
    await waitFor(() => expect(result.current.state.status).toBe("invalid-data"));
  });

  it("does not read results without a browser session", async () => {
    api.ensureBrowserSession.mockResolvedValue(null);
    const { result } = renderHook(() => useBacktestResults());
    await waitFor(() => expect(result.current.state.status).toBe("unavailable"));
    expect(api.loadBacktestResultsIndex).not.toHaveBeenCalled();
  });

  it("rejects a successfully returned invalid projection and distinguishes an empty index", async () => {
    api.loadBacktestResultsIndex.mockResolvedValueOnce({
      ...indexPage,
      items: [{ ...indexPage.items[0], result_digest: "invalid" }]
    });
    const invalid = renderHook(() => useBacktestResults());
    await waitFor(() => expect(invalid.result.current.state.status).toBe("invalid-data"));
    invalid.unmount();

    api.loadBacktestResultsIndex.mockResolvedValueOnce({
      items: [], next_cursor: null, revision: 1
    });
    const empty = renderHook(() => useBacktestResults());
    await waitFor(() => expect(empty.result.current.state.status).toBe("empty"));
    expect(empty.result.current.state).toMatchObject({ items: [] });
  });

  it("does not begin a cursor read after its request generation is invalidated", async () => {
    const sessionResponse = deferred<BrowserSession | null>();
    const hook = renderHook(() => useBacktestResults());
    await waitFor(() => expect(hook.result.current.state.status).toBe("index"));
    api.ensureBrowserSession.mockReturnValueOnce(sessionResponse.promise);
    let loadMore!: Promise<void>;
    act(() => { loadMore = hook.result.current.loadMoreIndex(); });
    hook.unmount();
    sessionResponse.resolve(session);
    await act(async () => loadMore);
    expect(api.loadBacktestResultsIndex).toHaveBeenCalledOnce();
  });
});
