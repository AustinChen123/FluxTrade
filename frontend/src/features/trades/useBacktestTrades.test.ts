// @vitest-environment jsdom

import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadBacktestResultsIndex: vi.fn(),
  loadBacktestResult: vi.fn(),
  loadBacktestResultTrades: vi.fn(),
  loadBacktestResultCandles: vi.fn(),
  ApiError: class ApiError extends Error {
    constructor(message: string, readonly status: number) {
      super(message);
    }
  }
}));

vi.mock("../../api", () => api);

import type {
  BacktestResultsCandlesPage,
  BacktestResultsDetail,
  BacktestResultsTrade
} from "../../api";
import { useBacktestTrades } from "./useBacktestTrades";

const session = {
  actor: "operator",
  capabilities: ["backtest_results:read"],
  permissions: { can_mutate: false, can_step_up: false },
  csrf_token: "",
  expires_at: "2030-01-01T00:00:00Z",
  step_up_expires_at: null
};

const firstTrade: BacktestResultsTrade = {
  id: "job-A:0",
  entry_time: "2026-01-01T00:07:00.000Z",
  exit_time: "2026-01-01T00:08:00.000Z",
  entry_price: "10.000000000000000001",
  exit_price: "11.000000000000000001",
  side: "LONG",
  quantity: "0.000000000000000001",
  pnl: "0.0000000000000000001",
  fee: "0.00000000000000000001"
};

const secondTrade: BacktestResultsTrade = {
  ...firstTrade,
  id: "job-A:1",
  entry_time: "2026-01-01T00:09:00.000Z",
  exit_time: "2026-01-01T00:10:00.000Z",
  side: "SHORT"
};

function detail(
  jobId = "job-A",
  overrides: Partial<BacktestResultsDetail> = {}
): BacktestResultsDetail {
  return {
    job_id: jobId,
    strategy_id: "strategy-1",
    subject_id: "strategy-1:v1",
    dataset_id: "dataset-1",
    product_id: "RITHMIC:MNQ-CONTINUOUS",
    timeframe: "1m",
    started_at: "2026-01-01T00:00:00.000Z",
    ended_at: "2026-01-01T00:20:00.000Z",
    currency: "USD",
    metrics: {
      net_pnl: "0.0000000000000000001",
      return_pct: "0.000000000000000001",
      max_drawdown: "0",
      sharpe: "1",
      sortino: "1",
      calmar: "1"
    },
    equity: [],
    monthly_returns: [],
    pnl_distribution: [],
    trade_page: {
      items: [{ ...firstTrade, id: `${jobId}:0` }],
      total_count: 1,
      next_cursor: null
    },
    input_digest: "a".repeat(64),
    result_digest: "b".repeat(64),
    revision: 1,
    ...overrides
  };
}

function candle(
  timestamp: string,
  close = "10.0000000000000000001"
) {
  return {
    timestamp,
    open: "9.0000000000000000001",
    high: "11.0000000000000000001",
    low: "8.0000000000000000001",
    close,
    volume: "1.0000000000000000001"
  };
}

function candlePage(
  items: BacktestResultsCandlesPage["items"],
  next_cursor: string | null = null
): BacktestResultsCandlesPage {
  return { items, next_cursor, revision: 1 };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}

describe("useBacktestTrades", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    api.ensureBrowserSession.mockResolvedValue(session);
    api.loadBacktestResult.mockImplementation((jobId: string) =>
      Promise.resolve(detail(jobId))
    );
    api.loadBacktestResultTrades.mockResolvedValue({
      items: [secondTrade], next_cursor: null, revision: 1
    });
    api.loadBacktestResultCandles.mockResolvedValue(candlePage([]));
  });

  it("does no I/O without a selected result or while disabled", () => {
    const missing = renderHook(() => useBacktestTrades({
      selectedResultId: null, selectedTradeId: null
    }));
    expect(missing.result.current.state.status).toBe("unavailable");
    missing.unmount();

    const disabled = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: "job-A:0", enabled: false
    }));
    expect(disabled.result.current.state.status).toBe("pending");
    expect(api.ensureBrowserSession).not.toHaveBeenCalled();
    expect(api.loadBacktestResult).not.toHaveBeenCalled();
    expect(api.loadBacktestResultTrades).not.toHaveBeenCalled();
    expect(api.loadBacktestResultCandles).not.toHaveBeenCalled();
  });

  it("loads direct result detail after session and exposes trade selection without candles", async () => {
    const { result } = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: null
    }));
    await waitFor(() => expect(result.current.state.status).toBe("selection"));
    expect(api.ensureBrowserSession.mock.invocationCallOrder[0])
      .toBeLessThan(api.loadBacktestResult.mock.invocationCallOrder[0]);
    expect(api.loadBacktestResult).toHaveBeenCalledWith("job-A");
    expect(api.loadBacktestResultsIndex).not.toHaveBeenCalled();
    expect(api.loadBacktestResultTrades).not.toHaveBeenCalled();
    expect(api.loadBacktestResultCandles).not.toHaveBeenCalled();
    expect(result.current.state).toMatchObject({
      status: "selection",
      resultId: "job-A",
      tradeId: null,
      totalCount: 1,
      tradePage: { items: [{ id: "job-A:0" }] }
    });
  });

  it("searches signed trade pages for the exact deep-linked ID then loads the whole window", async () => {
    const identity = detail("job-A", {
      trade_page: {
        items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
      }
    });
    api.loadBacktestResult.mockResolvedValue(identity);
    api.loadBacktestResultTrades.mockResolvedValue({
      items: [secondTrade], next_cursor: null, revision: 1
    });
    api.loadBacktestResultCandles
      .mockResolvedValueOnce(candlePage([
        candle("2026-01-01T00:07:00.000Z")
      ], "signed.candle+A"))
      .mockResolvedValueOnce(candlePage([
        candle("2026-01-01T00:09:00.000Z"),
        candle("2026-01-01T00:12:59.999Z")
      ]));

    const { result } = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: "job-A:1"
    }));
    await waitFor(() => expect(result.current.state.status).toBe("valid"));
    expect(api.loadBacktestResultTrades).toHaveBeenCalledWith("job-A", {
      cursor: "signed.trade+A"
    });
    const start = Date.parse("2026-01-01T00:07:00.000Z");
    const end = Date.parse("2026-01-01T00:13:00.000Z");
    expect(api.loadBacktestResultCandles).toHaveBeenNthCalledWith(1, "job-A", {
      start, end
    });
    expect(api.loadBacktestResultCandles).toHaveBeenNthCalledWith(2, "job-A", {
      start, end, cursor: "signed.candle+A"
    });
    expect(result.current.state).toMatchObject({
      status: "valid",
      resultId: "job-A",
      tradeId: "job-A:1",
      totalCount: 2,
      tradePage: { items: [{ id: "job-A:0" }, { id: "job-A:1" }] },
      snapshot: {
        trades: [{ id: "job-A:1" }],
        candles: [
          { open: "9.0000000000000000001" },
          { open: "9.0000000000000000001" },
          { open: "9.0000000000000000001" }
        ]
      }
    });
    expect(api.loadBacktestResult).toHaveBeenCalledOnce();
    expect(api.loadBacktestResultsIndex).not.toHaveBeenCalled();
    for (const read of [
      api.loadBacktestResult,
      api.loadBacktestResultTrades,
      api.loadBacktestResultCandles
    ]) {
      expect(api.ensureBrowserSession.mock.invocationCallOrder.some(
        (order: number) => order < read.mock.invocationCallOrder[0]
      )).toBe(true);
    }
  });

  it("uses the same shared pager for explicit load-more and returns its merged page", async () => {
    api.loadBacktestResult.mockResolvedValue(detail("job-A", {
      trade_page: {
        items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
      }
    }));
    const { result } = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: null
    }));
    await waitFor(() => expect(result.current.state.status).toBe("selection"));
    await act(async () => result.current.loadMoreTrades());
    await waitFor(() => expect(result.current.state).toMatchObject({
      status: "selection", tradePage: { items: [{ id: "job-A:0" }, { id: "job-A:1" }] }
    }));
    expect(api.loadBacktestResultTrades).toHaveBeenCalledWith("job-A", {
      cursor: "signed.trade+A"
    });
  });

  it("reports exhausted exact-ID search unavailable and requests no candles", async () => {
    api.loadBacktestResult.mockResolvedValue(detail("job-A", {
      trade_page: {
        items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
      }
    }));
    api.loadBacktestResultTrades.mockResolvedValue({
      items: [secondTrade], next_cursor: null, revision: 1
    });
    const { result } = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: "job-A:missing"
    }));
    await waitFor(() => expect(api.loadBacktestResultTrades).toHaveBeenCalledOnce());
    await waitFor(() => expect(result.current.state.status).toBe("unavailable"));
    expect(api.loadBacktestResultCandles).not.toHaveBeenCalled();
  });

  it.each([
    ["detail", 401, "permission-unavailable"],
    ["detail", 409, "unavailable"],
    ["detail", 422, "error"],
    ["trade", 422, "error"],
    ["candle", 422, "candle-window-unavailable"]
  ] as const)("maps %s HTTP %i to fixed %s without exposing raw detail", async (read, status, expected) => {
    const error = new api.ApiError("private backend detail", status);
    if (read === "detail") api.loadBacktestResult.mockRejectedValue(error);
    if (read === "trade") {
      api.loadBacktestResult.mockResolvedValue(detail("job-A", {
        trade_page: {
          items: [{ ...firstTrade, id: "job-A:other" }],
          total_count: 2,
          next_cursor: "signed.trade+A"
        }
      }));
      api.loadBacktestResultTrades.mockRejectedValue(error);
    }
    if (read === "candle") api.loadBacktestResultCandles.mockRejectedValue(error);

    const { result } = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A",
      selectedTradeId: read === "trade" ? "job-A:missing" : "job-A:0"
    }));
    await waitFor(() => expect(result.current.state.status).toBe(expected));
    expect(JSON.stringify(result.current.state)).not.toContain("private backend detail");
    if (read === "candle") {
      expect(api.loadBacktestResultCandles).toHaveBeenCalledOnce();
      expect(result.current.state).not.toHaveProperty("snapshot");
    }
  });

  it.each(["first candle session", "continuation candle session"] as const)(
    "does not classify a 422 from the %s as a candle-window rejection",
    async (stage) => {
      const error = new api.ApiError("private session detail", 422);
      if (stage === "first candle session") {
        api.ensureBrowserSession
          .mockResolvedValueOnce(session)
          .mockRejectedValueOnce(error);
      } else {
        api.ensureBrowserSession
          .mockResolvedValueOnce(session)
          .mockResolvedValueOnce(session)
          .mockRejectedValueOnce(error);
        api.loadBacktestResultCandles.mockResolvedValueOnce(candlePage([
          candle("2026-01-01T00:07:00.000Z")
        ], "signed.candle+A"));
      }
      const { result } = renderHook(() => useBacktestTrades({
        selectedResultId: "job-A", selectedTradeId: "job-A:0"
      }));
      await waitFor(() => expect(result.current.state.status).toBe("error"));
      expect(result.current.state.status).not.toBe("candle-window-unavailable");
      expect(JSON.stringify(result.current.state)).not.toContain("private session detail");
      expect(api.loadBacktestResultCandles).toHaveBeenCalledTimes(
        stage === "first candle session" ? 0 : 1
      );
    }
  );

  it.each(["resolve", "reject"] as const)(
    "ignores an old same-result page %s after the selected trade changes",
    async (settlement) => {
      const oldPage = deferred<{
        items: BacktestResultsTrade[];
        next_cursor: string | null;
        revision: 1;
      }>();
      api.loadBacktestResult.mockResolvedValue(detail("job-A", {
        trade_page: {
          items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
        }
      }));
      api.loadBacktestResultTrades
        .mockReturnValueOnce(oldPage.promise)
        .mockResolvedValueOnce({
          items: [secondTrade], next_cursor: null, revision: 1
        });
      const hook = renderHook(
        ({ tradeId }: { tradeId: string }) => useBacktestTrades({
          selectedResultId: "job-A", selectedTradeId: tradeId
        }),
        { initialProps: { tradeId: "job-A:missing" } }
      );
      await waitFor(() => expect(api.loadBacktestResultTrades).toHaveBeenCalledOnce());
      hook.rerender({ tradeId: "job-A:1" });
      await waitFor(() => expect(hook.result.current.state.status).toBe("valid"));
      if (settlement === "resolve") {
        oldPage.resolve({ items: [secondTrade], next_cursor: null, revision: 1 });
      } else {
        oldPage.reject(new api.ApiError("stale private failure", 401));
      }
      await act(async () => { await oldPage.promise.catch(() => undefined); });
      expect(hook.result.current.state).toMatchObject({
        status: "valid", resultId: "job-A", tradeId: "job-A:1",
        snapshot: { trades: [{ id: "job-A:1" }] }
      });
      expect(api.loadBacktestResultTrades).toHaveBeenCalledTimes(2);
    }
  );

  it("does not continue an obsolete page request after its pending session resolves", async () => {
    const oldSession = deferred<typeof session>();
    api.ensureBrowserSession
      .mockResolvedValueOnce(session)
      .mockReturnValueOnce(oldSession.promise);
    api.loadBacktestResult.mockResolvedValue(detail("job-A", {
      trade_page: {
        items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
      }
    }));
    const hook = renderHook(
      ({ tradeId }: { tradeId: string }) => useBacktestTrades({
        selectedResultId: "job-A", selectedTradeId: tradeId
      }),
      { initialProps: { tradeId: "job-A:missing" } }
    );
    await waitFor(() => expect(api.ensureBrowserSession).toHaveBeenCalledTimes(2));
    hook.rerender({ tradeId: "job-A:0" });
    await waitFor(() => expect(hook.result.current.state.status).toBe("valid"));
    oldSession.resolve(session);
    await act(async () => oldSession.promise);
    expect(api.loadBacktestResultTrades).not.toHaveBeenCalled();
  });

  it.each([
    ["selection", 401, "permission-unavailable"],
    ["selection", 409, "unavailable"],
    ["selection", "merge", "invalid-data"],
    ["valid", 401, "permission-unavailable"],
    ["valid", 409, "unavailable"],
    ["valid", "merge", "invalid-data"]
  ] as const)(
    "surfaces explicit page failure %s/%s as %s instead of stale data",
    async (view, failure, expected) => {
      const hasSelection = view === "valid";
      api.loadBacktestResult.mockResolvedValue(detail("job-A", {
        trade_page: {
          items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
        }
      }));
      if (failure === "merge") {
        api.loadBacktestResultTrades.mockResolvedValue({
          items: [{ ...firstTrade, pnl: "99" }], next_cursor: null, revision: 1
        });
      } else {
        api.loadBacktestResultTrades.mockRejectedValue(
          new api.ApiError("private page failure", failure)
        );
      }
      const hook = renderHook(() => useBacktestTrades({
        selectedResultId: "job-A", selectedTradeId: hasSelection ? "job-A:0" : null
      }));
      await waitFor(() => expect(hook.result.current.state.status)
        .toBe(hasSelection ? "valid" : "selection"));
      await act(async () => hook.result.current.loadMoreTrades());
      await waitFor(() => expect(hook.result.current.state.status).toBe(expected));
      expect(hook.result.current.state).not.toHaveProperty("snapshot");
    }
  );

  it("revokes retained callbacks and awaited detail sessions on unmount", async () => {
    api.loadBacktestResult.mockResolvedValue(detail("job-A", {
      trade_page: {
        items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
      }
    }));
    const selection = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: null
    }));
    await waitFor(() => expect(selection.result.current.state.status).toBe("selection"));
    const retained = selection.result.current.loadMoreTrades;
    selection.unmount();
    const sessionCalls = api.ensureBrowserSession.mock.calls.length;
    await retained();
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(sessionCalls);
    expect(api.loadBacktestResultTrades).not.toHaveBeenCalled();

    const pendingSession = deferred<typeof session>();
    api.loadBacktestResult.mockResolvedValueOnce(detail("job-B", {
      trade_page: {
        items: [{ ...firstTrade, id: "job-B:0" }],
        total_count: 2,
        next_cursor: "signed.trade+B"
      }
    }));
    const pending = renderHook(() => useBacktestTrades({
      selectedResultId: "job-B", selectedTradeId: "job-B:0"
    }));
    await waitFor(() => expect(pending.result.current.state.status).toBe("valid"));
    const pendingSessionCalls = api.ensureBrowserSession.mock.calls.length;
    api.ensureBrowserSession.mockReturnValueOnce(pendingSession.promise);
    let pendingLoad!: Promise<void>;
    await act(async () => { pendingLoad = pending.result.current.loadMoreTrades(); });
    await waitFor(() => expect(api.ensureBrowserSession).toHaveBeenCalledTimes(pendingSessionCalls + 1));
    pending.unmount();
    pendingSession.resolve(session);
    await act(async () => pendingLoad);
    expect(api.loadBacktestResultTrades).not.toHaveBeenCalled();
  });

  it("allows StrictMode cleanup and remount to complete fresh reads", async () => {
    const { result } = renderHook(
      () => useBacktestTrades({
        selectedResultId: "job-A", selectedTradeId: "job-A:0"
      }),
      { reactStrictMode: true }
    );
    await waitFor(() => expect(result.current.state.status).toBe("valid"));
    expect(api.loadBacktestResult).toHaveBeenCalled();
    expect(api.loadBacktestResultCandles).toHaveBeenCalled();
  });

  it("maps empty results and rejects malformed result-bound candle identity without candle I/O", async () => {
    api.loadBacktestResult.mockResolvedValueOnce(detail("other-job"));
    const mismatch = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: "job-A:0"
    }));
    await waitFor(() => expect(mismatch.result.current.state.status).toBe("invalid-data"));
    expect(api.loadBacktestResultCandles).not.toHaveBeenCalled();
    mismatch.unmount();

    api.loadBacktestResult.mockResolvedValueOnce(detail("job-A", {
      trade_page: { items: [], total_count: 0, next_cursor: null }
    }));
    const empty = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: null
    }));
    await waitFor(() => expect(empty.result.current.state.status).toBe("empty"));
    expect(api.loadBacktestResultCandles).not.toHaveBeenCalled();
  });

  it("maps a shared-pager merge failure to invalid-data without a transport failure", async () => {
    api.loadBacktestResult.mockResolvedValue(detail("job-A", {
      trade_page: {
        items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
      }
    }));
    api.loadBacktestResultTrades.mockResolvedValue({
      items: [{ ...firstTrade, pnl: "9" }], next_cursor: null, revision: 1
    });
    const { result } = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: "job-A:missing"
    }));
    await waitFor(() => expect(result.current.state.status).toBe("invalid-data"));
    expect(api.loadBacktestResultCandles).not.toHaveBeenCalled();
  });

  it.each([
    ["conflicting candle values", [candle("2026-01-01T00:07:00.000Z", "12")]],
    ["no new instant on continuation", [candle("2026-01-01T00:07:00.000Z")]]
  ])("rejects %s across candle pages and exposes no partial chart", async (_label, lastPage) => {
    api.loadBacktestResultCandles
      .mockResolvedValueOnce(candlePage([
        candle("2026-01-01T00:07:00.000Z")
      ], "signed.candle+A"))
      .mockResolvedValueOnce(candlePage(lastPage));
    const { result } = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: "job-A:0"
    }));
    await waitFor(() => expect(result.current.state.status).toBe("invalid-data"));
    expect(api.loadBacktestResultCandles).toHaveBeenCalledTimes(2);
    expect(result.current.state).not.toHaveProperty("snapshot");
  });

  it("rejects repeated candle cursors without issuing another page request", async () => {
    api.loadBacktestResultCandles
      .mockResolvedValueOnce(candlePage([
        candle("2026-01-01T00:07:00.000Z")
      ], "signed.candle+A"))
      .mockResolvedValueOnce(candlePage([
        candle("2026-01-01T00:09:00.000Z")
      ], "signed.candle+A"));
    const { result } = renderHook(() => useBacktestTrades({
      selectedResultId: "job-A", selectedTradeId: "job-A:0"
    }));
    await waitFor(() => expect(result.current.state.status).toBe("invalid-data"));
    expect(api.loadBacktestResultCandles).toHaveBeenCalledTimes(2);
    expect(result.current.state).not.toHaveProperty("snapshot");
  });

  it("masks a completed chart immediately on trade change and does not reload same-result detail", async () => {
    api.loadBacktestResult.mockResolvedValue(detail("job-A", {
      trade_page: {
        items: [firstTrade, secondTrade], total_count: 2, next_cursor: null
      }
    }));
    const hook = renderHook(
      ({ tradeId }: { tradeId: string }) => useBacktestTrades({
        selectedResultId: "job-A", selectedTradeId: tradeId
      }),
      { initialProps: { tradeId: "job-A:0" } }
    );
    await waitFor(() => expect(hook.result.current.state.status).toBe("valid"));
    const before = api.loadBacktestResultCandles.mock.calls.length;
    hook.rerender({ tradeId: "job-A:1" });
    expect(hook.result.current.state).toMatchObject({ status: "pending", tradeId: "job-A:1" });
    expect(hook.result.current.state).not.toHaveProperty("snapshot");
    await waitFor(() => expect(hook.result.current.state).toMatchObject({
      status: "valid", tradeId: "job-A:1", snapshot: { trades: [{ id: "job-A:1" }] }
    }));
    expect(api.loadBacktestResult).toHaveBeenCalledOnce();
    expect(api.loadBacktestResultCandles).toHaveBeenCalledTimes(before + 1);
  });

  it("rejects a retained load-more callback before I/O after result identity changes", async () => {
    api.loadBacktestResult.mockImplementation((jobId: string) =>
      Promise.resolve(detail(jobId, {
        trade_page: {
          items: [firstTrade], total_count: 2, next_cursor: "signed.trade+A"
        }
      }))
    );
    const hook = renderHook(
      ({ resultId }: { resultId: string }) => useBacktestTrades({
        selectedResultId: resultId, selectedTradeId: null
      }),
      { initialProps: { resultId: "job-A" } }
    );
    await waitFor(() => expect(hook.result.current.state.status).toBe("selection"));
    const staleLoadMore = hook.result.current.loadMoreTrades;
    const oldCalls = api.loadBacktestResultTrades.mock.calls.length;
    hook.rerender({ resultId: "job-B" });
    expect(hook.result.current.state).toMatchObject({
      status: "pending", resultId: "job-B", tradeId: null
    });
    await staleLoadMore();
    expect(api.loadBacktestResultTrades).toHaveBeenCalledTimes(oldCalls);
    await waitFor(() => expect(hook.result.current.state).toMatchObject({
      status: "selection", resultId: "job-B"
    }));
  });

  it("does not expose a stale candle response after changing result", async () => {
    const oldPage = deferred<BacktestResultsCandlesPage>();
    api.loadBacktestResultCandles
      .mockReturnValueOnce(oldPage.promise)
      .mockResolvedValue(candlePage([]));
    const hook = renderHook(
      ({ resultId }: { resultId: string }) => useBacktestTrades({
        selectedResultId: resultId, selectedTradeId: `${resultId}:0`
      }),
      { initialProps: { resultId: "job-A" } }
    );
    await waitFor(() => expect(api.loadBacktestResultCandles).toHaveBeenCalledOnce());
    hook.rerender({ resultId: "job-B" });
    expect(hook.result.current.state).toMatchObject({
      status: "pending", resultId: "job-B", tradeId: "job-B:0"
    });
    expect(hook.result.current.state).not.toHaveProperty("snapshot");
    await waitFor(() => expect(hook.result.current.state).toMatchObject({
      status: "valid", resultId: "job-B", tradeId: "job-B:0"
    }));
    oldPage.resolve(candlePage([
      candle("2026-01-01T00:07:00.000Z", "999")
    ]));
    await act(async () => oldPage.promise);
    expect(hook.result.current.state).toMatchObject({
      status: "valid", resultId: "job-B", tradeId: "job-B:0"
    });
    expect((hook.result.current.state as { snapshot?: { candles: { close: string }[] } })
      .snapshot?.candles[0]?.close).not.toBe("999");
  });
});
