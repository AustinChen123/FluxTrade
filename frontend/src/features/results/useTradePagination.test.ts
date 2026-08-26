// @vitest-environment jsdom

import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { demoBacktestSnapshot } from "./demo";
import type { TradePage } from "./resultsModel";
import { useTradePagination } from "./useTradePagination";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const trades = demoBacktestSnapshot.tradePage.items;
const firstPage: TradePage = {
  items: [trades[0]],
  totalCount: trades.length,
  nextCursor: "cursor-1"
};

describe("useTradePagination", () => {
  it.each([
    [
      "zero",
      { items: [], totalCount: 0, nextCursor: null } satisfies TradePage,
      true
    ],
    [
      "complete",
      {
        items: trades,
        totalCount: trades.length,
        nextCursor: null
      } satisfies TradePage,
      true
    ],
    ["partial", firstPage, true],
    [
      "invalid",
      { items: [trades[0]], totalCount: 2, nextCursor: null } satisfies TradePage,
      false
    ]
  ])(
    "classifies the initial %s page without issuing a request",
    (_name, page, valid) => {
      const load = vi.fn();
      const { result } = renderHook(() =>
        useTradePagination({
          jobId: "job-one",
          initialPage: page,
          onLoadMoreTrades: load
        })
      );

      expect(result.current.tradePage).toBe(page);
      expect(result.current.tradePageValid).toBe(valid);
      expect(result.current.tradePageLoading).toBe(false);
      expect(result.current.tradePageError).toBe(false);
      expect(load).not.toHaveBeenCalled();
    }
  );

  it("keeps a valid partial page explicit when no loader is wired", async () => {
    const { result } = renderHook(() =>
      useTradePagination({ jobId: "job-one", initialPage: firstPage })
    );

    await act(() => result.current.loadMoreTrades());
    expect(result.current.tradePage).toBe(firstPage);
    expect(result.current.tradePageValid).toBe(true);
    expect(result.current.tradePageLoading).toBe(false);
    expect(result.current.tradePageError).toBe(false);
  });

  it("retries the same cursor and merges an exact non-conflicting page", async () => {
    const load = vi
      .fn<(cursor: string) => Promise<TradePage>>()
      .mockRejectedValueOnce(new Error("temporary"))
      .mockResolvedValueOnce({
        items: trades,
        totalCount: trades.length,
        nextCursor: null
      });
    const { result } = renderHook(() =>
      useTradePagination({
        jobId: "job-one",
        initialPage: firstPage,
        onLoadMoreTrades: load
      })
    );

    await act(() => result.current.loadMoreTrades());
    expect(result.current.tradePageError).toBe(true);
    expect(result.current.tradePage).toBe(firstPage);

    await act(() => result.current.loadMoreTrades());
    expect(result.current.tradePageError).toBe(false);
    expect(result.current.tradePage?.items).toEqual(trades);
    expect(load.mock.calls).toEqual([["cursor-1"], ["cursor-1"]]);
  });

  it("retains the existing object and appends new IDs in provider order", async () => {
    const exactCopy = { ...trades[0] };
    const load = vi.fn().mockResolvedValue({
      items: [exactCopy, trades[1], trades[2]],
      totalCount: trades.length,
      nextCursor: null
    } satisfies TradePage);
    const { result } = renderHook(() =>
      useTradePagination({
        jobId: "job-one",
        initialPage: firstPage,
        onLoadMoreTrades: load
      })
    );

    await act(() => result.current.loadMoreTrades());
    expect(result.current.tradePage?.items).toEqual(trades);
    expect(result.current.tradePage?.items[0]).toBe(trades[0]);
    expect(result.current.tradePage?.nextCursor).toBeNull();
    expect(result.current.tradePageError).toBe(false);
  });

  it.each([
    [
      "only exact duplicates",
      {
        items: [{ ...trades[0] }],
        totalCount: trades.length,
        nextCursor: "cursor-2"
      }
    ],
    [
      "a conflicting duplicate",
      {
        items: [{ ...trades[0], pnl: "999.00" }],
        totalCount: trades.length,
        nextCursor: "cursor-2"
      }
    ],
    [
      "an empty ID",
      {
        items: [{ ...trades[1], id: "" }],
        totalCount: trades.length,
        nextCursor: "cursor-2"
      }
    ],
    [
      "a changed total",
      { items: [trades[1]], totalCount: 99, nextCursor: "cursor-2" }
    ],
    [
      "an unchanged cursor",
      {
        items: [trades[1]],
        totalCount: trades.length,
        nextCursor: "cursor-1"
      }
    ],
    [
      "a blank cursor",
      { items: [trades[1]], totalCount: trades.length, nextCursor: " " }
    ],
    [
      "an incomplete terminal page",
      { items: [trades[1]], totalCount: trades.length, nextCursor: null }
    ],
    [
      "a complete page with another cursor",
      {
        items: [trades[1], trades[2]],
        totalCount: trades.length,
        nextCursor: "cursor-2"
      }
    ]
  ] satisfies Array<[string, TradePage]>)(
    "rejects %s atomically and retries the original cursor",
    async (_name, invalidPage) => {
      const load = vi.fn().mockResolvedValue(invalidPage);
      const { result } = renderHook(() =>
        useTradePagination({
          jobId: "job-one",
          initialPage: firstPage,
          onLoadMoreTrades: load
        })
      );

      await act(() => result.current.loadMoreTrades());
      expect(result.current.tradePage).toBe(firstPage);
      expect(result.current.tradePageError).toBe(true);
      expect(result.current.tradePageLoading).toBe(false);
      await act(() => result.current.loadMoreTrades());
      expect(load.mock.calls).toEqual([["cursor-1"], ["cursor-1"]]);
    }
  );

  it("allows only one request for a cursor while it is in flight", async () => {
    const pending = deferred<TradePage>();
    const load = vi.fn().mockReturnValue(pending.promise);
    const { result } = renderHook(() =>
      useTradePagination({
        jobId: "job-one",
        initialPage: firstPage,
        onLoadMoreTrades: load
      })
    );

    act(() => {
      void result.current.loadMoreTrades();
      void result.current.loadMoreTrades();
    });
    expect(load).toHaveBeenCalledTimes(1);

    await act(async () => {
      pending.resolve({
        items: trades,
        totalCount: trades.length,
        nextCursor: null
      });
      await pending.promise;
    });
    expect(result.current.tradePage?.items).toEqual(trades);
  });

  it("ignores a stale response after the job and initial page change", async () => {
    const pending = deferred<TradePage>();
    const load = vi.fn().mockReturnValue(pending.promise);
    const { result, rerender } = renderHook(
      ({ jobId, page }: { jobId: string; page: TradePage }) =>
        useTradePagination({
          jobId,
          initialPage: page,
          onLoadMoreTrades: load
        }),
      { initialProps: { jobId: "job-one", page: firstPage } }
    );

    act(() => {
      void result.current.loadMoreTrades();
    });
    const replacement: TradePage = {
      items: [trades[2]],
      totalCount: 1,
      nextCursor: null
    };
    rerender({ jobId: "job-two", page: replacement });

    await act(async () => {
      pending.resolve({
        items: trades,
        totalCount: trades.length,
        nextCursor: null
      });
      await pending.promise;
    });
    await waitFor(() => expect(result.current.tradePage).toBe(replacement));
    expect(result.current.tradePageLoading).toBe(false);
    expect(result.current.tradePageError).toBe(false);
  });

  it("ignores a stale rejection after the job and initial page change", async () => {
    let reject!: (reason: unknown) => void;
    const promise = new Promise<TradePage>((_resolve, rejectPromise) => {
      reject = rejectPromise;
    });
    const load = vi.fn().mockReturnValue(promise);
    const { result, rerender } = renderHook(
      ({ jobId, page }: { jobId: string; page: TradePage }) =>
        useTradePagination({
          jobId,
          initialPage: page,
          onLoadMoreTrades: load
        }),
      { initialProps: { jobId: "job-one", page: firstPage } }
    );

    act(() => {
      void result.current.loadMoreTrades();
    });
    const replacement: TradePage = {
      items: [trades[2]],
      totalCount: 1,
      nextCursor: null
    };
    rerender({ jobId: "job-two", page: replacement });

    await act(async () => {
      reject(new Error("late"));
      await promise.catch(() => undefined);
    });
    expect(result.current.tradePage).toBe(replacement);
    expect(result.current.tradePageLoading).toBe(false);
    expect(result.current.tradePageError).toBe(false);
  });

  it("ignores a stale success after only the initial page identity changes", async () => {
    const pending = deferred<TradePage>();
    const load = vi.fn().mockReturnValue(pending.promise);
    const { result, rerender } = renderHook(
      ({ page }: { page: TradePage }) =>
        useTradePagination({
          jobId: "job-one",
          initialPage: page,
          onLoadMoreTrades: load
        }),
      { initialProps: { page: firstPage } }
    );

    act(() => {
      void result.current.loadMoreTrades();
    });
    const replacement: TradePage = {
      items: [trades[2]],
      totalCount: 1,
      nextCursor: null
    };
    rerender({ page: replacement });
    await act(async () => {
      pending.resolve({
        items: trades,
        totalCount: trades.length,
        nextCursor: null
      });
      await pending.promise;
    });

    expect(result.current.tradePage).toBe(replacement);
    expect(result.current.tradePageLoading).toBe(false);
    expect(result.current.tradePageError).toBe(false);
  });

  it("ignores a stale rejection after only the job changes", async () => {
    let reject!: (reason: unknown) => void;
    const promise = new Promise<TradePage>((_resolve, rejectPromise) => {
      reject = rejectPromise;
    });
    const load = vi.fn().mockReturnValue(promise);
    const { result, rerender } = renderHook(
      ({ jobId }: { jobId: string }) =>
        useTradePagination({
          jobId,
          initialPage: firstPage,
          onLoadMoreTrades: load
        }),
      { initialProps: { jobId: "job-one" } }
    );

    act(() => {
      void result.current.loadMoreTrades();
    });
    rerender({ jobId: "job-two" });
    await act(async () => {
      reject(new Error("late"));
      await promise.catch(() => undefined);
    });

    expect(result.current.tradePage).toBe(firstPage);
    expect(result.current.tradePageLoading).toBe(false);
    expect(result.current.tradePageError).toBe(false);
  });

  it("starts fresh after unmount and re-entry", () => {
    const first = renderHook(() =>
      useTradePagination({ jobId: "job-one", initialPage: firstPage })
    );
    first.unmount();

    const replacement: TradePage = {
      items: [],
      totalCount: 0,
      nextCursor: null
    };
    const second = renderHook(() =>
      useTradePagination({ jobId: "job-two", initialPage: replacement })
    );
    expect(second.result.current.tradePage).toBe(replacement);
    expect(second.result.current.tradePageLoading).toBe(false);
    expect(second.result.current.tradePageError).toBe(false);
  });

  it("uses only the re-entry loader and exact re-entry page", async () => {
    const oldLoad = vi.fn();
    const first = renderHook(() =>
      useTradePagination({
        jobId: "job-one",
        initialPage: firstPage,
        onLoadMoreTrades: oldLoad
      })
    );
    first.unmount();

    const replacement: TradePage = {
      items: [trades[1]],
      totalCount: 2,
      nextCursor: "cursor-next"
    };
    const newLoad = vi.fn().mockResolvedValue({
      items: [trades[2]],
      totalCount: 2,
      nextCursor: null
    } satisfies TradePage);
    const second = renderHook(() =>
      useTradePagination({
        jobId: "job-two",
        initialPage: replacement,
        onLoadMoreTrades: newLoad
      })
    );

    await act(() => second.result.current.loadMoreTrades());
    expect(oldLoad).not.toHaveBeenCalled();
    expect(newLoad).toHaveBeenCalledWith("cursor-next");
    expect(second.result.current.tradePage?.items).toEqual([
      trades[1],
      trades[2]
    ]);
  });
});
