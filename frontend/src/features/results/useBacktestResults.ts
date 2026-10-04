import { useCallback, useEffect, useRef, useState } from "react";

import {
  ApiError,
  ensureBrowserSession,
  loadBacktestResult,
  loadBacktestResultTrades,
  loadBacktestResultsIndex
} from "../../api";
import type { BacktestResultIndexItem, BacktestResultSnapshot, TradePage } from "./resultsModel";
import {
  projectBacktestResultDetail,
  projectBacktestResultsIndexItem,
  projectBacktestTradesPage
} from "./resultsModel";

type Selection = string | null;

export type BacktestResultsReadState =
  | { status: "pending"; selection: Selection }
  | { status: "permission-unavailable"; selection: Selection }
  | { status: "unavailable"; selection: Selection }
  | { status: "error"; selection: Selection }
  | { status: "invalid-data"; selection: Selection }
  | { status: "empty"; selection: null; items: [] }
  | {
    status: "index";
    selection: null;
    items: BacktestResultIndexItem[];
    nextCursor: string | null;
  }
  | {
    status: "detail";
    selection: string;
    resultId: string;
    snapshot: BacktestResultSnapshot;
  };

type BacktestResultsInput = {
  selectedResultId: Selection;
  enabled?: boolean;
};

type BacktestResultsRead = {
  state: BacktestResultsReadState;
  loadMoreIndex: () => Promise<void>;
  loadMoreTrades: (cursor: string) => Promise<TradePage>;
};

function failureStatus(error: unknown): "permission-unavailable" | "unavailable" | "error" | "invalid-data" {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) return "permission-unavailable";
    if (error.status === 404 || error.status === 409) return "unavailable";
    if (error.status === 200 && error.message === "invalid_response") return "invalid-data";
    return "error";
  }
  if (error instanceof Error && error.message === "invalid_response") {
    return "invalid-data";
  }
  return "error";
}

async function requireBrowserSession(): Promise<void> {
  if (await ensureBrowserSession() === null) {
    throw new ApiError("session_unavailable", 404);
  }
}

export function useBacktestResults({
  selectedResultId,
  enabled = true
}: BacktestResultsInput): BacktestResultsRead {
  const [storedState, setStoredState] = useState<BacktestResultsReadState>({
    status: "pending",
    selection: selectedResultId
  });
  const generation = useRef(0);
  const inFlight = useRef(false);
  const currentRequest = useRef({ selectedResultId, enabled });
  currentRequest.current = { selectedResultId, enabled };
  const renderGeneration = generation.current;

  useEffect(() => {
    const requestToken = ++generation.current;
    inFlight.current = false;
    setStoredState({ status: "pending", selection: selectedResultId });
    if (!enabled) {
      return () => {
        if (generation.current === requestToken) generation.current += 1;
        inFlight.current = false;
      };
    }
    const isCurrent = () => generation.current === requestToken &&
      currentRequest.current.selectedResultId === selectedResultId &&
      currentRequest.current.enabled === enabled;
    void (async () => {
      try {
        await requireBrowserSession();
        if (!isCurrent()) return;
        if (selectedResultId === null) {
          const page = await loadBacktestResultsIndex();
          if (!isCurrent()) return;
          const items: BacktestResultIndexItem[] = [];
          for (const item of page.items) {
            const projected = projectBacktestResultsIndexItem(item);
            if (projected === null) {
              setStoredState({ status: "invalid-data", selection: null });
              return;
            }
            items.push(projected);
          }
          setStoredState(items.length === 0 && page.next_cursor === null
            ? { status: "empty", selection: null, items: [] }
            : { status: "index", selection: null, items, nextCursor: page.next_cursor });
          return;
        }
        const detail = await loadBacktestResult(selectedResultId);
        if (!isCurrent()) return;
        const snapshot = projectBacktestResultDetail(detail);
        setStoredState(snapshot === null || snapshot.jobId !== selectedResultId
          ? { status: "invalid-data", selection: selectedResultId }
          : {
            status: "detail",
            selection: selectedResultId,
            resultId: selectedResultId,
            snapshot
          });
      } catch (error) {
        if (isCurrent()) {
          setStoredState({ status: failureStatus(error), selection: selectedResultId });
        }
      }
    })();
    return () => {
      if (generation.current === requestToken) generation.current += 1;
      inFlight.current = false;
    };
  }, [enabled, selectedResultId]);

  const loadMoreIndex = useCallback(async () => {
    if (
      !enabled || selectedResultId !== null || storedState.status !== "index" ||
      inFlight.current || renderGeneration !== generation.current ||
      !currentRequest.current.enabled || currentRequest.current.selectedResultId !== null
    ) {
      return;
    }
    const cursor = storedState.nextCursor;
    if (cursor === null) return;
    const previousItems = storedState.items;
    const requestToken = generation.current;
    inFlight.current = true;
    setStoredState({ status: "pending", selection: null });
    try {
      await requireBrowserSession();
      if (
        generation.current !== requestToken ||
        currentRequest.current.selectedResultId !== null || !currentRequest.current.enabled
      ) return;
      const page = await loadBacktestResultsIndex({ cursor });
      if (
        generation.current !== requestToken ||
        currentRequest.current.selectedResultId !== null || !currentRequest.current.enabled
      ) return;
      const items: BacktestResultIndexItem[] = [];
      for (const item of page.items) {
        const projected = projectBacktestResultsIndexItem(item);
        if (projected === null) {
          setStoredState({ status: "invalid-data", selection: null });
          return;
        }
        items.push(projected);
      }
      const merged = [...previousItems, ...items];
      setStoredState(merged.length === 0 && page.next_cursor === null
        ? { status: "empty", selection: null, items: [] }
        : { status: "index", selection: null, items: merged, nextCursor: page.next_cursor });
    } catch (error) {
      if (generation.current === requestToken) {
        setStoredState({ status: failureStatus(error), selection: null });
      }
    } finally {
      if (generation.current === requestToken) inFlight.current = false;
    }
  }, [enabled, selectedResultId, renderGeneration, storedState]);

  const loadMoreTrades = useCallback(async (cursor: string): Promise<TradePage> => {
    const current = storedState;
    const requestToken = renderGeneration;
    if (
      !enabled || selectedResultId === null || current.status !== "detail" ||
      current.resultId !== selectedResultId || current.selection !== selectedResultId ||
      requestToken !== generation.current ||
      currentRequest.current.selectedResultId !== selectedResultId || !currentRequest.current.enabled
    ) {
      throw new Error("result_unavailable");
    }
    await requireBrowserSession();
    if (
      generation.current !== requestToken ||
      currentRequest.current.selectedResultId !== selectedResultId || !currentRequest.current.enabled
    ) throw new Error("stale_result_request");
    const page = await loadBacktestResultTrades(selectedResultId, { cursor });
    if (
      generation.current !== requestToken ||
      currentRequest.current.selectedResultId !== selectedResultId || !currentRequest.current.enabled
    ) throw new Error("stale_result_request");
    const projected = projectBacktestTradesPage(page, current.snapshot.tradePage.totalCount);
    if (projected === null) throw new Error("invalid_response");
    return projected;
  }, [enabled, renderGeneration, selectedResultId, storedState]);

  const state = enabled && storedState.selection === selectedResultId
    ? storedState
    : { status: "pending" as const, selection: selectedResultId };
  return { state, loadMoreIndex, loadMoreTrades };
}
