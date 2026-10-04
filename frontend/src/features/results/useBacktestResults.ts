import { useCallback, useEffect, useRef, useState } from "react";

import {
  ApiError,
  ensureBrowserSession,
  loadBacktestResultsIndex
} from "../../api";
import type { BacktestResultIndexItem } from "./resultsModel";
import { projectBacktestResultsIndexItem } from "./resultsModel";

export type BacktestResultsReadState =
  | { status: "pending" }
  | { status: "permission-unavailable" }
  | { status: "unavailable" }
  | { status: "error" }
  | { status: "invalid-data" }
  | { status: "empty"; selection: null; items: [] }
  | {
    status: "index";
    selection: null;
    items: BacktestResultIndexItem[];
    nextCursor: string | null;
  };

type BacktestResultsRead = {
  state: BacktestResultsReadState;
  loadMoreIndex: () => Promise<void>;
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

export function useBacktestResults(): BacktestResultsRead {
  const [state, setState] = useState<BacktestResultsReadState>({ status: "pending" });
  const generation = useRef(0);
  const inFlight = useRef(false);

  useEffect(() => {
    const requestToken = ++generation.current;
    inFlight.current = false;
    setState({ status: "pending" });
    const isCurrent = () => generation.current === requestToken;
    void (async () => {
      try {
        await requireBrowserSession();
        if (!isCurrent()) return;
        const page = await loadBacktestResultsIndex();
        if (!isCurrent()) return;
        const items: BacktestResultIndexItem[] = [];
        for (const item of page.items) {
          const projected = projectBacktestResultsIndexItem(item);
          if (projected === null) {
            setState({ status: "invalid-data" });
            return;
          }
          items.push(projected);
        }
        setState(items.length === 0 && page.next_cursor === null
          ? { status: "empty", selection: null, items: [] }
          : { status: "index", selection: null, items, nextCursor: page.next_cursor });
      } catch (error) {
        if (isCurrent()) setState({ status: failureStatus(error) });
      }
    })();
    return () => {
      if (generation.current === requestToken) generation.current += 1;
      inFlight.current = false;
    };
  }, []);

  const loadMoreIndex = useCallback(async () => {
    if (state.status !== "index" || inFlight.current) {
      return;
    }
    const cursor = state.nextCursor;
    if (cursor === null) return;
    const previousItems = state.items;
    const requestToken = generation.current;
    inFlight.current = true;
    setState({ status: "pending" });
    try {
      await requireBrowserSession();
      if (generation.current !== requestToken) return;
      const page = await loadBacktestResultsIndex({ cursor });
      if (generation.current !== requestToken) return;
      const items: BacktestResultIndexItem[] = [];
      for (const item of page.items) {
        const projected = projectBacktestResultsIndexItem(item);
        if (projected === null) {
          setState({ status: "invalid-data" });
          return;
        }
        items.push(projected);
      }
      const merged = [...previousItems, ...items];
      setState(merged.length === 0 && page.next_cursor === null
        ? { status: "empty", selection: null, items: [] }
        : { status: "index", selection: null, items: merged, nextCursor: page.next_cursor });
    } catch (error) {
      if (generation.current === requestToken) setState({ status: failureStatus(error) });
    } finally {
      if (generation.current === requestToken) inFlight.current = false;
    }
  }, [state]);

  return { state, loadMoreIndex };
}
