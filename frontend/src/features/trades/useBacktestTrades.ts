import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  ApiError,
  ensureBrowserSession,
  loadBacktestResult,
  loadBacktestResultCandles,
  loadBacktestResultTrades
} from "../../api";
import {
  projectClosedTradePage,
  type TradePage
} from "../../shared/trading/closedTradePage";
import { useTradePagination } from "../../shared/trading/useTradePagination";
import {
  projectBacktestCandlesPage,
  projectBacktestTradeIdentity,
  tradeCandleWindow,
  type BacktestTradeIdentity,
  type TradeCandle,
  type TradeChartSnapshot,
  type TradeCandleWindow
} from "./tradeModel";

type Selection = string | null;
type FailureStatus =
  | "permission-unavailable"
  | "unavailable"
  | "error"
  | "invalid-data";
type CandleFailureStatus = FailureStatus | "candle-window-unavailable";

type ResultRead =
  | { status: "pending"; resultId: string }
  | { status: FailureStatus; resultId: string }
  | { status: "ready"; resultId: string; identity: BacktestTradeIdentity };

type StateIdentity = {
  resultId: string;
  tradeId: Selection;
  identity: BacktestTradeIdentity;
  tradePage: TradePage;
  totalCount: number;
};

export type BacktestTradesReadState =
  | {
    status: "pending";
    resultId: Selection;
    tradeId: Selection;
    identity?: BacktestTradeIdentity;
    tradePage?: TradePage;
    totalCount?: number;
  }
  | { status: FailureStatus; resultId: Selection; tradeId: Selection }
  | { status: "candle-window-unavailable"; resultId: string; tradeId: string }
  | (StateIdentity & { status: "selection"; tradeId: null })
  | (StateIdentity & { status: "empty"; tradeId: null; totalCount: 0 })
  | (StateIdentity & {
    status: "valid";
    tradeId: string;
    snapshot: TradeChartSnapshot;
  });

type UseBacktestTradesInput = {
  selectedResultId: Selection;
  selectedTradeId: Selection;
  enabled?: boolean;
};

type CandleRead =
  | {
    status: "pending";
    token: number;
    resultId: string;
    tradeId: string;
    window: TradeCandleWindow;
  }
  | {
    status: CandleFailureStatus;
    token: number;
    resultId: string;
    tradeId: string;
    window: TradeCandleWindow;
  }
  | {
    status: "valid";
    token: number;
    resultId: string;
    tradeId: string;
    window: TradeCandleWindow;
    snapshot: TradeChartSnapshot;
  };

function failureStatus(error: unknown): FailureStatus {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) return "permission-unavailable";
    if (error.status === 404 || error.status === 409) return "unavailable";
    if (error.status === 200 && error.message === "invalid_response") {
      return "invalid-data";
    }
    return "error";
  }
  return error instanceof Error && error.message === "invalid_response"
    ? "invalid-data"
    : "error";
}

async function requireBrowserSession(): Promise<void> {
  if (await ensureBrowserSession() === null) {
    throw new ApiError("session_unavailable", 404);
  }
}

function sameSelection(
  current: { resultId: Selection; tradeId: Selection; enabled: boolean; token: number },
  token: number,
  resultId: Selection,
  tradeId: Selection,
  enabled: boolean
): boolean {
  return current.token === token && current.resultId === resultId &&
    current.tradeId === tradeId && current.enabled === enabled;
}

export function useBacktestTrades({
  selectedResultId,
  selectedTradeId,
  enabled = true
}: UseBacktestTradesInput): {
  state: BacktestTradesReadState;
  loadMoreTrades: () => Promise<void>;
} {
  const currentSelection = useRef({
    resultId: selectedResultId,
    tradeId: selectedTradeId,
    enabled,
    token: 0
  });
  const previous = currentSelection.current;
  if (
    previous.resultId !== selectedResultId || previous.tradeId !== selectedTradeId ||
    previous.enabled !== enabled
  ) {
    currentSelection.current = {
      resultId: selectedResultId,
      tradeId: selectedTradeId,
      enabled,
      token: previous.token + 1
    };
  }
  const selectionToken = currentSelection.current.token;
  const detailGeneration = useRef(0);
  const mounted = useRef(false);
  const [resultRead, setResultRead] = useState<ResultRead>({
    status: "pending",
    resultId: selectedResultId ?? ""
  });
  const [candleRead, setCandleRead] = useState<CandleRead | null>(null);
  const tradeFailure = useRef<{ token: number; status: FailureStatus } | null>(null);

  useEffect(() => {
    mounted.current = true;
    return () => { mounted.current = false; };
  }, []);

  useEffect(() => {
    const requestToken = ++detailGeneration.current;
    if (!enabled || selectedResultId === null) return;
    setResultRead({ status: "pending", resultId: selectedResultId });
    const isCurrent = () => mounted.current &&
      detailGeneration.current === requestToken &&
      currentSelection.current.resultId === selectedResultId &&
      currentSelection.current.enabled === enabled;
    void (async () => {
      try {
        await requireBrowserSession();
        if (!isCurrent()) return;
        const detail = await loadBacktestResult(selectedResultId);
        if (!isCurrent()) return;
        const identity = projectBacktestTradeIdentity(detail);
        if (identity === null || identity.resultId !== selectedResultId) {
          setResultRead({ status: "invalid-data", resultId: selectedResultId });
          return;
        }
        setResultRead({ status: "ready", resultId: selectedResultId, identity });
      } catch (error) {
        if (isCurrent()) {
          setResultRead({
            status: failureStatus(error),
            resultId: selectedResultId
          });
        }
      }
    })();
    return () => {
      if (detailGeneration.current === requestToken) detailGeneration.current += 1;
    };
  }, [enabled, selectedResultId]);

  const resultReady = enabled && selectedResultId !== null &&
    resultRead.status === "ready" && resultRead.resultId === selectedResultId
    ? resultRead
    : null;
  const identity = resultReady?.identity ?? null;
  const loadTradePage = useCallback(async (cursor: string): Promise<TradePage> => {
    const resultId = selectedResultId;
    const tradeId = selectedTradeId;
    const token = selectionToken;
    if (
      !mounted.current || !enabled || resultId === null || identity === null ||
      !sameSelection(currentSelection.current, token, resultId, tradeId, enabled)
    ) {
      throw new Error("stale_result_request");
    }
    tradeFailure.current = null;
    try {
      await requireBrowserSession();
      if (
        !mounted.current ||
        !sameSelection(currentSelection.current, token, resultId, tradeId, enabled)
      ) {
        throw new Error("stale_result_request");
      }
      const page = await loadBacktestResultTrades(resultId, { cursor });
      if (
        !mounted.current ||
        !sameSelection(currentSelection.current, token, resultId, tradeId, enabled)
      ) {
        throw new Error("stale_result_request");
      }
      const projected = projectClosedTradePage(
        page.items,
        identity.tradePage.totalCount,
        page.next_cursor,
        false
      );
      if (projected === null) throw new Error("invalid_response");
      return projected;
    } catch (error) {
      if (
        mounted.current &&
        sameSelection(currentSelection.current, token, resultId, tradeId, enabled)
      ) {
        tradeFailure.current = { token, status: failureStatus(error) };
      }
      throw error;
    }
  }, [enabled, identity, selectedResultId, selectedTradeId, selectionToken]);

  const pagerIdentity = resultReady ? selectionToken : null;
  const pagination = useTradePagination({
    jobId: pagerIdentity === null ? null : `trades:${pagerIdentity}`,
    initialPage: resultReady?.identity.tradePage ?? null,
    onLoadMoreTrades: resultReady ? loadTradePage : undefined
  });
  const [boundPagerIdentity, setBoundPagerIdentity] = useState<number | null>(null);
  useEffect(() => {
    if (mounted.current) setBoundPagerIdentity(pagerIdentity);
  }, [pagerIdentity]);
  const pagerBound = pagerIdentity !== null && boundPagerIdentity === pagerIdentity;
  const tradePage = resultReady && pagerBound && pagination.tradePageValid
    ? pagination.tradePage
    : null;
  const selectedTrade = selectedTradeId === null
    ? undefined
    : tradePage?.items.find((trade) => trade.id === selectedTradeId);
  const window = useMemo(
    () => resultReady && selectedTrade
      ? tradeCandleWindow(resultReady.identity, selectedTrade)
      : null,
    [resultReady, selectedTrade]
  );

  useEffect(() => {
    if (!resultReady || selectedResultId === null || selectedTradeId === null || !selectedTrade) {
      return;
    }
    const token = selectionToken;
    if (!window) {
      setCandleRead({
        status: "invalid-data",
        token,
        resultId: selectedResultId,
        tradeId: selectedTradeId,
        window: { start: 0, end: 0 }
      });
      return;
    }
    let active = true;
    const isCurrent = () => active && mounted.current && sameSelection(
      currentSelection.current,
      token,
      selectedResultId,
      selectedTradeId,
      enabled
    );
    setCandleRead({
      status: "pending",
      token,
      resultId: selectedResultId,
      tradeId: selectedTradeId,
      window
    });
    void (async () => {
      const items: TradeCandle[] = [];
      const instants = new Set<number>();
      const cursors = new Set<string>();
      let cursor: string | undefined;
      try {
        while (true) {
          if (!isCurrent()) return;
          if (cursor !== undefined) {
            if (cursors.has(cursor)) throw new Error("invalid_response");
            cursors.add(cursor);
          }
          await requireBrowserSession();
          if (!isCurrent()) return;
          let page: Awaited<ReturnType<typeof loadBacktestResultCandles>>;
          try {
            page = await loadBacktestResultCandles(selectedResultId, {
              start: window.start,
              end: window.end,
              ...(cursor === undefined ? {} : { cursor })
            });
          } catch (error) {
            if (isCurrent()) {
              setCandleRead({
                status: error instanceof ApiError && error.status === 422
                  ? "candle-window-unavailable"
                  : failureStatus(error),
                token,
                resultId: selectedResultId,
                tradeId: selectedTradeId,
                window
              });
            }
            return;
          }
          if (!isCurrent()) return;
          const projected = projectBacktestCandlesPage(page, window);
          if (projected === null) throw new Error("invalid_response");
          let advanced = false;
          for (const candle of projected) {
            const instant = Date.parse(candle.timestamp);
            if (!instants.has(instant)) {
              instants.add(instant);
              advanced = true;
            }
          }
          if (
            (cursor !== undefined && !advanced) ||
            (page.next_cursor !== null && !advanced) ||
            (page.next_cursor !== null && cursors.has(page.next_cursor))
          ) {
            throw new Error("invalid_response");
          }
          items.push(...projected);
          const complete = projectBacktestCandlesPage({
            items,
            next_cursor: page.next_cursor,
            revision: 1
          }, window);
          if (complete === null) throw new Error("invalid_response");
          if (page.next_cursor === null) {
            if (isCurrent()) {
              setCandleRead({
                status: "valid",
                token,
                resultId: selectedResultId,
                tradeId: selectedTradeId,
                window,
                snapshot: {
                  strategyId: resultReady.identity.strategyId,
                  productId: resultReady.identity.productId,
                  timeframe: resultReady.identity.timeframe,
                  candles: complete,
                  trades: [selectedTrade]
                }
              });
            }
            return;
          }
          cursor = page.next_cursor;
        }
      } catch (error) {
        if (isCurrent()) {
          setCandleRead({
            status: failureStatus(error),
            token,
            resultId: selectedResultId,
            tradeId: selectedTradeId,
            window
          });
        }
      }
    })();
    return () => { active = false; };
  }, [
    enabled,
    resultReady,
    selectedResultId,
    selectedTrade,
    selectedTradeId,
    selectionToken,
    window
  ]);

  const loadMoreTrades = useCallback(async () => {
    if (
      !mounted.current || !resultReady || !pagerBound || !sameSelection(
        currentSelection.current,
        selectionToken,
        selectedResultId,
        selectedTradeId,
        enabled
      )
    ) return;
    await pagination.loadMoreTrades();
    if (!mounted.current || !sameSelection(
      currentSelection.current,
      selectionToken,
      selectedResultId,
      selectedTradeId,
      enabled
    )) return;
  }, [
    enabled,
    pagination.loadMoreTrades,
    resultReady,
    pagerBound,
    selectedResultId,
    selectedTradeId,
    selectionToken
  ]);

  useEffect(() => {
    if (
      !mounted.current || !resultReady || !pagerBound ||
      selectedResultId === null || selectedTradeId === null ||
      tradePage === null || !pagination.tradePageValid || pagination.tradePageLoading ||
      pagination.tradePageError ||
      tradePage.items.some((trade) => trade.id === selectedTradeId) ||
      tradePage.nextCursor === null
    ) return;
    void pagination.loadMoreTrades();
  }, [
    pagination.loadMoreTrades,
    pagination.tradePageError,
    pagination.tradePageLoading,
    pagination.tradePageValid,
    resultReady,
    pagerBound,
    selectedResultId,
    selectedTradeId,
    tradePage
  ]);

  let state: BacktestTradesReadState;
  if (!enabled) {
    state = { status: "pending", resultId: selectedResultId, tradeId: selectedTradeId };
  } else if (selectedResultId === null) {
    state = { status: "unavailable", resultId: null, tradeId: selectedTradeId };
  } else if (!resultReady) {
    state = resultRead.resultId === selectedResultId && resultRead.status !== "ready"
      ? { status: resultRead.status, resultId: selectedResultId, tradeId: selectedTradeId }
      : { status: "pending", resultId: selectedResultId, tradeId: selectedTradeId };
  } else if (tradePage === null) {
    state = {
      status: "pending", resultId: selectedResultId, tradeId: selectedTradeId
    };
  } else if (pagination.tradePageError && pagerBound) {
    const failure = tradeFailure.current?.token === selectionToken
      ? tradeFailure.current.status
      : "invalid-data";
    state = { status: failure, resultId: selectedResultId, tradeId: selectedTradeId };
  } else if (selectedTradeId === null) {
    state = tradePage.totalCount === 0
      ? {
        status: "empty", resultId: selectedResultId, tradeId: null,
        identity: resultReady.identity, tradePage, totalCount: 0
      }
      : {
        status: "selection", resultId: selectedResultId, tradeId: null,
        identity: resultReady.identity, tradePage, totalCount: tradePage.totalCount
      };
  } else if (!selectedTrade) {
    if (tradePage.nextCursor === null) {
      state = { status: "unavailable", resultId: selectedResultId, tradeId: selectedTradeId };
    } else {
      state = { status: "pending", resultId: selectedResultId, tradeId: selectedTradeId };
    }
  } else if (!window) {
    state = { status: "invalid-data", resultId: selectedResultId, tradeId: selectedTradeId };
  } else if (
    candleRead === null || candleRead.token !== selectionToken ||
    candleRead.resultId !== selectedResultId || candleRead.tradeId !== selectedTradeId ||
    candleRead.window.start !== window.start || candleRead.window.end !== window.end
  ) {
    state = {
      status: "pending",
      resultId: selectedResultId,
      tradeId: selectedTradeId,
      identity: resultReady.identity,
      tradePage,
      totalCount: tradePage.totalCount
    };
  } else if (candleRead.status === "valid") {
    state = {
      status: "valid",
      resultId: selectedResultId,
      tradeId: selectedTradeId,
      identity: resultReady.identity,
      tradePage,
      totalCount: tradePage.totalCount,
      snapshot: candleRead.snapshot
    };
  } else if (candleRead.status === "pending") {
    state = {
      status: "pending",
      resultId: selectedResultId,
      tradeId: selectedTradeId,
      identity: resultReady.identity,
      tradePage,
      totalCount: tradePage.totalCount
    };
  } else {
    state = candleRead.status === "candle-window-unavailable"
      ? { status: candleRead.status, resultId: selectedResultId, tradeId: selectedTradeId }
      : { status: candleRead.status, resultId: selectedResultId, tradeId: selectedTradeId };
  }

  return { state, loadMoreTrades };
}
