import { useEffect, useRef, useState } from "react";

import {
  mergeTradeItems,
  validLoadedTradePage,
  validTradePage,
  type TradePage
} from "./resultsModel";

type TradePaginationInput = {
  jobId: string | null;
  initialPage: TradePage | null;
  onLoadMoreTrades?: (cursor: string) => Promise<TradePage>;
};

export type TradePagination = {
  tradePage: TradePage | null;
  tradePageLoading: boolean;
  tradePageError: boolean;
  tradePageValid: boolean;
  loadMoreTrades: () => Promise<void>;
};

export function useTradePagination({
  jobId,
  initialPage,
  onLoadMoreTrades
}: TradePaginationInput): TradePagination {
  const [tradePage, setTradePage] = useState<TradePage | null>(initialPage);
  const [tradePageLoading, setTradePageLoading] = useState(false);
  const [tradePageError, setTradePageError] = useState(false);
  const requestGeneration = useRef(0);
  const requestInFlight = useRef(false);

  useEffect(() => {
    requestGeneration.current += 1;
    requestInFlight.current = false;
    setTradePage(initialPage);
    setTradePageLoading(false);
    setTradePageError(false);
    return () => {
      requestGeneration.current += 1;
      requestInFlight.current = false;
    };
  }, [initialPage, jobId]);

  const tradePageValid =
    tradePage !== null && validLoadedTradePage(tradePage);

  const loadMoreTrades = async () => {
    if (
      !tradePageValid ||
      tradePage.nextCursor === null ||
      !onLoadMoreTrades ||
      requestInFlight.current
    ) {
      return;
    }
    requestInFlight.current = true;
    const requestedCursor = tradePage.nextCursor;
    const generation = requestGeneration.current;
    setTradePageLoading(true);
    setTradePageError(false);
    try {
      const nextPage = await onLoadMoreTrades(requestedCursor);
      if (generation !== requestGeneration.current) {
        return;
      }
      const items = validTradePage(nextPage)
        ? mergeTradeItems(tradePage.items, nextPage.items)
        : null;
      const cursorAdvanced =
        nextPage.nextCursor === null || nextPage.nextCursor !== requestedCursor;
      const pageComplete =
        nextPage.nextCursor === null
          ? items?.length === nextPage.totalCount
          : (items?.length ?? 0) < nextPage.totalCount;
      if (
        items === null ||
        items.length === tradePage.items.length ||
        nextPage.totalCount !== tradePage.totalCount ||
        !cursorAdvanced ||
        !pageComplete
      ) {
        throw new Error("invalid_trade_page");
      }
      setTradePage({ ...nextPage, items });
    } catch {
      if (generation === requestGeneration.current) {
        setTradePageError(true);
      }
    } finally {
      if (generation === requestGeneration.current) {
        requestInFlight.current = false;
        setTradePageLoading(false);
      }
    }
  };

  return {
    tradePage,
    tradePageLoading,
    tradePageError,
    tradePageValid,
    loadMoreTrades
  };
}
