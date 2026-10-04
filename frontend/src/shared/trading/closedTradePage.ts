import type { ClosedTrade } from "./closedTrade";

export type TradePage = {
  items: ClosedTrade[];
  totalCount: number;
  nextCursor: string | null;
};

export function validTradePage(page: TradePage): boolean {
  if (
    !Number.isSafeInteger(page.totalCount) ||
    page.totalCount < page.items.length ||
    (page.nextCursor !== null && page.nextCursor.trim() === "")
  ) {
    return false;
  }
  const ids = new Set<string>();
  for (const trade of page.items) {
    if (trade.id.length === 0 || ids.has(trade.id)) {
      return false;
    }
    ids.add(trade.id);
  }
  return true;
}

export function validLoadedTradePage(page: TradePage): boolean {
  return (
    validTradePage(page) &&
    (page.nextCursor === null
      ? page.items.length === page.totalCount
      : page.items.length < page.totalCount)
  );
}

function sameTrade(left: ClosedTrade, right: ClosedTrade): boolean {
  return (
    left.id === right.id &&
    left.side === right.side &&
    left.quantity === right.quantity &&
    left.entryTime === right.entryTime &&
    left.entryPrice === right.entryPrice &&
    left.exitTime === right.exitTime &&
    left.exitPrice === right.exitPrice &&
    left.fee === right.fee &&
    left.pnl === right.pnl
  );
}

export function mergeTradeItems(
  current: ClosedTrade[],
  incoming: ClosedTrade[]
): ClosedTrade[] | null {
  const merged = new Map(current.map((trade) => [trade.id, trade]));
  for (const trade of incoming) {
    const existing = merged.get(trade.id);
    if (existing && !sameTrade(existing, trade)) {
      return null;
    }
    merged.set(trade.id, existing ?? trade);
  }
  return [...merged.values()];
}
