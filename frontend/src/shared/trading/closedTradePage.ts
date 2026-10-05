import type { ClosedTrade } from "./closedTrade";
import { isDecimalString } from "../format/decimal";
import { parseUtcTimestamp } from "../time/utc";

type ClosedTradeWireItem = {
  id: string;
  entry_time: string;
  exit_time: string;
  entry_price: string;
  exit_price: string;
  side: string;
  quantity: string;
  pnl: string;
  fee: string;
};

export type TradePage = {
  items: ClosedTrade[];
  totalCount: number;
  nextCursor: string | null;
};

function projectClosedTrade(trade: ClosedTradeWireItem): ClosedTrade | null {
  const entryTime = parseUtcTimestamp(trade.entry_time);
  const exitTime = parseUtcTimestamp(trade.exit_time);
  if (
    trade.id.length === 0 ||
    (trade.side !== "LONG" && trade.side !== "SHORT") ||
    entryTime === null || exitTime === null || entryTime > exitTime ||
    !isDecimalString(trade.quantity) ||
    !isDecimalString(trade.entry_price) ||
    !isDecimalString(trade.exit_price) ||
    !isDecimalString(trade.fee) ||
    !isDecimalString(trade.pnl)
  ) {
    return null;
  }
  return {
    id: trade.id,
    side: trade.side,
    quantity: trade.quantity,
    entryTime: trade.entry_time,
    entryPrice: trade.entry_price,
    exitTime: trade.exit_time,
    exitPrice: trade.exit_price,
    fee: trade.fee,
    pnl: trade.pnl
  };
}

export function projectClosedTradePage(
  items: ClosedTradeWireItem[],
  totalCount: number,
  nextCursor: string | null,
  loaded: boolean
): TradePage | null {
  const projected: ClosedTrade[] = [];
  for (const item of items) {
    const trade = projectClosedTrade(item);
    if (trade === null) return null;
    projected.push(trade);
  }
  const page = { items: projected, totalCount, nextCursor };
  return (loaded ? validLoadedTradePage(page) : validTradePage(page))
    ? page
    : null;
}

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
