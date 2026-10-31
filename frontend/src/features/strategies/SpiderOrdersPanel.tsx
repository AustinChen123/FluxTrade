import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { useTranslation } from "react-i18next";

import { ApiError, loadSpiderOrderTrades, loadSpiderOrders, type SpiderOrderQuery } from "../../api";
import type { Locale } from "../../shared/i18n";
import { formatPresentationTimestamp, PRESENTATION_TIME_ZONE } from "../../shared/time/presentation";
import {
  decodeSpiderOrdersPage,
  decodeSpiderTradesPage,
  parseSpiderUtcRange,
  type SpiderLocalOrder,
  type SpiderOrderCategory,
  type SpiderOrdersPage,
  type SpiderTradesPage
} from "./spiderOrdersModel";

type Translate = ReturnType<typeof useTranslation>["t"];
const PAGE_SIZE = 20;

type FilterDraft = {
  category: SpiderOrderCategory;
  localOrderId: string;
  fromUtc: string;
  toUtc: string;
};

function queryFor(
  strategyId: string,
  filters: FilterDraft,
  range: { from: number | null; to: number | null },
  offset: number,
  viewVersion?: string
): SpiderOrderQuery {
  return {
    strategy_id: strategyId,
    category: filters.category,
    ...(filters.localOrderId === "" ? {} : { local_order_id: filters.localOrderId }),
    ...(range.from === null ? {} : { order_created_from_ms: range.from }),
    ...(range.to === null ? {} : { order_created_to_ms: range.to }),
    limit: PAGE_SIZE,
    offset,
    ...(viewVersion === undefined ? {} : { view_version: viewVersion })
  };
}

function formatInstant(value: string | null, formatter: Intl.DateTimeFormat): string {
  return value === null ? "—" : formatPresentationTimestamp(value, formatter);
}

function responseMatchesQuery(
  page: SpiderOrdersPage | SpiderTradesPage,
  filters: FilterDraft,
  range: { from: number | null; to: number | null },
  offset: number
): boolean {
  return page.limit === PAGE_SIZE && page.offset === offset &&
    page.filters.category === filters.category &&
    page.filters.local_order_id === (filters.localOrderId === "" ? null : filters.localOrderId) &&
    page.filters.order_created_from_ms === range.from &&
    page.filters.order_created_to_ms === range.to;
}

function errorCode(reason: unknown): { kind: "changed" | "permission" | "unavailable" | "read"; code: string } {
  if (reason instanceof ApiError) {
    if (reason.status === 409 && reason.message === "spider_order_view_changed") {
      return { kind: "changed", code: reason.message };
    }
    if (reason.status === 401 || reason.status === 403) {
      return { kind: "permission", code: "unauthorized" };
    }
    if (reason.status === 503 && reason.message === "spider_order_query_unavailable") {
      return { kind: "unavailable", code: reason.message };
    }
    return { kind: "read", code: reason.message };
  }
  return { kind: "read", code: "read_failed" };
}

export interface SpiderOrdersPanelProps {
  strategyId: string;
  locale: Locale;
  t: Translate;
  onClose: () => void;
}

export function SpiderOrdersPanel({ strategyId, locale, t, onClose }: SpiderOrdersPanelProps) {
  const [filters, setFilters] = useState<FilterDraft>({
    category: "ALL", localOrderId: "", fromUtc: "", toUtc: ""
  });
  const [ordersPage, setOrdersPage] = useState<SpiderOrdersPage | null>(null);
  const [tradesPage, setTradesPage] = useState<SpiderTradesPage | null>(null);
  const [selectedOrderId, setSelectedOrderId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [ordersLoading, setOrdersLoading] = useState(false);
  const [tradesLoading, setTradesLoading] = useState(false);
  const [hasRequested, setHasRequested] = useState(false);
  const [stale, setStale] = useState(false);
  const [problem, setProblem] = useState<{ kind: string; code: string } | null>(null);
  const [filterError, setFilterError] = useState(false);
  const ordersController = useRef<AbortController | null>(null);
  const tradesController = useRef<AbortController | null>(null);
  const ordersSequence = useRef(0);
  const tradesSequence = useRef(0);
  const formatter = useMemo(
    () => new Intl.DateTimeFormat(locale, {
      dateStyle: "medium", timeStyle: "medium", timeZone: PRESENTATION_TIME_ZONE
    }),
    [locale]
  );
  const parsedRange = parseSpiderUtcRange(filters.fromUtc, filters.toUtc);

  const abortOrders = useCallback(() => {
    ordersSequence.current += 1;
    ordersController.current?.abort();
    ordersController.current = null;
  }, []);
  const abortTrades = useCallback(() => {
    tradesSequence.current += 1;
    tradesController.current?.abort();
    tradesController.current = null;
  }, []);
  const invalidateView = useCallback(() => {
    abortOrders();
    abortTrades();
    setOrdersPage(null);
    setTradesPage(null);
    setSelectedOrderId(null);
    setStale(false);
    setLoading(false);
    setOrdersLoading(false);
    setTradesLoading(false);
  }, [abortOrders, abortTrades]);

  const loadOrderPage = useCallback(async (
    offset: number,
    viewVersion?: string,
    establishNewView = false
  ) => {
    const range = parseSpiderUtcRange(filters.fromUtc, filters.toUtc);
    if (range === null) {
      setFilterError(true);
      return;
    }
    abortOrders();
    abortTrades();
    const requestId = ordersSequence.current;
    const controller = new AbortController();
    ordersController.current = controller;
    setFilterError(false);
    setProblem(null);
    setLoading(true);
    setOrdersLoading(true);
    setTradesLoading(true);
    setHasRequested(true);
    if (establishNewView) {
      setTradesPage(null);
      setSelectedOrderId(null);
      setStale(ordersPage !== null);
    }
    try {
      const value = await loadSpiderOrders(
        queryFor(strategyId, filters, range, offset, viewVersion), controller.signal
      );
      const page = decodeSpiderOrdersPage(value, strategyId);
      if (!responseMatchesQuery(page, filters, range, offset) ||
          (viewVersion !== undefined && page.view_version !== viewVersion)) throw new Error("invalid_response");
      if (requestId !== ordersSequence.current) return;
      setOrdersPage(page);
      setTradesPage(null);
      setSelectedOrderId(null);
      setStale(false);
      setProblem(null);
    } catch (reason) {
      if (requestId !== ordersSequence.current) return;
      const classified = errorCode(reason);
      if (classified.kind === "changed") {
        invalidateView();
      } else if (classified.kind === "permission") {
        invalidateView();
      } else {
        setStale(ordersPage !== null);
      }
      setProblem(classified);
    } finally {
      if (requestId === ordersSequence.current) {
        setLoading(false);
        setOrdersLoading(false);
        setTradesLoading(false);
        ordersController.current = null;
      }
    }
  }, [abortOrders, abortTrades, filters, invalidateView, ordersPage, strategyId]);

  const loadTradePage = useCallback(async (orderId: string, offset: number) => {
    const parent = ordersPage;
    const range = parseSpiderUtcRange(filters.fromUtc, filters.toUtc);
    if (parent === null || parent === undefined || stale || ordersLoading || range === null) return;
    abortTrades();
    const requestId = tradesSequence.current;
    const controller = new AbortController();
    tradesController.current = controller;
    setProblem(null);
    const changedSelection = selectedOrderId !== orderId;
    setSelectedOrderId(orderId);
    if (changedSelection || offset === 0) setTradesPage(null);
    setLoading(true);
    setTradesLoading(true);
    try {
      const value = await loadSpiderOrderTrades(
        orderId,
        queryFor(strategyId, filters, range, offset, parent.view_version),
        controller.signal
      );
      const page = decodeSpiderTradesPage(value, strategyId, orderId, parent.view_version);
      if (!responseMatchesQuery(page, filters, range, offset)) throw new Error("invalid_response");
      if (requestId !== tradesSequence.current) return;
      if (page.view_version !== parent.view_version) {
        invalidateView();
        setProblem({ kind: "changed", code: "spider_order_view_changed" });
        return;
      }
      setSelectedOrderId(orderId);
      setTradesPage(page);
      setStale(false);
    } catch (reason) {
      if (requestId !== tradesSequence.current) return;
      const classified = errorCode(reason);
      if (classified.kind === "changed" || classified.kind === "permission") {
        invalidateView();
      } else {
        setStale(true);
      }
      setProblem(classified);
    } finally {
      if (requestId === tradesSequence.current) {
        setLoading(false);
        setTradesLoading(false);
        tradesController.current = null;
      }
    }
  }, [abortTrades, filters, invalidateView, ordersLoading, ordersPage, selectedOrderId, stale, strategyId]);

  useEffect(() => {
    const initialRead = window.setTimeout(() => void loadOrderPage(0), 0);
    return () => {
      window.clearTimeout(initialRead);
      abortOrders();
      abortTrades();
    };
  }, []);

  const changeFilters = (next: FilterDraft) => {
    abortOrders();
    abortTrades();
    setFilters(next);
    setOrdersPage(null);
    setTradesPage(null);
    setSelectedOrderId(null);
    setProblem(null);
    setStale(false);
    setHasRequested(false);
    setLoading(false);
    setOrdersLoading(false);
    setTradesLoading(false);
  };

  const selectedOrder = ordersPage?.orders.find((order) => order.local_order_id === selectedOrderId) ?? null;
  const errorText = problem === null ? null :
    problem.kind === "changed" ? t("strategies.orders.changed") :
      problem.kind === "permission" ? t("strategies.orders.permission") :
        problem.kind === "unavailable" ? t("strategies.orders.unavailable", { reason: problem.code }) :
          t("strategies.orders.readError", { reason: problem.code });

  return (
    <section className="spider-orders" aria-labelledby="spider-orders-title" aria-busy={loading}>
      <div className="spider-orders-heading">
        <div>
          <p className="panel-kicker">{t("strategies.orders.kicker")}</p>
          <h3 id="spider-orders-title">{t("strategies.orders.title", { strategyId })}</h3>
          <p>{t("strategies.orders.provenance")}</p>
        </div>
        <button type="button" className="spider-close" onClick={onClose}>{t("strategies.orders.close")}</button>
      </div>

      <form className="spider-order-filters" onSubmit={(event) => {
        event.preventDefault();
        void loadOrderPage(0, undefined, true);
      }}>
        <label>
          <span>{t("strategies.orders.category")}</span>
          <select value={filters.category} onChange={(event) => changeFilters({ ...filters, category: event.target.value as SpiderOrderCategory })}>
            {(["ALL", "CURRENT", "PENDING", "HISTORY"] as const).map((category) =>
              <option key={category} value={category}>{t(`strategies.orders.categories.${category}`)}</option>
            )}
          </select>
        </label>
        <label>
          <span>{t("strategies.orders.localOrderFilter")}</span>
          <input value={filters.localOrderId} maxLength={36} onChange={(event) => changeFilters({ ...filters, localOrderId: event.target.value })} />
        </label>
        <label>
          <span>{t("strategies.orders.fromUtc")}</span>
          <input type="text" inputMode="numeric" autoComplete="off" value={filters.fromUtc} placeholder="2026-01-01T00:00:00.000Z" onChange={(event) => changeFilters({ ...filters, fromUtc: event.target.value })} />
        </label>
        <label>
          <span>{t("strategies.orders.toUtc")}</span>
          <input type="text" inputMode="numeric" autoComplete="off" value={filters.toUtc} placeholder="2026-01-02T00:00:00.000Z" onChange={(event) => changeFilters({ ...filters, toUtc: event.target.value })} />
        </label>
        <div className="spider-filter-actions">
          <button type="submit" disabled={loading}>{t("strategies.orders.applyFilters")}</button>
          <button type="button" onClick={() => void loadOrderPage(0, undefined, true)} disabled={loading}>{t("strategies.orders.refresh")}</button>
        </div>
        <p className="spider-utc-note">{t("strategies.orders.utcNote")}</p>
        {filterError && <p className="spider-inline-error" role="alert">{t("strategies.orders.invalidUtcRange")}</p>}
      </form>

      <p className="spider-source-note" role="note">{t("strategies.orders.localOnly")}</p>
      {loading && <p className="spider-live-status" role="status" aria-live="polite">{t("strategies.orders.loading")}</p>}
      {errorText && <p className="spider-inline-error" role="alert">{errorText}{stale ? ` ${t("strategies.orders.stale")}` : ""}</p>}
      {problem?.kind === "changed" && (
        <button type="button" onClick={() => void loadOrderPage(0, undefined, true)} disabled={loading}>
          {t("strategies.orders.reloadChanged")}
        </button>
      )}
      {!loading && hasRequested && problem === null && ordersPage === null && (
        <p role="status">{t("strategies.orders.empty")}</p>
      )}

      {ordersPage && (
        <>
          <p className="spider-page-summary" role="status">
            {t("strategies.orders.count", { count: ordersPage.total_count })} · {t("strategies.orders.version", { version: ordersPage.view_version })}
          </p>
          {ordersPage.orders.length === 0 ? <p role="status">{t("strategies.orders.empty")}</p> : (
            <div className="spider-table-scroll" tabIndex={0} role="region" aria-label={t("strategies.orders.orderTableRegion")}>
              <table>
                <thead><tr>
                  <th scope="col">{t("strategies.orders.localOrderId")}</th>
                  <th scope="col">{t("strategies.orders.providerOrderId")}</th>
                  <th scope="col">{t("strategies.orders.product")}</th>
                  <th scope="col">{t("strategies.orders.category")}</th>
                  <th scope="col">{t("strategies.orders.status")}</th>
                  <th scope="col">{t("strategies.orders.quantity")}</th>
                  <th scope="col">{t("strategies.orders.price")}</th>
                  <th scope="col">{t("strategies.orders.created")}</th>
                  <th scope="col">{t("strategies.orders.inspect")}</th>
                </tr></thead>
                <tbody>{ordersPage.orders.map((order) => <tr key={order.local_order_id}>
                  <th scope="row"><code>{order.local_order_id}</code></th>
                  <td>{order.provider_order_id ?? t("strategies.orders.notRecorded")}</td>
                  <td>{order.product_id}</td>
                  <td>{t(`strategies.orders.categories.${order.category}`)}</td>
                  <td>{order.order_status_recognized ? order.order_status : `${t("strategies.orders.unknownStatus")}: ${order.order_status}`}</td>
                  <td>{order.quantity ?? "—"}</td>
                  <td>{order.price ?? "—"}</td>
                  <td>{formatPresentationTimestamp(order.created_timestamp_ms, formatter)}</td>
                  <td><button type="button" disabled={ordersLoading || stale} onClick={() => void loadTradePage(order.local_order_id, 0)} aria-label={t("strategies.orders.inspectOrder", { orderId: order.local_order_id })}>{t("strategies.orders.inspect")}</button></td>
                </tr>)}</tbody>
              </table>
            </div>
          )}
          <nav className="spider-pagination" aria-label={t("strategies.orders.orderPagination")}>
            <button type="button" disabled={ordersLoading || stale || ordersPage.offset === 0} onClick={() => void loadOrderPage(Math.max(0, ordersPage.offset - ordersPage.limit), ordersPage.view_version)}>{t("strategies.orders.previous")}</button>
            <span>{t("strategies.orders.page", { offset: ordersPage.offset + 1, count: ordersPage.total_count })}</span>
            <button type="button" disabled={ordersLoading || stale || ordersPage.next_offset === null} onClick={() => ordersPage.next_offset !== null && void loadOrderPage(ordersPage.next_offset, ordersPage.view_version)}>{t("strategies.orders.next")}</button>
          </nav>
          {selectedOrder && <OrderDetails order={selectedOrder} formatter={formatter} t={t} />}
          {tradesPage && selectedOrderId && (
            <TradesInspection page={tradesPage} selectedOrder={selectedOrder} formatter={formatter} t={t}
              loading={tradesLoading} stale={stale}
              onPage={(offset) => void loadTradePage(selectedOrderId, offset)} />
          )}
        </>
      )}
    </section>
  );
}

function OrderDetails({ order, formatter, t }: { order: SpiderLocalOrder; formatter: Intl.DateTimeFormat; t: Translate }) {
  const fields: [string, string | number | boolean | null][] = Object.entries(order) as [string, string | number | boolean | null][];
  const nullIDFields = new Set(["client_order_id", "source_client_order_id", "provider_wire_client_order_id", "provider_order_id"]);
  const timestampFields = new Set(["submitted_at", "acked_at", "last_reconciled_at"]);
  return <details className="spider-order-details" open>
    <summary>{t("strategies.orders.orderDetails", { orderId: order.local_order_id })}</summary>
    <dl>{fields.map(([key, value]) => <div key={key}>
      <dt>{t(`strategies.orders.fields.${key}`, { defaultValue: key.replaceAll("_", " ") })}</dt>
      <dd>{value === null ? (nullIDFields.has(key) ? t("strategies.orders.notRecorded") : "—") :
        timestampFields.has(key) ? formatInstant(value as string, formatter) : String(value)}</dd>
    </div>)}</dl>
  </details>;
}

function TradesInspection({
  page, selectedOrder, formatter, t, loading, stale, onPage
}: {
  page: SpiderTradesPage;
  selectedOrder: SpiderLocalOrder | null;
  formatter: Intl.DateTimeFormat;
  t: Translate;
  loading: boolean;
  stale: boolean;
  onPage: (offset: number) => void;
}) {
  return <section className="spider-trades" aria-labelledby="spider-trades-title" aria-busy={loading}>
    <h4 id="spider-trades-title">{t("strategies.orders.tradesTitle", { orderId: page.local_order_id })}</h4>
    {selectedOrder && <p>{t("strategies.orders.parentCategory", { category: t(`strategies.orders.categories.${selectedOrder.category}`) })}</p>}
    {page.trades.length === 0 ? <p role="status">{t("strategies.orders.emptyTrades")}</p> : (
      <div className="spider-table-scroll" tabIndex={0} role="region" aria-label={t("strategies.orders.tradeTableRegion")}>
        <table>
          <thead><tr>
            <th scope="col">{t("strategies.orders.localTradeId")}</th>
            <th scope="col">{t("strategies.orders.providerTradeId")}</th>
            <th scope="col">{t("strategies.orders.product")}</th>
            <th scope="col">{t("strategies.orders.side")}</th>
            <th scope="col">{t("strategies.orders.price")}</th>
            <th scope="col">{t("strategies.orders.quantity")}</th>
            <th scope="col">{t("strategies.orders.fee")}</th>
            <th scope="col">{t("strategies.orders.feeEvidence")}</th>
            <th scope="col">{t("strategies.orders.created")}</th>
          </tr></thead>
          <tbody>{page.trades.map((trade) => <tr key={trade.local_trade_id}>
            <th scope="row"><code>{trade.local_trade_id}</code></th>
            <td>{trade.provider_trade_id ?? t("strategies.orders.notRecorded")}</td>
            <td>{trade.product_id}</td>
            <td>{trade.side}</td>
            <td>{trade.price ?? "—"}</td>
            <td>{trade.quantity ?? "—"}</td>
            <td>{trade.fee ?? "—"} {trade.fee_asset ?? ""}</td>
            <td>{trade.fee_evidence === "INCOMPLETE" ? t("strategies.orders.incompleteFee") : trade.fee_evidence}</td>
            <td>{formatPresentationTimestamp(trade.timestamp_ms, formatter)}</td>
          </tr>)}</tbody>
        </table>
      </div>
    )}
    <nav className="spider-pagination" aria-label={t("strategies.orders.tradePagination")}>
    <button type="button" disabled={loading || stale || page.offset === 0} onClick={() => onPage(Math.max(0, page.offset - page.limit))}>{t("strategies.orders.previous")}</button>
      <span>{t("strategies.orders.page", { offset: page.offset + 1, count: page.total_count })}</span>
      <button type="button" disabled={loading || stale || page.next_offset === null} onClick={() => page.next_offset !== null && onPage(page.next_offset)}>{t("strategies.orders.next")}</button>
    </nav>
  </section>;
}
