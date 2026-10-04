import { lazy, Suspense, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  finiteDecimalNumber,
  isDecimalString
} from "../../shared/format/decimal";
import type { Locale } from "../../shared/i18n";
import type { Theme } from "../../shared/theme";
import {
  formatPresentationTimestamp,
  PRESENTATION_TIME_ZONE
} from "../../shared/time/presentation";
import { demoTradeSnapshot } from "./demo";
import {
  selectedTradeOption,
  tradeChartOption,
  type TradeChartCopy
} from "./tradeCharts";
import {
  buildTradeChartModel,
  tradeIdFromChartData,
  type TradeChartSnapshot
} from "./tradeModel";
import { useBacktestTrades } from "./useBacktestTrades";

const CandlestickChart = lazy(() =>
  import("./CandlestickChart").then((module) => ({
    default: module.CandlestickChart
  }))
);

type Props = {
  demoMode: boolean;
  theme: Theme;
  snapshot?: TradeChartSnapshot | null;
  selectedResultId?: string | null;
  selectedTradeId?: string | null;
  initialTradeId?: string | null;
  onSelectTrade?: (tradeId: string) => void;
  onNavigateResults?: () => void;
};

function displayDecimal(
  value: string,
  formatter: Intl.NumberFormat
): string {
  if (!isDecimalString(value)) {
    return "—";
  }
  // ECMA-402 preserves validated decimal strings; the ES2022 type only accepts numbers.
  return formatter.format(value as unknown as number);
}

function displayFinancial(
  value: string,
  currency: string | undefined,
  production: boolean,
  formatter: Intl.NumberFormat
): string {
  if (!isDecimalString(value)) return "—";
  return production && currency
    ? `${value} ${currency}`
    : displayDecimal(value, formatter);
}

export function TradeChartView({
  demoMode,
  theme,
  snapshot,
  selectedResultId = null,
  selectedTradeId: controlledTradeId = null,
  initialTradeId,
  onSelectTrade,
  onNavigateResults
}: Props) {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "en" ? "en" : "zh-TW";
  const suppliedSnapshot = snapshot !== undefined;
  const useDemoSnapshot =
    import.meta.env.DEV === true && snapshot === undefined && demoMode;
  const productionMode = !suppliedSnapshot && !useDemoSnapshot;
  const productionRead = useBacktestTrades({
    selectedResultId,
    selectedTradeId: controlledTradeId,
    enabled: productionMode
  });
  const data =
    suppliedSnapshot
      ? snapshot
      : useDemoSnapshot
        ? demoTradeSnapshot
        : productionRead.state.status === "valid"
          ? productionRead.state.snapshot
          : null;
  const [localSelectedTradeId, setLocalSelectedTradeId] = useState<string | null>(
    initialTradeId ?? null
  );
  const selectedTradeId = productionMode ? controlledTradeId : localSelectedTradeId;
  const resultPage = productionMode && "tradePage" in productionRead.state
    ? productionRead.state.tradePage ?? null
    : null;
  const productionIdentity = productionMode && "identity" in productionRead.state
    ? productionRead.state.identity
    : null;
  const ledgerTrades = resultPage?.items ?? data?.trades ?? [];
  const totalTradeCount = resultPage?.totalCount ?? data?.trades.length ?? 0;
  const model = useMemo(
    () => (data ? buildTradeChartModel(data) : null),
    [data]
  );
  const copy = useMemo<TradeChartCopy>(
    () => ({
      price: t("trades.price"),
      entry: t("trades.entry"),
      exit: t("trades.exit"),
      longEntry: t("trades.longEntry"),
      longExit: t("trades.longExit"),
      shortEntry: t("trades.shortEntry"),
      shortExit: t("trades.shortExit")
    }),
    [t]
  );
  const option = useMemo(
    () => (model ? tradeChartOption(model, copy, locale, theme) : {}),
    [copy, locale, model, theme]
  );
  const selection = useMemo(
    () => (model ? selectedTradeOption(model, selectedTradeId) : {}),
    [model, selectedTradeId]
  );
  const selectedTrade =
    ledgerTrades.find((trade) => trade.id === selectedTradeId) ?? null;
  const number = useMemo(
    () =>
      new Intl.NumberFormat(locale, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 8
      }),
    [locale]
  );
  const date = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false,
        timeZone: PRESENTATION_TIME_ZONE,
        timeZoneName: "short"
      }),
    [locale]
  );
  const quality =
    model && (!productionMode || (data?.candles.length ?? 0) > 0) &&
    (model.skippedCandles > 0 || model.skippedMarkers > 0) ? (
      <p className="trade-quality" role="status">
        {t("trades.dataQuality", {
          candles: model.skippedCandles.toLocaleString(locale),
          markers: model.skippedMarkers.toLocaleString(locale)
        })}
      </p>
    ) : null;
  const selectTrade = (tradeId: string) => {
    if (!productionMode) setLocalSelectedTradeId(tradeId);
    onSelectTrade?.(tradeId);
  };

  const productionState = productionMode ? productionRead.state : null;
  const statusMessage = productionState === null
    ? null
    : productionState.status === "pending"
      ? t("trades.readLoading")
      : productionState.status === "selection"
        ? t("trades.selectPrompt")
        : productionState.status === "empty"
          ? t("trades.noTrades")
          : productionState.status === "permission-unavailable"
            ? t("trades.permissionBody")
            : productionState.status === "unavailable"
              ? t("trades.resultUnavailableBody")
              : productionState.status === "error"
                ? t("trades.readErrorBody")
                : productionState.status === "invalid-data"
                  ? t("trades.invalidDataBody")
      : productionState.status === "candle-window-unavailable"
                    ? t("trades.windowUnavailableBody")
                    : !model?.timestamps.length
                      ? t("trades.emptyBody")
                      : null;

  if (!data && !productionMode) {
    return (
      <section className="trade-console">
        <div className="trade-intro">
          <div>
            <p className="panel-kicker">{t("trades.kicker")}</p>
            <h2>{t("trades.title")}</h2>
            <p>{t("trades.body")}</p>
          </div>
        </div>
        <div className="empty-panel">
          <strong>{t("trades.unavailableTitle")}</strong>
          <p>{t("trades.unavailableBody")}</p>
        </div>
      </section>
    );
  }

  if (data && !productionMode && !model?.timestamps.length) {
    return (
      <section className="trade-console">
        {quality}
        <div className="empty-panel">
          <strong>{t("trades.emptyTitle")}</strong>
          <p>{t("trades.emptyBody")}</p>
        </div>
      </section>
    );
  }

  return (
    <section className="trade-console">
      <div className="trade-intro">
        <div>
          <p className="panel-kicker">{t("trades.kicker")}</p>
          <h2>{t("trades.title")}</h2>
          <p>{t("trades.body")}</p>
        </div>
        {(data || productionIdentity) && (
          <dl>
            <div>
              <dt>{t("trades.strategy")}</dt>
              <dd>{data?.strategyId ?? productionIdentity?.strategyId}</dd>
            </div>
            <div>
              <dt>{t("trades.instrument")}</dt>
              <dd>{data?.productId ?? productionIdentity?.productId}</dd>
            </div>
            <div>
              <dt>{productionMode ? t("trades.decisionTimeframe") : t("trades.timeframe")}</dt>
              <dd>{data?.timeframe ?? productionIdentity?.timeframe}</dd>
            </div>
            <div>
              <dt>{t("trades.tradeCount")}</dt>
              <dd>{totalTradeCount.toLocaleString(locale)}</dd>
            </div>
          </dl>
        )}
      </div>

      {quality}

      <div className="trade-layout">
        <article className="panel trade-chart-panel">
          <div className="panel-heading">
            <div>
              <p className="panel-kicker">{t("trades.chartKicker")}</p>
              <h2>{productionMode ? t("trades.sourceCandles") : t("trades.chartTitle")}</h2>
            </div>
            <span>{t("trades.chartHint")}</span>
          </div>
          {productionMode && productionState?.status !== "valid" ? (
            <div
              className="empty-panel"
              role={productionState?.status === "pending" || productionState?.status === "selection"
                ? "status"
                : "alert"}
            >
              <strong>{productionState?.status === "pending"
                ? t("trades.loading")
                : productionState?.status === "selection"
                  ? t("trades.selectPrompt")
                  : productionState?.status === "empty"
                    ? t("trades.noTrades")
                    : productionState?.status === "permission-unavailable"
                      ? t("trades.permissionTitle")
                      : productionState?.status === "unavailable"
                        ? t("trades.resultUnavailableTitle")
                        : productionState?.status === "error"
                          ? t("trades.readErrorTitle")
                          : productionState?.status === "invalid-data"
                            ? t("trades.invalidDataTitle")
                            : productionState?.status === "candle-window-unavailable"
                              ? t("trades.windowUnavailableTitle")
                              : t("trades.unavailableTitle")}</strong>
              {statusMessage && <p>{statusMessage}</p>}
              {onNavigateResults && productionState?.status !== "selection" && (
                <button type="button" onClick={onNavigateResults}>
                  {t("trades.backToResults")}
                </button>
              )}
            </div>
          ) : model?.timestamps.length ? (
            <Suspense
              fallback={
                <div className="chart-message" aria-live="polite">
                  {t("trades.loading")}
                </div>
              }
            >
              <CandlestickChart
                option={option}
                updateOption={selection}
                className="chart trade-chart"
                ariaLabel={t("trades.ariaChart")}
                onDataClick={(chartData) => {
                  const tradeId = tradeIdFromChartData(chartData);
                  if (tradeId !== null) {
                    selectTrade(tradeId);
                  }
                }}
              />
            </Suspense>
          ) : (
            <div className="empty-panel" role="status">
              <strong>{t("trades.emptyTitle")}</strong>
              <p>{statusMessage ?? t("trades.emptyBody")}</p>
            </div>
          )}
        </article>

        <aside className="panel trade-ledger">
          <div className="panel-heading">
            <div>
              <p className="panel-kicker">{t("trades.ledgerKicker")}</p>
              <h2>{t("trades.ledgerTitle")}</h2>
            </div>
            <span>{totalTradeCount.toLocaleString(locale)}</span>
          </div>
          {ledgerTrades.length ? (
            <ol>
              {ledgerTrades.map((trade) => (
                <li key={trade.id}>
                  <button
                    type="button"
                    className={
                      trade.id === selectedTradeId ? "is-selected" : undefined
                    }
                    aria-pressed={trade.id === selectedTradeId}
                    onClick={() => selectTrade(trade.id)}
                  >
                    <span className={`trade-side side-${trade.side.toLowerCase()}`}>
                      {trade.side}
                    </span>
                    <span>
                      <strong>{trade.id}</strong>
                      <small>
                        {formatPresentationTimestamp(trade.entryTime, date)}
                      </small>
                    </span>
                    <b
                      className={
                        !productionMode && (finiteDecimalNumber(trade.pnl) ?? 0) < 0
                          ? "is-loss"
                          : undefined
                      }
                    >
                      {displayFinancial(
                        trade.pnl,
                        productionIdentity?.currency,
                        productionMode,
                        number
                      )}
                    </b>
                  </button>
                </li>
              ))}
            </ol>
          ) : productionMode && productionState?.status !== "empty" ? (
            <div className="chart-message">
              {statusMessage}
            </div>
          ) : (
            <div className="chart-message">
              {t("trades.noTrades")}
            </div>
          )}
          {selectedTrade && (!productionMode || productionState?.status === "valid") ? (
            <div className="trade-detail" aria-live="polite">
              <div>
                <span>{t("trades.selected")}</span>
                <strong>{selectedTrade.id}</strong>
              </div>
              <dl>
                <div>
                  <dt>{t("trades.side")}</dt>
                  <dd>{selectedTrade.side}</dd>
                </div>
                <div>
                  <dt>{t("trades.quantity")}</dt>
                  <dd>{selectedTrade.quantity}</dd>
                </div>
                <div>
                  <dt>{t("trades.entry")}</dt>
                  <dd>{displayDecimal(selectedTrade.entryPrice, number)}</dd>
                </div>
                <div>
                  <dt>{t("trades.exit")}</dt>
                  <dd>{displayDecimal(selectedTrade.exitPrice, number)}</dd>
                </div>
                <div>
                  <dt>{t("trades.fee")}{productionMode ? ` · ${productionIdentity?.currency ?? ""}` : ""}</dt>
                  <dd>{displayFinancial(selectedTrade.fee, productionIdentity?.currency, productionMode, number)}</dd>
                </div>
                <div>
                  <dt>{t("trades.pnl")}{productionMode ? ` · ${productionIdentity?.currency ?? ""}` : ""}</dt>
                  <dd>{displayFinancial(selectedTrade.pnl, productionIdentity?.currency, productionMode, number)}</dd>
                </div>
              </dl>
            </div>
          ) : (!productionMode || productionState?.status === "selection") ? (
            <p className="trade-prompt">{t("trades.selectPrompt")}</p>
          ) : null}
          {productionMode && resultPage && resultPage.nextCursor !== null && (
            <div className="trade-page-controls">
              <span>
                {t("results.tradeProgress", {
                  loaded: resultPage.items.length.toLocaleString(locale),
                  total: resultPage.totalCount.toLocaleString(locale)
                })}
              </span>
              <button
                type="button"
                disabled={productionRead.tradePageLoading}
                onClick={() => void productionRead.loadMoreTrades()}
              >
                {productionRead.tradePageLoading
                  ? t("results.loadingMoreTrades")
                  : t("results.loadMoreTrades")}
              </button>
            </div>
          )}
        </aside>
      </div>
    </section>
  );
}
