import { lazy, Suspense, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { demoTradeSnapshot } from "./tradeDemo";
import { finiteDecimalNumber, isDecimalString } from "./decimal";
import {
  buildTradeChartModel,
  selectedTradeOption,
  tradeChartOption,
  tradeIdFromChartData,
  type TradeChartCopy,
  type TradeChartSnapshot
} from "./tradeChart";
import type { Locale } from "./i18n";
import type { Theme } from "./theme";
import { parseUtcTimestamp } from "./utc";

const CandlestickChart = lazy(() =>
  import("./CandlestickChart").then((module) => ({
    default: module.CandlestickChart
  }))
);

type Props = {
  demoMode: boolean;
  theme: Theme;
  snapshot?: TradeChartSnapshot | null;
  initialTradeId?: string | null;
  onSelectTrade?: (tradeId: string) => void;
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

function displayTimestamp(
  value: string,
  formatter: Intl.DateTimeFormat
): string {
  const timestamp = parseUtcTimestamp(value);
  return timestamp === null ? "—" : formatter.format(timestamp);
}

export function TradeChartView({
  demoMode,
  theme,
  snapshot,
  initialTradeId,
  onSelectTrade
}: Props) {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "en" ? "en" : "zh-TW";
  const data =
    snapshot === undefined ? (demoMode ? demoTradeSnapshot : null) : snapshot;
  const [selectedTradeId, setSelectedTradeId] = useState<string | null>(
    initialTradeId ?? null
  );
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
    data?.trades.find((trade) => trade.id === selectedTradeId) ?? null;
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
        timeZone: "UTC",
        timeZoneName: "short"
      }),
    [locale]
  );
  const quality =
    model && (model.skippedCandles > 0 || model.skippedMarkers > 0) ? (
      <p className="trade-quality" role="status">
        {t("trades.dataQuality", {
          candles: model.skippedCandles.toLocaleString(locale),
          markers: model.skippedMarkers.toLocaleString(locale)
        })}
      </p>
    ) : null;
  const selectTrade = (tradeId: string) => {
    setSelectedTradeId(tradeId);
    onSelectTrade?.(tradeId);
  };

  if (!data) {
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

  if (!model?.timestamps.length) {
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
        <dl>
          <div>
            <dt>{t("trades.strategy")}</dt>
            <dd>{data.strategyId}</dd>
          </div>
          <div>
            <dt>{t("trades.instrument")}</dt>
            <dd>{data.productId}</dd>
          </div>
          <div>
            <dt>{t("trades.timeframe")}</dt>
            <dd>{data.timeframe}</dd>
          </div>
          <div>
            <dt>{t("trades.tradeCount")}</dt>
            <dd>{data.trades.length.toLocaleString(locale)}</dd>
          </div>
        </dl>
      </div>

      {quality}

      <div className="trade-layout">
        <article className="panel trade-chart-panel">
          <div className="panel-heading">
            <div>
              <p className="panel-kicker">{t("trades.chartKicker")}</p>
              <h2>{t("trades.chartTitle")}</h2>
            </div>
            <span>{t("trades.chartHint")}</span>
          </div>
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
        </article>

        <aside className="panel trade-ledger">
          <div className="panel-heading">
            <div>
              <p className="panel-kicker">{t("trades.ledgerKicker")}</p>
              <h2>{t("trades.ledgerTitle")}</h2>
            </div>
            <span>{data.trades.length.toLocaleString(locale)}</span>
          </div>
          {data.trades.length ? (
            <ol>
              {data.trades.map((trade) => (
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
                      <small>{displayTimestamp(trade.entryTime, date)}</small>
                    </span>
                    <b
                      className={
                        (finiteDecimalNumber(trade.pnl) ?? 0) < 0
                          ? "is-loss"
                          : undefined
                      }
                    >
                      {displayDecimal(trade.pnl, number)}
                    </b>
                  </button>
                </li>
              ))}
            </ol>
          ) : (
            <div className="chart-message">
              {t("trades.noTrades")}
            </div>
          )}
          {selectedTrade ? (
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
                  <dt>{t("trades.fee")}</dt>
                  <dd>{displayDecimal(selectedTrade.fee, number)}</dd>
                </div>
                <div>
                  <dt>{t("trades.pnl")}</dt>
                  <dd>{displayDecimal(selectedTrade.pnl, number)}</dd>
                </div>
              </dl>
            </div>
          ) : (
            <p className="trade-prompt">{t("trades.selectPrompt")}</p>
          )}
        </aside>
      </div>
    </section>
  );
}
