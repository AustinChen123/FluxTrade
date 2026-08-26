import { useMemo, type CSSProperties } from "react";
import { useTranslation } from "react-i18next";

import { EChart } from "../../shared/charts/EChart";
import {
  finiteDecimalNumber,
  isDecimalString
} from "../../shared/format/decimal";
import type { Locale } from "../../shared/i18n";
import type { Theme } from "../../shared/theme";
import { parseUtcTimestamp } from "../../shared/time/utc";
import { demoBacktestSnapshot } from "./demo";
import { resultChartOption } from "./resultsCharts";
import {
  validDistributionBuckets,
  validateEquitySamples,
  type BacktestResultSnapshot,
  type DistributionBucket,
  type TradePage
} from "./resultsModel";
import { useTradePagination } from "./useTradePagination";

type Props = {
  demoMode: boolean;
  theme: Theme;
  snapshot?: BacktestResultSnapshot | null;
  loading?: boolean;
  loadError?: boolean;
  onInspectTrade?: (tradeId: string) => void;
  onLoadMoreTrades?: (cursor: string) => Promise<TradePage>;
};

function currencyFormatter(
  locale: Locale,
  currency: string
): Intl.NumberFormat | null {
  try {
    return new Intl.NumberFormat(locale, {
      style: "currency",
      currency,
      maximumFractionDigits: 2
    });
  } catch {
    return null;
  }
}

function displayTimestamp(
  value: string,
  formatter: Intl.DateTimeFormat
): string {
  const timestamp = parseUtcTimestamp(value);
  return timestamp === null ? "—" : formatter.format(timestamp);
}

function monthLabel(value: string, locale: Locale): string {
  const match = /^(\d{4})-(\d{2})$/.exec(value);
  const month = match ? Number(match[2]) : 0;
  if (!match || month < 1 || month > 12) {
    return "—";
  }
  return new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "short",
    timeZone: "UTC"
  }).format(new Date(Date.UTC(Number(match[1]), month - 1, 1)));
}

function monthlyReturnIntensity(value: number, maximum: number): number {
  if (value === 0 || maximum <= 0) {
    return 0;
  }
  return Math.round(8 + (Math.abs(value) / maximum) * 28);
}

export function BacktestResultsView({
  demoMode,
  theme,
  snapshot,
  loading = false,
  loadError = false,
  onInspectTrade,
  onLoadMoreTrades
}: Props) {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "en" ? "en" : "zh-TW";
  const data =
    snapshot === undefined ? (demoMode ? demoBacktestSnapshot : null) : snapshot;
  const {
    tradePage,
    tradePageLoading,
    tradePageError,
    tradePageValid,
    loadMoreTrades
  } = useTradePagination({
    jobId: data?.jobId ?? null,
    initialPage: data?.tradePage ?? null,
    onLoadMoreTrades
  });
  const equity = useMemo(
    () => validateEquitySamples(data?.equity ?? []),
    [data?.equity]
  );
  const money = useMemo(
    () => currencyFormatter(locale, data?.currency ?? "USD"),
    [data?.currency, locale]
  );
  const number = useMemo(
    () =>
      new Intl.NumberFormat(locale, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 4
      }),
    [locale]
  );
  const periodDate = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        timeZone: "UTC"
      }),
    [locale]
  );
  const tradeTimestamp = useMemo(
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
  const chartOption = useMemo(
    () =>
      data
        ? resultChartOption(
            { ...data, equity: equity ?? [] },
            locale,
            theme,
            t("results.equity"),
            t("results.drawdown"),
            money
          )
        : {},
    [data, equity, locale, money, t, theme]
  );

  const decimal = (
    value: string | null,
    formatter: Intl.NumberFormat | null = number
  ) => {
    if (!isDecimalString(value) || formatter === null) {
      return "—";
    }
    // ECMA-402 preserves validated decimal strings; the ES2022 type only accepts numbers.
    return formatter.format(value as unknown as number);
  };
  const percent = (value: string | null) => {
    const formatted = decimal(value);
    return formatted === "—" ? formatted : `${formatted}%`;
  };

  if (loading) {
    return (
      <section className="results-console" aria-live="polite">
        <div className="empty-panel">{t("results.loading")}</div>
      </section>
    );
  }

  if (loadError) {
    return (
      <section className="results-console">
        <div className="error-panel" role="alert">
          <div>
            <strong>{t("results.loadErrorTitle")}</strong>
            <p>{t("results.loadErrorBody")}</p>
          </div>
        </div>
      </section>
    );
  }

  if (!data) {
    return (
      <section className="results-console">
        <div className="results-intro">
          <div>
            <p className="panel-kicker">{t("results.kicker")}</p>
            <h2>{t("results.title")}</h2>
            <p>{t("results.body")}</p>
          </div>
        </div>
        <div className="empty-panel">
          <strong>{t("results.unavailableTitle")}</strong>
          <p>{t("results.unavailableBody")}</p>
        </div>
      </section>
    );
  }

  const distributionValid = validDistributionBuckets(data.pnlDistribution);
  const maxBucketCount = distributionValid
    ? Math.max(1, ...data.pnlDistribution.map((bucket) => bucket.count))
    : 1;
  const maxMonthlyReturn = data.monthlyReturns.reduce((maximum, month) => {
    const value = finiteDecimalNumber(month.returnPct);
    return value === null ? maximum : Math.max(maximum, Math.abs(value));
  }, 0);
  const displayedTradePage = tradePageValid ? tradePage : null;
  const tradeCount = displayedTradePage
    ? displayedTradePage.totalCount.toLocaleString(locale)
    : "—";
  const formatBucket = (bucket: DistributionBucket) => {
    if (bucket.lower === null) {
      return `< ${decimal(bucket.upper ?? "")}`;
    }
    if (bucket.upper === null) {
      return `≥ ${decimal(bucket.lower)}`;
    }
    return `${decimal(bucket.lower)} – ${decimal(bucket.upper)}`;
  };

  return (
    <section className="results-console" data-testid="backtest-results-view">
      <div className="results-intro">
        <div>
          <p className="panel-kicker">{t("results.kicker")}</p>
          <h2>{t("results.title")}</h2>
          <p>{t("results.body")}</p>
        </div>
        <dl>
          <div>
            <dt>{t("results.strategy")}</dt>
            <dd>{data.strategyId}</dd>
          </div>
          <div>
            <dt>{t("results.instrument")}</dt>
            <dd>{data.productId}</dd>
          </div>
          <div>
            <dt>{t("results.timeframe")}</dt>
            <dd>{data.timeframe}</dd>
          </div>
          <div>
            <dt>{t("results.period")}</dt>
            <dd>
              {displayTimestamp(data.startedAt, periodDate)} –{" "}
              {displayTimestamp(data.endedAt, periodDate)}
            </dd>
          </div>
        </dl>
      </div>

      <div className="results-metrics" aria-label={t("results.metricsAria")}>
        <div className="metric-primary">
          <span>{t("results.netPnl")}</span>
          <strong>{decimal(data.metrics.netPnl, money)}</strong>
          <small>{percent(data.metrics.returnPct)}</small>
        </div>
        <div>
          <span>{t("results.maxDrawdown")}</span>
          <strong>{decimal(data.metrics.maxDrawdown, money)}</strong>
        </div>
        <div>
          <span>{t("results.sharpe")}</span>
          <strong>{decimal(data.metrics.sharpe)}</strong>
        </div>
        <div>
          <span>{t("results.sortino")}</span>
          <strong>{decimal(data.metrics.sortino)}</strong>
        </div>
        <div>
          <span>{t("results.calmar")}</span>
          <strong>{decimal(data.metrics.calmar)}</strong>
        </div>
        <div>
          <span>{t("results.tradeCount")}</span>
          <strong>{tradeCount}</strong>
        </div>
      </div>

      <article className="panel results-risk-panel">
        <div className="panel-heading">
          <div>
            <p className="panel-kicker">{t("results.pathKicker")}</p>
            <h2>{t("results.pathTitle")}</h2>
          </div>
          <span>{t("results.pathHint")}</span>
        </div>
        {equity === null ? (
          <div className="chart-message">{t("results.invalidEquity")}</div>
        ) : equity.length ? (
          <EChart
            option={chartOption}
            className="chart results-risk-chart"
            ariaLabel={t("results.ariaChart")}
          />
        ) : (
          <div className="chart-message">{t("results.noEquity")}</div>
        )}
      </article>

      <div className="results-breakdown">
        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="panel-kicker">{t("results.monthlyKicker")}</p>
              <h2>{t("results.monthlyTitle")}</h2>
            </div>
          </div>
          {data.monthlyReturns.length ? (
            <div className="monthly-grid">
              {data.monthlyReturns.map((month) => {
                const value = finiteDecimalNumber(month.returnPct);
                const style =
                  value === null
                    ? undefined
                    : ({
                        "--return-intensity": `${monthlyReturnIntensity(
                          value,
                          maxMonthlyReturn
                        )}%`
                      } as CSSProperties);
                return (
                  <div
                    key={month.month}
                    style={style}
                    className={
                      value === null
                        ? undefined
                        : value < 0
                          ? "is-negative"
                          : "is-positive"
                    }
                  >
                    <span>{monthLabel(month.month, locale)}</span>
                    <strong>{percent(month.returnPct)}</strong>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="chart-message">{t("results.noMonthly")}</div>
          )}
        </article>

        <article className="panel">
          <div className="panel-heading">
            <div>
              <p className="panel-kicker">{t("results.distributionKicker")}</p>
              <h2>{t("results.distributionTitle")}</h2>
            </div>
            <span>{t("results.distributionUnit", { currency: data.currency })}</span>
          </div>
          {data.pnlDistribution.length && distributionValid ? (
            <ol className="distribution-list">
              {data.pnlDistribution.map((bucket) => (
                <li key={`${bucket.lower ?? "min"}-${bucket.upper ?? "max"}`}>
                  <span>{formatBucket(bucket)}</span>
                  <i
                    style={{ width: `${(bucket.count / maxBucketCount) * 100}%` }}
                  />
                  <strong>{bucket.count.toLocaleString(locale)}</strong>
                </li>
              ))}
            </ol>
          ) : data.pnlDistribution.length ? (
            <div className="chart-message">
              {t("results.invalidDistribution")}
            </div>
          ) : (
            <div className="chart-message">{t("results.noDistribution")}</div>
          )}
        </article>
      </div>

      <article className="panel results-trades">
        <div className="panel-heading">
          <div>
            <p className="panel-kicker">{t("results.tradesKicker")}</p>
            <h2>{t("results.tradesTitle")}</h2>
          </div>
          <span>{data.jobId}</span>
        </div>
        {displayedTradePage && displayedTradePage.items.length ? (
          <div className="results-table-wrap">
            <table>
              <thead>
                <tr>
                  <th>{t("results.tradeId")}</th>
                  <th>{t("trades.side")}</th>
                  <th>{t("trades.entry")}</th>
                  <th>{t("trades.exit")}</th>
                  <th>{t("trades.pnl")}</th>
                  <th>{t("trades.fee")}</th>
                  <th aria-label={t("results.action")} />
                </tr>
              </thead>
              <tbody>
                {displayedTradePage.items.map((trade) => (
                  <tr key={trade.id}>
                    <td>{trade.id}</td>
                    <td>
                      <span
                        className={`trade-side side-${trade.side.toLowerCase()}`}
                      >
                        {trade.side}
                      </span>
                    </td>
                    <td>{displayTimestamp(trade.entryTime, tradeTimestamp)}</td>
                    <td>{displayTimestamp(trade.exitTime, tradeTimestamp)}</td>
                    <td
                      className={
                        (finiteDecimalNumber(trade.pnl) ?? 0) < 0
                          ? "is-loss"
                          : undefined
                      }
                    >
                      {decimal(trade.pnl, money)}
                    </td>
                    <td>{decimal(trade.fee, money)}</td>
                    <td>
                      <button
                        type="button"
                        onClick={() => onInspectTrade?.(trade.id)}
                      >
                        {t("results.inspect")}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : displayedTradePage ? (
          <div className="chart-message">{t("results.noTrades")}</div>
        ) : (
          <div className="chart-message">{t("results.invalidTradePage")}</div>
        )}
        {displayedTradePage && (
          <div className="trade-page-controls">
            <span>
              {t("results.tradeProgress", {
                loaded: displayedTradePage.items.length.toLocaleString(locale),
                total: displayedTradePage.totalCount.toLocaleString(locale)
              })}
            </span>
            {displayedTradePage.nextCursor !== null &&
              (onLoadMoreTrades ? (
                <button
                  type="button"
                  disabled={tradePageLoading}
                  onClick={() => void loadMoreTrades()}
                >
                  {tradePageLoading
                    ? t("results.loadingMoreTrades")
                    : tradePageError
                      ? t("results.retryTrades")
                      : t("results.loadMoreTrades")}
                </button>
              ) : (
                <span role="alert">{t("results.paginationUnavailable")}</span>
              ))}
          </div>
        )}
      </article>
    </section>
  );
}
