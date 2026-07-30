import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties
} from "react";
import { useTranslation } from "react-i18next";
import type { EChartsCoreOption } from "echarts/core";

import { EChart } from "./EChart";
import { demoBacktestSnapshot } from "./backtestDemo";
import { finiteDecimalNumber, isDecimalString } from "./decimal";
import type { Locale } from "./i18n";
import type { Theme } from "./theme";
import type { ClosedTrade } from "./tradeChart";
import { parseUtcTimestamp } from "./utc";

type EquitySample = {
  timestamp: string;
  equity: string;
  drawdown: string;
};

type MonthlyReturn = {
  month: string;
  returnPct: string;
};

type DistributionBucket = {
  lower: string | null;
  upper: string | null;
  count: number;
};

export type BacktestResultSnapshot = {
  jobId: string;
  strategyId: string;
  productId: string;
  timeframe: string;
  startedAt: string;
  endedAt: string;
  currency: string;
  metrics: {
    netPnl: string | null;
    returnPct: string | null;
    maxDrawdown: string | null;
    sharpe: string | null;
    sortino: string | null;
    calmar: string | null;
  };
  equity: EquitySample[];
  monthlyReturns: MonthlyReturn[];
  pnlDistribution: DistributionBucket[];
  tradePage: TradePage;
};

export type TradePage = {
  items: ClosedTrade[];
  totalCount: number;
  nextCursor: string | null;
};

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

function resultChartOption(
  snapshot: BacktestResultSnapshot,
  locale: Locale,
  theme: Theme,
  equityLabel: string,
  drawdownLabel: string,
  money: Intl.NumberFormat | null
): EChartsCoreOption {
  const dark = theme === "dark";
  const ink = dark ? "#e5ece9" : "#182128";
  const muted = dark ? "#94a29d" : "#66726f";
  const grid = dark ? "#3b4949" : "#cbd5d1";
  const axisTimestamp = new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC"
  });
  const tooltipTimestamp = new Intl.DateTimeFormat(locale, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    timeZone: "UTC",
    timeZoneName: "short"
  });
  const axisNumber = new Intl.NumberFormat(locale, {
    maximumFractionDigits: 2,
    useGrouping: true
  });
  const timestamps = snapshot.equity.map((sample) => sample.timestamp);

  return {
    animation: false,
    textStyle: { color: ink },
    tooltip: {
      trigger: "axis",
      backgroundColor: dark ? "#182226" : "#f8faf9",
      borderColor: grid,
      textStyle: { color: ink },
      axisPointer: {
        label: {
          formatter: ({ value }: { value: unknown }) => {
            return displayTimestamp(String(value), tooltipTimestamp);
          }
        }
      },
      valueFormatter: (value: unknown) => {
        const parsed = typeof value === "number" ? value : Number(value);
        return Number.isFinite(parsed) && money ? money.format(parsed) : "—";
      }
    },
    legend: {
      top: 4,
      right: 12,
      textStyle: { color: muted },
      data: [equityLabel, drawdownLabel]
    },
    grid: [
      { left: 76, right: 24, top: 42, height: "52%" },
      { left: 76, right: 24, top: "72%", height: "17%" }
    ],
    xAxis: [
      {
        type: "category",
        data: timestamps,
        boundaryGap: false,
        axisLabel: { show: false },
        axisLine: { lineStyle: { color: grid } },
        splitLine: { show: false }
      },
      {
        type: "category",
        gridIndex: 1,
        data: timestamps,
        boundaryGap: false,
        axisLabel: {
          color: muted,
          hideOverlap: true,
          formatter: (value: string) => displayTimestamp(value, axisTimestamp)
        },
        axisLine: { lineStyle: { color: grid } }
      }
    ],
    yAxis: [
      {
        type: "value",
        scale: true,
        axisLabel: {
          color: muted,
          formatter: (value: number) => axisNumber.format(value)
        },
        splitLine: { lineStyle: { color: grid } }
      },
      {
        type: "value",
        gridIndex: 1,
        inverse: true,
        min: 0,
        axisLabel: {
          color: muted,
          formatter: (value: number) => money?.format(value) ?? "—"
        },
        splitLine: { lineStyle: { color: grid } }
      }
    ],
    series: [
      {
        id: "equity",
        name: equityLabel,
        type: "line",
        data: snapshot.equity.map((sample) =>
          finiteDecimalNumber(sample.equity)
        ),
        symbol: "none",
        lineStyle: { color: dark ? "#49b2ae" : "#0e6b6f", width: 2 },
        itemStyle: { color: dark ? "#49b2ae" : "#0e6b6f" },
        areaStyle: { color: dark ? "rgba(73,178,174,.12)" : "rgba(14,107,111,.09)" }
      },
      {
        id: "drawdown",
        name: drawdownLabel,
        type: "line",
        xAxisIndex: 1,
        yAxisIndex: 1,
        data: snapshot.equity.map((sample) =>
          finiteDecimalNumber(sample.drawdown)
        ),
        symbol: "none",
        lineStyle: { color: dark ? "#ef8a6b" : "#d46a4c", width: 1.5 },
        itemStyle: { color: dark ? "#ef8a6b" : "#d46a4c" },
        areaStyle: { color: dark ? "rgba(239,138,107,.26)" : "rgba(212,106,76,.2)" }
      }
    ]
  };
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

function validTradePage(page: TradePage): boolean {
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

function validLoadedTradePage(page: TradePage): boolean {
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

function mergeTradeItems(
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
  const [tradePage, setTradePage] = useState<TradePage | null>(
    data?.tradePage ?? null
  );
  const [tradePageLoading, setTradePageLoading] = useState(false);
  const [tradePageError, setTradePageError] = useState(false);
  const tradeRequestGeneration = useRef(0);
  const tradeRequestInFlight = useRef(false);
  useEffect(() => {
    tradeRequestGeneration.current += 1;
    tradeRequestInFlight.current = false;
    setTradePage(data?.tradePage ?? null);
    setTradePageLoading(false);
    setTradePageError(false);
    return () => {
      tradeRequestGeneration.current += 1;
      tradeRequestInFlight.current = false;
    };
  }, [data?.jobId, data?.tradePage]);
  const equity = useMemo(
    () => {
      const samples = data?.equity ?? [];
      let previousTimestamp = Number.NEGATIVE_INFINITY;
      for (const sample of samples) {
        const timestamp = parseUtcTimestamp(sample.timestamp);
        if (
          timestamp === null ||
          timestamp <= previousTimestamp ||
          finiteDecimalNumber(sample.equity) === null ||
          finiteDecimalNumber(sample.drawdown) === null
        ) {
          return null;
        }
        previousTimestamp = timestamp;
      }
      return samples;
    },
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

  const distributionValid = data.pnlDistribution.every(
    (bucket) => Number.isSafeInteger(bucket.count) && bucket.count >= 0
  );
  const maxBucketCount = distributionValid
    ? Math.max(1, ...data.pnlDistribution.map((bucket) => bucket.count))
    : 1;
  const maxMonthlyReturn = data.monthlyReturns.reduce((maximum, month) => {
    const value = finiteDecimalNumber(month.returnPct);
    return value === null ? maximum : Math.max(maximum, Math.abs(value));
  }, 0);
  const tradePageValid = tradePage !== null && validLoadedTradePage(tradePage);
  const tradeCount = tradePageValid
    ? tradePage.totalCount.toLocaleString(locale)
    : "—";
  const loadMoreTrades = async () => {
    if (
      !tradePageValid ||
      tradePage.nextCursor === null ||
      !onLoadMoreTrades ||
      tradeRequestInFlight.current
    ) {
      return;
    }
    tradeRequestInFlight.current = true;
    const requestedCursor = tradePage.nextCursor;
    const requestGeneration = tradeRequestGeneration.current;
    setTradePageLoading(true);
    setTradePageError(false);
    try {
      const nextPage = await onLoadMoreTrades(requestedCursor);
      if (requestGeneration !== tradeRequestGeneration.current) {
        return;
      }
      const items = validTradePage(nextPage)
        ? mergeTradeItems(tradePage.items, nextPage.items)
        : null;
      const cursorAdvanced =
        nextPage.nextCursor === null ||
        nextPage.nextCursor !== requestedCursor;
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
      if (requestGeneration === tradeRequestGeneration.current) {
        setTradePageError(true);
      }
    } finally {
      if (requestGeneration === tradeRequestGeneration.current) {
        tradeRequestInFlight.current = false;
        setTradePageLoading(false);
      }
    }
  };
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
        {tradePageValid && tradePage.items.length ? (
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
                {tradePage.items.map((trade) => (
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
        ) : tradePageValid ? (
          <div className="chart-message">{t("results.noTrades")}</div>
        ) : (
          <div className="chart-message">{t("results.invalidTradePage")}</div>
        )}
        {tradePageValid && (
          <div className="trade-page-controls">
            <span>
              {t("results.tradeProgress", {
                loaded: tradePage.items.length.toLocaleString(locale),
                total: tradePage.totalCount.toLocaleString(locale)
              })}
            </span>
            {tradePage.nextCursor !== null &&
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
