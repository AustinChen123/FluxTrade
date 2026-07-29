import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  ApiError,
  ensureBrowserSession,
  loadEpochs,
  loadGenerationGenes,
  loadGenerationSummaries,
  type Epoch,
  type Gene,
  type GenerationSummary
} from "./api";
import { buildDemoGenes, demoEpoch, demoSummaries } from "./demo";
import {
  compareGenes,
  convergenceOption,
  epochObjective,
  finiteNumber,
  fitnessObservationRows,
  fitnessSurfaceOption,
  fitnessSurfaceRows,
  parallelOption,
  parameterDimensions,
  selectedSurfaceOption,
  selectBestGene,
  surfaceRow,
  type ChartCopy
} from "./ga";
import type { Locale } from "./i18n";
import {
  applyTheme,
  initialTheme,
  saveTheme,
  type Theme
} from "./theme";

const EChart = lazy(() =>
  import("./EChart").then((module) => ({ default: module.EChart }))
);
const FitnessSurface3D = lazy(() =>
  import("./FitnessSurface3D").then((module) => ({
    default: module.FitnessSurface3D
  }))
);
const StrategyManager = lazy(() =>
  import("./StrategyManager").then((module) => ({
    default: module.StrategyManager
  }))
);
const TradeChartView = lazy(() =>
  import("./TradeChartView").then((module) => ({
    default: module.TradeChartView
  }))
);

type View = "research" | "strategies" | "trades";

function initialView(): View {
  const requested = new URLSearchParams(window.location.search).get("view");
  return requested === "strategies" || requested === "trades"
    ? requested
    : "research";
}

function displayNumber(
  value: string | number | null,
  locale: string,
  digits = 4
): string {
  const parsed = finiteNumber(value);
  return parsed === null
    ? "—"
    : new Intl.NumberFormat(locale, {
        maximumFractionDigits: digits
      }).format(parsed);
}

function displayValue(value: unknown, locale: string): string {
  const parsed = finiteNumber(value);
  return parsed === null
    ? String(value)
    : new Intl.NumberFormat(locale, {
        maximumFractionDigits: 8
      }).format(parsed);
}

function geneIdFromChartData(data: unknown): number | null {
  if (Array.isArray(data)) {
    return typeof data[3] === "number" ? data[3] : null;
  }
  if (data && typeof data === "object" && "geneId" in data) {
    const value = (data as { geneId?: unknown }).geneId;
    return typeof value === "number" ? value : null;
  }
  return null;
}

type Translate = ReturnType<typeof useTranslation>["t"];

function errorMessage(error: unknown, t: Translate): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return t("error.unauthorized");
    }
    return t("error.service", {
      status: error.status,
      message: error.message
    });
  }
  return error instanceof Error
    ? t("error.unexpected", { message: error.message })
    : t("error.fallback");
}

export function App() {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "en" ? "en" : "zh-TW";
  const demoMode =
    import.meta.env.DEV &&
    new URLSearchParams(window.location.search).get("demo") === "1";
  const [view, setView] = useState<View>(initialView);
  const [researchActivated, setResearchActivated] = useState(
    view === "research"
  );
  const [theme, setTheme] = useState<Theme>(initialTheme);
  const [epochs, setEpochs] = useState<Epoch[]>([]);
  const [epochId, setEpochId] = useState("");
  const [summaries, setSummaries] = useState<GenerationSummary[]>([]);
  const [generationIndex, setGenerationIndex] = useState<number | null>(null);
  const [genes, setGenes] = useState<Gene[]>([]);
  const [selectedGeneId, setSelectedGeneId] = useState<number | null>(null);
  const [xParameter, setXParameter] = useState("");
  const [yParameter, setYParameter] = useState("");
  const [surfaceMode, setSurfaceMode] = useState<"2d" | "3d">("2d");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown | null>(null);
  const [epochsLoaded, setEpochsLoaded] = useState(false);
  const [reloadToken, setReloadToken] = useState(0);

  const epoch = epochs.find((item) => item.id === epochId) ?? null;

  useEffect(() => {
    applyTheme(theme);
  }, [theme]);

  useEffect(() => {
    if (!researchActivated) {
      return;
    }
    if (epochsLoaded) {
      return;
    }
    let active = true;
    setLoading(true);
    setError(null);
    const load = async () => {
      if (demoMode) {
        return [demoEpoch];
      }
      await ensureBrowserSession();
      return loadEpochs();
    };
    void load()
      .then((items) => {
        if (!active) {
          return;
        }
        setEpochs(items);
        setEpochsLoaded(true);
        setEpochId((current) =>
          items.some((item) => item.id === current) ? current : (items[0]?.id ?? "")
        );
      })
      .catch((reason) => active && setError(reason))
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epochsLoaded, reloadToken, researchActivated]);

  useEffect(() => {
    if (!epoch) {
      setSummaries([]);
      setGenerationIndex(null);
      return;
    }
    let active = true;
    setSummaries([]);
    setGenerationIndex(null);
    setGenes([]);
    setSelectedGeneId(null);
    setLoading(true);
    setError(null);
    const load = demoMode
      ? Promise.resolve(demoSummaries)
      : loadGenerationSummaries(epoch.id);
    void load
      .then((items) => {
        if (!active) {
          return;
        }
        setSummaries(items);
        setGenerationIndex(items.at(-1)?.generation_index ?? null);
      })
      .catch((reason) => active && setError(reason))
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epoch, reloadToken]);

  useEffect(() => {
    if (!epoch || generationIndex === null) {
      setGenes([]);
      return;
    }
    let active = true;
    setLoading(true);
    setError(null);
    const load =
      demoMode && epoch.id === demoEpoch.id
        ? Promise.resolve(buildDemoGenes(generationIndex))
        : loadGenerationGenes(epoch.id, generationIndex);
    void load
      .then((items) => {
        if (!active) {
          return;
        }
        setGenes(items);
        setSelectedGeneId(selectBestGene(epoch, items)?.id ?? null);
      })
      .catch((reason) => active && setError(reason))
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epoch, generationIndex]);

  const dimensions = useMemo(() => parameterDimensions(genes), [genes]);
  const numericParameters = useMemo(
    () =>
      dimensions
        .filter((dimension) => dimension.type === "value")
        .map((dimension) => dimension.name),
    [dimensions]
  );

  useEffect(() => {
    const nextX = numericParameters.includes(xParameter)
      ? xParameter
      : (numericParameters[0] ?? "");
    const nextY =
      numericParameters.includes(yParameter) && yParameter !== nextX
        ? yParameter
        : (numericParameters.find((name) => name !== nextX) ?? "");
    setXParameter(nextX);
    setYParameter(nextY);
  }, [numericParameters]);

  const selectedGene =
    genes.find((gene) => gene.id === selectedGeneId) ?? null;
  const objective = epoch ? epochObjective(epoch) : null;
  const rankedGenes = useMemo(() => {
    if (!epoch || objective === null) {
      return [];
    }
    return [...genes]
      .sort((left, right) => compareGenes(epoch, left, right))
      .slice(0, 8);
  }, [epoch, genes, objective]);

  const chartCopy = useMemo<ChartCopy>(
    () => ({
      locale,
      generation: t("chart.generation"),
      drawdown: t("chart.drawdown"),
      score: t("chart.score"),
      fitness: t("chart.fitness"),
      lowerDrawdown: t("chart.lowerDrawdown"),
      upperDrawdown: t("chart.upperDrawdown"),
      lowerScore: t("chart.lowerScore"),
      upperScore: t("chart.upperScore"),
      high: t("chart.high"),
      low: t("chart.low"),
      selected: t("chart.selected"),
      bestObserved: t("chart.bestObserved")
    }),
    [locale, t]
  );
  const convergence = useMemo(
    () =>
      epoch && objective
        ? convergenceOption(epoch, summaries, chartCopy, theme)
        : {},
    [chartCopy, epoch, objective, summaries, theme]
  );
  const surfaceRows = useMemo(
    () =>
      epoch && xParameter && yParameter
        ? fitnessSurfaceRows(epoch, genes, xParameter, yParameter)
        : [],
    [epoch, genes, xParameter, yParameter]
  );
  const observationRows = useMemo(
    () =>
      epoch && xParameter && yParameter
        ? fitnessObservationRows(epoch, genes, xParameter, yParameter)
        : [],
    [epoch, genes, xParameter, yParameter]
  );
  const selectedSurfaceRow = useMemo(
    () =>
      epoch
        ? surfaceRow(epoch, selectedGene, xParameter, yParameter)
        : null,
    [epoch, selectedGene, xParameter, yParameter]
  );
  const surface = useMemo(
    () =>
      epoch && xParameter && yParameter
        ? fitnessSurfaceOption(
            epoch,
            surfaceRows,
            xParameter,
            yParameter,
            chartCopy,
            theme
          )
        : {},
    [chartCopy, epoch, surfaceRows, theme, xParameter, yParameter]
  );
  const surfaceSelection = useMemo(
    () => selectedSurfaceOption(selectedSurfaceRow),
    [selectedSurfaceRow]
  );
  const parallel = useMemo(
    () => parallelOption(genes, dimensions, selectedGeneId, theme, locale),
    [dimensions, genes, locale, selectedGeneId, theme]
  );

  const objectiveText =
    objective === "minimize_drawdown"
      ? t("objective.minimizeDrawdown")
      : objective === "maximize_return"
        ? t("objective.maximizeReturn")
        : objective === "maximize_score"
          ? t("objective.maximizeScore")
          : t("objective.unsupported");

  const chooseGeneFromChart = (data: unknown) => {
    const geneId = geneIdFromChartData(data);
    if (geneId !== null) {
      setSelectedGeneId(geneId);
    }
  };

  const chooseEpoch = (nextEpochId: string) => {
    setSummaries([]);
    setGenerationIndex(null);
    setGenes([]);
    setSelectedGeneId(null);
    setEpochId(nextEpochId);
  };
  const chooseView = (nextView: View) => {
    if (nextView === "research") {
      setResearchActivated(true);
    }
    const url = new URL(window.location.href);
    if (nextView === "research") {
      url.searchParams.delete("view");
    } else {
      url.searchParams.set("view", nextView);
    }
    window.history.replaceState(
      null,
      "",
      `${url.pathname}${url.search}${url.hash}`
    );
    setView(nextView);
  };
  const dateFormatter = new Intl.DateTimeFormat(locale, {
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });

  return (
    <main className="console-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">{t("app.eyebrow")}</p>
          <h1>
            {t(
              view === "research"
                ? "app.title"
                : view === "strategies"
                  ? "strategies.title"
                  : "trades.title"
            )}
          </h1>
        </div>
        <div className="toolbar">
          {view === "research" && (
            <div className="epoch-control">
              <label htmlFor="epoch">{t("controls.epoch")}</label>
              <select
                id="epoch"
                value={epochId}
                onChange={(event) => chooseEpoch(event.target.value)}
                disabled={!epochs.length}
              >
                {epochs.map((item) => (
                  <option key={item.id} value={item.id}>
                    {item.strategy_id} ·{" "}
                    {dateFormatter.format(new Date(item.started_at))}
                  </option>
                ))}
              </select>
            </div>
          )}
          <label className="language-control" htmlFor="language">
            {t("controls.language")}
            <select
              id="language"
              value={locale}
              onChange={(event) =>
                void i18n.changeLanguage(event.target.value as Locale)
              }
            >
              <option value="zh-TW">繁體中文</option>
              <option value="en">English</option>
            </select>
          </label>
          <button
            type="button"
            className="theme-control"
            aria-label={t(
              theme === "dark" ? "controls.light" : "controls.dark"
            )}
            title={t(theme === "dark" ? "controls.light" : "controls.dark")}
            onClick={() => {
              const next = theme === "dark" ? "light" : "dark";
              applyTheme(next);
              saveTheme(next);
              setTheme(next);
            }}
          >
            <span aria-hidden="true">{theme === "dark" ? "☀" : "☾"}</span>
            <small>{t("controls.theme")}</small>
          </button>
        </div>
      </header>

      <nav className="console-nav" aria-label={t("navigation.aria")}>
        <button
          type="button"
          aria-current={view === "research" ? "page" : undefined}
          onClick={() => chooseView("research")}
        >
          {t("navigation.research")}
        </button>
        <button
          type="button"
          aria-current={view === "strategies" ? "page" : undefined}
          onClick={() => chooseView("strategies")}
        >
          {t("navigation.strategies")}
        </button>
        <button
          type="button"
          aria-current={view === "trades" ? "page" : undefined}
          onClick={() => chooseView("trades")}
        >
          {t("navigation.trades")}
        </button>
      </nav>

      {view === "strategies" && (
        <Suspense
          fallback={
            <div className="loading-indicator" aria-live="polite">
              <span />
              {t("strategies.loading")}
            </div>
          }
        >
          <StrategyManager />
        </Suspense>
      )}

      {(view === "research" || view === "trades") && demoMode && (
        <p className="demo-notice">{t("demo")}</p>
      )}

      {view === "trades" && (
        <Suspense
          fallback={
            <div className="loading-indicator" aria-live="polite">
              <span />
              {t("trades.loading")}
            </div>
          }
        >
          <TradeChartView demoMode={demoMode} theme={theme} />
        </Suspense>
      )}

      {view === "research" && error !== null && (
        <section className="error-panel" role="alert">
          <div>
            <strong>{t("error.title")}</strong>
            <p>{errorMessage(error, t)}</p>
          </div>
          <button type="button" onClick={() => setReloadToken((value) => value + 1)}>
            {t("error.retry")}
          </button>
        </section>
      )}

      {view === "research" && error === null && !loading && !epoch && (
        <section className="empty-panel">
          <strong>{t("empty.title")}</strong>
          <p>{t("empty.body")}</p>
        </section>
      )}

      {view === "research" && epoch && (
        <Suspense
          fallback={
            <div className="loading-indicator" aria-live="polite">
              <span />
              {t("loading.charts")}
            </div>
          }
        >
          <section className="run-strip" aria-label={t("summary.aria")}>
            <div className="run-identity">
              <span className={`status-mark status-${epoch.status}`} />
              <div>
                <strong>{epoch.strategy_id}</strong>
                <span>{epoch.eval_pair} · {epoch.eval_timeframe}</span>
              </div>
            </div>
            <dl>
              <div>
                <dt>{t("summary.objective")}</dt>
                <dd>{objectiveText}</dd>
              </div>
              <div>
                <dt>{t("summary.generation")}</dt>
                <dd>
                  {(epoch.generations_run ?? 0).toLocaleString(locale)}/
                  {epoch.max_generations.toLocaleString(locale)}
                </dd>
              </div>
              <div>
                <dt>{t("summary.population")}</dt>
                <dd>{epoch.pop_size.toLocaleString(locale)}</dd>
              </div>
              <div>
                <dt>{t("summary.bestScore")}</dt>
                <dd>{displayNumber(epoch.best_score, locale)}</dd>
              </div>
            </dl>
          </section>

          <section className="analysis-grid">
            <article className="panel convergence-panel">
              <div className="panel-heading">
                <div>
                  <p className="panel-kicker">{t("evolution.kicker")}</p>
                  <h2>{t("evolution.title")}</h2>
                </div>
                <label>
                  {t("evolution.generation")}
                  <select
                    value={generationIndex ?? ""}
                    onChange={(event) =>
                      setGenerationIndex(Number(event.target.value))
                    }
                  >
                    {summaries.map((item) => (
                      <option
                        key={item.generation_index}
                        value={item.generation_index}
                      >
                        {item.generation_index.toLocaleString(locale)}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <EChart
                option={convergence}
                className="chart chart-convergence"
                ariaLabel={t("aria.convergence")}
              />
            </article>

            <article className="panel topology-panel">
              <div className="panel-heading">
                <div>
                  <p className="panel-kicker">{t("surface.kicker")}</p>
                  <h2>{t("surface.title")}</h2>
                </div>
                <div className="surface-controls">
                  <div
                    className="view-control"
                    role="group"
                    aria-label={t("surface.view")}
                  >
                    <button
                      type="button"
                      aria-pressed={surfaceMode === "2d"}
                      onClick={() => setSurfaceMode("2d")}
                    >
                      {t("surface.view2d")}
                    </button>
                    <button
                      type="button"
                      aria-pressed={surfaceMode === "3d"}
                      onClick={() => setSurfaceMode("3d")}
                    >
                      {t("surface.view3d")}
                    </button>
                  </div>
                  <div className="axis-controls">
                    <label>
                      X
                      <select
                        value={xParameter}
                        onChange={(event) => setXParameter(event.target.value)}
                      >
                        {numericParameters.map((name) => (
                          <option key={name} disabled={name === yParameter}>
                            {name}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Y
                      <select
                        value={yParameter}
                        onChange={(event) => setYParameter(event.target.value)}
                      >
                        {numericParameters.map((name) => (
                          <option key={name} disabled={name === xParameter}>
                            {name}
                          </option>
                        ))}
                      </select>
                    </label>
                  </div>
                </div>
              </div>
              {objective === null ? (
                <div className="chart-message">
                  {t("surface.unsupportedObjective")}
                </div>
              ) : numericParameters.length >= 2 ? (
                surfaceMode === "2d" ? (
                  <EChart
                    option={surface}
                    updateOption={surfaceSelection}
                    className="chart chart-topology"
                    ariaLabel={t("aria.surface")}
                    onDataClick={chooseGeneFromChart}
                  />
                ) : (
                  <Suspense
                    fallback={
                      <div className="chart-message">{t("loading.charts")}</div>
                    }
                  >
                    <FitnessSurface3D
                      surfaceRows={surfaceRows}
                      observations={observationRows}
                      selected={selectedSurfaceRow}
                      xParameter={xParameter}
                      yParameter={yParameter}
                      metricLabel={
                        objective === "minimize_drawdown"
                          ? chartCopy.drawdown
                          : chartCopy.fitness
                      }
                      observationLabel={t("chart.observed")}
                      hint={t("surface.interpolated")}
                      locale={locale}
                      theme={theme}
                      ariaLabel={t("aria.surface3d")}
                      onDataClick={chooseGeneFromChart}
                    />
                  </Suspense>
                )
              ) : (
                <div className="chart-message">
                  {t("surface.unavailable")}
                </div>
              )}
            </article>

            <aside className="panel selection-panel">
              <div className="panel-heading">
                <div>
                  <p className="panel-kicker">{t("candidates.kicker")}</p>
                  <h2>{t("candidates.title")}</h2>
                </div>
                <span>
                  {t("candidates.count", {
                    count: genes.length,
                    formattedCount: genes.length.toLocaleString(locale)
                  })}
                </span>
              </div>
              <ol className="ranking-list">
                {rankedGenes.map((gene, index) => (
                  <li key={gene.id}>
                    <button
                      type="button"
                      className={gene.id === selectedGeneId ? "is-selected" : ""}
                      onClick={() => setSelectedGeneId(gene.id)}
                    >
                      <span className="rank">{String(index + 1).padStart(2, "0")}</span>
                      <span>
                        <strong>{gene.candidate_id}</strong>
                        <small>
                          {t("candidates.drawdown")}{" "}
                          {displayNumber(gene.max_drawdown, locale, 3)}
                        </small>
                      </span>
                      <b>{displayNumber(gene.score_total, locale, 3)}</b>
                    </button>
                  </li>
                ))}
              </ol>
              {selectedGene && (
                <div className="selection-detail">
                  <div>
                    <span>{t("candidates.selected")}</span>
                    <strong>{selectedGene.candidate_id}</strong>
                  </div>
                  <dl>
                    {Object.entries(selectedGene.param_pack).map(([name, value]) => (
                      <div key={name}>
                        <dt>{name}</dt>
                        <dd>{displayValue(value, locale)}</dd>
                      </div>
                    ))}
                  </dl>
                  {Object.keys(selectedGene.score_breakdown).length > 0 && (
                    <>
                      <p>{t("candidates.metrics")}</p>
                      <dl>
                        {Object.entries(selectedGene.score_breakdown).map(
                          ([name, value]) => (
                            <div key={name}>
                              <dt>{name}</dt>
                              <dd>{displayValue(value, locale)}</dd>
                            </div>
                          )
                        )}
                      </dl>
                    </>
                  )}
                </div>
              )}
            </aside>

            <article className="panel parallel-panel">
              <div className="panel-heading">
                <div>
                  <p className="panel-kicker">{t("parallel.kicker")}</p>
                  <h2>{t("parallel.title")}</h2>
                </div>
                <span>{t("parallel.axes")}</span>
              </div>
              {dimensions.length ? (
                <EChart
                  option={parallel}
                  className="chart chart-parallel"
                  ariaLabel={t("aria.parallel")}
                  onDataClick={chooseGeneFromChart}
                />
              ) : (
                <div className="chart-message">{t("parallel.unavailable")}</div>
              )}
            </article>
          </section>
        </Suspense>
      )}

      {view === "research" && loading && (
        <div className="loading-indicator" aria-live="polite">
          <span />
          {t("loading.snapshot")}
        </div>
      )}
    </main>
  );
}
