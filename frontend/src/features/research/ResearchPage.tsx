import { lazy, Suspense } from "react";
import type { EChartsCoreOption } from "echarts/core";
import { useTranslation } from "react-i18next";

import { ApiError } from "../../api";
import type { Theme } from "../../shared/theme";
import type { ChartCopy } from "./gaCharts";
import type { SurfaceRow } from "./gaDomain";
import {
  displayNumber,
  displayValue,
  geneIdFromChartData
} from "./researchModel";
import type { ResearchWorkspace } from "./useResearchWorkspace";

const EChart = lazy(() =>
  import("../../shared/charts/EChart").then((module) => ({
    default: module.EChart
  }))
);
const FitnessSurface3D = lazy(() =>
  import("./FitnessSurface3D").then((module) => ({
    default: module.FitnessSurface3D
  }))
);

export type ResearchPageProps = {
  readonly workspace: ResearchWorkspace;
  readonly locale: string;
  readonly theme: Theme;
  readonly chartCopy: ChartCopy;
  readonly convergence: EChartsCoreOption;
  readonly surfaceRows: SurfaceRow[];
  readonly observationRows: SurfaceRow[];
  readonly selectedSurfaceRow: SurfaceRow | null;
  readonly surface: EChartsCoreOption;
  readonly surfaceSelection: EChartsCoreOption;
  readonly parallel: EChartsCoreOption;
};

type Translate = ReturnType<typeof useTranslation>["t"];

function errorMessage(reason: unknown, translate: Translate): string {
  if (reason instanceof ApiError) {
    if (reason.status === 401 || reason.status === 403) {
      return translate("error.unauthorized");
    }
    return translate("error.service", {
      status: reason.status,
      message: reason.message
    });
  }
  return reason instanceof Error
    ? translate("error.unexpected", { message: reason.message })
    : translate("error.fallback");
}

export function ResearchPage({
  workspace,
  locale,
  theme,
  chartCopy,
  convergence,
  surfaceRows,
  observationRows,
  selectedSurfaceRow,
  surface,
  surfaceSelection,
  parallel
}: ResearchPageProps) {
  const { t } = useTranslation();
  const {
    epoch,
    summaries,
    generationIndex,
    genes,
    selectedGeneId,
    xParameter,
    yParameter,
    surfaceMode,
    loading,
    error
  } = workspace;
  const { dimensions, numericParameters, selectedGene, objective, rankedGenes } =
    workspace.model;
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
      workspace.chooseGene(geneId);
    }
  };

  return (
    <>
      {error !== null && (
        <section className="error-panel" role="alert">
          <div>
            <strong>{t("error.title")}</strong>
            <p>{errorMessage(error, t)}</p>
          </div>
          <button type="button" onClick={workspace.retry}>
            {t("error.retry")}
          </button>
        </section>
      )}

      {error === null && !loading && !epoch && (
        <section className="empty-panel">
          <strong>{t("empty.title")}</strong>
          <p>{t("empty.body")}</p>
        </section>
      )}

      {epoch && (
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
                <span>
                  {epoch.eval_pair} · {epoch.eval_timeframe}
                </span>
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
                      workspace.chooseGeneration(Number(event.target.value))
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
                      onClick={() => workspace.chooseSurfaceMode("2d")}
                    >
                      {t("surface.view2d")}
                    </button>
                    <button
                      type="button"
                      aria-pressed={surfaceMode === "3d"}
                      onClick={() => workspace.chooseSurfaceMode("3d")}
                    >
                      {t("surface.view3d")}
                    </button>
                  </div>
                  <div className="axis-controls">
                    <label>
                      X
                      <select
                        value={xParameter}
                        onChange={(event) =>
                          workspace.chooseXParameter(event.target.value)
                        }
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
                        onChange={(event) =>
                          workspace.chooseYParameter(event.target.value)
                        }
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
                <div className="chart-message">{t("surface.unavailable")}</div>
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
                      onClick={() => workspace.chooseGene(gene.id)}
                    >
                      <span className="rank">
                        {String(index + 1).padStart(2, "0")}
                      </span>
                      <span>
                        <strong>{gene.candidate_id}</strong>
                        <small>
                          {t("candidates.drawdown")} {" "}
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

      {loading && (
        <div className="loading-indicator" aria-live="polite">
          <span />
          {t("loading.snapshot")}
        </div>
      )}
    </>
  );
}
