import { useMemo, type ReactNode } from "react";
import { useTranslation } from "react-i18next";

import type { Locale } from "../../shared/i18n";
import type { Theme } from "../../shared/theme";
import {
  formatPresentationTimestamp,
  PRESENTATION_TIME_ZONE
} from "../../shared/time/presentation";
import {
  convergenceOption,
  fitnessSurfaceOption,
  parallelOption,
  selectedSurfaceOption,
  type ChartCopy
} from "./gaCharts";
import {
  fitnessObservationRows,
  fitnessSurfaceRows,
  surfaceRow
} from "./gaDomain";
import { ResearchPage } from "./ResearchPage";
import { useResearchWorkspace } from "./useResearchWorkspace";

export type ResearchSlots = {
  readonly toolbar: ReactNode;
  readonly content: ReactNode;
};

export type ResearchRouteProps = {
  readonly visible: boolean;
  readonly demoMode: boolean;
  readonly theme: Theme;
  readonly children: (slots: ResearchSlots) => ReactNode;
};

export function ResearchRoute({
  visible,
  demoMode,
  theme,
  children
}: ResearchRouteProps) {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "en" ? "en" : "zh-TW";
  const workspace = useResearchWorkspace(demoMode);
  const model = workspace.model;
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
      workspace.epoch && model.objective
        ? convergenceOption(
            workspace.epoch,
            workspace.summaries,
            chartCopy,
            theme
          )
        : {},
    [
      chartCopy,
      model.objective,
      theme,
      workspace.epoch,
      workspace.summaries
    ]
  );
  const surfaceRows = useMemo(
    () =>
      workspace.epoch && workspace.xParameter && workspace.yParameter
        ? fitnessSurfaceRows(
            workspace.epoch,
            workspace.genes,
            workspace.xParameter,
            workspace.yParameter
          )
        : [],
    [
      workspace.epoch,
      workspace.genes,
      workspace.xParameter,
      workspace.yParameter
    ]
  );
  const observationRows = useMemo(
    () =>
      workspace.epoch && workspace.xParameter && workspace.yParameter
        ? fitnessObservationRows(
            workspace.epoch,
            workspace.genes,
            workspace.xParameter,
            workspace.yParameter
          )
        : [],
    [
      workspace.epoch,
      workspace.genes,
      workspace.xParameter,
      workspace.yParameter
    ]
  );
  const selectedSurfaceRow = useMemo(
    () =>
      workspace.epoch
        ? surfaceRow(
            workspace.epoch,
            model.selectedGene,
            workspace.xParameter,
            workspace.yParameter
          )
        : null,
    [
      model.selectedGene,
      workspace.epoch,
      workspace.xParameter,
      workspace.yParameter
    ]
  );
  const surfaceAxes = useMemo(() => {
    if (
      !workspace.epoch ||
      !workspace.xParameter ||
      !workspace.yParameter
    ) {
      return null;
    }
    return {
      epoch: workspace.epoch,
      xParameter: workspace.xParameter,
      yParameter: workspace.yParameter
    };
  }, [workspace.epoch, workspace.xParameter, workspace.yParameter]);
  const surface = useMemo(
    () =>
      surfaceAxes
        ? fitnessSurfaceOption(
            surfaceAxes.epoch,
            surfaceRows,
            surfaceAxes.xParameter,
            surfaceAxes.yParameter,
            chartCopy,
            theme
          )
        : {},
    [chartCopy, surfaceAxes, surfaceRows, theme]
  );
  const surfaceSelection = useMemo(
    () =>
      surfaceAxes ? selectedSurfaceOption(selectedSurfaceRow) : {},
    [selectedSurfaceRow, surfaceAxes]
  );
  const parallel = useMemo(
    () =>
      parallelOption(
        workspace.genes,
        model.dimensions,
        workspace.selectedGeneId,
        theme,
        locale
      ),
    [
      locale,
      model.dimensions,
      theme,
      workspace.genes,
      workspace.selectedGeneId
    ]
  );
  const dateFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        timeZone: PRESENTATION_TIME_ZONE
      }),
    [locale]
  );

  const toolbar = visible ? (
    <div className="epoch-control">
      <label htmlFor="epoch">{t("controls.epoch")}</label>
      <select
        id="epoch"
        value={workspace.epochId}
        onChange={(event) => workspace.chooseEpoch(event.target.value)}
        disabled={!workspace.epochs.length}
      >
        {workspace.epochs.map((item) => (
          <option key={item.id} value={item.id}>
            {item.strategy_id} ·{" "}
            {formatPresentationTimestamp(item.started_at, dateFormatter)}
          </option>
        ))}
      </select>
    </div>
  ) : null;
  const content = visible ? (
    <ResearchPage
      workspace={workspace}
      locale={locale}
      theme={theme}
      chartCopy={chartCopy}
      convergence={convergence}
      surfaceRows={surfaceRows}
      observationRows={observationRows}
      selectedSurfaceRow={selectedSurfaceRow}
      surface={surface}
      surfaceSelection={surfaceSelection}
      parallel={parallel}
    />
  ) : null;

  return children({ toolbar, content });
}
