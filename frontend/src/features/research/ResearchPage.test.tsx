// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { Epoch, Gene } from "../../api";
import i18n from "../../shared/i18n";
import { ResearchPage } from "./ResearchPage";
import { buildResearchModel } from "./researchModel";
import type {
  ResearchError,
  ResearchWorkspace
} from "./useResearchWorkspace";

vi.mock("../../shared/charts/EChart", () => ({
  EChart: ({
    ariaLabel,
    onDataClick
  }: {
    ariaLabel: string;
    onDataClick?: (data: unknown) => void;
  }) => (
    <button
      type="button"
      aria-label={ariaLabel}
      onClick={() => onDataClick?.([5, 20, 1, 1, "candidate"])}
    />
  )
}));
vi.mock("./FitnessSurface3D", () => ({
  FitnessSurface3D: ({ ariaLabel }: { ariaLabel: string }) => (
    <div aria-label={ariaLabel} />
  )
}));

const epoch: Epoch = {
  id: "epoch",
  strategy_id: "strategy",
  started_at: "2026-07-28T00:00:00Z",
  finished_at: "2026-07-28T01:00:00Z",
  pop_size: 1,
  max_generations: 1,
  generations_run: 1,
  best_score: "1",
  seed: 1,
  config_json: { objective: "maximize_score" },
  status: "completed",
  eval_pair: "RITHMIC:MNQ_ROLL-PERP",
  eval_start_date: "2026-01-01",
  eval_end_date: "2026-07-28",
  eval_timeframe: "5m"
};
const gene: Gene = {
  id: 1,
  strategy_id: "strategy",
  role: "challenger",
  param_pack: { fast: 5, slow: 20 },
  score_total: "1",
  score_breakdown: {},
  max_drawdown: "0.1",
  generation_index: 0,
  candidate_id: "candidate",
  epoch_id: "epoch",
  created_at: "2026-07-28T01:00:00Z"
};

function workspace(
  overrides: Partial<ResearchWorkspace> = {}
): ResearchWorkspace {
  return {
    epochs: [epoch],
    epoch,
    epochId: epoch.id,
    summaries: [],
    generationIndex: 0,
    genes: [gene],
    selectedGeneId: gene.id,
    xParameter: "fast",
    yParameter: "slow",
    surfaceMode: "2d",
    loading: false,
    error: null,
    model: buildResearchModel(epoch, [gene], gene.id),
    chooseEpoch: vi.fn(),
    chooseGeneration: vi.fn(),
    chooseGene: vi.fn(),
    chooseXParameter: vi.fn(),
    chooseYParameter: vi.fn(),
    chooseSurfaceMode: vi.fn(),
    retry: vi.fn(),
    ...overrides
  };
}

const chartCopy = {
  locale: "en",
  generation: "Generation",
  drawdown: "Drawdown",
  score: "Score",
  fitness: "Fitness",
  lowerDrawdown: "Lower drawdown",
  upperDrawdown: "Upper drawdown",
  lowerScore: "Lower score",
  upperScore: "Upper score",
  high: "High",
  low: "Low",
  selected: "Selected",
  bestObserved: "Best observed"
};

function renderPage(current: ResearchWorkspace) {
  return render(
    <ResearchPage
      workspace={current}
      locale="zh-TW"
      theme="light"
      chartCopy={chartCopy}
      convergence={{}}
      surfaceRows={[[5, 20, 1, 1, "candidate"]]}
      observationRows={[[5, 20, 1, 1, "candidate"]]}
      selectedSurfaceRow={[5, 20, 1, 1, "candidate"]}
      surface={{}}
      surfaceSelection={{}}
      parallel={{}}
    />
  );
}

describe("ResearchPage", () => {
  beforeEach(async () => {
    await i18n.changeLanguage("zh-TW");
  });
  afterEach(cleanup);

  it("renders research data and delegates user transitions to the owner", async () => {
    const current = workspace();
    renderPage(current);

    expect(await screen.findAllByText("candidate")).toHaveLength(2);
    fireEvent.click(
      screen.getByRole("button", {
        name: "兩個策略參數與候選適應度的觀測地形圖"
      })
    );
    expect(current.chooseGene).toHaveBeenCalledWith(1);
    fireEvent.click(screen.getByRole("button", { name: "3D 地形" }));
    expect(current.chooseSurfaceMode).toHaveBeenCalledWith("3d");
  });

  it("keeps error retry as an explicit owner action", () => {
    const current = workspace({
      epoch: null,
      genes: [],
      selectedGeneId: null,
      error: { type: "unexpected", message: "temporary" }
    });
    renderPage(current);

    fireEvent.click(screen.getByRole("button", { name: "重新讀取" }));
    expect(current.retry).toHaveBeenCalledTimes(1);
  });

  it.each([
    [
      { type: "unauthorized" },
      "目前工作階段沒有讀取研究結果的權限。請從受信任的 Tailscale 入口重新開啟。"
    ],
    [
      { type: "service", status: 503, message: "service unavailable" },
      "資料服務回覆 503：service unavailable"
    ],
    [
      { type: "unexpected", message: "unexpected failure" },
      "無法讀取 GA 資料：unexpected failure"
    ],
    [{ type: "fallback" }, "無法讀取 GA 資料"]
  ] satisfies readonly (readonly [ResearchError, string])[])(
    "renders the exact research error projection for $expected",
    (error, expected) => {
      renderPage(workspace({ error }));

      expect(screen.getByRole("alert").textContent).toContain(expected);
    }
  );
});
