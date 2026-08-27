// @vitest-environment jsdom

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { Epoch, Gene, GenerationSummary } from "../../api";
import i18n from "../../shared/i18n";
import { ResearchRoute } from "./ResearchRoute";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadEpochs: vi.fn(),
  loadGenerationGenes: vi.fn(),
  loadGenerationSummaries: vi.fn()
}));
const charts = vi.hoisted(() => ({
  render: vi.fn()
}));

vi.mock("../../api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api")>()),
  ...api
}));
vi.mock("../../shared/charts/EChart", () => ({
  EChart: (props: {
    ariaLabel: string;
    option: unknown;
    updateOption?: unknown;
  }) => {
    charts.render(props);
    return <div aria-label={props.ariaLabel} />;
  }
}));
vi.mock("./FitnessSurface3D", () => ({
  FitnessSurface3D: ({
    ariaLabel,
    observationLabel,
    onDataClick
  }: {
    ariaLabel: string;
    observationLabel: string;
    onDataClick: (data: unknown) => void;
  }) => (
    <button
      type="button"
      aria-label={ariaLabel}
      data-observation-label={observationLabel}
      onClick={() => onDataClick([5, 20, 1, 2, "candidate-b"])}
    />
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
const summary: GenerationSummary = {
  generation_index: 0,
  candidate_count: 1,
  score_min: "1",
  score_max: "1",
  drawdown_min: "0.1",
  drawdown_max: "0.1"
};
function gene(epochId = "epoch", id = 1): Gene {
  return {
    id,
    strategy_id: `strategy-${epochId}`,
    role: "challenger",
    param_pack: { fast: 5, slow: 20 },
    score_total: String(id),
    score_breakdown: {},
    max_drawdown: "0.1",
    generation_index: 0,
    candidate_id: epochId === "epoch" ? "candidate" : `candidate-${epochId}`,
    epoch_id: epochId,
    created_at: "2026-07-28T01:00:00Z"
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

describe("ResearchRoute", () => {
  beforeEach(async () => {
    vi.clearAllMocks();
    await i18n.changeLanguage("zh-TW");
    api.ensureBrowserSession.mockResolvedValue(undefined);
    api.loadEpochs.mockResolvedValue([epoch]);
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene()]);
  });

  afterEach(cleanup);

  it("keeps the request owner mounted while both hidden slots are null", async () => {
    const epochs = deferred<Epoch[]>();
    api.loadEpochs.mockReturnValue(epochs.promise);
    const view = render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => (
          <div>
            <div data-testid="toolbar-slot">{toolbar}</div>
            <div data-testid="content-slot">{content}</div>
          </div>
        )}
      </ResearchRoute>
    );
    await waitFor(() => expect(api.loadEpochs).toHaveBeenCalledTimes(1));

    view.rerender(
      <ResearchRoute visible={false} demoMode={false} theme="light">
        {({ toolbar, content }) => (
          <div>
            <div data-testid="toolbar-slot">{toolbar}</div>
            <div data-testid="content-slot">{content}</div>
          </div>
        )}
      </ResearchRoute>
    );
    expect(screen.getByTestId("toolbar-slot").childElementCount).toBe(0);
    expect(screen.getByTestId("content-slot").childElementCount).toBe(0);

    epochs.resolve([epoch]);
    await waitFor(() =>
      expect(api.loadGenerationGenes).toHaveBeenCalledWith("epoch", 0)
    );
    view.rerender(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => (
          <div>
            <div data-testid="toolbar-slot">{toolbar}</div>
            <div data-testid="content-slot">{content}</div>
          </div>
        )}
      </ResearchRoute>
    );

    expect(await screen.findAllByText("candidate")).toHaveLength(2);
    expect(screen.getByRole("combobox", { name: "演化批次" })).toBeTruthy();
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(1);
    expect(api.loadEpochs).toHaveBeenCalledTimes(1);
  });

  it("renders canonical epoch instants in the Berlin calendar and rejects malformed direct fields", async () => {
    api.loadEpochs.mockResolvedValue([
      { ...epoch, id: "winter", started_at: "2026-01-15T23:30:00Z" },
      { ...epoch, id: "summer", started_at: "2026-07-15T12:34:00Z" },
      { ...epoch, id: "invalid", started_at: "not-a-timestamp" }
    ]);
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar }) => toolbar}
      </ResearchRoute>
    );

    const options = await screen.findAllByRole("option");
    expect(options.map((option) => option.textContent)).toEqual([
      expect.stringContaining("2026/01/16"),
      expect.stringContaining("2026/07/15"),
      "strategy · —"
    ]);
  });

  it("keeps a pending summary request and its dependent cache across hiding", async () => {
    const summaries = deferred<GenerationSummary[]>();
    api.loadGenerationSummaries.mockReturnValue(summaries.promise);
    const view = render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await waitFor(() =>
      expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(1)
    );

    view.rerender(
      <ResearchRoute visible={false} demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    summaries.resolve([summary]);
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1));

    view.rerender(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    expect(await screen.findAllByText("candidate")).toHaveLength(2);
    expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);
  });

  it("keeps a pending gene request and exact selection across hiding", async () => {
    const genes = deferred<Gene[]>();
    api.loadGenerationGenes.mockReturnValue(genes.promise);
    const view = render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1));

    view.rerender(
      <ResearchRoute visible={false} demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    genes.resolve([gene()]);
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1));

    view.rerender(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    expect(await screen.findAllByText("candidate")).toHaveLength(2);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);
  });

  it("clears the previous epoch while the next epoch is loading", async () => {
    const epochB = { ...epoch, id: "b", strategy_id: "strategy-b" };
    const epochBSummaries = deferred<GenerationSummary[]>();
    api.loadEpochs.mockResolvedValue([epoch, epochB]);
    api.loadGenerationSummaries.mockImplementation((epochId: string) =>
      epochId === epoch.id ? Promise.resolve([summary]) : epochBSummaries.promise
    );
    api.loadGenerationGenes.mockImplementation((epochId: string) =>
      Promise.resolve([gene(epochId, epochId === epoch.id ? 1 : 2)])
    );
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");

    fireEvent.change(screen.getByRole("combobox", { name: "演化批次" }), {
      target: { value: "b" }
    });

    expect(screen.queryByText("candidate")).toBeNull();
    await waitFor(() =>
      expect(api.loadGenerationSummaries).toHaveBeenCalledWith("b")
    );
    epochBSummaries.resolve([summary]);
    await screen.findAllByText("candidate-b");

    const transitionalSurfaces = charts.render.mock.calls
      .map(([props]) => props as {
        ariaLabel: string;
        option: unknown;
        updateOption?: unknown;
      })
      .filter(
        ({ ariaLabel, option }) =>
          ariaLabel === "兩個策略參數與候選適應度的觀測地形圖" &&
          JSON.stringify(option) === "{}"
      );
    expect(transitionalSurfaces.length).toBeGreaterThan(0);
    expect(
      transitionalSurfaces.every(
        ({ updateOption }) => JSON.stringify(updateOption) === "{}"
      )
    ).toBe(true);
  });

  it("retries downstream data without reloading successful epochs", async () => {
    api.loadGenerationSummaries
      .mockRejectedValueOnce(new Error("summary unavailable"))
      .mockResolvedValueOnce([summary]);
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );

    expect(await screen.findByText("研究資料未載入")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "重新讀取" }));

    await waitFor(() => {
      expect(api.loadEpochs).toHaveBeenCalledTimes(1);
      expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(2);
      expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);
    });
  });

  it("keeps the two surface axes distinct", async () => {
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");

    const xAxis = screen.getByLabelText("X") as HTMLSelectElement;
    const yAxis = screen.getByLabelText("Y") as HTMLSelectElement;
    await waitFor(() => {
      expect(xAxis.value).toBe("fast");
      expect(yAxis.value).toBe("slow");
    });
    expect(
      Array.from(xAxis.options).find((option) => option.value === "slow")
        ?.disabled
    ).toBe(true);
    expect(
      Array.from(yAxis.options).find((option) => option.value === "fast")
        ?.disabled
    ).toBe(true);
  });

  it("owns the complete 2D/3D selection transition", async () => {
    api.loadGenerationGenes.mockResolvedValue([gene(), gene("b", 2)]);
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );
    await screen.findAllByText("candidate");

    fireEvent.click(screen.getByRole("button", { name: "3D 地形" }));
    const surface3d = await screen.findByRole("button", {
      name: "兩個策略參數與候選適應度的互動式三維插值地形圖"
    });
    expect(surface3d.dataset.observationLabel).toBe("觀測候選");
    fireEvent.click(surface3d);
    expect(screen.getAllByText("candidate-b")).toHaveLength(2);
    fireEvent.click(screen.getByRole("button", { name: "2D 色階" }));
    expect(
      screen.getByLabelText("兩個策略參數與候選適應度的觀測地形圖")
    ).toBeTruthy();
  });

  it("fails closed when the epoch objective is unsupported", async () => {
    api.loadEpochs.mockResolvedValue([
      { ...epoch, config_json: { objective: "maximize_magic" } }
    ]);
    render(
      <ResearchRoute visible demoMode={false} theme="light">
        {({ toolbar, content }) => <>{toolbar}{content}</>}
      </ResearchRoute>
    );

    expect(
      await screen.findByText(
        "無法辨識這個批次的最佳化目標，已停止繪製候選地形。"
      )
    ).toBeTruthy();
    expect(screen.getByText("不支援的最佳化目標")).toBeTruthy();
    expect(
      screen.queryByLabelText("兩個策略參數與候選適應度的觀測地形圖")
    ).toBeNull();
  });
});
