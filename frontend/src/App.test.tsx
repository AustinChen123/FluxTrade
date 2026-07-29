// @vitest-environment jsdom

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import i18n, { resolveLocale } from "./i18n";
import type { Epoch, Gene, GenerationSummary } from "./api";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadEpochs: vi.fn(),
  loadGenerationGenes: vi.fn(),
  loadGenerationSummaries: vi.fn()
}));

vi.mock("./api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./api")>()),
  ...api
}));

vi.mock("./EChart", () => ({
  EChart: ({
    ariaLabel,
    onDataClick
  }: {
    ariaLabel: string;
    onDataClick?: (data: unknown, dataIndex: number) => void;
  }) => (
    <button
      type="button"
      aria-label={ariaLabel}
      onClick={() =>
        onDataClick?.([5, 20, 1, 2, "candidate-epoch-b"], 0)
      }
    />
  )
}));

vi.mock("./FitnessSurface3D", async () => {
  const { useEffect, useRef } = await import("react");
  return {
    FitnessSurface3D: ({
      ariaLabel,
      observationLabel,
      onDataClick,
      theme
    }: {
      ariaLabel: string;
      observationLabel: string;
      onDataClick?: (data: unknown) => void;
      theme: string;
    }) => {
      const ref = useRef<HTMLButtonElement>(null);
      useEffect(() => {
        ref.current?.setAttribute(
          "data-drawn-theme",
          document.documentElement.dataset.theme ?? ""
        );
      }, [theme]);
      return (
        <button
          ref={ref}
          type="button"
          aria-label={ariaLabel}
          data-observation-label={observationLabel}
          onClick={() => onDataClick?.([5, 20, 1, 2, "candidate-epoch-b"])}
        />
      );
    }
  };
});

vi.mock("./StrategyManager", () => ({
  StrategyManager: () => <section data-testid="strategy-manager" />
}));

vi.mock("./TradeChartView", () => ({
  TradeChartView: ({ demoMode }: { demoMode: boolean }) => (
    <section data-demo={String(demoMode)} data-testid="trade-chart-view" />
  )
}));

const epoch = (id: string): Epoch => ({
  id,
  strategy_id: `strategy-${id}`,
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
});

const summary: GenerationSummary = {
  generation_index: 0,
  candidate_count: 1,
  score_min: "1",
  score_max: "1",
  drawdown_min: "0.1",
  drawdown_max: "0.1"
};

const gene = (epochId: string): Gene => ({
  id: epochId === "epoch-a" ? 1 : 2,
  strategy_id: `strategy-${epochId}`,
  role: "challenger",
  param_pack: { fast: 5, slow: 20 },
  score_total: "1",
  score_breakdown: {},
  max_drawdown: "0.1",
  generation_index: 0,
  candidate_id: `candidate-${epochId}`,
  epoch_id: epochId,
  created_at: "2026-07-28T01:00:00Z"
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const storage = new Map<string, string>();

describe("GA visualization state", () => {
  beforeEach(async () => {
    vi.clearAllMocks();
    window.history.replaceState({}, "", "/");
    storage.clear();
    delete document.documentElement.dataset.theme;
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      value: {
        getItem: (key: string) => storage.get(key) ?? null,
        setItem: (key: string, value: string) => storage.set(key, value),
        removeItem: (key: string) => storage.delete(key),
        clear: () => storage.clear()
      }
    });
    await i18n.changeLanguage("zh-TW");
    api.ensureBrowserSession.mockResolvedValue(undefined);
    api.loadEpochs.mockResolvedValue([epoch("epoch-a"), epoch("epoch-b")]);
  });

  afterEach(() => {
    cleanup();
  });

  it("clears the previous epoch while the next epoch is loading", async () => {
    const epochBSummaries = deferred<GenerationSummary[]>();
    api.loadGenerationSummaries.mockImplementation((epochId: string) =>
      epochId === "epoch-a"
        ? Promise.resolve([summary])
        : epochBSummaries.promise
    );
    api.loadGenerationGenes.mockImplementation((epochId: string) =>
      Promise.resolve([gene(epochId)])
    );

    render(<App />);
    await screen.findAllByText("candidate-epoch-a");

    fireEvent.change(
      screen.getByRole("combobox", { name: "演化批次" }),
      {
        target: { value: "epoch-b" }
      }
    );

    expect(screen.queryByText("candidate-epoch-a")).toBeNull();
    await waitFor(() =>
      expect(api.loadGenerationSummaries).toHaveBeenCalledWith("epoch-b")
    );

    epochBSummaries.resolve([summary]);
    await screen.findAllByText("candidate-epoch-b");
  });

  it("uses browser language with a Traditional Chinese fallback", () => {
    expect(resolveLocale(null, ["en-US"])).toBe("en");
    expect(resolveLocale(null, ["zh-TW", "en-US"])).toBe("zh-TW");
    expect(resolveLocale(null, ["en-US", "zh-TW"])).toBe("en");
    expect(resolveLocale(null, ["de-DE"])).toBe("zh-TW");
    expect(resolveLocale("zh-TW", ["en-US"])).toBe("zh-TW");
  });

  it("opens strategy management directly without loading GA data", async () => {
    window.history.replaceState({}, "", "/?view=strategies");

    render(<App />);

    expect(await screen.findByTestId("strategy-manager")).toBeTruthy();
    expect(api.ensureBrowserSession).not.toHaveBeenCalled();
    expect(api.loadEpochs).not.toHaveBeenCalled();
    expect(screen.queryByRole("combobox", { name: "演化批次" })).toBeNull();
    expect(
      screen.getByRole("button", { name: "策略管理", current: "page" })
    ).toBeTruthy();
  });

  it("opens trade inspection directly without loading GA data", async () => {
    window.history.replaceState({}, "", "/?view=trades");

    render(<App />);

    expect(await screen.findByTestId("trade-chart-view")).toBeTruthy();
    expect(api.ensureBrowserSession).not.toHaveBeenCalled();
    expect(api.loadEpochs).not.toHaveBeenCalled();
    expect(screen.queryByRole("combobox", { name: "演化批次" })).toBeNull();
    expect(
      screen.getByRole("button", { name: "進出場檢視", current: "page" })
    ).toBeTruthy();
    expect(new URL(window.location.href).searchParams.get("view")).toBe("trades");
  });

  it("keeps the initial epoch request alive across console view switches", async () => {
    const epochs = deferred<Epoch[]>();
    api.loadEpochs.mockReturnValue(epochs.promise);
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);
    render(<App />);
    await waitFor(() => expect(api.loadEpochs).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getByRole("button", { name: "策略管理" }));
    expect(await screen.findByTestId("strategy-manager")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "參數研究" }));

    expect(screen.getByText("讀取研究快照")).toBeTruthy();
    expect(api.loadEpochs).toHaveBeenCalledTimes(1);

    epochs.resolve([epoch("epoch-a"), epoch("epoch-b")]);
    await screen.findAllByText("candidate-epoch-a");
    expect(api.loadEpochs).toHaveBeenCalledTimes(1);
  });

  it("keeps the selected console page in the URL", async () => {
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);
    render(<App />);
    await screen.findAllByText("candidate-epoch-a");

    fireEvent.click(screen.getByRole("button", { name: "策略管理" }));

    expect(await screen.findByTestId("strategy-manager")).toBeTruthy();
    expect(new URL(window.location.href).searchParams.get("view")).toBe(
      "strategies"
    );

    fireEvent.click(screen.getByRole("button", { name: "參數研究" }));
    expect(new URL(window.location.href).searchParams.has("view")).toBe(false);
  });

  it("does not reload research data after visiting trade inspection", async () => {
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);
    render(<App />);
    await screen.findAllByText("candidate-epoch-a");

    fireEvent.click(screen.getByRole("button", { name: "進出場檢視" }));
    expect(await screen.findByTestId("trade-chart-view")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "參數研究" }));
    await screen.findAllByText("candidate-epoch-a");

    expect(api.loadEpochs).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);
  });

  it("does not reload research data when returning from strategy management", async () => {
    const genes = deferred<Gene[]>();
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockReturnValue(genes.promise);
    render(<App />);
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1));

    fireEvent.click(screen.getByRole("button", { name: "策略管理" }));
    expect(await screen.findByTestId("strategy-manager")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "參數研究" }));

    expect(screen.getByText("讀取研究快照")).toBeTruthy();
    expect(api.loadEpochs).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);

    genes.resolve([gene("epoch-a")]);
    await screen.findAllByText("candidate-epoch-a");
  });

  it("still reloads research data after a downstream load failure", async () => {
    api.loadGenerationSummaries
      .mockRejectedValueOnce(new Error("summary unavailable"))
      .mockResolvedValueOnce([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);
    render(<App />);

    expect(await screen.findByText("研究資料未載入")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "重新讀取" }));

    await waitFor(() => {
      expect(api.loadEpochs).toHaveBeenCalledTimes(1);
      expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(2);
      expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1);
    });
  });

  it("keeps the two surface axes distinct", async () => {
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);

    render(<App />);
    await screen.findAllByText("candidate-epoch-a");

    const xAxis = screen.getByLabelText("X") as HTMLSelectElement;
    const yAxis = screen.getByLabelText("Y") as HTMLSelectElement;
    await waitFor(() => {
      expect(xAxis.value).toBe("fast");
      expect(yAxis.value).toBe("slow");
    });
    expect(
      Array.from(xAxis.options).find((option) => option.value === "slow")?.disabled
    ).toBe(true);
    expect(
      Array.from(yAxis.options).find((option) => option.value === "fast")?.disabled
    ).toBe(true);
  });

  it("switches the complete interface language and saves the choice", async () => {
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);

    render(<App />);
    await screen.findAllByText("candidate-epoch-a");

    fireEvent.change(screen.getByLabelText("語言"), {
      target: { value: "en" }
    });

    await screen.findByRole("heading", { name: "Parameter terrain" });
    expect(
      (screen.getByLabelText("Language") as HTMLSelectElement).value
    ).toBe("en");
    expect(screen.getByText("Candidate comparison")).toBeTruthy();
    expect(screen.getByText("1 candidate")).toBeTruthy();
    expect(
      screen.getByLabelText(
        "Observed fitness surface for two strategy parameters"
      )
    ).toBeTruthy();
    expect(screen.getAllByText("candidate-epoch-a").length).toBeGreaterThan(0);
    expect(window.localStorage.getItem("fluxtrade.locale")).toBe("en");
    expect(document.documentElement.lang).toBe("en");
  });

  it("switches between saved light and dark themes", async () => {
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);

    render(<App />);
    await screen.findAllByText("candidate-epoch-a");

    fireEvent.click(screen.getByRole("button", { name: "3D 地形" }));
    const surface3d = await screen.findByRole("button", {
      name: "兩個策略參數與候選適應度的互動式三維插值地形圖"
    });
    expect(surface3d.dataset.drawnTheme).toBe("light");
    expect(window.localStorage.getItem("fluxtrade.theme")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "切換至夜間主題" }));

    expect(document.documentElement.dataset.theme).toBe("dark");
    expect(surface3d.dataset.drawnTheme).toBe("dark");
    expect(window.localStorage.getItem("fluxtrade.theme")).toBe("dark");
    expect(
      screen.getByRole("button", { name: "切換至日間主題" })
    ).toBeTruthy();
  });

  it("switches between 2D and 3D views and selects observed candidates", async () => {
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([
      gene("epoch-a"),
      gene("epoch-b")
    ]);

    render(<App />);
    await screen.findAllByText("candidate-epoch-a");

    fireEvent.click(screen.getByRole("button", { name: "3D 地形" }));
    const surface3d = await screen.findByRole("button", {
      name: "兩個策略參數與候選適應度的互動式三維插值地形圖"
    });
    expect(surface3d.dataset.observationLabel).toBe("觀測候選");
    fireEvent.click(surface3d);

    expect(screen.getAllByText("candidate-epoch-b")).toHaveLength(2);
    fireEvent.click(screen.getByRole("button", { name: "2D 色階" }));
    expect(
      screen.getByRole("button", {
        name: "兩個策略參數與候選適應度的觀測地形圖"
      })
    ).toBeTruthy();
  });

  it("fails closed when an epoch has an unsupported objective", async () => {
    api.loadEpochs.mockResolvedValue([
      {
        ...epoch("epoch-a"),
        config_json: { objective: "maximize_magic" }
      }
    ]);
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);

    render(<App />);

    expect(
      await screen.findByText("無法辨識這個批次的最佳化目標，已停止繪製候選地形。")
    ).toBeTruthy();
    expect(screen.getByText("不支援的最佳化目標")).toBeTruthy();
    expect(
      screen.queryByLabelText("兩個策略參數與候選適應度的觀測地形圖")
    ).toBeNull();
  });
});
