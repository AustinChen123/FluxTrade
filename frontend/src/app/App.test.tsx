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
import i18n, { resolveLocale } from "../i18n";
import type { Epoch, Gene, GenerationSummary } from "../api";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadEpochs: vi.fn(),
  loadGenerationGenes: vi.fn(),
  loadGenerationSummaries: vi.fn()
}));
const lifecycles = vi.hoisted(() => ({
  results: { mounts: 0, unmounts: 0 },
  strategies: { mounts: 0, unmounts: 0 },
  trades: { mounts: 0, unmounts: 0 }
}));

vi.mock("../api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api")>()),
  ...api
}));

vi.mock("../EChart", () => ({
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

vi.mock("../features/research/FitnessSurface3D", async () => {
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

vi.mock("../StrategyManager", async () => {
  const { useEffect, useState } = await import("react");
  return {
    StrategyManager: () => {
      const [state, setState] = useState("initial");
      useEffect(() => {
        lifecycles.strategies.mounts += 1;
        return () => {
          lifecycles.strategies.unmounts += 1;
        };
      }, []);
      return (
        <section data-testid="strategy-manager">
          <button
            type="button"
            data-testid="strategies-state"
            onClick={() => setState("changed")}
          >
            strategies:{state}
          </button>
        </section>
      );
    }
  };
});

vi.mock("../features/trades/TradeChartView", async () => {
  const { useEffect, useState } = await import("react");
  return {
    TradeChartView: ({
      demoMode,
      initialTradeId,
      onSelectTrade
    }: {
      demoMode: boolean;
      initialTradeId?: string | null;
      onSelectTrade?: (tradeId: string) => void;
    }) => {
      const [state, setState] = useState("initial");
      useEffect(() => {
        lifecycles.trades.mounts += 1;
        return () => {
          lifecycles.trades.unmounts += 1;
        };
      }, []);
      return (
        <section
          data-demo={String(demoMode)}
          data-selected-trade={initialTradeId ?? ""}
          data-testid="trade-chart-view"
        >
          <button
            type="button"
            onClick={() => onSelectTrade?.("trade-000184")}
          >
            Select mock trade
          </button>
          <button
            type="button"
            data-testid="trades-state"
            onClick={() => setState("changed")}
          >
            trades:{state}
          </button>
        </section>
      );
    }
  };
});

vi.mock("../features/results/BacktestResultsView", async () => {
  const { useEffect, useState } = await import("react");
  return {
    BacktestResultsView: ({
      demoMode,
      onInspectTrade
    }: {
      demoMode: boolean;
      onInspectTrade?: (tradeId: string) => void;
    }) => {
      const [state, setState] = useState("initial");
      useEffect(() => {
        lifecycles.results.mounts += 1;
        return () => {
          lifecycles.results.unmounts += 1;
        };
      }, []);
      return (
        <section data-demo={String(demoMode)} data-testid="backtest-results-view">
          <button
            type="button"
            onClick={() => onInspectTrade?.("trade-000184")}
          >
            Inspect mock trade
          </button>
          <button
            type="button"
            data-testid="results-state"
            onClick={() => setState("changed")}
          >
            results:{state}
          </button>
        </section>
      );
    }
  };
});

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
    for (const lifecycle of Object.values(lifecycles)) {
      lifecycle.mounts = 0;
      lifecycle.unmounts = 0;
    }
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
    window.history.replaceState(
      {},
      "",
      "/?view=trades&trade=trade-000185&demo=1"
    );

    render(<App />);

    const tradeView = await screen.findByTestId("trade-chart-view");
    expect(tradeView.getAttribute("data-selected-trade")).toBe("trade-000185");
    expect(api.ensureBrowserSession).not.toHaveBeenCalled();
    expect(api.loadEpochs).not.toHaveBeenCalled();
    expect(screen.queryByRole("combobox", { name: "演化批次" })).toBeNull();
    expect(
      screen.getByRole("button", { name: "進出場檢視", current: "page" })
    ).toBeTruthy();
    expect(new URL(window.location.href).searchParams.get("view")).toBe("trades");
    expect(new URL(window.location.href).searchParams.get("trade")).toBe(
      "trade-000185"
    );
  });

  it("keeps an in-page trade selection in the URL across reload", async () => {
    window.history.replaceState(
      {},
      "",
      "/?view=trades&trade=trade-000185&demo=1"
    );

    const firstRender = render(<App />);
    fireEvent.click(
      await screen.findByRole("button", { name: "Select mock trade" })
    );

    expect(new URL(window.location.href).searchParams.get("trade")).toBe(
      "trade-000184"
    );
    firstRender.unmount();

    render(<App />);
    expect(
      (await screen.findByTestId("trade-chart-view")).getAttribute(
        "data-selected-trade"
      )
    ).toBe("trade-000184");
  });

  it("recreates trade inspection from the current shell selection", async () => {
    window.history.replaceState(
      {},
      "",
      "/?view=trades&trade=trade-000185&demo=1"
    );
    render(<App />);

    expect(
      (await screen.findByTestId("trade-chart-view")).getAttribute(
        "data-selected-trade"
      )
    ).toBe("trade-000185");
    fireEvent.click(screen.getByRole("button", { name: "Select mock trade" }));
    expect(new URL(window.location.href).searchParams.get("trade")).toBe(
      "trade-000184"
    );

    fireEvent.click(screen.getByRole("button", { name: "回測績效" }));
    expect(await screen.findByTestId("backtest-results-view")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "進出場檢視" }));

    expect(
      (await screen.findByTestId("trade-chart-view")).getAttribute(
        "data-selected-trade"
      )
    ).toBe("");
    expect(lifecycles.trades).toEqual({ mounts: 2, unmounts: 1 });
  });

  it("opens backtest results directly without loading GA data", async () => {
    window.history.replaceState({}, "", "/?view=results");

    render(<App />);

    expect(await screen.findByTestId("backtest-results-view")).toBeTruthy();
    expect(api.ensureBrowserSession).not.toHaveBeenCalled();
    expect(api.loadEpochs).not.toHaveBeenCalled();
    expect(screen.queryByRole("combobox", { name: "演化批次" })).toBeNull();
    expect(
      screen.getByRole("button", { name: "回測績效", current: "page" })
    ).toBeTruthy();
    expect(new URL(window.location.href).searchParams.get("view")).toBe(
      "results"
    );
  });

  it("does not restore an invalid trade ID from the URL", async () => {
    window.history.replaceState(
      {},
      "",
      "/?view=trades&trade=%0Ainvalid&demo=1"
    );

    render(<App />);

    const tradeView = await screen.findByTestId("trade-chart-view");
    expect(tradeView.getAttribute("data-selected-trade")).toBe("");
  });

  it("opens the exact trade selected from backtest results", async () => {
    window.history.replaceState({}, "", "/?view=results&demo=1");
    render(<App />);

    fireEvent.click(
      await screen.findByRole("button", { name: "Inspect mock trade" })
    );

    const tradeView = await screen.findByTestId("trade-chart-view");
    expect(tradeView.getAttribute("data-selected-trade")).toBe("trade-000184");
    expect(new URL(window.location.href).searchParams.get("view")).toBe("trades");
    expect(new URL(window.location.href).searchParams.get("trade")).toBe(
      "trade-000184"
    );

    fireEvent.click(screen.getByRole("button", { name: "回測績效" }));
    expect(await screen.findByTestId("backtest-results-view")).toBeTruthy();
    expect(new URL(window.location.href).searchParams.has("trade")).toBe(false);
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

  it("replaces the owned query once without navigating the document", async () => {
    window.history.replaceState(
      {},
      "",
      "/console?view=results&view=trades&trade=old&trade=older&keep=1&keep=2#anchor"
    );
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);
    const replaceState = vi.spyOn(window.history, "replaceState");
    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "進出場檢視" }));
    expect(await screen.findByTestId("trade-chart-view")).toBeTruthy();
    expect(replaceState).toHaveBeenCalledTimes(1);
    expect(replaceState).toHaveBeenCalledWith(
      null,
      "",
      "/console?view=trades&keep=1&keep=2#anchor"
    );
    expect(window.location.pathname).toBe("/console");
    expect(window.location.hash).toBe("#anchor");
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

  it("keeps only research mounted and recreates every active-only feature", async () => {
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockImplementation((epochId: string) =>
      Promise.resolve([gene(epochId)])
    );
    render(<App />);
    await screen.findAllByText("candidate-epoch-a");

    expect(lifecycles).toEqual({
      results: { mounts: 0, unmounts: 0 },
      strategies: { mounts: 0, unmounts: 0 },
      trades: { mounts: 0, unmounts: 0 }
    });
    fireEvent.change(screen.getByRole("combobox", { name: "演化批次" }), {
      target: { value: "epoch-b" }
    });
    await screen.findAllByText("candidate-epoch-b");

    fireEvent.click(screen.getByRole("button", { name: "回測績效" }));
    fireEvent.click(await screen.findByTestId("results-state"));
    expect(screen.getByTestId("results-state").textContent).toBe(
      "results:changed"
    );

    fireEvent.click(screen.getByRole("button", { name: "策略管理" }));
    fireEvent.click(await screen.findByTestId("strategies-state"));
    expect(lifecycles.results).toEqual({ mounts: 1, unmounts: 1 });

    fireEvent.click(screen.getByRole("button", { name: "進出場檢視" }));
    fireEvent.click(await screen.findByTestId("trades-state"));
    expect(lifecycles.strategies).toEqual({ mounts: 1, unmounts: 1 });

    fireEvent.click(screen.getByRole("button", { name: "參數研究" }));
    await screen.findAllByText("candidate-epoch-b");
    expect(
      (screen.getByRole("combobox", { name: "演化批次" }) as HTMLSelectElement)
        .value
    ).toBe("epoch-b");
    expect(lifecycles.trades).toEqual({ mounts: 1, unmounts: 1 });
    expect(api.loadEpochs).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole("button", { name: "回測績效" }));
    expect((await screen.findByTestId("results-state")).textContent).toBe(
      "results:initial"
    );
    expect(lifecycles.results).toEqual({ mounts: 2, unmounts: 1 });
    fireEvent.click(screen.getByRole("button", { name: "策略管理" }));
    expect((await screen.findByTestId("strategies-state")).textContent).toBe(
      "strategies:initial"
    );
    fireEvent.click(screen.getByRole("button", { name: "進出場檢視" }));
    expect((await screen.findByTestId("trades-state")).textContent).toBe(
      "trades:initial"
    );
  });

  it("places opaque research slots at the frozen shell positions", async () => {
    window.history.replaceState({}, "", "/?view=strategies");
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockResolvedValue([gene("epoch-a")]);
    const { container } = render(<App />);

    const toolbarClasses = () =>
      Array.from(container.querySelector(".toolbar")?.children ?? []).map(
        (element) => element.className
      );
    expect(toolbarClasses()).toEqual(["language-control", "theme-control"]);
    expect(container.querySelector(".epoch-control")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "參數研究" }));
    await screen.findAllByText("candidate-epoch-a");
    expect(toolbarClasses()).toEqual([
      "epoch-control",
      "language-control",
      "theme-control"
    ]);
    expect(container.querySelectorAll(".epoch-control")).toHaveLength(1);
    const researchMainClasses = Array.from(
      container.querySelector("main")?.children ?? []
    ).map((element) => element.className);
    expect(researchMainClasses.slice(0, 4)).toEqual([
      "topbar",
      "console-nav",
      "run-strip",
      "analysis-grid"
    ]);

    fireEvent.click(screen.getByRole("button", { name: "回測績效" }));
    expect(await screen.findByTestId("backtest-results-view")).toBeTruthy();
    expect(toolbarClasses()).toEqual(["language-control", "theme-control"]);
    expect(container.querySelector(".run-strip")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "參數研究" }));
    await screen.findAllByText("candidate-epoch-a");
    expect(container.querySelectorAll(".epoch-control")).toHaveLength(1);
    expect(toolbarClasses()[0]).toBe("epoch-control");
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

});
