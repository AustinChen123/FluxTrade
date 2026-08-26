// @vitest-environment jsdom

import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { Epoch, Gene, GenerationSummary } from "../../api";
import { useResearchWorkspace } from "./useResearchWorkspace";

const api = vi.hoisted(() => ({
  ensureBrowserSession: vi.fn(),
  loadEpochs: vi.fn(),
  loadGenerationGenes: vi.fn(),
  loadGenerationSummaries: vi.fn()
}));

vi.mock("../../api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api")>()),
  ...api
}));

function epoch(id: string): Epoch {
  return {
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
  };
}

const summary: GenerationSummary = {
  generation_index: 0,
  candidate_count: 1,
  score_min: "1",
  score_max: "1",
  drawdown_min: "0.1",
  drawdown_max: "0.1"
};

function gene(epochId: string): Gene {
  return {
    id: epochId === "a" ? 1 : 2,
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
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

describe("useResearchWorkspace", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    api.ensureBrowserSession.mockResolvedValue(undefined);
    api.loadEpochs.mockResolvedValue([epoch("a"), epoch("b")]);
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockImplementation((epochId: string) =>
      Promise.resolve([gene(epochId)])
    );
  });

  it("loads session, epochs, summaries, and genes in authoritative order", async () => {
    const calls: string[] = [];
    api.ensureBrowserSession.mockImplementation(async () => {
      calls.push("session");
    });
    api.loadEpochs.mockImplementation(async () => {
      calls.push("epochs");
      return [epoch("a")];
    });
    api.loadGenerationSummaries.mockImplementation(async () => {
      calls.push("summaries");
      return [summary];
    });
    api.loadGenerationGenes.mockImplementation(async () => {
      calls.push("genes");
      return [gene("a")];
    });

    const { result } = renderHook(() => useResearchWorkspace(false));

    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));
    expect(calls).toEqual(["session", "epochs", "summaries", "genes"]);
    expect(result.current.selectedGeneId).toBe(1);
    expect(result.current.xParameter).toBe("fast");
    expect(result.current.yParameter).toBe("slow");
  });

  it("fences a stale summary response after an epoch transition", async () => {
    const stale = deferred<GenerationSummary[]>();
    api.loadGenerationSummaries.mockImplementation((epochId: string) =>
      epochId === "a" ? stale.promise : Promise.resolve([summary])
    );
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.epochId).toBe("a"));

    act(() => result.current.chooseEpoch("b"));
    await waitFor(() => expect(result.current.genes).toEqual([gene("b")]));
    act(() => stale.resolve([{ ...summary, generation_index: 99 }]));

    await waitFor(() => expect(result.current.epochId).toBe("b"));
    expect(result.current.summaries).toEqual([summary]);
    expect(result.current.generationIndex).toBe(0);
  });

  it("ignores a stale summary failure after the replacement epoch is ready", async () => {
    const stale = deferred<GenerationSummary[]>();
    api.loadGenerationSummaries.mockImplementation((epochId: string) =>
      epochId === "a" ? stale.promise : Promise.resolve([summary])
    );
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.epochId).toBe("a"));

    act(() => result.current.chooseEpoch("b"));
    await waitFor(() => expect(result.current.genes).toEqual([gene("b")]));
    act(() => stale.reject(new Error("stale summary")));

    await waitFor(() => expect(result.current.epochId).toBe("b"));
    expect(result.current.error).toBeNull();
    expect(result.current.summaries).toEqual([summary]);
    expect(result.current.genes).toEqual([gene("b")]);
    expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(2);
  });

  it("fences a stale gene response after a generation transition", async () => {
    const stale = deferred<Gene[]>();
    api.loadGenerationSummaries.mockResolvedValue([
      summary,
      { ...summary, generation_index: 1 }
    ]);
    api.loadGenerationGenes.mockImplementation(
      (_epochId: string, generationIndex: number) =>
        generationIndex === 1 ? stale.promise : Promise.resolve([gene("a")])
    );
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.generationIndex).toBe(1));

    act(() => result.current.chooseGeneration(0));
    expect(result.current.genes).toEqual([]);
    expect(result.current.selectedGeneId).toBeNull();
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));
    act(() => stale.resolve([{ ...gene("b"), generation_index: 1 }]));

    await waitFor(() => expect(result.current.generationIndex).toBe(0));
    expect(result.current.genes).toEqual([gene("a")]);
    expect(result.current.selectedGeneId).toBe(1);
  });

  it("ignores a stale gene failure after the replacement generation is ready", async () => {
    const stale = deferred<Gene[]>();
    api.loadGenerationSummaries.mockResolvedValue([
      summary,
      { ...summary, generation_index: 1 }
    ]);
    api.loadGenerationGenes.mockImplementation(
      (_epochId: string, generationIndex: number) =>
        generationIndex === 1 ? stale.promise : Promise.resolve([gene("a")])
    );
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.generationIndex).toBe(1));

    act(() => result.current.chooseGeneration(0));
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));
    act(() => stale.reject(new Error("stale genes")));

    await waitFor(() => expect(result.current.generationIndex).toBe(0));
    expect(result.current.error).toBeNull();
    expect(result.current.genes).toEqual([gene("a")]);
    expect(result.current.selectedGeneId).toBe(1);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(2);
  });

  it("retries an initial failure without duplicating a successful epoch load", async () => {
    api.loadEpochs
      .mockRejectedValueOnce(new Error("temporary"))
      .mockResolvedValueOnce([epoch("a")]);
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.error).toBeInstanceOf(Error));

    act(() => result.current.retry());
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(2);
    expect(api.loadEpochs).toHaveBeenCalledTimes(2);

    act(() => result.current.retry());
    await Promise.resolve();
    expect(api.loadEpochs).toHaveBeenCalledTimes(2);
  });

  it("retries only the failed gene stage without refetching its summary", async () => {
    api.loadGenerationGenes
      .mockRejectedValueOnce(new Error("gene unavailable"))
      .mockResolvedValueOnce([gene("a")]);
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.error).toBeInstanceOf(Error));

    act(() => result.current.retry());
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));

    expect(api.loadEpochs).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationGenes).toHaveBeenCalledTimes(2);
  });

  it("ignores a response that arrives after the owner unmounts", async () => {
    const pending = deferred<Epoch[]>();
    api.loadEpochs.mockReturnValue(pending.promise);
    const { unmount } = renderHook(() => useResearchWorkspace(false));
    unmount();

    act(() => pending.resolve([epoch("a")]));
    await Promise.resolve();
    expect(api.loadGenerationSummaries).not.toHaveBeenCalled();
  });

  it("uses the deterministic demo ledger without browser-session or API I/O", async () => {
    const { result } = renderHook(() => useResearchWorkspace(true));

    await waitFor(() => expect(result.current.genes.length).toBeGreaterThan(0));
    expect(api.ensureBrowserSession).not.toHaveBeenCalled();
    expect(api.loadEpochs).not.toHaveBeenCalled();
    expect(api.loadGenerationSummaries).not.toHaveBeenCalled();
    expect(api.loadGenerationGenes).not.toHaveBeenCalled();
  });
});
