// @vitest-environment jsdom

import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  ApiError,
  type Epoch,
  type Gene,
  type GenerationSummary
} from "../../api";
import {
  classifyResearchError,
  useResearchWorkspace
} from "./useResearchWorkspace";

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
    vi.resetAllMocks();
    api.ensureBrowserSession.mockResolvedValue(undefined);
    api.loadEpochs.mockResolvedValue([epoch("a"), epoch("b")]);
    api.loadGenerationSummaries.mockResolvedValue([summary]);
    api.loadGenerationGenes.mockImplementation((epochId: string) =>
      Promise.resolve([gene(epochId)])
    );
  });

  it.each([
    [new ApiError("unauthorized", 401), { type: "unauthorized" }],
    [new ApiError("forbidden", 403), { type: "unauthorized" }],
    [
      new ApiError("service unavailable", 503),
      { type: "service", status: 503, message: "service unavailable" }
    ],
    [
      new Error("unexpected failure"),
      { type: "unexpected", message: "unexpected failure" }
    ],
    ["opaque failure", { type: "fallback" }]
  ])(
    "projects research failure %s without exposing the API boundary to the view",
    (reason, expected) => {
      expect(classifyResearchError(reason)).toEqual(expected);
    }
  );

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

  it("refreshes the complete snapshot while preserving valid selections", async () => {
    const generations = [summary, { ...summary, generation_index: 1 }];
    api.loadGenerationSummaries
      .mockResolvedValueOnce(generations)
      .mockResolvedValueOnce(generations.map((item) => ({ ...item, score_max: "2" })));
    api.loadGenerationGenes.mockImplementation(
      (_epochId: string, generationIndex: number) =>
        Promise.resolve([
          {
            ...gene("a"),
            id: 1,
            generation_index: generationIndex,
            score_total: api.loadGenerationGenes.mock.calls.length > 2 ? "9" : "1"
          },
          {
            ...gene("a"),
            id: 2,
            generation_index: generationIndex,
            score_total: api.loadGenerationGenes.mock.calls.length > 2 ? "8" : "2"
          }
        ])
    );
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.genes).toHaveLength(2));
    act(() => result.current.chooseGeneration(0));
    await waitFor(() => expect(result.current.generationIndex).toBe(0));
    act(() => {
      result.current.chooseGene(1);
      result.current.chooseXParameter("fast");
      result.current.chooseYParameter("slow");
    });
    await waitFor(() => expect(result.current.selectedGeneId).toBe(1));

    await act(async () => result.current.refresh());

    expect(api.loadEpochs).toHaveBeenCalledTimes(2);
    expect(api.loadGenerationSummaries).toHaveBeenLastCalledWith("a");
    expect(api.loadGenerationGenes).toHaveBeenLastCalledWith("a", 0);
    expect(result.current.epochId).toBe("a");
    expect(result.current.generationIndex).toBe(0);
    expect(result.current.selectedGeneId).toBe(1);
    expect(result.current.xParameter).toBe("fast");
    expect(result.current.yParameter).toBe("slow");
    expect(result.current.genes[0]?.score_total).toBe("9");
    expect(result.current.summaries[0]?.score_max).toBe("2");
  });

  it("keeps an initial gene request from overwriting a newer in-flight refresh", async () => {
    const initialGenes = deferred<Gene[]>();
    const refreshedGenes = deferred<Gene[]>();
    api.loadGenerationGenes
      .mockReturnValueOnce(initialGenes.promise)
      .mockReturnValueOnce(refreshedGenes.promise);
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(1));

    let refresh!: Promise<void>;
    act(() => { refresh = result.current.refresh(); });
    await waitFor(() => expect(api.loadGenerationGenes).toHaveBeenCalledTimes(2));
    expect(result.current.loading).toBe(true);

    await act(async () => {
      initialGenes.resolve([{ ...gene("a"), score_total: "1" }]);
      await Promise.resolve();
      await Promise.resolve();
    });
    const staleState = {
      genes: result.current.genes,
      loading: result.current.loading,
      error: result.current.error
    };
    await act(async () => {
      refreshedGenes.resolve([{ ...gene("a"), score_total: "9" }]);
      await refresh;
    });

    expect(staleState).toEqual({ genes: [], loading: true, error: null });
    expect(result.current.genes[0]?.score_total).toBe("9");
    expect(result.current.loading).toBe(false);
  });

  it("keeps a pending selection summary from replacing a newer refresh", async () => {
    const staleSummary = deferred<GenerationSummary[]>();
    let selectedEpochCalls = 0;
    api.loadGenerationSummaries.mockImplementation((epochId: string) => {
      if (epochId !== "b") return Promise.resolve([summary]);
      selectedEpochCalls += 1;
      return selectedEpochCalls === 1
        ? staleSummary.promise
        : Promise.resolve([{ ...summary, score_max: "9" }]);
    });
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));

    act(() => result.current.chooseEpoch("b"));
    await waitFor(() => expect(selectedEpochCalls).toBe(1));
    await act(async () => result.current.refresh());
    expect(result.current.summaries[0]?.score_max).toBe("9");

    await act(async () => {
      staleSummary.resolve([{ ...summary, score_max: "1" }]);
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(result.current.epochId).toBe("b");
    expect(result.current.summaries[0]?.score_max).toBe("9");
    expect(result.current.error).toBeNull();
  });

  it("ignores a pending selection failure after a newer refresh completes", async () => {
    const staleSummary = deferred<GenerationSummary[]>();
    let selectedEpochCalls = 0;
    api.loadGenerationSummaries.mockImplementation((epochId: string) => {
      if (epochId !== "b") return Promise.resolve([summary]);
      selectedEpochCalls += 1;
      return selectedEpochCalls === 1
        ? staleSummary.promise
        : Promise.resolve([{ ...summary, score_max: "9" }]);
    });
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));

    act(() => result.current.chooseEpoch("b"));
    await waitFor(() => expect(selectedEpochCalls).toBe(1));
    await act(async () => result.current.refresh());
    expect(result.current.summaries[0]?.score_max).toBe("9");

    await act(async () => {
      staleSummary.reject(new Error("obsolete selection summary"));
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(result.current.epochId).toBe("b");
    expect(result.current.summaries[0]?.score_max).toBe("9");
    expect(result.current.error).toBeNull();
    expect(result.current.loading).toBe(false);
  });

  it("retries an epoch-stage full-refresh failure even when epochs were loaded", async () => {
    const refreshedEpochs = [epoch("a"), epoch("b")];
    api.loadEpochs
      .mockResolvedValueOnce([epoch("a"), epoch("b")])
      .mockRejectedValueOnce(new Error("refresh epochs failed"))
      .mockResolvedValueOnce(refreshedEpochs);
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));

    await act(async () => result.current.refresh());
    expect(api.loadEpochs).toHaveBeenCalledTimes(2);
    act(() => result.current.retry());
    await waitFor(() => expect(api.loadEpochs).toHaveBeenCalledTimes(3));

    expect(result.current.epochs).toEqual(refreshedEpochs);
    expect(result.current.ready).toBe(true);
  });

  it.each(["epochs", "summaries", "genes"] as const)(
    "retries a failed full refresh from the beginning after its %s stage",
    async (failedStage) => {
      const refreshedEpoch = { ...epoch("a"), best_score: "9" };
      const refreshedSummary = { ...summary, score_max: "9" };
      const refreshedGene = { ...gene("a"), score_total: "9" };
      const { result } = renderHook(() => useResearchWorkspace(false));
      await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));
      const calls: string[] = [];
      let failStageOnce = true;
      api.ensureBrowserSession.mockImplementation(async () => {
        calls.push("session");
      });
      api.loadEpochs.mockImplementation(async () => {
        calls.push("epochs");
        if (failedStage === "epochs" && failStageOnce) {
          failStageOnce = false;
          throw new Error("full refresh epochs failed");
        }
        return [refreshedEpoch];
      });
      api.loadGenerationSummaries.mockImplementation(async () => {
        calls.push("summaries");
        if (failedStage === "summaries" && failStageOnce) {
          failStageOnce = false;
          throw new Error("full refresh summaries failed");
        }
        return [refreshedSummary];
      });
      api.loadGenerationGenes.mockImplementation(async () => {
        calls.push("genes");
        if (failedStage === "genes" && failStageOnce) {
          failStageOnce = false;
          throw new Error("full refresh genes failed");
        }
        return [refreshedGene];
      });
      act(() => {
        result.current.chooseGene(1);
        result.current.chooseXParameter("fast");
        result.current.chooseYParameter("slow");
      });
      await waitFor(() => expect(result.current.selectedGeneId).toBe(1));
      calls.length = 0;

      await act(async () => result.current.refresh());

      expect(calls).toEqual(
        failedStage === "epochs"
          ? ["session", "epochs"]
          : failedStage === "summaries"
            ? ["session", "epochs", "summaries"]
            : ["session", "epochs", "summaries", "genes"]
      );
      expect(result.current.epochs[0]?.best_score).toBe("1");
      expect(result.current.summaries[0]?.score_max).toBe("1");
      expect(result.current.genes[0]?.score_total).toBe("1");
      expect(result.current.error).toEqual({
        type: "unexpected",
        message: `full refresh ${failedStage} failed`
      });

      calls.length = 0;
      act(() => result.current.retry());
      await waitFor(() => {
        expect(result.current.genes).toEqual([refreshedGene]);
        expect(result.current.error).toBeNull();
        expect(result.current.loading).toBe(false);
      });

      expect(calls).toEqual(["session", "epochs", "summaries", "genes"]);
      expect(result.current.epochs[0]).toEqual(refreshedEpoch);
      expect(result.current.summaries).toEqual([refreshedSummary]);
      expect(result.current.genes).toEqual([refreshedGene]);
      expect(result.current.selectedGeneId).toBe(1);
      expect(result.current.xParameter).toBe("fast");
      expect(result.current.yParameter).toBe("slow");
      expect(result.current.loading).toBe(false);
    }
  );

  it("marks the workspace ready when a full refresh recovers initial epoch failure", async () => {
    api.loadEpochs
      .mockRejectedValueOnce(new Error("initial epochs failed"))
      .mockResolvedValueOnce([epoch("a")]);
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.error).toEqual({
      type: "unexpected",
      message: "initial epochs failed"
    }));
    expect(result.current.ready).toBe(false);

    await act(async () => result.current.refresh());

    expect(result.current.ready).toBe(true);
    expect(result.current.genes).toEqual([gene("a")]);
    expect(result.current.error).toBeNull();
  });

  it("retries the failed summary stage without repeating epoch admission", async () => {
    api.loadGenerationSummaries
      .mockRejectedValueOnce(new Error("summary retry"))
      .mockResolvedValueOnce([summary]);
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.error).toEqual({
      type: "unexpected",
      message: "summary retry"
    }));
    expect(api.loadEpochs).toHaveBeenCalledTimes(1);

    act(() => result.current.retry());
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));

    expect(api.loadEpochs).toHaveBeenCalledTimes(1);
    expect(api.loadGenerationSummaries).toHaveBeenCalledTimes(2);
  });

  it("does not commit a full-refresh response after the epoch selection changes", async () => {
    const staleEpochs = deferred<Epoch[]>();
    api.loadEpochs
      .mockResolvedValueOnce([epoch("a"), epoch("b")])
      .mockReturnValueOnce(staleEpochs.promise);
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));

    let refresh!: Promise<void>;
    act(() => { refresh = result.current.refresh(); });
    act(() => result.current.chooseEpoch("b"));
    await waitFor(() => expect(result.current.genes).toEqual([gene("b")]));
    await act(async () => {
      staleEpochs.resolve([{ ...epoch("a"), best_score: "999" }]);
      await refresh;
    });

    expect(result.current.epochId).toBe("b");
    expect(result.current.epoch?.best_score).toBe("1");
    expect(result.current.genes).toEqual([gene("b")]);
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
    await waitFor(() =>
      expect(result.current.error).toEqual({
        type: "unexpected",
        message: "temporary"
      })
    );

    act(() => result.current.retry());
    await waitFor(() => expect(result.current.genes).toEqual([gene("a")]));
    expect(api.ensureBrowserSession).toHaveBeenCalledTimes(2);
    expect(api.loadEpochs).toHaveBeenCalledTimes(2);

    act(() => result.current.retry());
    await Promise.resolve();
    expect(api.loadEpochs).toHaveBeenCalledTimes(2);
  });

  it("projects an API authorization failure before exposing workspace state", async () => {
    api.loadEpochs.mockRejectedValueOnce(new ApiError("forbidden", 403));

    const { result } = renderHook(() => useResearchWorkspace(false));

    await waitFor(() =>
      expect(result.current.error).toEqual({ type: "unauthorized" })
    );
    expect(result.current.epochs).toEqual([]);
    expect(api.loadGenerationSummaries).not.toHaveBeenCalled();
    expect(api.loadGenerationGenes).not.toHaveBeenCalled();
  });

  it("retries only the failed gene stage without refetching its summary", async () => {
    api.loadGenerationGenes
      .mockRejectedValueOnce(new Error("gene unavailable"))
      .mockResolvedValueOnce([gene("a")]);
    const { result } = renderHook(() => useResearchWorkspace(false));
    await waitFor(() =>
      expect(result.current.error).toEqual({
        type: "unexpected",
        message: "gene unavailable"
      })
    );

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
