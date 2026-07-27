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
  EChart: ({ ariaLabel }: { ariaLabel: string }) => (
    <div aria-label={ariaLabel} />
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

describe("GA visualization state", () => {
  beforeEach(() => {
    vi.clearAllMocks();
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

    fireEvent.change(screen.getByLabelText("演化批次"), {
      target: { value: "epoch-b" }
    });

    expect(screen.queryByText("candidate-epoch-a")).toBeNull();
    await waitFor(() =>
      expect(api.loadGenerationSummaries).toHaveBeenCalledWith("epoch-b")
    );

    epochBSummaries.resolve([summary]);
    await screen.findAllByText("candidate-epoch-b");
  });
});
