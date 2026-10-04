import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ensureBrowserSession,
  loadStrategyStates,
  loadEpochs,
  loadGenerationSummaries,
  loadGenerationGenes,
  sendStrategyCommand,
  loadKillSwitchStatus,
  clearKillSwitch,
  loadBacktestResultsIndex,
  type BrowserSession,
  type StrategyState
} from "./api";

function response(status: number, body: object): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: "",
    json: async () => body
  } as Response;
}

const session: BrowserSession = {
  actor: "operator@example.com",
  capabilities: [],
  permissions: { can_mutate: true, can_step_up: true },
  csrf_token: "csrf-token",
  expires_at: "2026-07-29T12:00:00Z",
  step_up_expires_at: null
};

const strategy: StrategyState = {
  strategy_id: "test.py::ActiveStrategy",
  status: "ACTIVE",
  config: {},
  performance: {},
  last_heartbeat: null,
  uptime_start: null,
  last_error_message: null,
  entered_error_at: null,
  recovered_at: null,
  stopped_at: null,
  version: 1,
  available_commands: ["STOP"]
};

const resultIndexItem = {
  job_id: "run/one",
  subject_id: "subject-1",
  dataset_id: "dataset-1",
  product_id: "CME:MNQ",
  timeframe: "5m",
  started_at: "2026-01-01T00:00:00.000Z",
  ended_at: "2026-01-02T00:00:00.000Z",
  completed_at: "2026-01-02T00:00:01.000Z",
  result_digest: "digest"
};

describe("strategy control API", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("validates no-store kill-switch status and sends the confirmed CSRF clear shape", async () => {
    const fetch = vi.fn()
      .mockResolvedValueOnce(response(200, { state: "LOCKDOWN", redis_state: "LOCKDOWN", durable_state: "OK", listener_available: true }))
      .mockResolvedValueOnce(response(202, { status: "accepted" }));
    vi.stubGlobal("fetch", fetch);
    await expect(loadKillSwitchStatus()).resolves.toMatchObject({ state: "LOCKDOWN" });
    await clearKillSwitch("csrf-token");
    expect(fetch).toHaveBeenNthCalledWith(1, "/ops/kill-switch", expect.objectContaining({ cache: "no-store", credentials: "include" }));
    expect(fetch).toHaveBeenNthCalledWith(2, "/ops/kill-switch/clear", expect.objectContaining({
      method: "POST", headers: expect.objectContaining({ "X-CSRF-Token": "csrf-token" }), body: JSON.stringify({ confirm: true })
    }));
  });

  it("returns an existing browser session", async () => {
    const fetch = vi.fn().mockResolvedValue(response(200, session));
    vi.stubGlobal("fetch", fetch);

    await expect(ensureBrowserSession()).resolves.toEqual(session);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("creates a browser session after an unauthorized read", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(response(401, { error: "browser_session_required" }))
      .mockResolvedValueOnce(response(201, session));
    vi.stubGlobal("fetch", fetch);

    await expect(ensureBrowserSession()).resolves.toEqual(session);
    expect(fetch).toHaveBeenNthCalledWith(
      2,
      "/api/v1/auth/session",
      expect.objectContaining({ method: "POST", credentials: "include" })
    );
  });

  it("loads the complete bounded strategy state page", async () => {
    const fetch = vi.fn().mockResolvedValue(
      response(200, {
        states: [strategy],
        total: 1,
        limit: 500,
        offset: 0
      })
    );
    vi.stubGlobal("fetch", fetch);

    await expect(loadStrategyStates()).resolves.toEqual([strategy]);
    expect(fetch).toHaveBeenCalledWith(
      "/strategy-states?limit=500&offset=0",
      expect.objectContaining({ credentials: "include" })
    );
  });

  it("loads every strategy state page without truncation", async () => {
    const next = { ...strategy, strategy_id: "second" };
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        response(200, {
          states: [strategy],
          total: 501,
          limit: 500,
          offset: 0
        })
      )
      .mockResolvedValueOnce(
        response(200, {
          states: [next],
          total: 501,
          limit: 500,
          offset: 500
        })
      );
    vi.stubGlobal("fetch", fetch);

    await expect(loadStrategyStates()).resolves.toEqual([strategy, next]);
    expect(fetch).toHaveBeenNthCalledWith(
      2,
      "/strategy-states?limit=500&offset=500",
      expect.objectContaining({ credentials: "include" })
    );
  });

  it("encodes the strategy id and sends the in-memory CSRF token", async () => {
    const fetch = vi.fn().mockResolvedValue(
      response(202, { result: { success: true, accepted: true } })
    );
    vi.stubGlobal("fetch", fetch);

    await sendStrategyCommand(
      "test.py::ActiveStrategy",
      "STOP",
      1,
      "strategy-command-1",
      "csrf-token"
    );

    expect(fetch).toHaveBeenCalledWith(
      "/strategies/test.py%3A%3AActiveStrategy/commands",
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        headers: expect.objectContaining({
          Accept: "application/json",
          "Content-Type": "application/json",
          "Idempotency-Key": "strategy-command-1",
          "X-CSRF-Token": "csrf-token"
        }),
        body: '{"command":"STOP","expected_version":1}'
      })
    );
  });
});

describe("backtest results API", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("accepts an empty index page", async () => {
    const payload = { items: [], next_cursor: null, revision: 1 };
    const fetch = vi.fn().mockResolvedValue(response(200, payload));
    vi.stubGlobal("fetch", fetch);

    await expect(loadBacktestResultsIndex()).resolves.toEqual(payload);
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/backtest-results",
      expect.objectContaining({ credentials: "include", cache: "no-store" })
    );
  });

  it("passes an index cursor through as one encoded query value", async () => {
    const payload = { items: [resultIndexItem], next_cursor: null, revision: 1 };
    const fetch = vi.fn().mockResolvedValue(response(200, payload));
    vi.stubGlobal("fetch", fetch);

    await expect(
      loadBacktestResultsIndex({ limit: 25, cursor: "opaque.cursor_1" })
    ).resolves.toEqual(payload);
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/backtest-results?limit=25&cursor=opaque.cursor_1",
      expect.objectContaining({ credentials: "include", cache: "no-store" })
    );
  });

  it.each([
    "job_id",
    "subject_id",
    "dataset_id",
    "product_id",
    "timeframe",
    "started_at",
    "ended_at",
    "completed_at",
    "result_digest"
  ])("rejects an index item with invalid %s", async (field) => {
    const item = { ...resultIndexItem, [field]: null };
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        response(200, { items: [item], next_cursor: null, revision: 1 })
      )
    );
    await expect(loadBacktestResultsIndex()).rejects.toThrow("invalid_response");
  });

  it.each([
    ["items", { items: null, next_cursor: null, revision: 1 }],
    ["cursor", { items: [], next_cursor: 1, revision: 1 }],
    ["revision", { items: [], next_cursor: null, revision: 2 }]
  ])("rejects malformed successful index %s payloads", async (_name, payload) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, payload)));
    await expect(loadBacktestResultsIndex()).rejects.toThrow("invalid_response");
  });

  it.each([
    [401, "browser_session_required"],
    [403, "forbidden"],
    [404, "result_not_found"],
    [409, "result_unavailable"],
    [422, "validation_error"],
    [503, "browser_result_backend_unavailable"]
  ])("retains HTTP %s and its fixed error code", async (status, error) => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(response(status, { error }))
    );
    await expect(loadBacktestResultsIndex()).rejects.toMatchObject({
      status,
      message: error
    });
  });

  it("does not expose malformed JSON details from a successful response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        statusText: "",
        json: async () => {
          throw new Error("private detail");
        }
      })
    );
    await expect(loadBacktestResultsIndex()).rejects.toMatchObject({
      status: 200, message: "invalid_response"
    });
  });
});


describe("read-only response boundaries", () => {
  afterEach(() => vi.unstubAllGlobals());
  for (const [name, key, load] of [
    ["strategies", "states", loadStrategyStates],
    ["epochs", "epochs", loadEpochs],
    ["generations", "generations", () => loadGenerationSummaries("epoch")],
    ["genes", "genes", () => loadGenerationGenes("epoch", 0)]
  ] as const) {
    it.each([null, {}, [null], ["invalid"]])(`${name} rejects invalid rows %j before rendering`, async (items) => {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, {
        total: 1, limit: 500, offset: 0, [key]: items
      })));
      await expect(load()).rejects.toThrow("invalid_response");
    });
    it(`${name} accepts an empty read-only page`, async () => {
      vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, {
        total: 0, limit: 500, offset: 0, [key]: []
      })));
      await expect(load()).resolves.toEqual([]);
    });
  }
  it.each([401, 403, 503])("keeps HTTP status %s when an error body is not JSON", async (status) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false, status, json: async () => { throw new SyntaxError("SENSITIVE_SENTINEL"); }
    }));
    await expect(loadEpochs()).rejects.toMatchObject({ status, message: "invalid_response" });
  });
  it("rejects invalid pagination before allocating follow-up requests", async () => {
    const fetch = vi.fn().mockResolvedValue(response(200, {
      total: "invalid", limit: 500, offset: 0, states: []
    }));
    vi.stubGlobal("fetch", fetch);
    await expect(loadStrategyStates()).rejects.toThrow("invalid_response");
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});


it("keeps a malformed mutation response ambiguous instead of authorizing a retry", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
    ok: false, status: 403, json: async () => { throw new SyntaxError("invalid"); }
  }));
  try {
    await expect(sendStrategyCommand("s", "STOP", 1, "test-operation")).rejects.toThrow("invalid_response");
    await expect(sendStrategyCommand("s", "STOP", 1, "test-operation")).rejects.not.toHaveProperty("status");
  } finally {
    vi.unstubAllGlobals();
  }
});


it("accepts the backend BigInteger seed range without using seed for arithmetic", async () => {
  const epoch = {
    id: "epoch", strategy_id: "strategy", started_at: "2026-01-01T00:00:00Z",
    finished_at: null, pop_size: 1, max_generations: 1, generations_run: 0,
    best_score: null, seed: 2 ** 53, config_json: {}, status: "running",
    eval_pair: "TEST", eval_timeframe: "1m", eval_start_date: "2026-01-01", eval_end_date: "2026-01-02"
  };
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, {
    total: 1, limit: 100, offset: 0, epochs: [epoch]
  })));
  try {
    await expect(loadEpochs()).resolves.toEqual([epoch]);
  } finally {
    vi.unstubAllGlobals();
  }
});
