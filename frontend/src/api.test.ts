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
  loadBacktestResult,
  loadBacktestResultTrades,
  loadBacktestResultCandles,
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

const resultDetail = {
  job_id: "run/one",
  strategy_id: "strategy-1",
  subject_id: "subject-1",
  dataset_id: "dataset-1",
  product_id: "CME:MNQ",
  timeframe: "5m",
  started_at: "2026-01-01T00:00:00.000Z",
  ended_at: "2026-01-02T00:00:00.000Z",
  currency: "USD",
  metrics: {
    net_pnl: "12345678901234567890.00000000000000000001",
    return_pct: "0.00000000000000000000000002",
    max_drawdown: "0.00000000000000000000000003",
    sharpe: "1.25",
    sortino: "1.5",
    calmar: "2.0"
  },
  equity: [{ timestamp: "2026-01-01T00:00:00.000Z", equity: "100.00", drawdown: "0" }],
  monthly_returns: [{ month: "2026-01", return_pct: "0.00000000000000000000000002" }],
  pnl_distribution: [{ lower: null, upper: "0.00000000000000000000000001", count: 1 }],
  trade_page: {
    items: [{
      id: "run/one:0",
      entry_time: "2026-01-01T00:00:00.000Z",
      exit_time: "2026-01-01T00:05:00.000Z",
      entry_price: "100.00000000000000000000000001",
      exit_price: "101.00",
      side: "LONG",
      quantity: "0.125",
      pnl: "0.00000000000000000000000001",
      fee: "0.00000000000000000000000002"
    }],
    total_count: 1,
    next_cursor: null
  },
  input_digest: "input-digest",
  result_digest: "result-digest",
  revision: 1
};

function detailWithField(path: string, value: unknown): Record<string, unknown> {
  const copy = JSON.parse(JSON.stringify(resultDetail)) as Record<string, unknown>;
  const fields = path.split(".");
  let parent = copy;
  while (fields.length > 1) {
    parent = parent[fields.shift()!] as Record<string, unknown>;
  }
  parent[fields[0]] = value;
  return copy;
}

function detailWithoutField(path: string): Record<string, unknown> {
  const copy = JSON.parse(JSON.stringify(resultDetail)) as Record<string, unknown>;
  const fields = path.split(".");
  let parent = copy;
  while (fields.length > 1) {
    parent = parent[fields.shift()!] as Record<string, unknown>;
  }
  delete parent[fields[0]];
  return copy;
}

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

  it("loads the exact detail DTO through one encoded job ID segment", async () => {
    const fetch = vi.fn().mockResolvedValue(response(200, resultDetail));
    vi.stubGlobal("fetch", fetch);

    await expect(loadBacktestResult("job/one")).resolves.toEqual(resultDetail);
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/backtest-results/job%2Fone",
      expect.objectContaining({ credentials: "include", cache: "no-store" })
    );
  });

  it("accepts empty nested arrays and an empty trade page", async () => {
    const payload = {
      ...resultDetail,
      equity: [],
      monthly_returns: [],
      pnl_distribution: [],
      trade_page: { items: [], total_count: 0, next_cursor: null }
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, payload)));
    await expect(loadBacktestResult("job/one")).resolves.toEqual(payload);
  });

  it.each([
    "job_id", "strategy_id", "subject_id", "dataset_id", "product_id",
    "timeframe", "started_at", "ended_at", "currency", "input_digest",
    "result_digest", "metrics.net_pnl", "metrics.return_pct",
    "metrics.max_drawdown", "metrics.sharpe", "metrics.sortino", "metrics.calmar",
    "equity.0.timestamp", "equity.0.equity", "equity.0.drawdown",
    "monthly_returns.0.month", "monthly_returns.0.return_pct",
    "pnl_distribution.0.count", "trade_page.items.0.id",
    "trade_page.items.0.entry_time", "trade_page.items.0.exit_time",
    "trade_page.items.0.entry_price", "trade_page.items.0.exit_price",
    "trade_page.items.0.side", "trade_page.items.0.quantity",
    "trade_page.items.0.pnl", "trade_page.items.0.fee"
  ])("rejects a detail with invalid or missing %s", async (path) => {
    const payload = detailWithoutField(path);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, payload)));
    await expect(loadBacktestResult("run/one")).rejects.toThrow("invalid_response");
  });

  it.each([
    ["metrics", null],
    ["equity", null],
    ["monthly_returns", null],
    ["pnl_distribution", null],
    ["metrics.net_pnl", 1],
    ["pnl_distribution.0.lower", false],
    ["pnl_distribution.0.upper", 1],
    ["pnl_distribution.0.count", 1.5],
    ["trade_page", null],
    ["trade_page.items", null],
    ["trade_page.total_count", "1"],
    ["trade_page.next_cursor", 1],
    ["revision", 2]
  ])("rejects a detail with malformed %s", async (path, value) => {
    const payload = detailWithField(path, value);
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, payload)));
    await expect(loadBacktestResult("run/one")).rejects.toThrow("invalid_response");
  });

  it.each([
    [401, "browser_session_required"],
    [403, "forbidden"],
    [404, "result_not_found"],
    [409, "result_unavailable"],
    [422, "validation_error"],
    [503, "browser_result_backend_unavailable"]
  ])("preserves detail HTTP %s error status", async (status, error) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(status, { error })));
    await expect(loadBacktestResult("run/one")).rejects.toMatchObject({ status, message: error });
  });

  it("rejects malformed detail JSON without exposing parser details", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true, status: 200, statusText: "", json: async () => { throw new Error("private detail"); }
    }));
    await expect(loadBacktestResult("run/one")).rejects.toMatchObject({
      status: 200, message: "invalid_response"
    });
  });

  it("loads the supplemental trade page with its opaque cursor", async () => {
    const payload = {
      items: [resultDetail.trade_page.items[0]],
      next_cursor: "opaque+/cursor=1",
      revision: 1
    };
    const fetch = vi.fn().mockResolvedValue(response(200, payload));
    vi.stubGlobal("fetch", fetch);
    await expect(
      loadBacktestResultTrades("job/one", { limit: 25, cursor: "opaque+/cursor=1" })
    ).resolves.toEqual(payload);
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/backtest-results/job%2Fone/trades?limit=25&cursor=opaque%2B%2Fcursor%3D1",
      expect.objectContaining({ credentials: "include", cache: "no-store" })
    );
  });

  it("loads a candle page for the exact requested half-open range", async () => {
    const payload = {
      items: [{
        timestamp: "2026-01-01T00:00:00.000Z",
        open: "12345678901234567890.000000000000000001",
        high: "2", low: "3", close: "4", volume: "5"
      }],
      next_cursor: null,
      revision: 1
    };
    const fetch = vi.fn().mockResolvedValue(response(200, payload));
    vi.stubGlobal("fetch", fetch);
    await expect(loadBacktestResultCandles("job/one", {
      start: 1767225600000, end: 1767229200000, limit: 25, cursor: "opaque+/cursor=1"
    })).resolves.toEqual(payload);
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/backtest-results/job%2Fone/candles?start=1767225600000&end=1767229200000&limit=25&cursor=opaque%2B%2Fcursor%3D1",
      expect.objectContaining({ credentials: "include", cache: "no-store" })
    );
  });

  it.each([
    ["trade id", "id"], ["trade entry time", "entry_time"],
    ["trade exit time", "exit_time"], ["trade entry price", "entry_price"],
    ["trade exit price", "exit_price"], ["trade side", "side"],
    ["trade quantity", "quantity"], ["trade PnL", "pnl"], ["trade fee", "fee"]
  ])("rejects a supplemental trade page with an invalid %s", async (_label, field) => {
    const payload = {
      items: [{ ...resultDetail.trade_page.items[0], [field]: null }],
      next_cursor: null,
      revision: 1
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, payload)));
    await expect(loadBacktestResultTrades("job", {})).rejects.toThrow("invalid_response");
  });

  it.each([
    "id", "entry_time", "exit_time", "entry_price", "exit_price",
    "side", "quantity", "pnl", "fee"
  ])("rejects a supplemental trade missing %s", async (field) => {
    const item = Object.fromEntries(
      Object.entries(resultDetail.trade_page.items[0]).filter(([key]) => key !== field)
    );
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, {
      items: [item], next_cursor: null, revision: 1
    })));
    await expect(loadBacktestResultTrades("job", {})).rejects.toThrow("invalid_response");
  });

  it.each([
    ["items", { items: null, next_cursor: null, revision: 1 }],
    ["missing items", { next_cursor: null, revision: 1 }],
    ["cursor", { items: [], next_cursor: 1, revision: 1 }],
    ["missing cursor", { items: [], revision: 1 }],
    ["revision", { items: [], next_cursor: null, revision: 2 }],
    ["missing revision", { items: [], next_cursor: null }]
  ])("rejects malformed supplemental trade page %s", async (_name, payload) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, payload)));
    await expect(loadBacktestResultTrades("job", {})).rejects.toThrow("invalid_response");
  });

  it.each([
    "timestamp", "open", "high", "low", "close", "volume"
  ])("rejects a candle page with invalid %s", async (field) => {
    const item = {
      timestamp: "2026-01-01T00:00:00.000Z",
      open: "1", high: "2", low: "3", close: "4", volume: "5"
    };
    const payload = {
      items: [{ ...item, [field]: null }], next_cursor: null, revision: 1
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, payload)));
    await expect(loadBacktestResultCandles("job", { start: 0, end: 1 }))
      .rejects.toThrow("invalid_response");
  });

  it.each([
    "timestamp", "open", "high", "low", "close", "volume"
  ])("rejects a candle missing %s", async (field) => {
    const candle = {
      timestamp: "2026-01-01T00:00:00.000Z",
      open: "1", high: "2", low: "3", close: "4", volume: "5"
    };
    const item = Object.fromEntries(
      Object.entries(candle).filter(([key]) => key !== field)
    );
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, {
      items: [item], next_cursor: null, revision: 1
    })));
    await expect(loadBacktestResultCandles("job", { start: 0, end: 1 }))
      .rejects.toThrow("invalid_response");
  });

  it.each([
    ["items", { items: null, next_cursor: null, revision: 1 }],
    ["missing items", { next_cursor: null, revision: 1 }],
    ["cursor", { items: [], next_cursor: 1, revision: 1 }],
    ["missing cursor", { items: [], revision: 1 }],
    ["revision", { items: [], next_cursor: null, revision: 2 }],
    ["missing revision", { items: [], next_cursor: null }]
  ])("rejects malformed candle page %s", async (_name, payload) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(200, payload)));
    await expect(loadBacktestResultCandles("job", { start: 0, end: 1 }))
      .rejects.toThrow("invalid_response");
  });

  it("accepts empty supplemental pages and preserves candle strings", async () => {
    const empty = { items: [], next_cursor: null, revision: 1 };
    const candle = {
      timestamp: "2026-01-01T00:00:00.000Z",
      open: "999999999999999999999999.000000000000000001",
      high: "2", low: "3", close: "4", volume: "5"
    };
    const fetch = vi.fn()
      .mockResolvedValueOnce(response(200, empty))
      .mockResolvedValueOnce(response(200, { ...empty, items: [candle] }));
    vi.stubGlobal("fetch", fetch);
    await expect(loadBacktestResultTrades("job")).resolves.toEqual(empty);
    await expect(loadBacktestResultCandles("job", { start: 0, end: 1 }))
      .resolves.toEqual({ ...empty, items: [candle] });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("preserves a candle HTTP validation error and makes one request", async () => {
    const fetch = vi.fn().mockResolvedValue(response(400, { error: "validation_error" }));
    vi.stubGlobal("fetch", fetch);
    await expect(loadBacktestResultCandles("job", { start: 2, end: 1 }))
      .rejects.toMatchObject({ status: 400, message: "validation_error" });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("does not retry a supplemental trade network failure", async () => {
    const fetch = vi.fn().mockRejectedValue(new Error("network unavailable"));
    vi.stubGlobal("fetch", fetch);
    await expect(loadBacktestResultTrades("job")).rejects.toThrow("network unavailable");
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("sanitizes malformed candle JSON without retrying", async () => {
    const fetch = vi.fn().mockResolvedValue({
      ok: true, status: 200, statusText: "", json: async () => { throw new Error("private parser detail"); }
    });
    vi.stubGlobal("fetch", fetch);
    await expect(loadBacktestResultCandles("job", { start: 0, end: 1 }))
      .rejects.toMatchObject({ status: 200, message: "invalid_response" });
    expect(fetch).toHaveBeenCalledTimes(1);
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
