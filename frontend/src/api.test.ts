import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ensureBrowserSession,
  loadStrategyStates,
  sendStrategyCommand,
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

describe("strategy control API", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
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
