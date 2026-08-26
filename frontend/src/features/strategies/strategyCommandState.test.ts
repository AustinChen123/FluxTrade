import { describe, expect, it } from "vitest";

import type { StrategyState } from "../../api";
import {
  isDefiniteCommandRejection,
  parseAwaitingStrategies,
  serializeAwaitingStrategies,
  transitionStrategyCommandState,
  type StrategyCommandStateEvent
} from "./strategyCommandState";

function strategy(
  strategyId: string,
  status: StrategyState["status"] = "ACTIVE",
  version = 3
): StrategyState {
  return {
    strategy_id: strategyId,
    status,
    config: {},
    performance: {},
    last_heartbeat: null,
    uptime_start: null,
    last_error_message: null,
    entered_error_at: null,
    recovered_at: null,
    stopped_at: null,
    version,
    available_commands: []
  };
}

describe("strategy command state", () => {
  it.each([null, "", "{", "{}", '"value"', "1", "true"])(
    "treats unavailable or malformed top-level storage as empty: %s",
    (serialized) => {
      expect([...parseAwaitingStrategies(serialized)]).toEqual([]);
    }
  );

  it("filters entries independently without normalizing accepted snapshots", () => {
    const serialized = JSON.stringify([
      ["alpha", { extra: "keep", version: 1, status: "ACTIVE" }, "discard-me"],
      [3, { status: "ACTIVE", version: 1 }],
      ["missing-status", { version: 1 }],
      ["unknown-status", { status: "FUTURE", version: 2 }],
      ["", { status: "WARNING", version: 3 }]
    ]);

    const parsed = parseAwaitingStrategies(serialized);

    expect([...parsed.keys()]).toEqual(["alpha", "unknown-status", ""]);
    expect(serializeAwaitingStrategies(parsed)).toBe(
      '[["alpha",{"extra":"keep","version":1,"status":"ACTIVE"}],["unknown-status",{"status":"FUTURE","version":2}],["",{"status":"WARNING","version":3}]]'
    );
  });

  it("keeps a duplicate ID at its first insertion position with its last value", () => {
    const parsed = parseAwaitingStrategies(
      '[["alpha",{"status":"ACTIVE","version":1}],["beta",{"status":"STOPPED","version":2}],["alpha",{"status":"WARNING","version":3}]]'
    );

    expect([...parsed.keys()]).toEqual(["alpha", "beta"]);
    expect(serializeAwaitingStrategies(parsed)).toBe(
      '[["alpha",{"status":"WARNING","version":3}],["beta",{"status":"STOPPED","version":2}]]'
    );
  });

  it("serializes exact insertion and property order and removes empty storage", () => {
    expect(
      serializeAwaitingStrategies(
        new Map([
          ["alpha", { status: "ACTIVE", version: 3 }],
          ["beta", { status: "STOPPED", version: 4 }]
        ])
      )
    ).toBe(
      '[["alpha",{"status":"ACTIVE","version":3}],["beta",{"status":"STOPPED","version":4}]]'
    );
    expect(serializeAwaitingStrategies(new Map())).toBeNull();
  });

  it("exhaustively preserves or changes locks for every command event", () => {
    const initial = new Map([
      ["alpha", { status: "ACTIVE" as const, version: 3 }]
    ]);
    const unchangedEvents: StrategyCommandStateEvent[] = [
      { type: "accepted" },
      { type: "ambiguous_failure" },
      { type: "refresh_failure" }
    ];
    for (const event of unchangedEvents) {
      expect(transitionStrategyCommandState(initial, event)).toBe(initial);
    }

    expect(
      [...
        transitionStrategyCommandState(initial, {
          type: "command_started",
          strategy: strategy("beta", "STOPPED", 4)
        })
      ]
    ).toEqual([
      ["alpha", { status: "ACTIVE", version: 3 }],
      ["beta", { status: "STOPPED", version: 4 }]
    ]);
    expect(
      [...
        transitionStrategyCommandState(initial, {
          type: "definite_rejection",
          strategyId: "alpha"
        })
      ]
    ).toEqual([]);
  });

  it.each([
    ["unchanged", strategy("alpha", "ACTIVE", 3), true],
    ["changed status", strategy("alpha", "STOPPED", 3), false],
    ["changed version", strategy("alpha", "ACTIVE", 4), false],
    ["missing", strategy("beta", "ACTIVE", 3), true]
  ])("reconciles an %s authoritative row", (_name, row, retained) => {
    const current = new Map([
      ["alpha", { status: "ACTIVE" as const, version: 3 }]
    ]);
    const next = transitionStrategyCommandState(current, {
      type: "authoritative_snapshot",
      strategies: [row]
    });
    expect(next.has("alpha")).toBe(retained);
  });

  it.each([
    [{ type: "api", message: "bad_request", status: 400 }, true],
    [{ type: "api", message: "request_timeout", status: 408 }, false],
    [{ type: "api", message: "client_failure", status: 499 }, true],
    [{ type: "api", message: "server_failure", status: 500 }, false],
    [
      {
        type: "api",
        message: "strategy_engine_listener_unavailable",
        status: 503
      },
      true
    ],
    [{ type: "unknown" }, false]
  ] as const)("classifies command failure %s", (failure, definite) => {
    expect(isDefiniteCommandRejection(failure)).toBe(definite);
  });
});
