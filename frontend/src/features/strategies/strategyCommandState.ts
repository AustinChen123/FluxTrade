import type { StrategyCommand, StrategyState } from "../../api";

export const AWAITING_STORAGE_KEY = "fluxtrade.strategy.awaiting";

export type StrategySnapshot = Pick<StrategyState, "status" | "version">;
export type StrategyRecord = StrategyState;
export type StrategyCommandName = StrategyCommand;
export type StrategyErrorKind = "load" | "command" | "refresh" | "unknown";
export type StrategyErrorDetail =
  | { type: "unknown" }
  | { type: "step_up" }
  | { type: "unauthorized" }
  | { type: "service"; status: number }
  | { type: "generic" };
export interface StrategyManagerError {
  kind: StrategyErrorKind;
  detail: StrategyErrorDetail;
}
export type AwaitingStrategies = Map<string, StrategySnapshot>;
export type StrategyCommandFailure =
  | { type: "api"; status: number; message: string }
  | { type: "unknown" };

export type StrategyCommandStateEvent =
  | { type: "command_started"; strategy: StrategyState }
  | { type: "definite_rejection"; strategyId: string }
  | { type: "accepted" }
  | { type: "ambiguous_failure" }
  | { type: "refresh_failure" }
  | { type: "authoritative_snapshot"; strategies: StrategyState[] };

export function parseAwaitingStrategies(
  serialized: string | null
): AwaitingStrategies {
  try {
    const parsed: unknown = JSON.parse(serialized ?? "[]");
    if (!Array.isArray(parsed)) {
      return new Map();
    }
    return new Map(
      parsed.filter(
        (entry): entry is [string, StrategySnapshot] =>
          Array.isArray(entry) &&
          typeof entry[0] === "string" &&
          typeof entry[1] === "object" &&
          entry[1] !== null &&
          typeof entry[1].status === "string" &&
          typeof entry[1].version === "number"
      )
    );
  } catch {
    return new Map();
  }
}

export function serializeAwaitingStrategies(
  strategies: AwaitingStrategies
): string | null {
  return strategies.size === 0 ? null : JSON.stringify([...strategies]);
}

export function reconcileAwaitingStrategies(
  current: AwaitingStrategies,
  strategies: StrategyState[]
): AwaitingStrategies {
  if (current.size === 0) {
    return current;
  }
  const authoritative = new Map(
    strategies.map((strategy) => [strategy.strategy_id, strategy])
  );
  const next = new Map(
    [...current].filter(([strategyId, snapshot]) => {
      const strategy = authoritative.get(strategyId);
      return (
        strategy === undefined ||
        (strategy.status === snapshot.status &&
          strategy.version === snapshot.version)
      );
    })
  );
  return next.size === current.size ? current : next;
}

export function transitionStrategyCommandState(
  current: AwaitingStrategies,
  event: StrategyCommandStateEvent
): AwaitingStrategies {
  switch (event.type) {
    case "command_started": {
      const next = new Map(current);
      next.set(event.strategy.strategy_id, {
        status: event.strategy.status,
        version: event.strategy.version
      });
      return next;
    }
    case "definite_rejection": {
      const next = new Map(current);
      next.delete(event.strategyId);
      return next;
    }
    case "authoritative_snapshot":
      return reconcileAwaitingStrategies(current, event.strategies);
    case "accepted":
    case "ambiguous_failure":
    case "refresh_failure":
      return current;
  }
}

export function isDefiniteCommandRejection(
  failure: StrategyCommandFailure
): boolean {
  return (
    failure.type === "api" &&
    ((failure.status < 500 && failure.status !== 408) ||
      failure.message === "strategy_engine_listener_unavailable")
  );
}
