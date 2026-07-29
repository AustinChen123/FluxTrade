import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import {
  ApiError,
  ensureBrowserSession,
  loadStrategyStates,
  sendStrategyCommand,
  type BrowserSession,
  type StrategyCommand,
  type StrategyState,
  type StrategyStatus
} from "./api";
import type { Locale } from "./i18n";

type Translate = ReturnType<typeof useTranslation>["t"];
type ErrorKind = "load" | "command" | "refresh" | "unknown";
type StrategySnapshot = Pick<StrategyState, "status" | "version">;

const AWAITING_STORAGE_KEY = "fluxtrade.strategy.awaiting";

function loadAwaitingStrategies(): Map<string, StrategySnapshot> {
  try {
    const parsed: unknown = JSON.parse(
      window.sessionStorage.getItem(AWAITING_STORAGE_KEY) ?? "[]"
    );
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

function saveAwaitingStrategies(
  strategies: Map<string, StrategySnapshot>
): void {
  try {
    if (strategies.size === 0) {
      window.sessionStorage.removeItem(AWAITING_STORAGE_KEY);
    } else {
      window.sessionStorage.setItem(
        AWAITING_STORAGE_KEY,
        JSON.stringify([...strategies])
      );
    }
  } catch {
    // Browser storage is optional; the in-memory lock remains fail-closed.
  }
}

function commandLabel(command: StrategyCommand, t: Translate): string {
  return t(`strategies.command.${command}`);
}

function errorMessage(
  error: unknown,
  kind: ErrorKind,
  t: Translate
): string {
  if (kind === "unknown") {
    return t("strategies.unknownErrorBody");
  }
  if (error instanceof ApiError) {
    if (error.message === "step_up_required") {
      return t("strategies.stepUpRequired");
    }
    if (error.status === 401 || error.status === 403) {
      return t("strategies.unauthorized");
    }
    return t("strategies.serviceError", { status: error.status });
  }
  return t("strategies.errorBody");
}

function reconcileAwaitingStrategies(
  current: Map<string, StrategySnapshot>,
  strategies: StrategyState[]
): Map<string, StrategySnapshot> {
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

function displayDate(
  value: number | string | null,
  formatter: Intl.DateTimeFormat
): string {
  if (value === null) {
    return "—";
  }
  const date = new Date(typeof value === "number" ? value : value);
  return Number.isNaN(date.valueOf())
    ? "—"
    : formatter.format(date);
}

export function StrategyManager() {
  const { t, i18n } = useTranslation();
  const locale: Locale = i18n.resolvedLanguage === "en" ? "en" : "zh-TW";
  const [session, setSession] = useState<BrowserSession | null>(null);
  const [strategies, setStrategies] = useState<StrategyState[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<unknown | null>(null);
  const [errorKind, setErrorKind] = useState<ErrorKind>("load");
  const [notice, setNotice] = useState("");
  const [pendingStrategyId, setPendingStrategyId] = useState<string | null>(null);
  const [awaitingStrategies, setAwaitingStrategies] = useState<
    Map<string, StrategySnapshot>
  >(loadAwaitingStrategies);

  const applyStrategyStates = useCallback((items: StrategyState[]) => {
    setStrategies(items);
    setAwaitingStrategies((current) =>
      reconcileAwaitingStrategies(current, items)
    );
  }, []);

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const browserSession = await ensureBrowserSession();
      setSession(browserSession);
      applyStrategyStates(await loadStrategyStates());
    } catch (reason) {
      setErrorKind("load");
      setError(reason);
    } finally {
      setLoading(false);
    }
  }, [applyStrategyStates]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    saveAwaitingStrategies(awaitingStrategies);
  }, [awaitingStrategies]);

  const counts = useMemo(() => {
    const totals: Partial<Record<StrategyStatus, number>> = {};
    for (const strategy of strategies) {
      totals[strategy.status] = (totals[strategy.status] ?? 0) + 1;
    }
    return totals;
  }, [strategies]);
  const dateFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        dateStyle: "medium",
        timeStyle: "medium"
      }),
    [locale]
  );

  const submit = async (
    strategy: StrategyState,
    command: StrategyCommand
  ) => {
    if (
      !window.confirm(
        t("strategies.confirm", {
          command: commandLabel(command, t),
          strategyId: strategy.strategy_id
        })
      )
    ) {
      return;
    }
    setPendingStrategyId(strategy.strategy_id);
    setError(null);
    setNotice("");
    let idempotencyKey: string;
    try {
      idempotencyKey = window.crypto.randomUUID();
    } catch (reason) {
      setErrorKind("command");
      setError(reason);
      setPendingStrategyId(null);
      return;
    }
    const awaiting = new Map(awaitingStrategies);
    awaiting.set(strategy.strategy_id, {
      status: strategy.status,
      version: strategy.version
    });
    setAwaitingStrategies(awaiting);
    saveAwaitingStrategies(awaiting);
    try {
      await sendStrategyCommand(
        strategy.strategy_id,
        command,
        strategy.version,
        idempotencyKey,
        session?.csrf_token
      );
      setNotice(
        t("strategies.accepted", {
          command: commandLabel(command, t),
          strategyId: strategy.strategy_id
        })
      );
      try {
        const updatedStrategies = await loadStrategyStates();
        applyStrategyStates(updatedStrategies);
      } catch (reason) {
        setErrorKind("refresh");
        setError(reason);
      }
    } catch (reason) {
      const definiteRejection =
        reason instanceof ApiError &&
        ((reason.status < 500 && reason.status !== 408) ||
          reason.message === "strategy_engine_listener_unavailable");
      if (definiteRejection) {
        const unlocked = new Map(awaiting);
        unlocked.delete(strategy.strategy_id);
        setAwaitingStrategies(unlocked);
        saveAwaitingStrategies(unlocked);
      }
      setErrorKind(
        definiteRejection ? "command" : "unknown"
      );
      setError(reason);
    } finally {
      setPendingStrategyId(null);
    }
  };

  return (
    <section
      className="strategy-console"
      aria-labelledby="strategy-title"
      aria-busy={loading || pendingStrategyId !== null}
    >
      <div className="strategy-intro">
        <div>
          <p className="panel-kicker">{t("strategies.kicker")}</p>
          <h2 id="strategy-title">{t("strategies.title")}</h2>
          <p>{t("strategies.body")}</p>
        </div>
        <button
          type="button"
          onClick={() => void refresh()}
          disabled={loading || pendingStrategyId !== null}
        >
          {t("strategies.refresh")}
        </button>
      </div>

      {error !== null && (
        <div className="error-panel strategy-feedback" role="alert">
          <div>
            <strong>{t(`strategies.${errorKind}ErrorTitle`)}</strong>
            <p>{errorMessage(error, errorKind, t)}</p>
          </div>
        </div>
      )}
      {notice && (
        <p className="strategy-notice" role="status">
          {notice}
        </p>
      )}

      {(strategies.length > 0 || (!loading && error === null)) && (
        <>
          <dl className="strategy-summary" aria-label={t("strategies.summary")}>
            <div>
              <dt>{t("strategies.total")}</dt>
              <dd>{strategies.length.toLocaleString(locale)}</dd>
            </div>
            <div>
              <dt>{t("strategies.status.ACTIVE")}</dt>
              <dd>{(counts.ACTIVE ?? 0).toLocaleString(locale)}</dd>
            </div>
            <div>
              <dt>{t("strategies.status.WARNING")}</dt>
              <dd>{(counts.WARNING ?? 0).toLocaleString(locale)}</dd>
            </div>
            <div>
              <dt>{t("strategies.status.ERROR")}</dt>
              <dd>{(counts.ERROR ?? 0).toLocaleString(locale)}</dd>
            </div>
          </dl>

          {strategies.length === 0 ? (
            <div className="empty-panel">
              <strong>{t("strategies.emptyTitle")}</strong>
              <p>{t("strategies.emptyBody")}</p>
            </div>
          ) : (
            <ul className="strategy-list" aria-label={t("strategies.list")}>
              {strategies.map((strategy) => {
                const pending = pendingStrategyId === strategy.strategy_id;
                const awaitingState = awaitingStrategies.has(
                  strategy.strategy_id
                );
                return (
                  <li key={strategy.strategy_id}>
                    <div className="strategy-identity">
                      <span
                        className={`strategy-status status-${strategy.status.toLowerCase()}`}
                        aria-hidden="true"
                      />
                      <div>
                        <strong>{strategy.strategy_id}</strong>
                        <span>{t(`strategies.status.${strategy.status}`)}</span>
                      </div>
                    </div>
                    <dl>
                      <div>
                        <dt>{t("strategies.heartbeat")}</dt>
                        <dd>
                          {displayDate(strategy.last_heartbeat, dateFormatter)}
                        </dd>
                      </div>
                      <div>
                        <dt>{t("strategies.uptime")}</dt>
                        <dd>{displayDate(strategy.uptime_start, dateFormatter)}</dd>
                      </div>
                      <div>
                        <dt>{t("strategies.version")}</dt>
                        <dd>{strategy.version.toLocaleString(locale)}</dd>
                      </div>
                    </dl>
                    <div className="strategy-action">
                      {strategy.last_error_message && (
                        <p title={strategy.last_error_message}>
                          {strategy.last_error_message}
                        </p>
                      )}
                      {strategy.available_commands.map((command) => (
                        <button
                          key={command}
                          type="button"
                          className={command === "STOP" ? "danger-action" : ""}
                          disabled={pendingStrategyId !== null || awaitingState}
                          onClick={() => void submit(strategy, command)}
                        >
                          {pending
                            ? t("strategies.pending")
                            : awaitingState
                              ? t("strategies.awaitingState")
                              : commandLabel(command, t)}
                        </button>
                      ))}
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
        </>
      )}

      {loading && (
        <div className="loading-indicator" aria-live="polite">
          <span />
          {t("strategies.loading")}
        </div>
      )}
    </section>
  );
}
