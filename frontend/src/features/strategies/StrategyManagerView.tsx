import { useMemo } from "react";
import type { useTranslation } from "react-i18next";

import type { Locale } from "../../shared/i18n";
import {
  formatPresentationTimestamp,
  PRESENTATION_TIME_ZONE
} from "../../shared/time/presentation";
import type {
  AwaitingStrategies,
  StrategyCommandName,
  StrategyErrorDetail,
  StrategyManagerError,
  StrategyRecord
} from "./strategyCommandState";

type Translate = ReturnType<typeof useTranslation>["t"];

export interface StrategyManagerViewProps {
  strategies: StrategyRecord[];
  loading: boolean;
  error: StrategyManagerError | null;
  notice: string;
  pendingStrategyId: string | null;
  awaitingStrategies: AwaitingStrategies;
  locale: Locale;
  t: Translate;
  refresh: () => Promise<void>;
  submit: (
    strategy: StrategyRecord,
    command: StrategyCommandName
  ) => Promise<void>;
}

function commandLabel(command: StrategyCommandName, t: Translate): string {
  return t(`strategies.command.${command}`);
}

function errorMessage(detail: StrategyErrorDetail, t: Translate): string {
  switch (detail.type) {
    case "unknown":
      return t("strategies.unknownErrorBody");
    case "step_up":
      return t("strategies.stepUpRequired");
    case "unauthorized":
      return t("strategies.unauthorized");
    case "service":
      return t("strategies.serviceError", { status: detail.status });
    case "generic":
      return t("strategies.errorBody");
  }
}

export function StrategyManagerView({
  strategies,
  loading,
  error,
  notice,
  pendingStrategyId,
  awaitingStrategies,
  locale,
  t,
  refresh,
  submit
}: StrategyManagerViewProps) {
  const counts = useMemo(() => {
    const totals: Partial<Record<StrategyRecord["status"], number>> = {};
    for (const strategy of strategies) {
      totals[strategy.status] = (totals[strategy.status] ?? 0) + 1;
    }
    return totals;
  }, [strategies]);
  const dateFormatter = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, {
        dateStyle: "medium",
        timeStyle: "medium",
        timeZone: PRESENTATION_TIME_ZONE
      }),
    [locale]
  );

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
            <strong>{t(`strategies.${error.kind}ErrorTitle`)}</strong>
            <p>{errorMessage(error.detail, t)}</p>
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
                          {formatPresentationTimestamp(
                            strategy.last_heartbeat,
                            dateFormatter
                          )}
                        </dd>
                      </div>
                      <div>
                        <dt>{t("strategies.uptime")}</dt>
                        <dd>
                          {formatPresentationTimestamp(
                            strategy.uptime_start,
                            dateFormatter
                          )}
                        </dd>
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
