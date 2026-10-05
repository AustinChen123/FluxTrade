import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";

import type { Locale } from "../../shared/i18n";
import { formatPresentationTimestamp, PRESENTATION_TIME_ZONE } from "../../shared/time/presentation";
import type { ResearchWorkspace } from "./useResearchWorkspace";
import { buildGaSubmitPayload, gaActionsForStatus, type GaSubmitForm } from "./gaOperationsModel";
import type { GaOperations } from "./useGaOperations";

type Props = { readonly state: GaOperations; readonly workspace: ResearchWorkspace; readonly locale: Locale };
type EnumFieldDefinition = { readonly choices?: readonly unknown[] };

export function GaOperationsPanel({ state, workspace, locale }: Props) {
  const { t } = useTranslation();
  const [confirmSubmit, setConfirmSubmit] = useState(false);
  const [confirmRetry, setConfirmRetry] = useState(false);
  const [promotionReason, setPromotionReason] = useState("");
  const [promotionConfirmed, setPromotionConfirmed] = useState(false);
  useEffect(() => setConfirmRetry(false), [state.selectedJobId]);
  const date = new Intl.DateTimeFormat(locale, {
    year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit",
    timeZone: PRESENTATION_TIME_ZONE
  });
  const mutationAllowed = state.session?.permissions.can_mutate === true;
  const isSubmitting = (target: string) => state.submittingTargets.includes(target);
  const hasIntent = (target: string) => state.intents.some((intent) =>
    (intent.operation === "submit" ? "submit" : `job:${intent.jobId}`) === target
  );
  const submitLocked = hasIntent("submit") || isSubmitting("submit");
  const selectedTarget = state.selectedJob ? `job:${state.selectedJob.id}` : "";
  const selectedLocked = selectedTarget !== "" && (state.selectedJobStale || state.loadingDetail || hasIntent(selectedTarget) || isSubmitting(selectedTarget));
  const instrumentDefinitions = state.profile?.accepted_fields.instrument?.fields as
    Record<string, Readonly<Record<string, unknown>>> | undefined;
  const submissionPreview = (() => {
    if (!state.profile || !state.profileSupported || !state.form) return null;
    try {
      return buildGaSubmitPayload(state.profile, state.form);
    } catch {
      return null;
    }
  })();
  const submittedFees = asRecord(submissionPreview?.fees);
  const submittedInstrument = asRecord(submissionPreview?.instrument);
  const submittedParameters = asRecord(submissionPreview?.parameters);
  const submittedShortWindow = asRecord(submittedParameters?.short_window);
  const submittedLongWindow = asRecord(submittedParameters?.long_window);
  const labels = {
    QUEUED: t("ga.status.QUEUED"), RUNNING: t("ga.status.RUNNING"),
    PAUSING: t("ga.status.PAUSING"), PAUSED: t("ga.status.PAUSED"),
    CANCELLING: t("ga.status.CANCELLING"), CANCELLED: t("ga.status.CANCELLED"),
    SUCCEEDED: t("ga.status.SUCCEEDED"), FAILED: t("ga.status.FAILED")
  };
  const update = (change: (form: GaSubmitForm) => GaSubmitForm) => {
    if (state.form) state.setForm(change(state.form));
  };
  const field = (id: string, label: string, value: string, onChange: (value: string) => void, required = false) => (
    <label className="ga-field" key={id} htmlFor={`ga-${id}`}>
      <span>{label}{required ? " *" : ""}</span>
      <input id={`ga-${id}`} value={value} required={required} onChange={(event) => onChange(event.target.value)} />
    </label>
  );
  const decimalField = (id: string, label: string, value: string, onChange: (value: string) => void, required = false) => (
    <label className="ga-field" key={id} htmlFor={`ga-${id}`}>
      <span>{label}{required ? " *" : ""}</span>
      <input id={`ga-${id}`} inputMode="decimal" value={value} required={required} onChange={(event) => onChange(event.target.value)} />
    </label>
  );
  const integerField = (id: string, label: string, value: string, onChange: (value: string) => void, required = false) => (
    <label className="ga-field" key={id} htmlFor={`ga-${id}`}>
      <span>{label}{required ? " *" : ""}</span>
      <input id={`ga-${id}`} inputMode="numeric" pattern="[0-9]+" value={value} required={required} onChange={(event) => onChange(event.target.value)} />
    </label>
  );
  const enumField = (id: string, label: string, value: string, definition: EnumFieldDefinition, onChange: (value: string) => void) => {
    const choices = Array.isArray(definition.choices) ? definition.choices.filter((item): item is string => typeof item === "string") : [];
    return <label className="ga-field" key={id} htmlFor={`ga-${id}`}><span>{label}</span>
      <select id={`ga-${id}`} value={value} onChange={(event) => onChange(event.target.value)}>
        {choices.map((choice) => <option key={choice} value={choice}>{choice}</option>)}
      </select>
    </label>;
  };
  const errorText = state.error ? t(`ga.error.${state.error.type}`, {
    status: "status" in state.error ? state.error.status ?? "—" : "—",
    code: "code" in state.error ? state.error.code : ""
  }) : null;

  if (!state.session && !state.loading && !state.error && state.profile === null) {
    return <section className="ga-console panel" aria-labelledby="ga-title"><h2 id="ga-title">{t("ga.title")}</h2><p>{t("ga.demoDisabled")}</p></section>;
  }

  return (
    <section className="ga-console panel" aria-labelledby="ga-title">
      <div className="panel-heading">
        <div><span>{t("ga.kicker")}</span><h2 id="ga-title">{t("ga.title")}</h2></div>
        <button type="button" onClick={() => void state.refresh()} disabled={state.loading || !mutationAllowed}>{t("ga.refresh")}</button>
      </div>
      {errorText && <div className="error-panel ga-message" role="alert"><p>{errorText}</p></div>}
      {state.storageLimited && <p className="ga-notice" role="status">{t("ga.storageLimited")}</p>}
      {!mutationAllowed && state.error?.type === "permission" && <p className="ga-notice" role="status">{t("ga.permission")}</p>}
      {state.loading && <p className="ga-notice" role="status">{t("ga.loading")}</p>}
      {state.profile && !state.profileSupported && <p className="ga-notice" role="alert">{t("ga.unsupportedProfile")}</p>}

      {state.intents.map((intent) => {
        const target = intent.operation === "submit" ? "submit" : `job:${intent.jobId}`;
        const mismatch = state.session?.actor !== intent.actor;
        return <section className="ga-awaiting" aria-live="polite" key={intent.idempotencyKey}>
          <strong>{t("ga.awaitingTitle")}</strong>
          <p>{mismatch ? t("ga.actorMismatch", { actor: intent.actor }) : t("ga.awaitingBody", { operation: t(`ga.operation.${intent.operation}`) })}</p>
          <button type="button" disabled={isSubmitting(target) || mismatch || !mutationAllowed} onClick={() => void state.resolveIntent(intent)}>
            {isSubmitting(target) ? t("ga.sending") : t("ga.resolveIntent")}
          </button>
        </section>;
      })}

      {state.profile && state.profileSupported && state.form && mutationAllowed && <details className="ga-form-panel" open>
        <summary>{t("ga.submitTitle")}</summary>
        <p className="ga-help">{t("ga.researchOnly")}</p>
        <div className="ga-form-grid">
          {field("dataset", t("ga.dataset"), state.form.dataset_id, (value) => update((form) => ({ ...form, dataset_id: value })), true)}
          {integerField("start", t("ga.startUtcMs"), state.form.start_time, (value) => update((form) => ({ ...form, start_time: value })), true)}
          {integerField("end", t("ga.endUtcMs"), state.form.end_time, (value) => update((form) => ({ ...form, end_time: value })), true)}
          {decimalField("balance", t("ga.initialBalance"), state.form.initial_balance, (value) => update((form) => ({ ...form, initial_balance: value })), true)}
          {decimalField("maker", t("ga.makerFee"), state.form.fees.maker, (value) => update((form) => ({ ...form, fees: { ...form.fees, maker: value } })), true)}
          {decimalField("taker", t("ga.takerFee"), state.form.fees.taker, (value) => update((form) => ({ ...form, fees: { ...form.fees, taker: value } })), true)}
          {decimalField("multiplier", t("ga.multiplier"), state.form.instrument.multiplier ?? "", (value) => update((form) => ({ ...form, instrument: { ...form.instrument, multiplier: value } })))}
          {decimalField("quantity-step", t("ga.quantityStep"), state.form.instrument.quantity_step ?? "", (value) => update((form) => ({ ...form, instrument: { ...form.instrument, quantity_step: value } })))}
          {decimalField("price-tick", t("ga.priceTick"), state.form.instrument.price_tick ?? "", (value) => update((form) => ({ ...form, instrument: { ...form.instrument, price_tick: value } })))}
          {instrumentDefinitions?.fee_model && enumField("fee-model", t("ga.feeModel"), state.form.instrument.fee_model ?? "", instrumentDefinitions.fee_model, (value) => update((form) => ({ ...form, instrument: { ...form.instrument, fee_model: value } })))}
          {instrumentDefinitions?.capital_model && enumField("capital-model", t("ga.capitalModel"), state.form.instrument.capital_model ?? "", instrumentDefinitions.capital_model, (value) => update((form) => ({ ...form, instrument: { ...form.instrument, capital_model: value } })))}
          {decimalField("capital-contract", t("ga.capitalPerContract"), state.form.instrument.capital_per_contract ?? "", (value) => update((form) => ({ ...form, instrument: { ...form.instrument, capital_per_contract: value } })))}
          {(["short_window", "long_window"] as const).flatMap((name) => (["min", "max", "step"] as const).map((part) => integerField(`${name}-${part}`, t(`ga.${name}.${part}`), state.form!.parameters[name][part], (value) => update((form) => ({ ...form, parameters: { ...form.parameters, [name]: { ...form.parameters[name], [part]: value } } })), true)))}
          {decimalField("quantity", t("ga.quantity"), state.form.parameters.quantity, (value) => update((form) => ({ ...form, parameters: { ...form.parameters, quantity: value } })), true)}
          {integerField("population", t("ga.population"), state.form.population_size, (value) => update((form) => ({ ...form, population_size: value })))}
          {integerField("generations", t("ga.maxGenerations"), state.form.max_generations, (value) => update((form) => ({ ...form, max_generations: value })))}
          {integerField("seed", t("ga.seed"), state.form.seed, (value) => update((form) => ({ ...form, seed: value })))}
        </div>
        <ReadOnlyValues title={t("ga.fixedConfiguration")} value={state.profile.compiled_fields} />
        <ReadOnlyValues title={t("ga.evolutionDefaults")} value={state.profile.evolution_defaults} />
        <div className="ga-actions">
          {!confirmSubmit ? <button type="button" disabled={submitLocked || !submissionPreview} onClick={() => setConfirmSubmit(true)}>{t("ga.reviewSubmit")}</button> : <div className="ga-confirm" role="group" aria-label={t("ga.confirmSubmit")}>
            <p>{t("ga.confirmSubmitBody", { dataset: state.form.dataset_id, start: state.form.start_time, end: state.form.end_time, quantity: state.form.parameters.quantity, population: state.form.population_size, generations: state.form.max_generations })}</p>
            {submissionPreview && <dl className="ga-job-facts ga-confirm-facts">
              <Fact label={t("ga.initialBalance")} value={displayValue(submissionPreview.initial_balance, t("ga.none"))} />
              <Fact label={t("ga.makerFee")} value={displayValue(submittedFees?.maker, t("ga.none"))} />
              <Fact label={t("ga.takerFee")} value={displayValue(submittedFees?.taker, t("ga.none"))} />
              <Fact label={t("ga.multiplier")} value={displayValue(submittedInstrument?.multiplier, t("ga.none"))} />
              <Fact label={t("ga.quantityStep")} value={displayValue(submittedInstrument?.quantity_step, t("ga.none"))} />
              <Fact label={t("ga.priceTick")} value={displayValue(submittedInstrument?.price_tick, t("ga.none"))} />
              <Fact label={t("ga.feeModel")} value={displayValue(submittedInstrument?.fee_model, t("ga.none"))} />
              <Fact label={t("ga.capitalModel")} value={displayValue(submittedInstrument?.capital_model, t("ga.none"))} />
              <Fact label={t("ga.capitalPerContract")} value={displayValue(submittedInstrument?.capital_per_contract, t("ga.none"))} />
              <Fact label={t("ga.short_window.min")} value={displayValue(submittedShortWindow?.min, t("ga.none"))} />
              <Fact label={t("ga.short_window.max")} value={displayValue(submittedShortWindow?.max, t("ga.none"))} />
              <Fact label={t("ga.short_window.step")} value={displayValue(submittedShortWindow?.step, t("ga.none"))} />
              <Fact label={t("ga.long_window.min")} value={displayValue(submittedLongWindow?.min, t("ga.none"))} />
              <Fact label={t("ga.long_window.max")} value={displayValue(submittedLongWindow?.max, t("ga.none"))} />
              <Fact label={t("ga.long_window.step")} value={displayValue(submittedLongWindow?.step, t("ga.none"))} />
              <Fact label={t("ga.quantity")} value={displayValue(submittedParameters?.quantity, t("ga.none"))} />
              <Fact label={t("ga.population")} value={displayValue(submissionPreview.population_size, t("ga.none"))} />
              <Fact label={t("ga.maxGenerations")} value={displayValue(submissionPreview.max_generations, t("ga.none"))} />
              <Fact label={t("ga.seed")} value={displayValue(submissionPreview.seed, t("ga.none"))} />
            </dl>}
            <button type="button" disabled={submitLocked} onClick={() => { setConfirmSubmit(false); void state.submit(); }}>{t("ga.confirmSubmit")}</button>
            <button type="button" onClick={() => setConfirmSubmit(false)}>{t("ga.cancel")}</button>
          </div>}
        </div>
      </details>}

      <div className="ga-job-layout">
        <section aria-labelledby="ga-jobs-title">
          <div className="ga-subheading"><h3 id="ga-jobs-title">{t("ga.jobs")}</h3><span>{t("ga.jobCount", { count: state.totalCount })}</span></div>
          {state.jobs.length === 0 && !state.loading && <p className="ga-notice">{t("ga.noJobs")}</p>}
          <ul className="ga-job-list">
            {state.jobs.map((job) => <li key={job.id}>
              <button type="button" aria-pressed={job.id === state.selectedJobId} onClick={() => state.selectJob(job.id)}>
                <span className="ga-job-id">{job.id}</span><span>{labels[job.status]}</span><span>{t("ga.version", { version: job.version })}</span>
              </button>
            </li>)}
          </ul>
          {state.jobs.length < state.totalCount && <button type="button" disabled={state.loading} onClick={() => void state.loadMore()}>{t("ga.loadMore")}</button>}
        </section>
        <section className="ga-job-detail" aria-labelledby="ga-detail-title">
          <h3 id="ga-detail-title">{t("ga.detail")}</h3>
          {state.selectedJobStale && <p className="ga-notice" role="status">{t("ga.detailStale")}</p>}
          {state.loadingDetail && <p role="status">{t("ga.loadingDetail")}</p>}
          {!state.loadingDetail && !state.selectedJob && <p className="ga-notice">{t("ga.selectJob")}</p>}
          {state.selectedJob && <>
            <dl className="ga-job-facts">
              <Fact label={t("ga.jobId")} value={state.selectedJob.id} />
              <Fact label={t("ga.statusLabel")} value={labels[state.selectedJob.status]} />
              <Fact label={t("ga.versionLabel")} value={String(state.selectedJob.version)} />
              <Fact label={t("ga.completedGeneration")} value={state.selectedJob.completed_generation < 0 ? t("ga.none") : String(state.selectedJob.completed_generation)} />
              <Fact label={t("ga.checkpoint")} value={state.selectedJob.checkpoint ? `${state.selectedJob.checkpoint.epoch_id} · ${state.selectedJob.checkpoint.completed_generation}` : t("ga.none")} />
              <Fact label={t("ga.retryOf")} value={state.selectedJob.retry_of_job_id ?? t("ga.none")} />
              <Fact label={t("ga.errorCode")} value={state.selectedJob.error ?? t("ga.none")} />
            </dl>
            {state.selectedJob.epoch_id ? <div className="ga-epoch-link">
              <button type="button" onClick={() => void workspace.openEpoch(state.selectedJob!.epoch_id!)}>{t("ga.openEpoch", { epochId: state.selectedJob.epoch_id })}</button>
              {workspace.epoch?.id === state.selectedJob.epoch_id && <time>{workspace.epoch.started_at ? formatPresentationTimestamp(workspace.epoch.started_at, date) : t("ga.none")}</time>}
            </div> : <p className="ga-notice">{t("ga.noEpoch")}</p>}
            <ReadOnlyValues title={t("ga.immutableBinding")} value={state.selectedJob.request} />
            <div className="ga-actions">
              {gaActionsForStatus(state.selectedJob.status).map((operation) => operation === "retry" ? (
                <button type="button" key={operation} disabled={selectedLocked || !mutationAllowed} onClick={() => setConfirmRetry(true)}>{t(`ga.operation.${operation}`)}</button>
              ) : <button type="button" key={operation} disabled={selectedLocked || !mutationAllowed} onClick={() => void state.act(operation)}>{t(`ga.operation.${operation}`)}</button>)}
              {confirmRetry && <div className="ga-confirm" role="group" aria-label={t("ga.confirmRetry")}>
                <p>{t("ga.retryBody")}</p>
                <button type="button" disabled={selectedLocked} onClick={() => { setConfirmRetry(false); void state.act("retry"); }}>{t("ga.confirmRetry")}</button>
                <button type="button" onClick={() => setConfirmRetry(false)}>{t("ga.cancel")}</button>
              </div>}
            </div>
          </>}
        </section>
      </div>

      <section className="ga-candidate-section" aria-labelledby="ga-candidates-title">
        <div className="ga-subheading"><h3 id="ga-candidates-title">{t("ga.candidates")}</h3>
          {state.session?.permissions.can_mutate && !state.session.permissions.can_step_up && <button type="button" onClick={() => void state.renewStepUp()}>{t("ga.renewStepUp")}</button>}
        </div>
        {workspace.genes.map((gene) => <div className="ga-candidate-row" key={gene.id}>
          <span>{gene.candidate_id} · {gene.strategy_id} · {gene.role} · {gene.score_total}</span>
          {gene.role !== "champion" && <button type="button" disabled={!mutationAllowed || !state.session?.permissions.can_step_up || state.promotionBusy} onClick={() => { state.choosePromotion(gene); setPromotionConfirmed(false); setPromotionReason(""); }}>{t("ga.preparePromotion")}</button>}
        </div>)}
        {state.promotionStepUpRequired && <p className="ga-notice" role="alert">{t("ga.stepUpUnavailable")}</p>}
        {state.promotionCandidate && <div className="ga-confirm" role="group" aria-label={t("ga.confirmPromotion")}>
          <p>{t("ga.promotionWarning", { geneId: state.promotionCandidate.id, strategy: state.promotionCandidate.strategy_id })}</p>
          <label className="ga-field" htmlFor="ga-promotion-reason"><span>{t("ga.reason")}</span><input id="ga-promotion-reason" value={promotionReason} onChange={(event) => setPromotionReason(event.target.value)} /></label>
          {!promotionConfirmed ? <button type="button" disabled={state.promotionBusy} onClick={() => setPromotionConfirmed(true)}>{t("ga.reviewPromotion")}</button> : <button type="button" disabled={state.promotionBusy || !mutationAllowed || !state.session?.permissions.can_step_up} onClick={() => { setPromotionConfirmed(false); void state.promote(promotionReason.trim() || null); }}>{state.promotionBusy ? t("ga.sending") : t("ga.confirmPromotion")}</button>}
          <button type="button" disabled={state.promotionBusy} onClick={() => state.choosePromotion(null)}>{t("ga.cancel")}</button>
        </div>}
        {state.promotionReceipt && <p className="ga-notice" role="status">{t("ga.promotionAccepted", { retired: state.promotionReceipt.gene.retired_gene_ids.join(", ") || t("ga.none") })}</p>}
        {state.promotionUncertain && <div className="ga-awaiting" role="alert"><p>{t("ga.promotionUncertain")}</p><button type="button" disabled={state.promotionBusy} onClick={() => void state.refreshPromotion()}>{t("ga.refreshCandidateState")}</button></div>}
        {state.auditUnavailable && <p className="ga-notice" role="status">{t("ga.auditUnavailable")}</p>}
        {state.promotionEvents.length > 0 && <ul className="ga-audit-list">{state.promotionEvents.map((event) => <li key={event.id}><span>{event.event_type}</span><span>{event.related_gene_id}</span><time>{event.created_at ? formatPresentationTimestamp(event.created_at, date) : t("ga.none")}</time></li>)}</ul>}
      </section>
    </section>
  );
}

function Fact({ label, value }: { label: string; value: string }) {
  return <div><dt>{label}</dt><dd>{value}</dd></div>;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function displayValue(value: unknown, emptyLabel: string): string {
  return value === null || value === undefined ? emptyLabel : String(value);
}

function ReadOnlyValues({ title, value }: { title: string; value: unknown }) {
  const { t } = useTranslation();
  if (value === undefined) return null;
  const entries: [string, unknown][] = value && typeof value === "object" && !Array.isArray(value)
    ? Object.entries(value as Record<string, unknown>)
    : [[String(t("ga.value")), value]];
  return <details className="ga-readonly"><summary>{title}</summary><dl>{entries.map(([key, item]) => <Fact key={key} label={key} value={typeof item === "string" ? item : JSON.stringify(item)} />)}</dl></details>;
}
