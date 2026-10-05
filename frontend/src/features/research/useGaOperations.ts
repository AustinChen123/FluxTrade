import { useCallback, useEffect, useRef, useState } from "react";

import {
  ApiError,
  loadGaJob,
  loadGaJobs,
  loadGaProfile,
  loadGene,
  loadGeneLifecycleEvents,
  promoteResearchCandidate,
  renewBrowserSession,
  sendGaCommand,
  type BrowserSession,
  type GaJob,
  type GaJobOperation,
  type GaProfile,
  type Gene,
  type GeneLifecycleEvent,
  type ResearchCandidatePromotion
} from "../../api";
import type { ResearchWorkspace } from "./useResearchWorkspace";
import {
  buildGaSubmitPayload,
  GA_AWAITING_STORAGE_KEY,
  gaActionsForStatus,
  initialGaSubmitForm,
  isDefiniteGaRejection,
  parseGaIntents,
  gaIntentTarget,
  supportsGaProfile,
  type GaIntent,
  type GaSubmitForm
} from "./gaOperationsModel";

export type GaOperationsError =
  | { type: "permission" }
  | { type: "read"; status: number | null }
  | { type: "invalid" }
  | { type: "validation" }
  | { type: "rejected"; status: number; code: string }
  | { type: "uncertain"; status: number | null; code: string }
  | { type: "storage" }
  | { type: "promotion_uncertain"; status: number | null }
  | { type: "promotion_rejected"; status: number; code: string };

export type GaOperations = {
  session: BrowserSession | null;
  profile: GaProfile | null;
  profileSupported: boolean;
  form: GaSubmitForm | null;
  jobs: GaJob[];
  totalCount: number;
  selectedJobId: string | null;
  selectedJob: GaJob | null;
  selectedJobStale: boolean;
  loading: boolean;
  loadingDetail: boolean;
  submitting: boolean;
  submittingTargets: string[];
  error: GaOperationsError | null;
  intents: GaIntent[];
  storageLimited: boolean;
  selectedGene: Gene | null;
  promotionCandidate: Gene | null;
  promotionBusy: boolean;
  promotionReceipt: ResearchCandidatePromotion | null;
  promotionEvents: GeneLifecycleEvent[];
  promotionUncertain: boolean;
  promotionStepUpRequired: boolean;
  auditUnavailable: boolean;
  setForm: (form: GaSubmitForm) => void;
  refresh: () => Promise<boolean>;
  loadMore: () => Promise<void>;
  selectJob: (jobId: string) => void;
  submit: () => Promise<void>;
  act: (operation: Exclude<GaJobOperation, "submit">) => Promise<void>;
  resolveIntent: (intent: GaIntent) => Promise<void>;
  choosePromotion: (gene: Gene | null) => void;
  promote: (reason: string | null) => Promise<void>;
  refreshPromotion: () => Promise<void>;
  renewStepUp: () => Promise<void>;
};

export function useGaOperations(
  enabled: boolean,
  workspace: ResearchWorkspace
): GaOperations {
  const [session, setSession] = useState<BrowserSession | null>(workspace.session);
  const sessionRef = useRef(session);
  sessionRef.current = session;
  const [profile, setProfile] = useState<GaProfile | null>(null);
  const [profileSupported, setProfileSupported] = useState(false);
  const [form, setForm] = useState<GaSubmitForm | null>(null);
  const [jobs, setJobs] = useState<GaJob[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [selectedJob, setSelectedJob] = useState<GaJob | null>(null);
  const [selectedJobStale, setSelectedJobStale] = useState(false);
  const selectedJobStaleRef = useRef(false);
  const [loading, setLoading] = useState(false);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState<GaOperationsError | null>(null);
  const [intents, setIntents] = useState<GaIntent[]>([]);
  const intentsRef = useRef<GaIntent[]>([]);
  const [submittingTargets, setSubmittingTargets] = useState<string[]>([]);
  const submittingRef = useRef(new Set<string>());
  const [storageLimited, setStorageLimited] = useState(false);
  const [promotionCandidate, setPromotionCandidate] = useState<Gene | null>(null);
  const [promotionBusy, setPromotionBusy] = useState(false);
  const [promotionReceipt, setPromotionReceipt] = useState<ResearchCandidatePromotion | null>(null);
  const [promotionEvents, setPromotionEvents] = useState<GeneLifecycleEvent[]>([]);
  const [promotionUncertain, setPromotionUncertain] = useState(false);
  const [promotionStepUpRequired, setPromotionStepUpRequired] = useState(false);
  const [auditUnavailable, setAuditUnavailable] = useState(false);
  const requestVersion = useRef(0);
  const detailVersion = useRef(0);
  const pageVersion = useRef(0);
  const operationScope = useRef(0);
  const mounted = useRef(false);
  const selectedIdRef = useRef<string | null>(null);
  selectedIdRef.current = selectedJobId;
  const selectedJobRef = useRef<GaJob | null>(null);
  selectedJobRef.current = selectedJob;
  const markSelectedJobStale = useCallback((stale: boolean) => {
    selectedJobStaleRef.current = stale;
    setSelectedJobStale(stale);
  }, []);
  const workspaceRef = useRef(workspace);
  workspaceRef.current = workspace;
  const workspaceSession = workspace.session;

  const isCurrentOperationScope = useCallback((scope: number, actor: string, csrfToken: string) => {
    const currentWorkspaceSession = workspaceRef.current.session;
    const currentSession = sessionRef.current;
    return enabled && mounted.current && scope === operationScope.current &&
      workspaceRef.current.sessionResolved && currentWorkspaceSession?.actor === actor &&
      currentSession?.actor === actor &&
      currentSession.csrf_token === csrfToken;
  }, [enabled]);

  useEffect(() => {
    operationScope.current += 1;
    submittingRef.current.clear();
    setSubmittingTargets([]);
    setPromotionBusy(false);
    return () => { operationScope.current += 1; };
  }, [enabled, workspace.sessionResolved, workspaceSession?.actor, workspaceSession?.csrf_token,
    workspaceSession?.expires_at, workspaceSession?.permissions.can_mutate,
    workspaceSession?.permissions.can_step_up, session?.actor, session?.csrf_token,
    session?.permissions.can_mutate, session?.permissions.can_step_up]);

  const persistIntents = useCallback((next: GaIntent[]) => {
    intentsRef.current = next;
    setIntents(next);
    try {
      if (next.length === 0) window.sessionStorage.removeItem(GA_AWAITING_STORAGE_KEY);
      else window.sessionStorage.setItem(GA_AWAITING_STORAGE_KEY, JSON.stringify(next));
    } catch {
      setStorageLimited(true);
      setError({ type: "storage" });
    }
  }, []);

  const storeIntent = useCallback((next: GaIntent) => {
    const target = gaIntentTarget(next);
    const remaining = intentsRef.current.filter((item) => gaIntentTarget(item) !== target);
    persistIntents([...remaining, next]);
  }, [persistIntents]);

  const retainForResolution = useCallback((captured: GaIntent) => {
    const target = gaIntentTarget(captured);
    const remaining = intentsRef.current.filter((item) => gaIntentTarget(item) !== target);
    const next = [...remaining, { ...captured, awaiting: true }];
    intentsRef.current = next;
    try {
      window.sessionStorage.setItem(GA_AWAITING_STORAGE_KEY, JSON.stringify(next));
    } catch {
      if (enabled && mounted.current) {
        setStorageLimited(true);
        setError({ type: "storage" });
      }
    }
    if (enabled && mounted.current) setIntents(next);
  }, [enabled]);

  const clearIntent = useCallback((captured: GaIntent) => {
    const target = gaIntentTarget(captured);
    persistIntents(intentsRef.current.filter((item) =>
      gaIntentTarget(item) !== target || item.idempotencyKey !== captured.idempotencyKey
    ));
  }, [persistIntents]);

  const markSubmitting = useCallback((target: string, active: boolean) => {
    if (active) submittingRef.current.add(target);
    else submittingRef.current.delete(target);
    setSubmittingTargets([...submittingRef.current]);
  }, []);

  const refresh = useCallback(async (preferredId?: string | null) => {
    if (!enabled || !sessionRef.current?.permissions.can_mutate) return false;
    const version = ++requestVersion.current;
    pageVersion.current += 1;
    detailVersion.current += 1;
    const current = () => mounted.current && version === requestVersion.current;
    setLoading(true);
    setError(null);
    const previouslySelectedId = selectedIdRef.current;
    const retainedJob = selectedJobRef.current;
    if (retainedJob && retainedJob.id === previouslySelectedId) markSelectedJobStale(true);
    else markSelectedJobStale(false);
    try {
      const [page, profileResult] = await Promise.all([loadGaJobs(50, 0), loadGaProfile()]);
      if (!current()) return false;
      const supported = supportsGaProfile(profileResult.profile);
      setProfile(profileResult.profile);
      setProfileSupported(supported);
      setForm((old) => old ?? (supported ? initialGaSubmitForm(profileResult.profile) : null));
      setJobs(page.items);
      setTotalCount(page.total_count);
      const wanted = preferredId === undefined ? previouslySelectedId : preferredId;
      const nextId = wanted ?? (page.items[0]?.id ?? null);
      setSelectedJobId(nextId);
      if (nextId === null) {
        setSelectedJob(null);
        markSelectedJobStale(false);
        setLoadingDetail(false);
      } else {
        if (retainedJob?.id !== nextId) {
          setSelectedJob(null);
          markSelectedJobStale(false);
        } else {
          markSelectedJobStale(true);
        }
        setLoadingDetail(true);
        try {
          const detail = await loadGaJob(nextId);
          if (!current()) return false;
          if (detail.job.id !== nextId) throw new Error("invalid_response");
          setSelectedJob(detail.job);
          markSelectedJobStale(false);
        } finally {
          if (current()) setLoadingDetail(false);
        }
      }
      return true;
    } catch (reason) {
      if (current()) {
        setLoadingDetail(false);
        setError(reason instanceof Error && reason.message === "invalid_response"
          ? { type: "invalid" }
          : { type: "read", status: reason instanceof ApiError ? reason.status : null });
      }
      return false;
    } finally {
      if (current()) setLoading(false);
    }
  }, [enabled, markSelectedJobStale]);

  useEffect(() => {
    mounted.current = true;
    const invalidateContext = () => {
      mounted.current = false;
      requestVersion.current += 1;
      pageVersion.current += 1;
    };
    if (!enabled) {
      setSession(null);
      setProfile(null);
      setProfileSupported(false);
      setForm(null);
      setJobs([]);
      setTotalCount(0);
      setSelectedJobId(null);
      setSelectedJob(null);
      markSelectedJobStale(false);
      submittingRef.current.clear();
      setSubmittingTargets([]);
      intentsRef.current = [];
      setIntents([]);
      setError(null);
      return invalidateContext;
    }
    if (!workspace.sessionResolved) {
      return invalidateContext;
    }
    const nextSession = workspaceSession;
    const version = ++requestVersion.current;
    const current = () => mounted.current && version === requestVersion.current;
    setSession(nextSession);
    setSelectedJobId(null);
    setSelectedJob(null);
    markSelectedJobStale(false);
    if (!nextSession || !nextSession.permissions.can_mutate) {
      setError({ type: "permission" });
      return invalidateContext;
    }
    setLoading(true);
    void Promise.resolve().then(async () => {
      let restoredIntents: GaIntent[] = [];
      try {
        const rawIntent = window.sessionStorage.getItem(GA_AWAITING_STORAGE_KEY);
        restoredIntents = parseGaIntents(rawIntent).map((intent) => ({ ...intent, awaiting: true }));
        persistIntents(restoredIntents);
      } catch {
        setStorageLimited(true);
        setError({ type: "storage" });
      }
      const [profileResult, page] = await Promise.all([loadGaProfile(), loadGaJobs(50, 0)]);
      if (!current()) return;
      const supported = supportsGaProfile(profileResult.profile);
      setProfile(profileResult.profile);
      setProfileSupported(supported);
      setForm((old) => old ?? (supported ? initialGaSubmitForm(profileResult.profile) : null));
      setJobs(page.items);
      setTotalCount(page.total_count);
      const preferred = restoredIntents.find((intent) => intent.jobId !== null)?.jobId ?? null;
      const nextId = preferred && (page.items.some((job) => job.id === preferred) || restoredIntents.some((intent) => intent.operation === "retry" && intent.jobId === preferred))
        ? preferred
        : (page.items[0]?.id ?? null);
      setSelectedJobId(nextId);
      if (nextId !== null) {
        setLoadingDetail(true);
        const detail = await loadGaJob(nextId);
        if (!current()) return;
        setSelectedJob(detail.job);
      }
    }).catch((reason) => {
      if (!current()) return;
      setError(reason instanceof Error && reason.message === "invalid_response"
        ? { type: "invalid" }
        : { type: "read", status: reason instanceof ApiError ? reason.status : null });
    }).finally(() => {
      if (current()) {
        setLoading(false);
        setLoadingDetail(false);
      }
    });
    return invalidateContext;
  }, [enabled, markSelectedJobStale, persistIntents, workspace.sessionResolved, workspaceSession?.actor,
    workspaceSession?.csrf_token, workspaceSession?.expires_at,
    workspaceSession?.permissions.can_mutate, workspaceSession?.permissions.can_step_up]);

  const loadMore = useCallback(async () => {
    if (!enabled || !session?.permissions.can_mutate || loading) return;
    const version = ++pageVersion.current;
    try {
      const page = await loadGaJobs(50, jobs.length);
      if (!mounted.current || version !== pageVersion.current) return;
      setJobs((current) => [...current, ...page.items.filter((job) => !current.some((item) => item.id === job.id))]);
    } catch (reason) {
      if (mounted.current && version === pageVersion.current) {
        setError(reason instanceof Error && reason.message === "invalid_response"
          ? { type: "invalid" }
          : { type: "read", status: reason instanceof ApiError ? reason.status : null });
      }
    }
  }, [enabled, jobs.length, loading, session]);

  const selectJob = useCallback((jobId: string) => {
    if (!enabled || !session?.permissions.can_mutate) return;
    const version = ++requestVersion.current;
    const selectedVersion = ++detailVersion.current;
    setSelectedJobId(jobId);
    setSelectedJob(null);
    markSelectedJobStale(false);
    setLoading(false);
    setLoadingDetail(true);
    setError(null);
    void loadGaJob(jobId).then(({ job }) => {
      if (mounted.current && selectedVersion === detailVersion.current && version === requestVersion.current && selectedIdRef.current === jobId) {
        setSelectedJob(job);
        markSelectedJobStale(false);
      }
    }).catch((reason) => {
      if (mounted.current && selectedVersion === detailVersion.current && version === requestVersion.current && selectedIdRef.current === jobId) {
        setError(reason instanceof Error && reason.message === "invalid_response"
          ? { type: "invalid" }
          : { type: "read", status: reason instanceof ApiError ? reason.status : null });
      }
    }).finally(() => {
      if (mounted.current && selectedVersion === detailVersion.current && version === requestVersion.current && selectedIdRef.current === jobId) {
        setLoadingDetail(false);
      }
    });
  }, [enabled, markSelectedJobStale, session]);

  const sendIntent = useCallback(async (captured: GaIntent, manual: boolean) => {
    const target = gaIntentTarget(captured);
    if (submittingRef.current.has(target)) return;
    if (!session || !session.permissions.can_mutate || session.actor !== captured.actor) {
      setError({ type: "permission" });
      return;
    }
    const selectedAtSend = selectedIdRef.current;
    const capturedScope = operationScope.current;
    const contextIsCurrent = () => isCurrentOperationScope(capturedScope, captured.actor, session.csrf_token);
    let receiptAccepted = false;
    markSubmitting(target, true);
    setError(null);
    try {
      const receipt = await sendGaCommand(
        captured.operation,
        captured.jobId,
        captured.idempotencyKey,
        captured.body,
        session.csrf_token
      );
      receiptAccepted = true;
      if (!contextIsCurrent()) {
        retainForResolution(captured);
        return;
      }
      storeIntent({ ...captured, awaiting: true });
      const returnedId = receipt.job.id;
      const current = await loadGaJob(returnedId);
      if (!contextIsCurrent()) return;
      if (current.job.id !== returnedId) throw new Error("invalid_response");
      const selectedAfterSend = selectedIdRef.current;
      const refreshed = await refresh(selectedAfterSend === selectedAtSend ? returnedId : selectedAfterSend);
      if (!refreshed) {
        return;
      }
      clearIntent(captured);
      setError(null);
    } catch (reason) {
      if (!contextIsCurrent()) {
        if (manual || receiptAccepted || !(reason instanceof ApiError && isDefiniteGaRejection(reason))) {
          retainForResolution(captured);
        } else {
          clearIntent(captured);
        }
        return;
      }
      if (!manual && !receiptAccepted && reason instanceof ApiError && isDefiniteGaRejection(reason)) {
        clearIntent(captured);
        setError({ type: "rejected", status: reason.status, code: reason.message });
        if ((reason.message === "job_version_conflict" || reason.message === "job_transition_invalid") && captured.jobId !== null && selectedIdRef.current === captured.jobId && contextIsCurrent()) {
          await refresh(captured.jobId);
          if (contextIsCurrent()) setError({ type: "rejected", status: reason.status, code: reason.message });
        }
      } else if (reason instanceof Error && reason.message === "invalid_response") {
        const awaiting = { ...captured, awaiting: true };
        storeIntent(awaiting);
        setError({
          type: "uncertain",
          status: reason instanceof ApiError ? reason.status : null,
          code: reason instanceof ApiError
            ? reason.message
            : reason instanceof Error && reason.message === "invalid_response"
              ? "invalid_response"
              : "connection_unknown"
        });
      } else {
        const awaiting = { ...captured, awaiting: true };
        storeIntent(awaiting);
        setError({
          type: "uncertain",
          status: reason instanceof ApiError ? reason.status : null,
          code: reason instanceof ApiError ? reason.message : "connection_unknown"
        });
      }
    } finally {
      if (contextIsCurrent()) markSubmitting(target, false);
      else submittingRef.current.delete(target);
    }
  }, [clearIntent, isCurrentOperationScope, markSubmitting, refresh, retainForResolution, session, storeIntent]);

  const newIntent = useCallback((operation: GaJobOperation, jobId: string | null, body: Record<string, unknown>, version?: number) => {
    const target = operation === "submit" ? "submit" : `job:${jobId}`;
    if (!session || !session.permissions.can_mutate ||
        intentsRef.current.some((item) => gaIntentTarget(item) === target) ||
        submittingRef.current.has(target)) return null;
    const intentKey = window.crypto.randomUUID();
    const next: GaIntent = {
      operation, jobId, expectedVersion: version, body,
      idempotencyKey: intentKey, actor: session.actor, awaiting: false
    };
    storeIntent(next);
    return next;
  }, [session, storeIntent]);

  const submit = useCallback(async () => {
    if (!session?.permissions.can_mutate || !profile || !profileSupported || !form) return;
    try {
      const body = buildGaSubmitPayload(profile, form);
      const next = newIntent("submit", null, body);
      if (next) await sendIntent(next, false);
    } catch {
      setError({ type: "validation" });
    }
  }, [form, newIntent, profile, profileSupported, sendIntent, session]);

  const act = useCallback(async (operation: Exclude<GaJobOperation, "submit">) => {
    const job = selectedJobRef.current;
    if (selectedJobStaleRef.current || !job || !gaActionsForStatus(job.status).includes(operation)) return;
    const body = { expected_version: job.version };
    const next = newIntent(operation, job.id, body, job.version);
    if (next) await sendIntent(next, false);
  }, [newIntent, sendIntent]);

  const resolveIntent = useCallback(async (intent: GaIntent) => {
    if (!intent.awaiting || session?.actor !== intent.actor) return;
    await sendIntent(intent, true);
  }, [sendIntent, session]);

  const selectedGene = workspace.selectedGeneId === null
    ? null
    : workspace.genes.find((gene) => gene.id === workspace.selectedGeneId) ?? null;
  const choosePromotion = useCallback((gene: Gene | null) => {
    if (!session?.permissions.can_mutate || !session.permissions.can_step_up || promotionBusy) return;
    setPromotionCandidate(gene);
    setPromotionReceipt(null);
    setPromotionUncertain(false);
    setError(null);
  }, [promotionBusy, session]);

  const readPromotionEvidence = useCallback(async (geneId: number, strategyId: string) => {
    try {
      const [gene, promoteEvents, retireEvents] = await Promise.all([
        loadGene(geneId),
        loadGeneLifecycleEvents(geneId, strategyId, "gene_promote"),
        loadGeneLifecycleEvents(geneId, strategyId, "gene_retire")
      ]);
      return { events: [...promoteEvents, ...retireEvents], unavailable: false, gene };
    } catch {
      return { events: [], unavailable: true, gene: null };
    }
  }, []);

  const publishPromotionEvidence = useCallback(async (
    geneIds: number[], strategyId: string, scope: number, ownerSession: BrowserSession
  ) => {
    const current = () => isCurrentOperationScope(scope, ownerSession.actor, ownerSession.csrf_token);
    if (!current()) return;
    const evidence = await Promise.all(geneIds.map((geneId) => readPromotionEvidence(geneId, strategyId)));
    if (!current()) return;
    setPromotionEvents(evidence.flatMap((item) => item.events));
    setAuditUnavailable(evidence.some((item) => item.unavailable));
  }, [isCurrentOperationScope, readPromotionEvidence]);

  const promote = useCallback(async (reason: string | null) => {
    const candidate = promotionCandidate;
    if (!candidate || !session?.permissions.can_mutate || !session.permissions.can_step_up || promotionBusy || candidate.role === "champion") return;
    const capturedScope = operationScope.current;
    const current = () => isCurrentOperationScope(capturedScope, session.actor, session.csrf_token);
    setPromotionBusy(true);
    setError(null);
    try {
      const receipt = await promoteResearchCandidate(candidate.id, reason, session.csrf_token);
      if (!current()) return;
      setPromotionReceipt(receipt);
      setPromotionUncertain(false);
      setPromotionCandidate(null);
      await publishPromotionEvidence(
        [candidate.id, ...receipt.gene.retired_gene_ids], candidate.strategy_id, capturedScope, session
      );
      if (current()) await workspaceRef.current.refresh();
    } catch (failure) {
      if (!current()) return;
      const uncertain = !(failure instanceof ApiError) || failure.status >= 500 || failure.status === 408 ||
        (failure instanceof Error && failure.message === "invalid_response");
      setPromotionUncertain(uncertain);
      setError(uncertain
        ? { type: "promotion_uncertain", status: failure instanceof ApiError ? failure.status : null }
        : { type: "promotion_rejected", status: failure instanceof ApiError ? failure.status : 0, code: failure instanceof Error ? failure.message : "unknown" });
      if (uncertain) {
        await publishPromotionEvidence([candidate.id], candidate.strategy_id, capturedScope, session);
        if (current()) await workspaceRef.current.refresh();
      }
    } finally {
      if (current()) setPromotionBusy(false);
    }
  }, [isCurrentOperationScope, promotionBusy, promotionCandidate, publishPromotionEvidence, session]);

  const refreshPromotion = useCallback(async () => {
    if (!promotionCandidate || !session) return;
    const capturedScope = operationScope.current;
    await publishPromotionEvidence([promotionCandidate.id], promotionCandidate.strategy_id, capturedScope, session);
    if (isCurrentOperationScope(capturedScope, session.actor, session.csrf_token)) {
      await workspaceRef.current.refresh();
    }
  }, [isCurrentOperationScope, promotionCandidate, publishPromotionEvidence, session]);

  const renewStepUp = useCallback(async () => {
    if (!session) return;
    const capturedScope = operationScope.current;
    try {
      const next = await renewBrowserSession();
      if (!isCurrentOperationScope(capturedScope, session.actor, session.csrf_token)) return;
      setSession(next);
      setPromotionStepUpRequired(!next.permissions.can_step_up);
      setError(null);
    } catch (reason) {
      if (!isCurrentOperationScope(capturedScope, session.actor, session.csrf_token)) return;
      setPromotionStepUpRequired(true);
      setError(reason instanceof ApiError
        ? { type: "promotion_rejected", status: reason.status, code: reason.message }
        : { type: "promotion_rejected", status: 0, code: "session_unavailable" });
    }
  }, [isCurrentOperationScope, session]);

  return {
    session, profile, profileSupported, form, jobs, totalCount, selectedJobId, selectedJob, selectedJobStale,
    loading, loadingDetail, submitting: submittingTargets.length > 0, submittingTargets, error, intents, storageLimited,
    selectedGene, promotionCandidate, promotionBusy, promotionReceipt, promotionEvents,
    promotionUncertain, promotionStepUpRequired, auditUnavailable, setForm,
    refresh, loadMore, selectJob, submit, act, resolveIntent,
    choosePromotion, promote, refreshPromotion, renewStepUp
  };
}
