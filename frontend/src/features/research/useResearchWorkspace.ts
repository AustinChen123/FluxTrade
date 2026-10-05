import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  ApiError,
  ensureBrowserSession,
  loadEpochById,
  loadEpochs,
  loadGenerationGenes,
  loadGenerationSummaries,
  type Epoch,
  type BrowserSession,
  type Gene,
  type GenerationSummary
} from "../../api";
import { buildDemoGenes, demoEpoch, demoSummaries } from "./demo";
import { selectBestGene } from "./gaDomain";
import {
  buildResearchAxes,
  buildResearchSelection,
  type ResearchModel
} from "./researchModel";

export type SurfaceMode = "2d" | "3d";
type ResearchLoadStage = "epochs" | "summaries" | "genes";
type ResearchFailure = {
  readonly stage: ResearchLoadStage;
  readonly error: ResearchError;
  readonly fullRefresh?: true;
};
type ResearchReloadToken = Readonly<Record<ResearchLoadStage, number>>;

export type ResearchError =
  | Readonly<{ type: "unauthorized" }>
  | Readonly<{ type: "service"; status: number; message: string }>
  | Readonly<{ type: "unexpected"; message: string }>
  | Readonly<{ type: "fallback" }>;

export function classifyResearchError(reason: unknown): ResearchError {
  if (reason instanceof ApiError) {
    if (reason.status === 401 || reason.status === 403) {
      return { type: "unauthorized" };
    }
    return {
      type: "service",
      status: reason.status,
      message: reason.message
    };
  }
  return reason instanceof Error
    ? { type: "unexpected", message: reason.message }
    : { type: "fallback" };
}

export type ResearchWorkspace = {
  readonly session: BrowserSession | null;
  readonly sessionResolved: boolean;
  readonly epochs: Epoch[];
  readonly epoch: Epoch | null;
  readonly epochId: string;
  readonly summaries: GenerationSummary[];
  readonly generationIndex: number | null;
  readonly genes: Gene[];
  readonly selectedGeneId: number | null;
  readonly xParameter: string;
  readonly yParameter: string;
  readonly surfaceMode: SurfaceMode;
  readonly loading: boolean;
  readonly ready: boolean;
  readonly error: ResearchError | null;
  readonly model: ResearchModel;
  readonly chooseEpoch: (epochId: string) => void;
  readonly openEpoch: (epochId: string) => Promise<void>;
  readonly chooseGeneration: (generationIndex: number) => void;
  readonly chooseGene: (geneId: number) => void;
  readonly chooseXParameter: (parameter: string) => void;
  readonly chooseYParameter: (parameter: string) => void;
  readonly chooseSurfaceMode: (mode: SurfaceMode) => void;
  readonly retry: () => void;
  readonly refresh: () => Promise<void>;
};

export function useResearchWorkspace(demoMode: boolean): ResearchWorkspace {
  const [epochs, setEpochs] = useState<Epoch[]>([]);
  const [session, setSession] = useState<BrowserSession | null>(null);
  const [sessionResolved, setSessionResolved] = useState(demoMode);
  const [epochId, setEpochId] = useState("");
  const [summaries, setSummaries] = useState<GenerationSummary[]>([]);
  const [generationIndex, setGenerationIndex] = useState<number | null>(null);
  const [genes, setGenes] = useState<Gene[]>([]);
  const [selectedGeneId, setSelectedGeneId] = useState<number | null>(null);
  const [xParameter, setXParameter] = useState("");
  const [yParameter, setYParameter] = useState("");
  const [surfaceMode, setSurfaceMode] = useState<SurfaceMode>("2d");
  const [loading, setLoading] = useState(true);
  const [failure, setFailure] = useState<ResearchFailure | null>(null);
  const [epochsLoaded, setEpochsLoaded] = useState(false);
  const [reloadToken, setReloadToken] = useState<ResearchReloadToken>({
    epochs: 0,
    summaries: 0,
    genes: 0
  });
  const refreshVersion = useRef(0);
  const lastEpochRetry = useRef(0);
  const refreshSelection = useRef({
    demoMode,
    epochId,
    generationIndex,
    selectedGeneId
  });
  refreshSelection.current = {
    demoMode,
    epochId,
    generationIndex,
    selectedGeneId
  };

  useEffect(() => {
    refreshVersion.current += 1;
    return () => {
      refreshVersion.current += 1;
    };
  }, [demoMode]);

  const epoch = epochs.find((item) => item.id === epochId) ?? null;

  useEffect(() => {
    const retryRequested = reloadToken.epochs !== lastEpochRetry.current;
    if (epochsLoaded && !retryRequested) {
      return;
    }
    lastEpochRetry.current = reloadToken.epochs;
    let active = true;
    const version = refreshVersion.current;
    const current = () => active && version === refreshVersion.current;
    setLoading(true);
    setFailure(null);
    const load = async () => {
      if (demoMode) {
        return [demoEpoch];
      }
      const currentSession = await ensureBrowserSession();
      if (current()) {
        setSession(currentSession);
        setSessionResolved(true);
      }
      return loadEpochs();
    };
    void load()
      .then((items) => {
        if (!current()) {
          return;
        }
        setEpochs(items);
        setEpochsLoaded(true);
        setEpochId((current) =>
          items.some((item) => item.id === current)
            ? current
            : (items[0]?.id ?? "")
        );
      })
      .catch(
        (reason) =>
          current() &&
          setFailure({ stage: "epochs", error: classifyResearchError(reason) })
      )
      .finally(() => current() && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epochsLoaded, reloadToken.epochs]);

  useEffect(() => {
    if (!epoch) {
      setSummaries([]);
      setGenerationIndex(null);
      return;
    }
    let active = true;
    const version = refreshVersion.current;
    const current = () => active && version === refreshVersion.current;
    setSummaries([]);
    setGenerationIndex(null);
    setGenes([]);
    setSelectedGeneId(null);
    setLoading(true);
    setFailure(null);
    const load = demoMode
      ? Promise.resolve(demoSummaries)
      : loadGenerationSummaries(epoch.id);
    void load
      .then((items) => {
        if (!current()) {
          return;
        }
        setSummaries(items);
        setGenerationIndex(items.at(-1)?.generation_index ?? null);
      })
      .catch(
        (reason) =>
          current() &&
          setFailure({
            stage: "summaries",
            error: classifyResearchError(reason)
          })
      )
      .finally(() => current() && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epochId, reloadToken.summaries]);

  useEffect(() => {
    if (!epoch || generationIndex === null) {
      setGenes([]);
      return;
    }
    let active = true;
    const version = refreshVersion.current;
    const current = () => active && version === refreshVersion.current;
    setGenes([]);
    setSelectedGeneId(null);
    setLoading(true);
    setFailure(null);
    const load =
      demoMode && epoch.id === demoEpoch.id
        ? Promise.resolve(buildDemoGenes(generationIndex))
        : loadGenerationGenes(epoch.id, generationIndex);
    void load
      .then((items) => {
        if (!current()) {
          return;
        }
        setGenes(items);
        setSelectedGeneId(selectBestGene(epoch, items)?.id ?? null);
      })
      .catch(
        (reason) =>
          current() &&
          setFailure({ stage: "genes", error: classifyResearchError(reason) })
      )
      .finally(() => current() && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epochId, generationIndex, reloadToken.genes]);

  const axes = useMemo(() => buildResearchAxes(genes), [genes]);
  const selection = useMemo(
    () => buildResearchSelection(epoch, genes, selectedGeneId),
    [epoch, genes, selectedGeneId]
  );
  const model = useMemo(
    () => ({ ...axes, ...selection }),
    [axes, selection]
  );
  const numericParameters = axes.numericParameters;
  useEffect(() => {
    const nextX = numericParameters.includes(xParameter)
      ? xParameter
      : (numericParameters[0] ?? "");
    const nextY =
      numericParameters.includes(yParameter) && yParameter !== nextX
        ? yParameter
        : (numericParameters.find((name) => name !== nextX) ?? "");
    setXParameter(nextX);
    setYParameter(nextY);
  }, [numericParameters]);

  const refresh = useCallback(async () => {
    const selected = refreshSelection.current;
    if (selected.demoMode) return;
    const version = ++refreshVersion.current;
    let stage: ResearchLoadStage = "epochs";
    const current = () => version === refreshVersion.current;
    setLoading(true);
    setFailure(null);
    try {
      const currentSession = await ensureBrowserSession();
      if (!current()) return;
      setSession(currentSession);
      setSessionResolved(true);
      const nextEpochs = await loadEpochs();
      if (!current()) return;
      const listedEpoch = nextEpochs.find((item) => item.id === selected.epochId);
      const nextEpoch = listedEpoch ?? (selected.epochId
        ? await loadEpochById(selected.epochId)
        : nextEpochs[0] ?? null);
      const committedEpochs = nextEpoch && !nextEpochs.some((item) => item.id === nextEpoch.id)
        ? [...nextEpochs, nextEpoch]
        : nextEpochs;
      stage = "summaries";
      const nextSummaries = nextEpoch
        ? await loadGenerationSummaries(nextEpoch.id)
        : [];
      if (!current()) return;
      const sameEpoch = nextEpoch?.id === selected.epochId;
      const nextGeneration = sameEpoch && nextSummaries.some(
        (item) => item.generation_index === selected.generationIndex
      )
        ? selected.generationIndex
        : (nextSummaries.at(-1)?.generation_index ?? null);
      stage = "genes";
      const nextGenes = nextEpoch && nextGeneration !== null
        ? await loadGenerationGenes(nextEpoch.id, nextGeneration)
        : [];
      if (!current()) return;
      setEpochs(committedEpochs);
      setEpochsLoaded(true);
      setEpochId(nextEpoch?.id ?? "");
      setSummaries(nextSummaries);
      setGenerationIndex(nextGeneration);
      setGenes(nextGenes);
      setSelectedGeneId(
        sameEpoch && nextGeneration === selected.generationIndex &&
        nextGenes.some((item) => item.id === selected.selectedGeneId)
          ? selected.selectedGeneId
          : (nextEpoch ? selectBestGene(nextEpoch, nextGenes)?.id ?? null : null)
      );
    } catch (reason) {
      if (current()) {
        setFailure({
          stage,
          error: classifyResearchError(reason),
          fullRefresh: true
        });
      }
    } finally {
      if (current()) setLoading(false);
    }
  }, []);

  const chooseEpoch = (nextEpochId: string) => {
    refreshVersion.current += 1;
    setLoading(false);
    setSummaries([]);
    setGenerationIndex(null);
    setGenes([]);
    setSelectedGeneId(null);
    setEpochId(nextEpochId);
  };
  const openEpoch = useCallback(async (nextEpochId: string) => {
    if (demoMode) return;
    if (epochs.some((item) => item.id === nextEpochId)) {
      refreshVersion.current += 1;
      setLoading(false);
      setSummaries([]);
      setGenerationIndex(null);
      setGenes([]);
      setSelectedGeneId(null);
      setEpochId(nextEpochId);
      return;
    }
    const version = ++refreshVersion.current;
    const current = () => version === refreshVersion.current;
    setLoading(true);
    setFailure(null);
    try {
      const currentSession = await ensureBrowserSession();
      if (!current()) return;
      setSession(currentSession);
      setSessionResolved(true);
      const item = await loadEpochById(nextEpochId);
      if (!current()) return;
      setEpochs((items) => items.some((existing) => existing.id === item.id)
        ? items
        : [...items, item]);
      setSummaries([]);
      setGenerationIndex(null);
      setGenes([]);
      setSelectedGeneId(null);
      setEpochId(item.id);
    } catch (reason) {
      if (current()) {
        setFailure({ stage: "epochs", error: classifyResearchError(reason) });
      }
    } finally {
      if (current()) setLoading(false);
    }
  }, [demoMode, epochs]);
  const chooseGeneration = (nextGenerationIndex: number) => {
    refreshVersion.current += 1;
    setGenerationIndex(nextGenerationIndex);
  };
  const chooseGene = (nextGeneId: number) => {
    refreshVersion.current += 1;
    setLoading(false);
    setSelectedGeneId(nextGeneId);
  };

  return {
    session,
    sessionResolved,
    epochs,
    epoch,
    epochId,
    summaries,
    generationIndex,
    genes,
    selectedGeneId,
    xParameter,
    yParameter,
    surfaceMode,
    loading,
    ready: epochsLoaded,
    error: failure?.error ?? null,
    model,
    chooseEpoch,
    openEpoch,
    chooseGeneration,
    chooseGene,
    chooseXParameter: setXParameter,
    chooseYParameter: setYParameter,
    chooseSurfaceMode: setSurfaceMode,
    retry: () => {
      if (failure === null) {
        return;
      }
      if (failure.fullRefresh) {
        void refresh();
        return;
      }
      refreshVersion.current += 1;
      setReloadToken((current) => ({
        ...current,
        [failure.stage]: current[failure.stage] + 1
      }));
    },
    refresh
  };
}
