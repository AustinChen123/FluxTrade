import { useEffect, useMemo, useState } from "react";

import {
  ApiError,
  ensureBrowserSession,
  loadEpochs,
  loadGenerationGenes,
  loadGenerationSummaries,
  type Epoch,
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
  readonly error: ResearchError | null;
  readonly model: ResearchModel;
  readonly chooseEpoch: (epochId: string) => void;
  readonly chooseGeneration: (generationIndex: number) => void;
  readonly chooseGene: (geneId: number) => void;
  readonly chooseXParameter: (parameter: string) => void;
  readonly chooseYParameter: (parameter: string) => void;
  readonly chooseSurfaceMode: (mode: SurfaceMode) => void;
  readonly retry: () => void;
};

export function useResearchWorkspace(demoMode: boolean): ResearchWorkspace {
  const [epochs, setEpochs] = useState<Epoch[]>([]);
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

  const epoch = epochs.find((item) => item.id === epochId) ?? null;

  useEffect(() => {
    if (epochsLoaded) {
      return;
    }
    let active = true;
    setLoading(true);
    setFailure(null);
    const load = async () => {
      if (demoMode) {
        return [demoEpoch];
      }
      await ensureBrowserSession();
      return loadEpochs();
    };
    void load()
      .then((items) => {
        if (!active) {
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
          active &&
          setFailure({ stage: "epochs", error: classifyResearchError(reason) })
      )
      .finally(() => active && setLoading(false));
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
        if (!active) {
          return;
        }
        setSummaries(items);
        setGenerationIndex(items.at(-1)?.generation_index ?? null);
      })
      .catch(
        (reason) =>
          active &&
          setFailure({
            stage: "summaries",
            error: classifyResearchError(reason)
          })
      )
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epoch, reloadToken.summaries]);

  useEffect(() => {
    if (!epoch || generationIndex === null) {
      setGenes([]);
      return;
    }
    let active = true;
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
        if (!active) {
          return;
        }
        setGenes(items);
        setSelectedGeneId(selectBestGene(epoch, items)?.id ?? null);
      })
      .catch(
        (reason) =>
          active &&
          setFailure({ stage: "genes", error: classifyResearchError(reason) })
      )
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epoch, generationIndex, reloadToken.genes]);

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

  const chooseEpoch = (nextEpochId: string) => {
    setSummaries([]);
    setGenerationIndex(null);
    setGenes([]);
    setSelectedGeneId(null);
    setEpochId(nextEpochId);
  };

  return {
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
    error: failure?.error ?? null,
    model,
    chooseEpoch,
    chooseGeneration: setGenerationIndex,
    chooseGene: setSelectedGeneId,
    chooseXParameter: setXParameter,
    chooseYParameter: setYParameter,
    chooseSurfaceMode: setSurfaceMode,
    retry: () => {
      if (failure === null) {
        return;
      }
      setReloadToken((current) => ({
        ...current,
        [failure.stage]: current[failure.stage] + 1
      }));
    }
  };
}
