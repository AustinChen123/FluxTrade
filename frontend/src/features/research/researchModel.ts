import type { Epoch, Gene } from "../../api";
import {
  compareGenes,
  epochObjective,
  finiteNumber,
  parameterDimensions,
  type Objective,
  type ParameterDimension
} from "./gaDomain";

export type ResearchAxes = {
  readonly dimensions: ParameterDimension[];
  readonly numericParameters: string[];
};

export type ResearchSelection = {
  readonly selectedGene: Gene | null;
  readonly objective: Objective | null;
  readonly rankedGenes: Gene[];
};

export type ResearchModel = ResearchAxes & ResearchSelection;

export function buildResearchAxes(genes: Gene[]): ResearchAxes {
  const dimensions = parameterDimensions(genes);
  return {
    dimensions,
    numericParameters: dimensions
      .filter((dimension) => dimension.type === "value")
      .map((dimension) => dimension.name)
  };
}

export function buildResearchSelection(
  epoch: Epoch | null,
  genes: Gene[],
  selectedGeneId: number | null
): ResearchSelection {
  const selectedGene =
    genes.find((gene) => gene.id === selectedGeneId) ?? null;
  const objective = epoch ? epochObjective(epoch) : null;
  const rankedGenes =
    epoch && objective !== null
      ? [...genes]
          .sort((left, right) => compareGenes(epoch, left, right))
          .slice(0, 8)
      : [];
  return { selectedGene, objective, rankedGenes };
}

export function buildResearchModel(
  epoch: Epoch | null,
  genes: Gene[],
  selectedGeneId: number | null
): ResearchModel {
  return {
    ...buildResearchAxes(genes),
    ...buildResearchSelection(epoch, genes, selectedGeneId)
  };
}

export function displayNumber(
  value: string | number | null,
  locale: string,
  digits = 4
): string {
  const parsed = finiteNumber(value);
  return parsed === null
    ? "—"
    : new Intl.NumberFormat(locale, {
        maximumFractionDigits: digits
      }).format(parsed);
}

export function displayValue(value: unknown, locale: string): string {
  const parsed = finiteNumber(value);
  return parsed === null
    ? String(value)
    : new Intl.NumberFormat(locale, {
        maximumFractionDigits: 8
      }).format(parsed);
}

export function geneIdFromChartData(data: unknown): number | null {
  if (Array.isArray(data)) {
    return typeof data[3] === "number" ? data[3] : null;
  }
  if (data && typeof data === "object" && "geneId" in data) {
    const value = (data as { geneId?: unknown }).geneId;
    return typeof value === "number" ? value : null;
  }
  return null;
}
