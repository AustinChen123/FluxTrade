import type { Epoch, Gene } from "../../api";

export type ParameterDimension = {
  name: string;
  type: "value" | "category";
  categories?: string[];
};

export type Objective =
  | "maximize_score"
  | "maximize_return"
  | "minimize_drawdown";

type DecimalParts = {
  negative: boolean;
  integer: string;
  fraction: string;
};

export function finiteNumber(value: unknown): number | null {
  if (typeof value !== "number" && typeof value !== "string") {
    return null;
  }
  if (typeof value === "string" && value.trim() === "") {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function decimalParts(value: unknown, absolute = false): DecimalParts | null {
  if (typeof value !== "string") {
    return null;
  }
  const match = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(value.trim());
  if (!match) {
    return null;
  }
  const integer = match[2].replace(/^0+(?=\d)/, "");
  const fraction = (match[3] ?? "").replace(/0+$/, "");
  const zero = integer === "0" && fraction === "";
  return {
    negative: !absolute && match[1] === "-" && !zero,
    integer,
    fraction
  };
}

function compareMagnitude(left: DecimalParts, right: DecimalParts): number {
  if (left.integer.length !== right.integer.length) {
    return left.integer.length - right.integer.length;
  }
  const integer =
    left.integer === right.integer ? 0 : left.integer < right.integer ? -1 : 1;
  if (integer !== 0) {
    return integer;
  }
  const length = Math.max(left.fraction.length, right.fraction.length);
  const leftFraction = left.fraction.padEnd(length, "0");
  const rightFraction = right.fraction.padEnd(length, "0");
  return leftFraction === rightFraction
    ? 0
    : leftFraction < rightFraction
      ? -1
      : 1;
}

function compareDecimals(left: DecimalParts, right: DecimalParts): number {
  if (left.negative !== right.negative) {
    return left.negative ? -1 : 1;
  }
  const magnitude = compareMagnitude(left, right);
  return left.negative ? -magnitude : magnitude;
}

export function epochObjective(epoch: Epoch): Objective | null {
  const objective = epoch.config_json.objective;
  return objective === "maximize_score" ||
    objective === "maximize_return" ||
    objective === "minimize_drawdown"
    ? objective
    : null;
}

export function parameterDimensions(genes: Gene[]): ParameterDimension[] {
  const names = [...new Set(genes.flatMap((gene) => Object.keys(gene.param_pack)))]
    .sort();
  return names.map((name) => {
    const values = genes
      .map((gene) => gene.param_pack[name])
      .filter((value) => value !== undefined && value !== null);
    if (values.length > 0 && values.every((value) => finiteNumber(value) !== null)) {
      return { name, type: "value" };
    }
    return {
      name,
      type: "category",
      categories: [...new Set(values.map(String))].sort()
    };
  });
}

export function numericParameterNames(genes: Gene[]): string[] {
  return parameterDimensions(genes)
    .filter((dimension) => dimension.type === "value")
    .map((dimension) => dimension.name);
}

export type SurfaceRow = [
  number,
  number,
  number,
  number,
  string
];

function objectiveDecimal(epoch: Epoch, gene: Gene): DecimalParts | null {
  const objective = epochObjective(epoch);
  if (objective === null) {
    return null;
  }
  return objective === "minimize_drawdown"
    ? decimalParts(gene.max_drawdown, true)
    : decimalParts(gene.score_total);
}

function objectiveValue(epoch: Epoch, gene: Gene): number | null {
  const objective = epochObjective(epoch);
  const raw =
    objective === "minimize_drawdown"
      ? gene.max_drawdown
      : objective === null
        ? null
        : gene.score_total;
  if (raw === null || objectiveDecimal(epoch, gene) === null) {
    return null;
  }
  const value = finiteNumber(raw);
  return value === null
    ? null
    : objective === "minimize_drawdown"
      ? Math.abs(value)
      : value;
}

export function compareGenes(epoch: Epoch, left: Gene, right: Gene): number {
  const objective = epochObjective(epoch);
  const minimize = objective === "minimize_drawdown";
  const leftValue = objectiveDecimal(epoch, left);
  const rightValue = objectiveDecimal(epoch, right);
  if (leftValue === null) {
    return rightValue === null ? 0 : 1;
  }
  if (rightValue === null) {
    return -1;
  }
  const comparison = compareDecimals(leftValue, rightValue);
  const primary = minimize ? comparison : -comparison;
  if (primary !== 0 || !minimize) {
    return primary;
  }
  const leftScore = decimalParts(left.score_total);
  const rightScore = decimalParts(right.score_total);
  if (leftScore === null) {
    return rightScore === null ? 0 : 1;
  }
  return rightScore === null ? -1 : -compareDecimals(leftScore, rightScore);
}

export function selectBestGene(epoch: Epoch, genes: Gene[]): Gene | null {
  if (epochObjective(epoch) === null) {
    return null;
  }
  return genes.reduce<Gene | null>(
    (best, candidate) =>
      best === null || compareGenes(epoch, candidate, best) < 0
        ? candidate
        : best,
    null
  );
}

export function fitnessSurfaceRows(
  epoch: Epoch,
  genes: Gene[],
  xParameter: string,
  yParameter: string
): SurfaceRow[] {
  if (epochObjective(epoch) === null) {
    return [];
  }
  const observed = new Map<string, Gene>();
  for (const gene of genes) {
    const x = finiteNumber(gene.param_pack[xParameter]);
    const y = finiteNumber(gene.param_pack[yParameter]);
    if (x === null || y === null || objectiveValue(epoch, gene) === null) {
      continue;
    }
    const key = `${x}\0${y}`;
    const current = observed.get(key);
    if (current === undefined || compareGenes(epoch, gene, current) < 0) {
      observed.set(key, gene);
    }
  }
  return [...observed.values()].flatMap((gene) => {
    const row = surfaceRow(epoch, gene, xParameter, yParameter);
    return row === null ? [] : [row];
  });
}

export function fitnessObservationRows(
  epoch: Epoch,
  genes: Gene[],
  xParameter: string,
  yParameter: string
): SurfaceRow[] {
  if (epochObjective(epoch) === null) {
    return [];
  }
  return genes.flatMap((gene) => {
    const row = surfaceRow(epoch, gene, xParameter, yParameter);
    return row === null ? [] : [row];
  });
}

export function surfaceRow(
  epoch: Epoch,
  gene: Gene | null,
  xParameter: string,
  yParameter: string
): SurfaceRow | null {
  if (gene === null) {
    return null;
  }
  const x = finiteNumber(gene.param_pack[xParameter]);
  const y = finiteNumber(gene.param_pack[yParameter]);
  const value = objectiveValue(epoch, gene);
  return x === null || y === null || value === null
    ? null
    : [x, y, value, gene.id, gene.candidate_id];
}
