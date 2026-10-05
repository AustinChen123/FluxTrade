import type { GaJobOperation, GaProfile, GaProfileField } from "../../api";

export const GA_AWAITING_STORAGE_KEY = "fluxtrade.ga.awaiting";

export type GaIntent = Readonly<{
  operation: GaJobOperation;
  jobId: string | null;
  expectedVersion?: number;
  idempotencyKey: string;
  actor: string;
  body: Record<string, unknown>;
  awaiting: boolean;
}>;

export type GaSubmitForm = {
  dataset_id: string;
  start_time: string;
  end_time: string;
  initial_balance: string;
  fees: { maker: string; taker: string };
  instrument: Record<string, string>;
  parameters: {
    short_window: { min: string; max: string; step: string };
    long_window: { min: string; max: string; step: string };
    quantity: string;
  };
  population_size: string;
  max_generations: string;
  seed: string;
};

const requiredProfileFields = [
  "parameter_search_profile_id", "strategy_subject", "fitness_profile_id",
  "cost_profile_id", "profile_revision", "strategy_version", "dataset_id",
  "start_time", "end_time", "initial_balance", "fees", "instrument"
].sort();
const optionalProfileFields = ["parameters", "population_size", "max_generations", "seed"].sort();
const instrumentFields = [
  "capital_model", "capital_per_contract", "fee_model", "multiplier",
  "price_tick", "quantity_step"
].sort();

export function supportsGaProfile(profile: GaProfile): boolean {
  if (!isRecord(profile) || !sameKeys(profile, [
    "accepted_fields", "compiled_fields", "cost_profile_id", "evolution_defaults",
    "fitness_profile_id", "parameter_search_profile_id", "profile_revision",
    "strategy_subject", "strategy_version"
  ])) return false;
  if (profile.parameter_search_profile_id !== "golden_cross_research_v1" ||
      profile.strategy_subject !== "builtin:golden_cross" ||
      profile.fitness_profile_id !== "mark_to_market_pnl_v1" ||
      profile.cost_profile_id !== "explicit_accounting_v1" ||
      !isHash(profile.profile_revision) || !isHash(profile.strategy_version)) return false;
  if (!sameRecord(profile.compiled_fields, {
    kind: "parameter_search", strategy_type: "golden_cross", strategy_id: "golden_cross",
    objective: "maximize_score", market_data: "sealed_dataset", write_reports: false,
    capital_allocation: null, evaluation_set: null, fitness: null
  })) return false;
  if (!sameRecord(profile.evolution_defaults, {
    tournament_size: 2, elite_count: 1, crossover_probability: "0.9",
    mutation_probability: "0.1", mutation_sigma_steps: "1"
  })) return false;

  const fields = profile.accepted_fields;
  if (!sameKeys(fields, [...requiredProfileFields, ...optionalProfileFields].sort())) return false;
  const identityFields = [
    ["parameter_search_profile_id", "golden_cross_research_v1"],
    ["strategy_subject", "builtin:golden_cross"],
    ["fitness_profile_id", "mark_to_market_pnl_v1"],
    ["cost_profile_id", "explicit_accounting_v1"]
  ] as const;
  if (!identityFields.every(([name, value]) => matchesDefinition(fields[name], {
    required: true, type: "string", const: value, immutable: "server_profile"
  }))) return false;
  if (!matchesDefinition(fields.profile_revision, {
    required: true, type: "sha256", immutable: "server_profile"
  }) || !matchesDefinition(fields.strategy_version, {
    required: true, type: "sha256", immutable: "imported_builtin_source"
  })) return false;
  if (!matchesDefinition(fields.dataset_id, {
    required: true, type: "string", source: "sealed_metadata.id", immutable: "compiled_binding"
  }) || !matchesDefinition(fields.start_time, {
    required: true, type: "strict_integer_utc_ms", minimum: 0, maximum: 253402300799999,
    constraint: "within sealed coverage and <= end_time", immutable: "compiled_binding"
  }) || !matchesDefinition(fields.end_time, {
    required: true, type: "strict_integer_utc_ms", minimum: 0, maximum: 253402300799999,
    constraint: "inclusive; within sealed coverage and >= start_time", immutable: "compiled_binding"
  }) || !matchesDefinition(fields.initial_balance, {
    required: true, type: "finite_decimal_or_decimal_string", exclusive_minimum: "0", immutable: "compiled_binding"
  })) return false;
  const fees = objectField(fields.fees);
  if (!matchesDefinition(fields.fees, { required: true, type: "object", immutable: "compiled_binding" }, ["fields"], { fields: isRecord }) ||
      !sameKeys(fees, ["maker", "taker"]) ||
      !["maker", "taker"].every((name) => matchesDefinition(fees?.[name], {
        required: true, type: "finite_decimal_or_decimal_string", minimum: "0", immutable: "compiled_binding"
      }))) return false;

  const instrument = objectField(fields.instrument);
  if (!matchesDefinition(fields.instrument, {
    required: true, type: "object", immutable: "compiled_binding",
    constraint: "dated_future products require quantity_step and price_tick"
  }, ["fields"], { fields: isRecord }) || !sameKeys(instrument, instrumentFields)) return false;
  if (!matchesDefinition(instrument?.multiplier, {
    required: false, type: "finite_decimal_or_decimal_string", exclusive_minimum: "0", immutable: "compiled_binding"
  }, ["default"], { default: isDecimalText })) return false;
  for (const name of ["quantity_step", "price_tick"] as const) {
    if (!matchesDefinition(instrument?.[name], {
      required: false, type: "finite_decimal_or_decimal_string_or_null", default: null,
      exclusive_minimum_when_set: "0", constraint: "required for dated_future products", immutable: "compiled_binding"
    })) return false;
  }
  if (!enumDefinition(instrument?.fee_model, ["per_contract", "percentage_notional"]) ||
      !enumDefinition(instrument?.capital_model, ["notional", "per_contract"]) ||
      !matchesDefinition(instrument?.capital_per_contract, {
        required: false, type: "finite_decimal_or_decimal_string_or_null", default: null,
        exclusive_minimum_when_set: "0",
        constraint: "positive and required only when capital_model is per_contract", immutable: "compiled_binding"
      })) return false;

  const parameters = objectField(fields.parameters);
  if (!matchesDefinition(fields.parameters, {
    required: false, type: "object", dependency: "short_window.max < long_window.min",
    quantity_constraint: "must align to instrument.quantity_step when configured"
  }, ["fields"], { fields: isRecord }) || !sameKeys(parameters, ["short_window", "long_window", "quantity"])) return false;
  if (!rangeDefinition(parameters?.short_window, { min: 5, max: 50, step: 5 }, 1) ||
      !rangeDefinition(parameters?.long_window, { min: 60, max: 200, step: 10 }, 2) ||
      !matchesDefinition(parameters?.quantity, {
        required: false, type: "finite_decimal_or_decimal_string", default: "0.01",
        exclusive_minimum: "0", immutable: "compiled_binding"
      })) return false;
  return matchesDefinition(fields.population_size, {
    required: false, type: "strict_integer", default: 32, minimum: 2, maximum: 256,
    constraint: "must not exceed parameter Cartesian cardinality", immutable: "compiled_binding"
  }) && matchesDefinition(fields.max_generations, {
    required: false, type: "strict_integer", default: 10, minimum: 1, maximum: 100, immutable: "compiled_binding"
  }) && matchesDefinition(fields.seed, {
    required: false, type: "strict_integer", default: 0, minimum: 0, maximum: 2147483647, immutable: "compiled_binding"
  });
}

export function buildGaSubmitPayload(
  profile: GaProfile,
  form: GaSubmitForm
): Record<string, unknown> {
  if (!supportsGaProfile(profile)) throw new Error("unsupported_profile");
  const fields = profile.accepted_fields;
  const instrumentSchema = objectField(fields.instrument) ?? {};
  const parameterSchema = objectField(fields.parameters) ?? {};
  const instrument: Record<string, unknown> = {};
  for (const name of instrumentFields) {
    const definition = instrumentSchema[name];
    const value = form.instrument[name] ?? "";
    instrument[name] = value === "" && definition.default === null
      ? null
      : value === "" ? definition.default : value;
  }
  const range = (name: "short_window" | "long_window") => ({
    min: strictInteger(form.parameters[name].min, `${name}.min`),
    max: strictInteger(form.parameters[name].max, `${name}.max`),
    step: strictInteger(form.parameters[name].step, `${name}.step`)
  });
  const defaults = (name: string) => fields[name]?.default;
  return {
    parameter_search_profile_id: profile.parameter_search_profile_id,
    profile_revision: profile.profile_revision,
    strategy_subject: profile.strategy_subject,
    strategy_version: profile.strategy_version,
    fitness_profile_id: profile.fitness_profile_id,
    cost_profile_id: profile.cost_profile_id,
    dataset_id: requiredText(form.dataset_id, "dataset_id"),
    start_time: strictInteger(form.start_time, "start_time"),
    end_time: strictInteger(form.end_time, "end_time"),
    initial_balance: requiredText(form.initial_balance, "initial_balance"),
    fees: {
      maker: requiredText(form.fees.maker, "fees.maker"),
      taker: requiredText(form.fees.taker, "fees.taker")
    },
    instrument,
    parameters: {
      short_window: range("short_window"),
      long_window: range("long_window"),
      quantity: requiredText(
        form.parameters.quantity || String(parameterSchema.quantity.default ?? ""),
        "parameters.quantity"
      )
    },
    population_size: optionalInteger(form.population_size, defaults("population_size"), "population_size"),
    max_generations: optionalInteger(form.max_generations, defaults("max_generations"), "max_generations"),
    seed: optionalInteger(form.seed, defaults("seed"), "seed")
  };
}

export function initialGaSubmitForm(profile: GaProfile): GaSubmitForm {
  const fields = profile.accepted_fields;
  const instrumentFieldsMap = objectField(fields.instrument) ?? {};
  const parameters = objectField(fields.parameters) ?? {};
  const instrument: Record<string, string> = {};
  for (const name of instrumentFields) {
    const value = instrumentFieldsMap[name]?.default;
    instrument[name] = value === null || value === undefined ? "" : String(value);
  }
  const range = (name: "short_window" | "long_window") => {
    const value = parameters[name]?.default as Record<string, unknown> | undefined;
    return {
      min: value?.min === undefined ? "" : String(value.min),
      max: value?.max === undefined ? "" : String(value.max),
      step: value?.step === undefined ? "" : String(value.step)
    };
  };
  return {
    dataset_id: "", start_time: "", end_time: "", initial_balance: "",
    fees: { maker: "", taker: "" }, instrument,
    parameters: {
      short_window: range("short_window"),
      long_window: range("long_window"),
      quantity: String(parameters.quantity?.default ?? "")
    },
    population_size: String(fields.population_size?.default ?? ""),
    max_generations: String(fields.max_generations?.default ?? ""),
    seed: String(fields.seed?.default ?? "")
  };
}

export function gaActionsForStatus(status: string): Exclude<GaJobOperation, "submit">[] {
  switch (status) {
    case "QUEUED":
    case "RUNNING": return ["pause", "cancel"];
    case "PAUSING": return ["cancel"];
    case "PAUSED": return ["resume", "cancel"];
    case "CANCELLED":
    case "SUCCEEDED":
    case "FAILED": return ["retry"];
    default: return [];
  }
}

export function isDefiniteGaRejection(error: { status: number; message: string }): boolean {
  return (error.status === 401 || error.status === 403 || error.status === 404 || error.status === 422) ||
    (error.status === 409 && error.message !== "idempotency_conflict");
}

export function parseGaIntent(serialized: string | null, actor?: string): GaIntent | null {
  if (serialized === null) return null;
  try {
    const value: unknown = JSON.parse(serialized);
    return isGaIntent(value, actor) ? value : null;
  } catch {
    return null;
  }
}

export function parseGaIntents(serialized: string | null): GaIntent[] {
  if (serialized === null) return [];
  try {
    const raw: unknown = JSON.parse(serialized);
    const values = Array.isArray(raw) ? raw : [raw];
    const parsed = values.filter((value) => isGaIntent(value)).map((value) => value as GaIntent);
    const seen = new Set<string>();
    return parsed.filter((item) => {
      const key = item.operation === "submit" ? "submit" : `job:${item.jobId}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  } catch {
    return [];
  }
}

export function gaIntentTarget(intent: Pick<GaIntent, "operation" | "jobId">): string {
  return intent.operation === "submit" ? "submit" : `job:${intent.jobId}`;
}

function strictInteger(value: string, field: string): number {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) throw new Error(`invalid_${field}`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`invalid_${field}`);
  return parsed;
}

function optionalInteger(value: string, fallback: unknown, field: string): number {
  return value === "" ? strictSchemaInteger(fallback, field) : strictInteger(value, field);
}

function strictSchemaInteger(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value)) throw new Error(`invalid_${field}`);
  return value as number;
}

function requiredText(value: string, field: string): string {
  if (value.trim() === "") throw new Error(`required_${field}`);
  return value;
}

function objectField(value: GaProfileField | undefined): Record<string, GaProfileField> | null {
  return value && isRecord(value.fields)
    ? value.fields as Record<string, GaProfileField>
    : null;
}

function sameKeys(value: object | null | undefined, expected: string[]): boolean {
  return value !== null && value !== undefined &&
    JSON.stringify(Object.keys(value).sort()) === JSON.stringify([...expected].sort());
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function matchesDefinition(
  value: unknown,
  expected: Record<string, unknown>,
  dynamicKeys: string[] = [],
  predicates: Record<string, (item: unknown) => boolean> = {}
): boolean {
  if (!isRecord(value) || !sameKeys(value, [...Object.keys(expected), ...dynamicKeys])) return false;
  return Object.entries(expected).every(([key, item]) => sameValue(value[key], item)) &&
    dynamicKeys.every((key) => predicates[key]?.(value[key]) === true);
}

function sameRecord(value: unknown, expected: Record<string, unknown>): boolean {
  return isRecord(value) && sameKeys(value, Object.keys(expected)) &&
    Object.entries(expected).every(([key, item]) => sameValue(value[key], item));
}

function sameValue(value: unknown, expected: unknown): boolean {
  if (Object.is(value, expected)) return true;
  if (Array.isArray(value) && Array.isArray(expected)) {
    return value.length === expected.length && value.every((item, index) => sameValue(item, expected[index]));
  }
  if (isRecord(value) && isRecord(expected)) return sameRecord(value, expected);
  return false;
}

function enumDefinition(value: unknown, choices: string[]): boolean {
  if (!isRecord(value) || !sameKeys(value, ["choices", "default", "immutable", "required", "type"]) ||
      value.required !== false || value.type !== "string_enum" || value.immutable !== "compiled_binding" ||
      typeof value.default !== "string" || !Array.isArray(value.choices)) return false;
  const received = value.choices;
  return received.every((item): item is string => typeof item === "string") &&
    sameValue(received, choices) && received.includes(value.default);
}

function rangeDefinition(
  value: unknown,
  defaultRange: { min: number; max: number; step: number },
  minimum: number
): boolean {
  if (!isRecord(value) || !sameKeys(value, ["constraint", "default", "fields", "immutable", "required", "type"]) ||
      value.required !== false || value.type !== "integer_range" ||
      value.constraint !== "min <= max; step > 0" || value.immutable !== "compiled_binding" ||
      !sameRecord(value.default, defaultRange)) return false;
  const children = objectField(value as GaProfileField);
  const bound = (key: "min" | "max") => matchesDefinition(children?.[key], {
    required: true, type: "strict_integer", minimum, maximum: 10000, immutable: "compiled_binding"
  });
  return sameKeys(children, ["min", "max", "step"]) && bound("min") && bound("max") &&
    matchesDefinition(children?.step, {
      required: true, type: "strict_integer", exclusive_minimum: 0, immutable: "compiled_binding"
    });
}

function isHash(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

function isDecimalText(value: unknown): value is string {
  return typeof value === "string" &&
    /^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$/.test(value);
}

function isGaIntent(value: unknown, actor?: string): value is GaIntent {
  if (!isRecord(value) || !["submit", "pause", "resume", "cancel", "retry"].includes(String(value.operation)) ||
      typeof value.actor !== "string" || value.actor.length === 0 || (actor !== undefined && value.actor !== actor) ||
      typeof value.idempotencyKey !== "string" || value.idempotencyKey.length === 0 ||
      typeof value.awaiting !== "boolean" || !isRecord(value.body)) return false;
  if (value.operation === "submit") {
    return value.jobId === null && sameKeys(value, ["actor", "awaiting", "body", "idempotencyKey", "jobId", "operation"]) &&
      isSubmitIntentBody(value.body);
  }
  return typeof value.jobId === "string" && value.jobId.length > 0 &&
    sameKeys(value, ["actor", "awaiting", "body", "expectedVersion", "idempotencyKey", "jobId", "operation"]) &&
    Number.isSafeInteger(value.expectedVersion) && (value.expectedVersion as number) > 0 &&
    sameKeys(value.body, ["expected_version"]) && value.body.expected_version === value.expectedVersion;
}

function isSubmitIntentBody(body: Record<string, unknown>): boolean {
  if (!sameKeys(body, [
    "cost_profile_id", "dataset_id", "end_time", "fees", "fitness_profile_id", "initial_balance",
    "instrument", "max_generations", "parameter_search_profile_id", "parameters", "population_size",
    "profile_revision", "seed", "start_time", "strategy_subject", "strategy_version"
  ]) || body.parameter_search_profile_id !== "golden_cross_research_v1" ||
      body.strategy_subject !== "builtin:golden_cross" || body.fitness_profile_id !== "mark_to_market_pnl_v1" ||
      body.cost_profile_id !== "explicit_accounting_v1" || !isHash(body.profile_revision) || !isHash(body.strategy_version) ||
      !nonEmptyText(body.dataset_id) || !safeInteger(body.start_time) || !safeInteger(body.end_time) ||
      (body.end_time as number) < (body.start_time as number) || !nonEmptyText(body.initial_balance)) return false;
  if (!isRecord(body.fees) || !sameKeys(body.fees, ["maker", "taker"]) ||
      !nonEmptyText(body.fees.maker) || !nonEmptyText(body.fees.taker)) return false;
  const instrument = body.instrument;
  if (!isRecord(instrument) || !sameKeys(instrument, instrumentFields)) return false;
  if (!isDecimalText(instrument.multiplier) ||
      !["per_contract", "percentage_notional"].includes(String(instrument.fee_model)) ||
      !["notional", "per_contract"].includes(String(instrument.capital_model)) ||
      !["quantity_step", "price_tick", "capital_per_contract"].every((key) =>
        instrument[key] === null || isDecimalText(instrument[key]))) return false;
  if (!isRecord(body.parameters) || !sameKeys(body.parameters, ["long_window", "quantity", "short_window"]) ||
      !rangeValue(body.parameters.short_window) || !rangeValue(body.parameters.long_window) ||
      !nonEmptyText(body.parameters.quantity)) return false;
  return safeInteger(body.population_size) && safeInteger(body.max_generations) && safeInteger(body.seed);
}

function rangeValue(value: unknown): boolean {
  return isRecord(value) && sameKeys(value, ["max", "min", "step"]) &&
    safeInteger(value.min) && safeInteger(value.max) && safeInteger(value.step);
}

function safeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function nonEmptyText(value: unknown): value is string {
  return typeof value === "string" && value.trim() !== "";
}
