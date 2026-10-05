import { describe, expect, it } from "vitest";

import type { GaProfile } from "../../api";

import {
  buildGaSubmitPayload,
  gaActionsForStatus,
  initialGaSubmitForm,
  isDefiniteGaRejection,
  parseGaIntent,
  parseGaIntents,
  supportsGaProfile,
  type GaIntent
} from "./gaOperationsModel";

describe("research GA operation rules", () => {
  it("exposes only the backend-supported action for each persisted status", () => {
    expect(gaActionsForStatus("QUEUED")).toEqual(["pause", "cancel"]);
    expect(gaActionsForStatus("RUNNING")).toEqual(["pause", "cancel"]);
    expect(gaActionsForStatus("PAUSING")).toEqual(["cancel"]);
    expect(gaActionsForStatus("PAUSED")).toEqual(["resume", "cancel"]);
    expect(gaActionsForStatus("CANCELLING")).toEqual([]);
    expect(gaActionsForStatus("CANCELLED")).toEqual(["retry"]);
    expect(gaActionsForStatus("SUCCEEDED")).toEqual(["retry"]);
    expect(gaActionsForStatus("FAILED")).toEqual(["retry"]);
    expect(gaActionsForStatus("UNKNOWN")).toEqual([]);
  });

  it("distinguishes first-attempt rejection from unresolved receipt conflicts", () => {
    expect(isDefiniteGaRejection({ status: 422, message: "validation_error" })).toBe(true);
    expect(isDefiniteGaRejection({ status: 409, message: "job_version_conflict" })).toBe(true);
    expect(isDefiniteGaRejection({ status: 409, message: "idempotency_conflict" })).toBe(false);
    expect(isDefiniteGaRejection({ status: 503, message: "ga_backend_unavailable" })).toBe(false);
  });

  it("preserves exact decimal text while applying only declared profile defaults", () => {
    const profile = {
      parameter_search_profile_id: "golden_cross_research_v1",
      profile_revision: "a".repeat(64),
      strategy_subject: "builtin:golden_cross",
      strategy_version: "b".repeat(64),
      fitness_profile_id: "mark_to_market_pnl_v1",
      cost_profile_id: "explicit_accounting_v1",
      accepted_fields: {
        parameter_search_profile_id: { required: true, type: "string", const: "golden_cross_research_v1", immutable: "server_profile" },
        strategy_subject: { required: true, type: "string", const: "builtin:golden_cross", immutable: "server_profile" },
        fitness_profile_id: { required: true, type: "string", const: "mark_to_market_pnl_v1", immutable: "server_profile" },
        cost_profile_id: { required: true, type: "string", const: "explicit_accounting_v1", immutable: "server_profile" },
        profile_revision: { required: true, type: "sha256", immutable: "server_profile" },
        strategy_version: { required: true, type: "sha256", immutable: "imported_builtin_source" },
        dataset_id: { required: true, type: "string", source: "sealed_metadata.id", immutable: "compiled_binding" },
        start_time: { required: true, type: "strict_integer_utc_ms", minimum: 0, maximum: 253402300799999, constraint: "within sealed coverage and <= end_time", immutable: "compiled_binding" },
        end_time: { required: true, type: "strict_integer_utc_ms", minimum: 0, maximum: 253402300799999, constraint: "inclusive; within sealed coverage and >= start_time", immutable: "compiled_binding" },
        initial_balance: { required: true, type: "finite_decimal_or_decimal_string", exclusive_minimum: "0", immutable: "compiled_binding" },
        fees: { required: true, type: "object", immutable: "compiled_binding", fields: {
          maker: { required: true, type: "finite_decimal_or_decimal_string", minimum: "0", immutable: "compiled_binding" },
          taker: { required: true, type: "finite_decimal_or_decimal_string", minimum: "0", immutable: "compiled_binding" }
        } },
        instrument: { required: true, type: "object", immutable: "compiled_binding", constraint: "dated_future products require quantity_step and price_tick", fields: {
          multiplier: { required: false, type: "finite_decimal_or_decimal_string", default: "1", exclusive_minimum: "0", immutable: "compiled_binding" },
          quantity_step: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "required for dated_future products", immutable: "compiled_binding" },
          price_tick: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "required for dated_future products", immutable: "compiled_binding" },
          fee_model: { required: false, type: "string_enum", default: "percentage_notional", choices: ["per_contract", "percentage_notional"], immutable: "compiled_binding" },
          capital_model: { required: false, type: "string_enum", default: "notional", choices: ["notional", "per_contract"], immutable: "compiled_binding" },
          capital_per_contract: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "positive and required only when capital_model is per_contract", immutable: "compiled_binding" }
        } },
        parameters: { required: false, type: "object", dependency: "short_window.max < long_window.min", quantity_constraint: "must align to instrument.quantity_step when configured", fields: {
          short_window: { required: false, type: "integer_range", default: { min: 5, max: 50, step: 5 }, constraint: "min <= max; step > 0", immutable: "compiled_binding", fields: {
            min: { required: true, type: "strict_integer", minimum: 1, maximum: 10000, immutable: "compiled_binding" },
            max: { required: true, type: "strict_integer", minimum: 1, maximum: 10000, immutable: "compiled_binding" },
            step: { required: true, type: "strict_integer", exclusive_minimum: 0, immutable: "compiled_binding" }
          } },
          long_window: { required: false, type: "integer_range", default: { min: 60, max: 200, step: 10 }, constraint: "min <= max; step > 0", immutable: "compiled_binding", fields: {
            min: { required: true, type: "strict_integer", minimum: 2, maximum: 10000, immutable: "compiled_binding" },
            max: { required: true, type: "strict_integer", minimum: 2, maximum: 10000, immutable: "compiled_binding" },
            step: { required: true, type: "strict_integer", exclusive_minimum: 0, immutable: "compiled_binding" }
          } },
          quantity: { required: false, type: "finite_decimal_or_decimal_string", default: "0.01", exclusive_minimum: "0", immutable: "compiled_binding" }
        } },
        population_size: { required: false, type: "strict_integer", default: 32, minimum: 2, maximum: 256, constraint: "must not exceed parameter Cartesian cardinality", immutable: "compiled_binding" },
        max_generations: { required: false, type: "strict_integer", default: 10, minimum: 1, maximum: 100, immutable: "compiled_binding" },
        seed: { required: false, type: "strict_integer", default: 0, minimum: 0, maximum: 2147483647, immutable: "compiled_binding" }
      },
      compiled_fields: {
        kind: "parameter_search", strategy_type: "golden_cross", strategy_id: "golden_cross",
        objective: "maximize_score", market_data: "sealed_dataset", write_reports: false,
        capital_allocation: null, evaluation_set: null, fitness: null
      },
      evolution_defaults: { tournament_size: 2, elite_count: 1, crossover_probability: "0.9", mutation_probability: "0.1", mutation_sigma_steps: "1" }
    };
    expect(supportsGaProfile(profile as unknown as GaProfile)).toBe(true);
    expect(initialGaSubmitForm(profile as unknown as GaProfile).parameters).toEqual({
      short_window: { min: "5", max: "50", step: "5" },
      long_window: { min: "60", max: "200", step: "10" },
      quantity: "0.01"
    });
    const payload = buildGaSubmitPayload(profile as unknown as GaProfile, {
      dataset_id: "sealed-1", start_time: "1767225600000", end_time: "1767311999999",
      initial_balance: "100000000000000000.000000000000000001",
      fees: { maker: "0.0000000000000000001", taker: "0.0000000000000000002" },
      instrument: {
        multiplier: "1", quantity_step: "0.001", price_tick: "0.25",
        fee_model: "percentage_notional", capital_model: "notional", capital_per_contract: ""
      },
      parameters: {
        short_window: { min: "5", max: "20", step: "5" },
        long_window: { min: "30", max: "60", step: "10" },
        quantity: "0.001"
      },
      population_size: "12", max_generations: "4", seed: "9"
    });

    expect(payload.initial_balance).toBe("100000000000000000.000000000000000001");
    expect(payload.fees).toEqual({ maker: "0.0000000000000000001", taker: "0.0000000000000000002" });
    expect((payload.instrument as Record<string, unknown>).capital_per_contract).toBeNull();
    expect((payload.parameters as Record<string, unknown>).quantity).toBe("0.001");
    expect(payload.population_size).toBe(12);
    expect(payload.max_generations).toBe(4);
    expect(payload.seed).toBe(9);
    expect("write_reports" in payload).toBe(false);
    expect("tournament_size" in payload).toBe(false);
  });

  it("rejects malformed or unsupported server profile grammar", () => {
    const base = {
      parameter_search_profile_id: "golden_cross_research_v1",
      profile_revision: "a".repeat(64), strategy_subject: "builtin:golden_cross",
      strategy_version: "b".repeat(64), fitness_profile_id: "mark_to_market_pnl_v1",
      cost_profile_id: "explicit_accounting_v1",
      accepted_fields: {
        parameter_search_profile_id: { required: true, type: "string", const: "golden_cross_research_v1", immutable: "server_profile" },
        strategy_subject: { required: true, type: "string", const: "builtin:golden_cross", immutable: "server_profile" },
        fitness_profile_id: { required: true, type: "string", const: "mark_to_market_pnl_v1", immutable: "server_profile" },
        cost_profile_id: { required: true, type: "string", const: "explicit_accounting_v1", immutable: "server_profile" },
        profile_revision: { required: true, type: "sha256", immutable: "server_profile" },
        strategy_version: { required: true, type: "sha256", immutable: "imported_builtin_source" },
        dataset_id: { required: true, type: "string", source: "sealed_metadata.id", immutable: "compiled_binding" },
        start_time: { required: true, type: "strict_integer_utc_ms", minimum: 0, maximum: 253402300799999, constraint: "within sealed coverage and <= end_time", immutable: "compiled_binding" },
        end_time: { required: true, type: "strict_integer_utc_ms", minimum: 0, maximum: 253402300799999, constraint: "inclusive; within sealed coverage and >= start_time", immutable: "compiled_binding" },
        initial_balance: { required: true, type: "finite_decimal_or_decimal_string", exclusive_minimum: "0", immutable: "compiled_binding" },
        fees: { required: true, type: "object", immutable: "compiled_binding", fields: {
          maker: { required: true, type: "finite_decimal_or_decimal_string", minimum: "0", immutable: "compiled_binding" },
          taker: { required: true, type: "finite_decimal_or_decimal_string", minimum: "0", immutable: "compiled_binding" }
        } },
        instrument: { required: true, type: "object", immutable: "compiled_binding", constraint: "dated_future products require quantity_step and price_tick", fields: {
          multiplier: { required: false, type: "finite_decimal_or_decimal_string", default: "1", exclusive_minimum: "0", immutable: "compiled_binding" },
          quantity_step: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "required for dated_future products", immutable: "compiled_binding" },
          price_tick: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "required for dated_future products", immutable: "compiled_binding" },
          fee_model: { required: false, type: "string_enum", default: "percentage_notional", choices: ["per_contract", "percentage_notional"], immutable: "compiled_binding" },
          capital_model: { required: false, type: "string_enum", default: "notional", choices: ["notional", "per_contract"], immutable: "compiled_binding" },
          capital_per_contract: { required: false, type: "finite_decimal_or_decimal_string_or_null", default: null, exclusive_minimum_when_set: "0", constraint: "positive and required only when capital_model is per_contract", immutable: "compiled_binding" }
        } },
        parameters: { required: false, type: "object", dependency: "short_window.max < long_window.min", quantity_constraint: "must align to instrument.quantity_step when configured", fields: {
          short_window: { required: false, type: "integer_range", default: { min: 5, max: 50, step: 5 }, constraint: "min <= max; step > 0", immutable: "compiled_binding", fields: {
            min: { required: true, type: "strict_integer", minimum: 1, maximum: 10000, immutable: "compiled_binding" },
            max: { required: true, type: "strict_integer", minimum: 1, maximum: 10000, immutable: "compiled_binding" },
            step: { required: true, type: "strict_integer", exclusive_minimum: 0, immutable: "compiled_binding" }
          } },
          long_window: { required: false, type: "integer_range", default: { min: 60, max: 200, step: 10 }, constraint: "min <= max; step > 0", immutable: "compiled_binding", fields: {
            min: { required: true, type: "strict_integer", minimum: 2, maximum: 10000, immutable: "compiled_binding" },
            max: { required: true, type: "strict_integer", minimum: 2, maximum: 10000, immutable: "compiled_binding" },
            step: { required: true, type: "strict_integer", exclusive_minimum: 0, immutable: "compiled_binding" }
          } },
          quantity: { required: false, type: "finite_decimal_or_decimal_string", default: "0.01", exclusive_minimum: "0", immutable: "compiled_binding" }
        } },
        population_size: { required: false, type: "strict_integer", default: 32, minimum: 2, maximum: 256, constraint: "must not exceed parameter Cartesian cardinality", immutable: "compiled_binding" },
        max_generations: { required: false, type: "strict_integer", default: 10, minimum: 1, maximum: 100, immutable: "compiled_binding" },
        seed: { required: false, type: "strict_integer", default: 0, minimum: 0, maximum: 2147483647, immutable: "compiled_binding" }
      },
      compiled_fields: {
        kind: "parameter_search", strategy_type: "golden_cross", strategy_id: "golden_cross",
        objective: "maximize_score", market_data: "sealed_dataset", write_reports: false,
        capital_allocation: null, evaluation_set: null, fitness: null
      },
      evolution_defaults: { tournament_size: 2, elite_count: 1, crossover_probability: "0.9", mutation_probability: "0.1", mutation_sigma_steps: "1" }
    } as unknown as GaProfile;
    const malformed = (mutate: (candidate: GaProfile) => void) => {
      const candidate = structuredClone(base) as GaProfile;
      mutate(candidate);
      return supportsGaProfile(candidate);
    };
    const set = (target: object, key: string, value: unknown) => Object.defineProperty(target, key, { value, configurable: true, writable: true });
    const property = (target: unknown, key: string) => (target as Record<string, unknown>)[key];
    expect(malformed((candidate) => { set(candidate.accepted_fields.initial_balance!, "type", "number"); })).toBe(false);
    expect(malformed((candidate) => { set(candidate.accepted_fields.dataset_id!, "source", "browser"); })).toBe(false);
    expect(malformed((candidate) => { set(candidate.accepted_fields.cost_profile_id!, "immutable", "caller"); })).toBe(false);
    expect(malformed((candidate) => { set(property(property(candidate.accepted_fields.parameters, "fields"), "quantity") as object, "default", 1); })).toBe(false);
    expect(malformed((candidate) => { set(property(property(candidate.accepted_fields.instrument, "fields"), "fee_model") as object, "choices", ["percentage_notional"]); })).toBe(false);
    expect(malformed((candidate) => { set(candidate.accepted_fields.parameters!, "required", true); })).toBe(false);
  });

  it("restores only a structurally valid captured intent for the same actor", () => {
    const intent: GaIntent = {
      operation: "pause", jobId: "ga-job-1", expectedVersion: 3,
      idempotencyKey: "original-key", actor: "operator@example.test",
      body: { expected_version: 3 }, awaiting: true
    };
    expect(parseGaIntent(JSON.stringify(intent), "operator@example.test")).toEqual(intent);
    expect(parseGaIntent(JSON.stringify(intent), "other@example.test")).toEqual(null);
    expect(parseGaIntent('{"operation":"submit"}', "operator@example.test")).toEqual(null);
    expect(parseGaIntent(JSON.stringify({ ...intent, body: { expected_version: 2 } }), "operator@example.test")).toBeNull();
    expect(parseGaIntent(JSON.stringify({ ...intent, body: { expected_version: 3, unexpected: true } }), "operator@example.test")).toBeNull();
  });

  it("restores independent submit and source-job intents while discarding duplicate targets", () => {
    const submitBody = {
      parameter_search_profile_id: "golden_cross_research_v1", profile_revision: "a".repeat(64),
      strategy_subject: "builtin:golden_cross", strategy_version: "b".repeat(64),
      fitness_profile_id: "mark_to_market_pnl_v1", cost_profile_id: "explicit_accounting_v1",
      dataset_id: "sealed-1", start_time: 1, end_time: 2, initial_balance: "100.000000000000000001",
      fees: { maker: "0.0001", taker: "0.0002" },
      instrument: {
        multiplier: "1", quantity_step: null, price_tick: null,
        fee_model: "percentage_notional", capital_model: "notional", capital_per_contract: null
      },
      parameters: {
        short_window: { min: 5, max: 50, step: 5 },
        long_window: { min: 60, max: 200, step: 10 }, quantity: "0.01"
      },
      population_size: 32, max_generations: 10, seed: 0
    };
    const source = (operation: GaIntent["operation"], jobId: string | null, idempotencyKey: string): GaIntent => ({
      operation, jobId, ...(operation === "submit" ? {} : { expectedVersion: 3 }),
      idempotencyKey, actor: "operator@example.test",
      body: operation === "submit" ? submitBody : { expected_version: 3 }, awaiting: true
    });
    expect(parseGaIntent(JSON.stringify(source("submit", null, "submit-key")))?.body).toEqual(submitBody);
    expect(parseGaIntent(JSON.stringify({ ...source("submit", null, "submit-key"), body: { ...submitBody, caller_field: true } }))).toBeNull();
    expect(parseGaIntents(JSON.stringify([
      source("pause", "ga-a", "a-first"),
      source("cancel", "ga-a", "a-duplicate"),
      source("cancel", "ga-b", "b"),
      source("submit", null, "submit")
    ]))).toEqual([
      source("pause", "ga-a", "a-first"),
      source("cancel", "ga-b", "b"),
      source("submit", null, "submit")
    ]);
  });
});
