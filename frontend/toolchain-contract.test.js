import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import path from "node:path";

import { describe, expect, it } from "vitest";

import playwrightConfig from "./playwright.config.ts";
import {
  BROWSER_LOCALE,
  BROWSER_NOW,
  BROWSER_SESSION,
  BROWSER_TIME_ZONE,
  CASE_IDS,
  EPOCH_A,
  EPOCH_B,
  EXPECTED_DOCUMENT_COUNTS,
  EXPECTED_REQUEST_COUNTS,
  GENE_A,
  GENE_B,
  GENERATION_A,
  GENERATION_B,
  SCENARIO_IDS,
  STOPPED_STRATEGY_PAGE,
  STRATEGY_PAGE
} from "./e2e/fixtures.ts";

const INVALID = "invalid_frontend_toolchain_contract";
const ROOT = process.cwd();
const EXPECTED_SCENARIOS = [
  "direct-navigation",
  "research-cache",
  "navigation-serialization",
  "demo-dev",
  "demo-production-denied",
  "strategy-command",
  "locale-theme-reload",
  "lazy-chunk-inventory",
  "responsive-overflow"
];
const EXPECTED_CASES = {
  "direct-navigation": ["results", "strategies", "trades"],
  "research-cache": ["main"],
  "navigation-serialization": ["main"],
  "demo-dev": ["main"],
  "demo-production-denied": ["results", "trades"],
  "strategy-command": ["main"],
  "locale-theme-reload": ["main"],
  "lazy-chunk-inventory": ["main"],
  "responsive-overflow": ["research", "results", "strategies", "trades"]
};
const EXPECTED_NON_ZERO_COUNTS = {
  "direct-navigation:dev:strategies": { S: 2, T: 2 },
  "direct-navigation:production:strategies": { S: 1, T: 1 },
  "research-cache:dev:main": { S: 4, E: 2, A: 1, a: 1, T: 2 },
  "research-cache:production:main": { S: 2, E: 1, A: 1, a: 1, T: 1 },
  "navigation-serialization:dev:main": { S: 2, T: 2 },
  "strategy-command:dev:main": { S: 2, T: 3, C: 1 },
  "strategy-command:production:main": { S: 1, T: 2, C: 1 },
  "locale-theme-reload:dev:main": { S: 4, E: 4, A: 2, a: 2 },
  "locale-theme-reload:production:main": { S: 2, E: 2, A: 2, a: 2 },
  "lazy-chunk-inventory:dev:main": { S: 4, T: 4 },
  "lazy-chunk-inventory:production:main": { S: 3, E: 1, A: 1, a: 1, T: 2 },
  "responsive-overflow:dev:research": { S: 2, E: 2, A: 1, a: 1 },
  "responsive-overflow:production:research": { S: 1, E: 1, A: 1, a: 1 },
  "responsive-overflow:dev:strategies": { S: 2, T: 2 },
  "responsive-overflow:production:strategies": { S: 1, T: 1 }
};
const EXPECTED_SCRIPTS = {
  dev: "vite",
  preview: "vite preview",
  build: "tsc --noEmit && vite build",
  test: "vitest run",
  lint: "tsc --noEmit",
  "test:browser": "playwright test"
};
const EXPECTED_DEPENDENCIES = {
  echarts: "6.1.0",
  i18next: "26.3.6",
  react: "19.2.8",
  "react-dom": "19.2.8",
  "react-i18next": "17.0.11"
};
const EXPECTED_DEV_DEPENDENCIES = {
  "@playwright/test": "1.62.1",
  "@testing-library/react": "16.3.2",
  "@types/node": "22.12.0",
  "@types/react": "19.2.14",
  "@types/react-dom": "19.2.3",
  "@vitejs/plugin-react": "6.0.4",
  jsdom: "29.1.1",
  typescript: "7.0.2",
  vite: "8.1.5",
  vitest: "4.1.10"
};
const EXPECTED_FIXTURES = {
  browserSession: {
    actor: "frontend-smoke@example.invalid",
    capabilities: [],
    csrf_token: "csrf-browser-smoke",
    expires_at: "2026-08-24T12:00:00Z",
    step_up_expires_at: null
  },
  epochA: {
    id: "epoch-a",
    strategy_id: "strategy-epoch-a",
    started_at: "2026-01-15T12:34:00Z",
    finished_at: "2026-01-15T13:34:00Z",
    pop_size: 2,
    max_generations: 1,
    generations_run: 1,
    best_score: "9007199254740993.00",
    seed: 1,
    config_json: { objective: "maximize_score" },
    status: "completed",
    eval_pair: "RITHMIC:MNQ_ROLL-PERP",
    eval_start_date: "2026-01-01",
    eval_end_date: "2026-01-15",
    eval_timeframe: "5m"
  },
  epochB: {
    id: "epoch-b",
    strategy_id: "strategy-epoch-b",
    started_at: "2026-07-15T12:34:00Z",
    finished_at: "2026-07-15T13:34:00Z",
    pop_size: 2,
    max_generations: 1,
    generations_run: 1,
    best_score: "2.00",
    seed: 2,
    config_json: { objective: "maximize_score" },
    status: "completed",
    eval_pair: "RITHMIC:MNQ_ROLL-PERP",
    eval_start_date: "2026-01-01",
    eval_end_date: "2026-01-15",
    eval_timeframe: "5m"
  },
  generationA: {
    generation_index: 0,
    candidate_count: 1,
    score_min: "9007199254740993.00",
    score_max: "9007199254740993.00",
    drawdown_min: "0.1000",
    drawdown_max: "0.1000"
  },
  generationB: {
    generation_index: 0,
    candidate_count: 1,
    score_min: "2.00",
    score_max: "2.00",
    drawdown_min: "0.1000",
    drawdown_max: "0.1000"
  },
  geneA: {
    id: 1,
    strategy_id: "strategy-epoch-a",
    role: "challenger",
    param_pack: { fast: 5, slow: 20 },
    score_total: "9007199254740993.00",
    score_breakdown: {},
    max_drawdown: "0.1000",
    generation_index: 0,
    candidate_id: "candidate-a",
    epoch_id: "epoch-a",
    created_at: "2026-01-15T13:34:00Z"
  },
  geneB: {
    id: 2,
    strategy_id: "strategy-epoch-b",
    role: "challenger",
    param_pack: { fast: 6, slow: 20 },
    score_total: "2.00",
    score_breakdown: {},
    max_drawdown: "0.1000",
    generation_index: 0,
    candidate_id: "candidate-b",
    epoch_id: "epoch-b",
    created_at: "2026-07-15T13:34:00Z"
  },
  strategyPage: {
    total: 1,
    limit: 500,
    offset: 0,
    states: [
      {
        strategy_id: "active-strategy",
        status: "ACTIVE",
        config: {},
        performance: {},
        last_heartbeat: 1768480440000,
        uptime_start: 1768476840000,
        last_error_message: null,
        entered_error_at: null,
        recovered_at: null,
        stopped_at: null,
        version: 3,
        available_commands: ["STOP"]
      }
    ]
  },
  stoppedStrategyPage: {
    total: 1,
    limit: 500,
    offset: 0,
    states: [
      {
        strategy_id: "active-strategy",
        status: "STOPPED",
        config: {},
        performance: {},
        last_heartbeat: 1768480440000,
        uptime_start: 1768476840000,
        last_error_message: null,
        entered_error_at: null,
        recovered_at: null,
        stopped_at: null,
        version: 4,
        available_commands: ["RESUME"]
      }
    ]
  }
};

function same(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function exactTripleKeys() {
  const rows = [];
  for (const scenario of EXPECTED_SCENARIOS) {
    const servers =
      scenario === "navigation-serialization" || scenario === "demo-dev"
        ? ["dev"]
        : scenario === "demo-production-denied"
          ? ["production"]
          : ["dev", "production"];
    for (const server of servers) {
      for (const caseId of EXPECTED_CASES[scenario]) {
        rows.push(`${scenario}:${server}:${caseId}`);
      }
    }
  }
  return rows.sort();
}

export function validateToolchain(snapshot) {
  try {
    const pkg = snapshot.packageJson;
    const lock = snapshot.packageLock;
    const rootLock = lock.packages[""];
    if (
      snapshot.nodeVersion !== "22.23.2" ||
      snapshot.runtimeNode !== "22.23.2" ||
      snapshot.runtimeNpm !== "10.9.8" ||
      !same(pkg.scripts, EXPECTED_SCRIPTS) ||
      !same(pkg.dependencies, EXPECTED_DEPENDENCIES) ||
      !same(pkg.devDependencies, EXPECTED_DEV_DEPENDENCIES) ||
      !same(rootLock.dependencies, EXPECTED_DEPENDENCIES) ||
      !same(rootLock.devDependencies, EXPECTED_DEV_DEPENDENCIES) ||
      pkg.dependencies?.nanoid !== undefined ||
      pkg.devDependencies?.nanoid !== undefined ||
      pkg.overrides?.nanoid !== undefined
    ) {
      return INVALID;
    }
    const lockContracts = {
      "node_modules/@playwright/test": [
        "1.62.1",
        "sha512-DTcUc8qii+cpHvtOwggMtBRMjKZHXYWdw8syRYu2vtzuq4Wxphqq4NfCs5Zt44L6mA8rfDfj+PHnxFc/FeK6mQ=="
      ],
      "node_modules/playwright": [
        "1.62.1",
        "sha512-0M+L3LAD8/nm554LOla9Ayx0j0tmFZ0FBcoQ7F1VuVHpM/XpiC8RcDzBQB8W5+hA8L22THxELzeF+2WcUzvcLg=="
      ],
      "node_modules/playwright-core": [
        "1.62.1",
        "sha512-wPYSwEBJY9GHraISXqyqtx0na0LpO3XEX7jNDhntbex7tzUS7kLnZsOlFruFJB4Hi/rhDMjXGqHewDZ68nYZVw=="
      ],
      "node_modules/playwright/node_modules/fsevents": [
        "2.3.2",
        "sha512-xiqMQR4xAeHTuB9uWm+fFRcIOgKBMiOBP+eXiyT7jsgVCq1bkVygt00oASowB7EdtpOHaaPgKt812P9ab+DDKA=="
      ],
      "node_modules/@types/node": [
        "22.12.0",
        "sha512-Fll2FZ1riMjNmlmJOdAyY5pUbkftXslB5DgEzlIuNaiWhXd00FhWxVC/r4yV/4wBb9JfImTu+jiSvXTkJ7F/gA=="
      ],
      "node_modules/undici-types": [
        "6.20.0",
        "sha512-Ny6QZ2Nju20vw1SRHe3d9jVu6gJ+4e3+MMpqu7pqE5HT6WsTSlce++GQmK5UXS8mzV8DSYHrQH+Xrf2jVcuKNg=="
      ],
      "node_modules/nanoid": [
        "3.3.18",
        "sha512-DTg4MJbGMWkfi6VZFdNt2/caMbQy4Ou+Op/hJQvGEWcnVfoA1QA+xzRKAzw9jD6+GVOOeYr/mIcuDSdug6F6+w=="
      ]
    };
    for (const [name, [version, integrity]] of Object.entries(lockContracts)) {
      if (
        lock.packages[name]?.version !== version ||
        lock.packages[name]?.integrity !== integrity
      ) {
        return INVALID;
      }
    }
    if (
      lock.packages["node_modules/@playwright/test"].dependencies?.playwright !==
        "1.62.1" ||
      lock.packages["node_modules/playwright"].dependencies?.[
        "playwright-core"
      ] !== "1.62.1" ||
      lock.packages["node_modules/playwright"].optionalDependencies?.fsevents !==
        "2.3.2" ||
      lock.packages["node_modules/@types/node"].dependencies?.[
        "undici-types"
      ] !== "~6.20.0" ||
      snapshot.tsconfigBytes !==
        '{\n  "extends": "./tsconfig.json",\n  "compilerOptions": {\n    "types": ["node"],\n    "skipLibCheck": false\n  },\n  "include": ["playwright.config.ts", "e2e/**/*.ts"]\n}\n'
    ) {
      return INVALID;
    }
    const config = snapshot.playwrightConfig;
    const expectedProjects = [
      ["desktop-1440x900", { width: 1440, height: 900 }],
      ["tablet-1024x768", { width: 1024, height: 768 }],
      ["mobile-390x844", { width: 390, height: 844 }]
    ];
    if (
      config.testDir !== "./e2e" ||
      config.testMatch !== "**/*.e2e.ts" ||
      config.outputDir !== "test-results" ||
      config.fullyParallel !== false ||
      config.workers !== 1 ||
      config.forbidOnly !== true ||
      config.retries !== 0 ||
      config.timeout !== 60_000 ||
      !same(config.reporter, [
        ["list"],
        ["json", { outputFile: "playwright-report/results.json" }]
      ]) ||
      !same(
        config.projects.map((project) => [project.name, project.use.viewport]),
        expectedProjects
      ) ||
      config.projects.some((project) => project.use.channel !== undefined) ||
      !same(config.webServer, [
        {
          command:
            "npm run dev -- --host 127.0.0.1 --port 4173 --strictPort",
          url: "http://127.0.0.1:4173",
          timeout: 60_000,
          reuseExistingServer: false
        },
        {
          command:
            "npm run preview -- --host 127.0.0.1 --port 4174 --strictPort",
          url: "http://127.0.0.1:4174",
          timeout: 60_000,
          reuseExistingServer: false
        }
      ])
    ) {
      return INVALID;
    }
    const fixtures = snapshot.fixtures;
    if (
      fixtures.now !== "2026-08-23T12:00:00Z" ||
      fixtures.locale !== "de-DE" ||
      fixtures.timeZone !== "UTC" ||
      !same(fixtures.scenarios, EXPECTED_SCENARIOS) ||
      !same(fixtures.cases, EXPECTED_CASES) ||
      !same(fixtures.responses, EXPECTED_FIXTURES)
    ) {
      return INVALID;
    }
    const tripleKeys = exactTripleKeys();
    if (
      !same(Object.keys(fixtures.requestCounts).sort(), tripleKeys) ||
      !same(Object.keys(fixtures.documentCounts).sort(), tripleKeys)
    ) {
      return INVALID;
    }
    for (const key of tripleKeys) {
      const counts = fixtures.requestCounts[key];
      const expectedCounts = {
        S: 0,
        P: 0,
        E: 0,
        A: 0,
        B: 0,
        a: 0,
        b: 0,
        T: 0,
        C: 0,
        ...(EXPECTED_NON_ZERO_COUNTS[key] ?? {})
      };
      if (
        !counts ||
        !same(Object.keys(counts), ["S", "P", "E", "A", "B", "a", "b", "T", "C"]) ||
        !same(counts, expectedCounts) ||
        Object.values(counts).some(
          (count) => !Number.isInteger(count) || count < 0
        ) ||
        fixtures.documentCounts[key] !==
          (key.startsWith("locale-theme-reload:") ? 2 : 1)
      ) {
        return INVALID;
      }
    }
    return null;
  } catch {
    return INVALID;
  }
}

function snapshot() {
  return {
    packageJson: JSON.parse(readFileSync(path.join(ROOT, "package.json"), "utf8")),
    packageLock: JSON.parse(
      readFileSync(path.join(ROOT, "package-lock.json"), "utf8")
    ),
    nodeVersion: readFileSync(path.join(ROOT, ".node-version"), "utf8").trim(),
    runtimeNode: process.versions.node,
    runtimeNpm: execFileSync("npm", ["--version"], { encoding: "utf8" }).trim(),
    tsconfigBytes: readFileSync(
      path.join(ROOT, "tsconfig.playwright.json"),
      "utf8"
    ),
    playwrightConfig,
    fixtures: {
      now: BROWSER_NOW,
      locale: BROWSER_LOCALE,
      timeZone: BROWSER_TIME_ZONE,
      scenarios: SCENARIO_IDS,
      cases: CASE_IDS,
      responses: {
        browserSession: BROWSER_SESSION,
        epochA: EPOCH_A,
        epochB: EPOCH_B,
        generationA: GENERATION_A,
        generationB: GENERATION_B,
        geneA: GENE_A,
        geneB: GENE_B,
        strategyPage: STRATEGY_PAGE,
        stoppedStrategyPage: STOPPED_STRATEGY_PAGE
      },
      requestCounts: EXPECTED_REQUEST_COUNTS,
      documentCounts: EXPECTED_DOCUMENT_COUNTS
    }
  };
}

function leafPaths(value, prefix = []) {
  if (value === null || typeof value !== "object") {
    return [[prefix, value]];
  }
  return Object.entries(value).flatMap(([key, child]) =>
    leafPaths(child, [...prefix, key])
  );
}

function valueAt(root, pathParts) {
  return pathParts.reduce((value, part) => value[part], root);
}

function setAt(root, pathParts, value) {
  const parent = pathParts
    .slice(0, -1)
    .reduce((current, part) => current[part], root);
  parent[pathParts.at(-1)] = value;
}

function changedPrimitive(value) {
  if (value === null) return "not-null";
  if (typeof value === "string") return `${value}-changed`;
  if (typeof value === "number") return value + 1;
  if (typeof value === "boolean") return !value;
  throw new Error(`unsupported leaf ${typeof value}`);
}

describe("frontend toolchain contract", () => {
  it("accepts only the tracked deterministic toolchain", () => {
    expect(validateToolchain(snapshot())).toBeNull();
  });

  it("fails closed for independent contract mutations", () => {
    const structuralMutations = [
      (value) => { value.nodeVersion = "22.23.1"; },
      (value) => { value.runtimeNode = "25.5.0"; },
      (value) => { value.runtimeNpm = "11.8.0"; },
      (value) => { value.tsconfigBytes = value.tsconfigBytes.trim(); },
      (value) => { value.fixtures.scenarios.pop(); },
      (value) => { value.fixtures.cases["responsive-overflow"] = ["main"]; },
      (value) => { value.fixtures.cases["direct-navigation"].push("results"); },
      (value) => { delete value.fixtures.requestCounts["direct-navigation:dev:results"]; },
      (value) => { value.fixtures.requestCounts.extra = { S: 0, P: 0, E: 0, A: 0, B: 0, a: 0, b: 0, T: 0, C: 0 }; },
      (value) => { delete value.fixtures.requestCounts["direct-navigation:dev:results"].S; },
      (value) => { value.fixtures.requestCounts["direct-navigation:dev:results"].extra = 0; },
      (value) => { delete value.fixtures.documentCounts["direct-navigation:dev:results"]; },
      (value) => { value.fixtures.documentCounts.extra = 1; },
      (value) => { value.packageJson.dependencies.nanoid = "3.3.18"; },
      (value) => { value.packageJson.overrides = { nanoid: "3.3.18" }; },
      (value) => { value.playwrightConfig.projects[0].use.channel = "chrome"; }
    ];
    const selectedLockPaths = [
      "node_modules/@playwright/test",
      "node_modules/playwright",
      "node_modules/playwright-core",
      "node_modules/playwright/node_modules/fsevents",
      "node_modules/@types/node",
      "node_modules/undici-types",
      "node_modules/nanoid"
    ].flatMap((name) => [
      ["packageLock", "packages", name, "version"],
      ["packageLock", "packages", name, "integrity"]
    ]);
    const dependencyEdgePaths = [
      ["packageLock", "packages", "node_modules/@playwright/test", "dependencies", "playwright"],
      ["packageLock", "packages", "node_modules/playwright", "dependencies", "playwright-core"],
      ["packageLock", "packages", "node_modules/playwright", "optionalDependencies", "fsevents"],
      ["packageLock", "packages", "node_modules/@types/node", "dependencies", "undici-types"]
    ];
    const contractRoots = [
      ["packageJson", "scripts"],
      ["packageJson", "dependencies"],
      ["packageJson", "devDependencies"],
      ["packageLock", "packages", "", "dependencies"],
      ["packageLock", "packages", "", "devDependencies"],
      ["playwrightConfig"],
      ["fixtures"]
    ];
    const source = snapshot();
    const leafMutationPaths = [
      ...selectedLockPaths,
      ...dependencyEdgePaths,
      ...contractRoots.flatMap((rootPath) =>
        leafPaths(valueAt(source, rootPath), rootPath).map(([pathParts]) => pathParts)
      )
    ];
    for (const [index, mutate] of structuralMutations.entries()) {
      const candidate = structuredClone(snapshot());
      mutate(candidate);
      expect(validateToolchain(candidate), `structural mutation ${index}`).toBe(INVALID);
    }
    for (const [index, pathParts] of leafMutationPaths.entries()) {
      const candidate = structuredClone(source);
      setAt(candidate, pathParts, changedPrimitive(valueAt(candidate, pathParts)));
      expect(validateToolchain(candidate), `leaf mutation ${index}:${pathParts.join(".")}`).toBe(INVALID);
    }
  });
});
