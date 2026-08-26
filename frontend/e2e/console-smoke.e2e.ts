import {
  expect,
  test,
  type Browser,
  type BrowserContext,
  type Page,
  type Request,
  type Route,
  type TestInfo,
  type ViewportSize
} from "@playwright/test";

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
  STRATEGY_PAGE,
  type RouteCounts,
  type ScenarioId,
  type ServerId
} from "./fixtures";

type CaseId = (typeof CASE_IDS)[ScenarioId][number];
type Triple = Readonly<{ scenario: ScenarioId; server: ServerId; caseId: CaseId }>;
type RouteId = keyof RouteCounts;
type MutableRouteCounts = { -readonly [Key in RouteId]: number };
type CommandGate = Readonly<{
  received: Promise<void>;
  released: Promise<void>;
  signalReceived: () => void;
  release: () => void;
}>;

const ZERO_COUNTS: MutableRouteCounts = {
  S: 0,
  P: 0,
  E: 0,
  A: 0,
  B: 0,
  a: 0,
  b: 0,
  T: 0,
  C: 0
};
const PROJECT_VIEWPORTS = {
  "desktop-1440x900": { width: 1440, height: 900 },
  "tablet-1024x768": { width: 1024, height: 768 },
  "mobile-390x844": { width: 390, height: 844 }
} as const;
const BASE_URLS = {
  dev: "http://127.0.0.1:4173",
  production: "http://127.0.0.1:4174"
} as const;
const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const PROJECTED_HEADERS = new Set([
  "accept",
  "content-type",
  "idempotency-key",
  "x-csrf-token",
  "authorization"
]);

function triplesFor(scenario: ScenarioId): readonly Triple[] {
  const servers: readonly ServerId[] =
    scenario === "navigation-serialization" || scenario === "demo-dev"
      ? ["dev"]
      : scenario === "demo-production-denied"
        ? ["production"]
        : ["dev", "production"];
  return servers.flatMap((server) =>
    CASE_IDS[scenario].map((caseId) => ({ scenario, server, caseId }))
  );
}

function tripleKey(triple: Triple): keyof typeof EXPECTED_REQUEST_COUNTS {
  return `${triple.scenario}:${triple.server}:${triple.caseId}` as keyof typeof EXPECTED_REQUEST_COUNTS;
}

function validatedViewport(testInfo: TestInfo): ViewportSize {
  const expected =
    PROJECT_VIEWPORTS[
      testInfo.project.name as keyof typeof PROJECT_VIEWPORTS
    ];
  const configured = testInfo.project.use.viewport;
  expect(expected, `unknown project ${testInfo.project.name}`).toBeDefined();
  expect(configured).toEqual(expected);
  return { width: expected.width, height: expected.height };
}

function projectHeaders(request: Request): Record<string, string> {
  return Object.fromEntries(
    Object.entries(request.headers())
      .filter(
        ([name]) => PROJECTED_HEADERS.has(name) || name.startsWith("x-")
      )
      .sort(([left], [right]) => left.localeCompare(right))
  );
}

function expectedGetHeaders(): Record<string, string> {
  return { accept: "application/json" };
}

function createCommandGate(): CommandGate {
  let signalReceived = () => {};
  let release = () => {};
  const received = new Promise<void>((resolve) => {
    signalReceived = resolve;
  });
  const released = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { received, released, signalReceived, release };
}

async function installApiFixtures(
  context: BrowserContext,
  counts: MutableRouteCounts,
  commandGate: CommandGate | null,
  expectedOrigin: string
): Promise<void> {
  let commandAccepted = false;
  await context.route("**/*", async (route: Route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    let routeId: RouteId | null = null;
    let body: unknown;

    if (url.pathname === "/api/v1/auth/session" && url.search === "") {
      if (method === "GET") {
        routeId = "S";
        body = BROWSER_SESSION;
      } else if (method === "POST") {
        routeId = "P";
        body = BROWSER_SESSION;
      }
    } else if (
      method === "GET" &&
      url.pathname === "/evolution-epochs" &&
      url.search === "?limit=100&offset=0"
    ) {
      routeId = "E";
      body = { total: 2, limit: 100, offset: 0, epochs: [EPOCH_A, EPOCH_B] };
    } else if (
      method === "GET" &&
      url.pathname === "/evolution-epochs/epoch-a/generations" &&
      url.search === ""
    ) {
      routeId = "A";
      body = { generations: [GENERATION_A] };
    } else if (
      method === "GET" &&
      url.pathname === "/evolution-epochs/epoch-b/generations" &&
      url.search === ""
    ) {
      routeId = "B";
      body = { generations: [GENERATION_B] };
    } else if (
      method === "GET" &&
      url.pathname === "/genes" &&
      url.search ===
        "?epoch_id=epoch-a&generation_index=0&limit=10000&offset=0"
    ) {
      routeId = "a";
      body = { total: 1, limit: 10000, offset: 0, genes: [GENE_A] };
    } else if (
      method === "GET" &&
      url.pathname === "/genes" &&
      url.search ===
        "?epoch_id=epoch-b&generation_index=0&limit=10000&offset=0"
    ) {
      routeId = "b";
      body = { total: 1, limit: 10000, offset: 0, genes: [GENE_B] };
    } else if (
      method === "GET" &&
      url.pathname === "/strategy-states" &&
      url.search === "?limit=500&offset=0"
    ) {
      routeId = "T";
      body = commandAccepted ? STOPPED_STRATEGY_PAGE : STRATEGY_PAGE;
    } else if (
      method === "POST" &&
      url.pathname === "/strategies/active-strategy/commands" &&
      url.search === ""
    ) {
      routeId = "C";
      body = { status: "accepted" };
    }

    const apiLike =
      url.pathname.startsWith("/api/") ||
      url.pathname.startsWith("/evolution-") ||
      url.pathname.startsWith("/genes") ||
      url.pathname.startsWith("/strateg");
    if (apiLike && url.origin !== expectedOrigin) {
      throw new Error(`unexpected_api_origin:${url.origin}`);
    }
    if (routeId === null) {
      if (apiLike) {
        throw new Error(`unexpected_api_request:${method}:${url.pathname}${url.search}`);
      }
      await route.continue();
      return;
    }

    if (routeId === "C") {
      const headers = projectHeaders(request);
      const idempotencyKey = headers["idempotency-key"];
      expect(idempotencyKey).toMatch(UUID_V4);
      expect(headers).toEqual({
        accept: "application/json",
        "content-type": "application/json",
        "idempotency-key": idempotencyKey,
        "x-csrf-token": "csrf-browser-smoke"
      });
      expect(request.postData()).toBe(
        '{"command":"STOP","expected_version":3}'
      );
    } else {
      expect(projectHeaders(request)).toEqual(expectedGetHeaders());
      expect(request.postData()).toBeNull();
    }
    counts[routeId] += 1;
    if (routeId === "C" && commandGate !== null) {
      commandGate.signalReceived();
      await commandGate.released;
      commandAccepted = true;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(body)
    });
  });
}

function nav(page: Page, index: 0 | 1 | 2 | 3) {
  return page.locator(".console-nav button").nth(index);
}

async function waitForResearch(page: Page): Promise<void> {
  await expect(page.getByText("candidate-a", { exact: true }).first()).toBeVisible();
}

async function waitForStrategy(page: Page): Promise<void> {
  await expect(page.getByText("active-strategy", { exact: true })).toBeVisible();
}

function moduleCount(requests: readonly string[], basename: string): number {
  return requests.filter((url) => url.includes(basename)).length;
}

async function assertSingleModuleActivation(
  page: Page,
  moduleRequests: string[],
  basename: string,
  activate: () => Promise<void>
): Promise<void> {
  const before = moduleCount(moduleRequests, basename);
  expect(before).toBe(0);
  await activate();
  await expect.poll(() => moduleCount(moduleRequests, basename)).toBe(1);
  await nav(page, 0).click();
  await nav(
    page,
    basename === "BacktestResultsView"
      ? 1
      : basename === "StrategyManager"
        ? 2
        : 3
  ).click();
  await expect.poll(() => moduleCount(moduleRequests, basename)).toBe(1);
}

async function exerciseScenario(
  page: Page,
  triple: Triple,
  moduleRequests: string[],
  commandGate: CommandGate | null
): Promise<void> {
  const base = BASE_URLS[triple.server];
  const open = (path: string) => page.goto(`${base}${path}`);

  if (triple.scenario === "direct-navigation") {
    const paths = {
      results: "/?view=results&keep=1#anchor",
      strategies: "/?view=strategies&keep=1#anchor",
      trades: "/?view=trades&trade=trade-000184&keep=1#anchor"
    } as const;
    await open(paths[triple.caseId as keyof typeof paths]);
    if (triple.caseId === "strategies") {
      await waitForStrategy(page);
    } else {
      await expect(page.locator(".empty-panel")).toBeVisible();
    }
    const expectedTitle = {
      results: "回測績效",
      strategies: "策略管理",
      trades: "K 線與進出場"
    } as const;
    await expect(page.locator("h1")).toHaveText(
      expectedTitle[triple.caseId as keyof typeof expectedTitle]
    );
    expect(new URL(page.url()).searchParams.get("keep")).toBe("1");
    expect(new URL(page.url()).hash).toBe("#anchor");
    expect(new URL(page.url()).searchParams.get("trade")).toBe(
      triple.caseId === "trades" ? "trade-000184" : null
    );
    if (triple.caseId === "trades") {
      await expect(page.getByText("trade-000184")).toHaveCount(0);
    }
    return;
  }

  if (triple.scenario === "research-cache") {
    await open("/");
    await waitForResearch(page);
    await expect(page.locator(".ranking-list button.is-selected")).toContainText(
      "candidate-a"
    );
    await nav(page, 2).click();
    await waitForStrategy(page);
    await nav(page, 0).click();
    await waitForResearch(page);
    await expect(page.locator(".ranking-list button.is-selected")).toContainText(
      "candidate-a"
    );
    return;
  }

  if (triple.scenario === "navigation-serialization") {
    await page.addInitScript(() => {
      const original = window.history.replaceState.bind(window.history);
      Object.defineProperty(window, "__replaceStateCount", {
        configurable: false,
        value: 0,
        writable: true
      });
      window.history.replaceState = (...args) => {
        (window as typeof window & { __replaceStateCount: number }).__replaceStateCount += 1;
        return original(...args);
      };
    });
    await open("/?view=results&demo=1&keep=1#anchor");
    await page
      .getByRole("button", { name: /查看 K 線|Inspect candles/ })
      .first()
      .click();
    await expect.poll(() => new URL(page.url()).searchParams.get("view")).toBe("trades");
    await nav(page, 1).click();
    await nav(page, 2).click();
    await waitForStrategy(page);
    await nav(page, 0).click();
    expect(
      await page.evaluate(
        () => (window as typeof window & { __replaceStateCount: number }).__replaceStateCount
      )
    ).toBe(4);
    const url = new URL(page.url());
    expect(url.searchParams.get("keep")).toBe("1");
    expect(url.searchParams.get("trade")).toBeNull();
    expect(url.hash).toBe("#anchor");
    return;
  }

  if (triple.scenario === "demo-dev") {
    await open("/?view=results&demo=1");
    await expect(page.getByText("job-research-0042", { exact: true })).toBeVisible();
    await page
      .getByRole("button", { name: /查看 K 線|Inspect candles/ })
      .first()
      .click();
    await expect(page.getByText("trade-000184", { exact: true }).first()).toBeVisible();
    expect(new URL(page.url()).searchParams.get("view")).toBe("trades");
    return;
  }

  if (triple.scenario === "demo-production-denied") {
    await open(`/?view=${triple.caseId}&demo=1`);
    await expect(page.locator(".empty-panel")).toBeVisible();
    await expect(page.getByText("job-research-0042")).toHaveCount(0);
    await expect(page.getByText("trade-000184")).toHaveCount(0);
    return;
  }

  if (triple.scenario === "strategy-command") {
    await open("/?view=strategies");
    await waitForStrategy(page);
    page.once("dialog", (dialog) => void dialog.accept());
    await page.locator(".strategy-action button").click();
    expect(commandGate).not.toBeNull();
    await commandGate!.received;
    await expect(page.locator(".strategy-action button")).toBeDisabled();
    commandGate!.release();
    await expect(page.locator(".strategy-status.status-stopped")).toBeVisible();
    await expect(page.locator(".strategy-action button")).toBeEnabled();
    return;
  }

  if (triple.scenario === "locale-theme-reload") {
    await open("/");
    await waitForResearch(page);
    await expect(page.locator("html")).toHaveAttribute("lang", "zh-Hant");
    await page.locator("#language").selectOption("en");
    await page.locator(".theme-control").click();
    expect(await page.evaluate(() => localStorage.getItem("fluxtrade.locale"))).toBe("en");
    expect(await page.evaluate(() => localStorage.getItem("fluxtrade.theme"))).toBe("dark");
    await page.reload();
    await waitForResearch(page);
    await expect(page.locator("html")).toHaveAttribute("lang", "en");
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    await expect(page.getByRole("button", { name: "Parameter research" })).toBeVisible();
    return;
  }

  if (triple.scenario === "lazy-chunk-inventory") {
    await open(triple.server === "dev" ? "/?demo=1" : "/");
    if (triple.server === "production") {
      await waitForResearch(page);
    }
    await assertSingleModuleActivation(
      page,
      moduleRequests,
      "BacktestResultsView",
      async () => {
        await nav(page, 1).click();
        await expect(page.locator(".results-console")).toBeVisible();
      }
    );
    await assertSingleModuleActivation(
      page,
      moduleRequests,
      "StrategyManager",
      async () => {
        await nav(page, 2).click();
        await waitForStrategy(page);
      }
    );
    await assertSingleModuleActivation(
      page,
      moduleRequests,
      "TradeChartView",
      async () => {
        await nav(page, 3).click();
        await expect(page.locator(".trade-console")).toBeVisible();
      }
    );
    if (triple.server === "dev") {
      await expect.poll(() => moduleCount(moduleRequests, "CandlestickChart")).toBe(1);
      await page.getByText("trade-000184", { exact: true }).first().click();
      await expect.poll(() => moduleCount(moduleRequests, "CandlestickChart")).toBe(1);
    } else {
      expect(moduleCount(moduleRequests, "CandlestickChart")).toBe(0);
    }
    await nav(page, 0).click();
    await page.locator(".view-control button").nth(1).click();
    await expect.poll(() => moduleCount(moduleRequests, "FitnessSurface3D")).toBe(1);
    await page.locator(".view-control button").nth(0).click();
    await page.locator(".view-control button").nth(1).click();
    await expect.poll(() => moduleCount(moduleRequests, "FitnessSurface3D")).toBe(1);
    return;
  }

  const responsivePaths = {
    research: "/",
    results: "/?view=results&demo=1",
    strategies: "/?view=strategies",
    trades: "/?view=trades&demo=1&trade=trade-000184"
  } as const;
  await open(responsivePaths[triple.caseId as keyof typeof responsivePaths]);
  if (triple.caseId === "research") {
    await waitForResearch(page);
  } else if (triple.caseId === "strategies") {
    await waitForStrategy(page);
  } else if (triple.server === "production") {
    await expect(page.locator(".empty-panel")).toBeVisible();
    await expect(page.getByText("job-research-0042")).toHaveCount(0);
    await expect(page.getByText("trade-000184")).toHaveCount(0);
  } else {
    if (triple.caseId === "results") {
      await expect(page.getByText("job-research-0042", { exact: true })).toBeVisible();
    } else {
      await expect(page.getByText("trade-000184", { exact: true }).first()).toBeVisible();
    }
  }
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= document.documentElement.clientWidth
    )
  ).toBe(true);
}

async function runTriple(
  browser: Browser,
  triple: Triple,
  testInfo: TestInfo
): Promise<void> {
  const viewport = validatedViewport(testInfo);
  const context = await browser.newContext({
    locale: BROWSER_LOCALE,
    timezoneId: BROWSER_TIME_ZONE,
    viewport
  });
  const observedCounts: MutableRouteCounts = { ...ZERO_COUNTS };
  const moduleRequests: string[] = [];
  const failures: string[] = [];
  const commandGate =
    triple.scenario === "strategy-command" ? createCommandGate() : null;
  let documents = 0;
  try {
    await installApiFixtures(
      context,
      observedCounts,
      commandGate,
      BASE_URLS[triple.server]
    );
    const page = await context.newPage();
    page.on("console", (message) => {
      if (message.type() === "error") failures.push(`console:${message.text()}`);
    });
    page.on("pageerror", (error) => failures.push(`pageerror:${error.message}`));
    page.on("request", (request) => {
      if (request.resourceType() === "document" && request.frame() === page.mainFrame()) {
        documents += 1;
      }
      if (request.resourceType() === "script") moduleRequests.push(request.url());
    });
    await page.clock.install({ time: BROWSER_NOW });
    expect(await page.evaluate(() => [window.innerWidth, window.innerHeight])).toEqual([
      viewport.width,
      viewport.height
    ]);
    expect(await page.evaluate(() => navigator.language)).toBe(BROWSER_LOCALE);
    expect(
      await page.evaluate(() => Intl.DateTimeFormat().resolvedOptions().timeZone)
    ).toBe(BROWSER_TIME_ZONE);

    await exerciseScenario(page, triple, moduleRequests, commandGate);
    const key = tripleKey(triple);
    expect(observedCounts).toEqual(EXPECTED_REQUEST_COUNTS[key]);
    expect(documents).toBe(EXPECTED_DOCUMENT_COUNTS[key]);
    expect(failures).toEqual([]);
    await page.screenshot({
      path: testInfo.outputPath(
        `${testInfo.project.name}-${triple.server}-${triple.scenario}-${triple.caseId}.png`
      ),
      fullPage: true
    });
  } finally {
    await context.close();
  }
}

for (const scenario of SCENARIO_IDS) {
  test(scenario, async ({ browser }, testInfo) => {
    for (const triple of triplesFor(scenario)) {
      await runTriple(browser, triple, testInfo);
    }
  });
}
