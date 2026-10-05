import { expect, test } from "@playwright/test";
import { existsSync, readFileSync, writeFileSync } from "node:fs";

type ContinueSignal = { epochId: string };

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`real research integration fixture is missing ${name}`);
  return value;
}

test("real browser preserves missed invalidation and completes GA candidate lifecycle", async ({
  page,
  context
}) => {
  const origin = process.env.RESEARCH_REAL_ORIGIN;
  const cookie = process.env.RESEARCH_REAL_COOKIE;
  const readyPath = process.env.RESEARCH_REAL_READY_FILE;
  const continuePath = process.env.RESEARCH_REAL_CONTINUE_FILE;
  const initialEpochId = process.env.RESEARCH_REAL_INITIAL_EPOCH;
  if (!origin || !cookie || !readyPath || !continuePath || !initialEpochId) {
    throw new Error("real research integration fixture is incomplete");
  }
  const [name, value] = cookie.split("=", 2);
  if (!name || !value) throw new Error("browser session cookie is malformed");
  await context.addCookies([{
    name,
    value,
    url: origin,
    secure: true,
    httpOnly: true,
    sameSite: "Strict"
  }]);

  let streamResponses = 0;
  let epochReads = 0;
  page.on("response", (response) => {
    if (response.url().endsWith("/api/v1/events")) streamResponses += 1;
  });
  page.on("request", (request) => {
    if (new URL(request.url()).pathname === "/evolution-epochs") epochReads += 1;
  });
  const stream = page.waitForResponse((response) =>
    response.url().endsWith("/api/v1/events") && response.status() === 200
  );
  await page.goto(origin);
  await page.getByRole("button", { name: "Parameter research" }).click();
  await expect(page.locator("#epoch")).toHaveValue(initialEpochId);
  await expect(page.locator(".ranking-list button.is-selected")).toHaveCount(1);
  await stream;

  const selectedCandidate = (
    await page.locator(".ranking-list button.is-selected strong").innerText()
  ).trim();
  const readsBeforeDisconnect = epochReads;
  writeFileSync(readyPath, JSON.stringify({ selectedCandidate }));

  await expect.poll(() => existsSync(continuePath), {
    timeout: 75_000,
    intervals: [50, 100, 250]
  }).toBe(true);
  const signal = JSON.parse(readFileSync(continuePath, "utf8")) as ContinueSignal;

  await expect.poll(() => streamResponses, { timeout: 15_000 }).toBeGreaterThan(1);
  await expect.poll(() => epochReads, { timeout: 15_000 }).toBeGreaterThan(
    readsBeforeDisconnect
  );
  await expect(
    page.locator('#epoch option[value="' + signal.epochId + '"]')
  ).toHaveCount(1);
  await expect(page.locator("#epoch")).toHaveValue(initialEpochId);
  await expect(page.locator(".ranking-list button.is-selected")).toHaveCount(1);
  await expect(
    page.locator(".ranking-list button.is-selected strong")
  ).toHaveText(selectedCandidate);

  // Exercise the real Research form and unchanged HTTPS -> HTTP backend path.
  // Financial inputs stay strings, including the high-precision balance.
  const datasetId = requiredEnv("RESEARCH_REAL_DATASET_ID");
  const start = requiredEnv("RESEARCH_REAL_START");
  const end = requiredEnv("RESEARCH_REAL_END");
  for (const [selector, value] of [
    ["#ga-dataset", datasetId], ["#ga-start", start], ["#ga-end", end],
    ["#ga-balance", "10000.123456789012345678901234"],
    ["#ga-maker", "0.000001"], ["#ga-taker", "0.000123"],
    ["#ga-quantity-step", "0.001"], ["#ga-price-tick", "0.01"],
    ["#ga-short_window-min", "1"], ["#ga-short_window-max", "2"],
    ["#ga-short_window-step", "1"], ["#ga-long_window-min", "3"],
    ["#ga-long_window-max", "4"], ["#ga-long_window-step", "1"],
    ["#ga-quantity", "0.01"], ["#ga-population", "2"],
    ["#ga-generations", "1"], ["#ga-seed", "17"]
  ] as const) {
    await page.locator(selector).fill(value);
  }
  const submitResponsePromise = page.waitForResponse((response) =>
    response.request().method() === "POST" &&
    new URL(response.url()).pathname === "/api/v1/ga-jobs" &&
    response.status() === 202
  );
  await page.getByRole("button", { name: "Review research submission" }).click();
  await page.getByRole("button", { name: "Confirm and submit once" }).click();
  const submitResponse = await submitResponsePromise;
  const submitReceipt = await submitResponse.json() as { schema_version?: number; job?: { id?: string } };
  expect(submitReceipt.schema_version).toBe(1);

  const submittedJobId = submitReceipt.job?.id ?? "";
  expect(submittedJobId).not.toBe("");
  const submittedJobButton = page.locator(".ga-job-list button").filter({
    has: page.getByText(submittedJobId, { exact: true })
  });
  await expect(submittedJobButton).toHaveCount(1);
  await submittedJobButton.click();
  await expect(submittedJobButton).toHaveAttribute("aria-pressed", "true");
  const epochLink = page.getByRole("button", { name: /open this job's committed epoch/i });
  await expect(epochLink).toBeVisible();
  const jobDetail = page.locator(".ga-job-detail");
  await expect(jobDetail.locator("dd").nth(0)).toHaveText(submittedJobId);
  await page.getByRole("button", { name: "Refresh GA state" }).click();
  await expect(jobDetail.locator("dd").nth(1)).toHaveText("Succeeded");
  await epochLink.click();
  await expect(page.locator("#epoch")).not.toHaveValue(initialEpochId);
  const challenger = page.locator(".ga-candidate-row")
    .filter({ hasText: /· golden_cross · challenger ·/ }).first();
  await expect(challenger).toBeVisible();
  await challenger.getByRole("button", { name: "Prepare promotion" }).click();
  await page.getByRole("button", { name: "Review candidate promotion" }).click();
  const promotion = page.waitForResponse((response) =>
    response.request().method() === "POST" &&
    /\/genes\/\d+\/promote$/.test(new URL(response.url()).pathname) &&
    response.status() === 200
  );
  await page.getByRole("button", { name: "Confirm research-only promotion" }).click();
  await promotion;
  await expect(page.getByRole("status")).toContainText(/research candidate only/i);
  await expect(page.locator(".ga-audit-list")).toContainText("gene_promote");
  await expect(page.locator(".ga-audit-list")).toContainText("gene_retire");

  const resultPath = requiredEnv("RESEARCH_REAL_RESULT_FILE");
  writeFileSync(resultPath, JSON.stringify({ jobId: submittedJobId }));
});
