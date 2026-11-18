import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";

// npm audit has no ignore list; this keeps the high-severity gate but waives named advisories until a date.
const WAIVERS = {
  "GHSA-fc5q-39xc-vjpf": {
    until: "2026-12-31",
    reason: "picomatch ReDoS, no patched release; dev/build toolchain only (vite, vitest)"
  }
};

const BLOCKING = new Set(["high", "critical"]);

function blockingAdvisories(report, waivers, today) {
  const blocked = new Map();
  for (const vulnerability of Object.values(report.vulnerabilities ?? {})) {
    for (const via of vulnerability.via) {
      if (typeof via !== "object" || !BLOCKING.has(via.severity)) continue;
      const id = via.url.split("/").pop();
      const waiver = waivers[id];
      if (!waiver || today > waiver.until) blocked.set(id, `${via.name}: ${via.title} (${via.url})`);
    }
  }
  return [...blocked.values()];
}

function selfTest() {
  const advisory = (id, severity) => ({
    name: "pkg",
    title: "t",
    severity,
    url: `https://github.com/advisories/${id}`
  });
  const report = {
    vulnerabilities: {
      pkg: { via: [advisory("GHSA-a", "high"), advisory("GHSA-b", "moderate")] },
      parent: { via: ["pkg"] }
    }
  };
  const waivers = { "GHSA-a": { until: "2026-12-31", reason: "r" } };
  assert.equal(blockingAdvisories(report, {}, "2026-11-18").length, 1);
  assert.deepEqual(blockingAdvisories(report, waivers, "2026-12-31"), []);
  assert.equal(blockingAdvisories(report, waivers, "2027-01-01").length, 1);
  assert.deepEqual(blockingAdvisories({}, {}, "2026-11-18"), []);
}

selfTest();
let raw;
try {
  raw = execFileSync("npm", ["audit", "--json"], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
} catch (error) {
  raw = error.stdout;
}
const report = JSON.parse(raw);
if (report.error) throw new Error(`npm audit failed: ${JSON.stringify(report.error)}`);
const blocked = blockingAdvisories(report, WAIVERS, new Date().toISOString().slice(0, 10));
for (const line of blocked) console.error(`blocking advisory: ${line}`);
console.log(`audit gate: ${blocked.length} blocking high/critical advisories`);
process.exit(blocked.length ? 1 : 0);
