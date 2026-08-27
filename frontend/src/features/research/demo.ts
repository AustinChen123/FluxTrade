import type { Epoch, Gene, GenerationSummary } from "./gaDomain";

export const demoEpoch: Epoch = {
  id: "epoch_20260728_demo",
  strategy_id: "golden_cross_research",
  started_at: "2026-07-28T08:30:00Z",
  finished_at: "2026-07-28T09:12:00Z",
  pop_size: 2_500,
  max_generations: 20,
  generations_run: 20,
  best_score: "2.8421",
  seed: 14721,
  config_json: { objective: "maximize_score" },
  status: "completed",
  eval_pair: "CME:MNQ-CONTINUOUS",
  eval_start_date: "2019-01-01",
  eval_end_date: "2026-07-27",
  eval_timeframe: "5m"
};

export const demoSummaries: GenerationSummary[] = Array.from(
  { length: 20 },
  (_, generation) => ({
    generation_index: generation,
    candidate_count: 2_500,
    score_min: String(-1.4 + generation * 0.025),
    score_max: String(0.8 + Math.log1p(generation) * 0.68),
    drawdown_min: String(0.07 - generation * 0.0015),
    drawdown_max: String(0.38 - generation * 0.004)
  })
);

export function buildDemoGenes(generation: number): Gene[] {
  return Array.from({ length: 2_500 }, (_, index) => {
  const shortWindow = 4 + (index % 45);
  const longWindow = 38 + ((index * 17) % 185);
  const riskBudget = 0.15 + ((index * 23) % 75) / 100;
  const ridge =
    Math.exp(-Math.pow((shortWindow - 18) / 11, 2)) *
    Math.exp(-Math.pow((longWindow - 112) / 43, 2));
  const texture = Math.sin(index * 1.93) * 0.13 + Math.cos(index * 0.37) * 0.09;
  const maturity = 0.58 + generation / 48;
  const score =
    ridge * 2.65 * maturity +
    texture -
    Math.abs(riskBudget - 0.42) * 0.35;
  return {
    id: index + 1,
    strategy_id: demoEpoch.strategy_id,
    role: index === 1_842 ? "champion" : "challenger",
    param_pack: {
      short_window: shortWindow,
      long_window: longWindow,
      risk_budget: riskBudget.toFixed(2),
      session_filter: index % 3 === 0 ? "rth" : "full"
    },
    score_total: score.toFixed(8),
    score_breakdown: {
      sharpe_annualized: (0.4 + ridge * 2.3 + texture).toFixed(4),
      trade_count: 318 + (index % 286)
    },
    max_drawdown: (0.06 + (1 - ridge) * 0.24).toFixed(8),
    generation_index: generation,
    candidate_id: `g${String(generation).padStart(6, "0")}_c${String(index).padStart(6, "0")}`,
    epoch_id: demoEpoch.id,
    created_at: "2026-07-28T09:12:00Z"
  };
  });
}

export const demoGenes = buildDemoGenes(19);
