import { lazy, Suspense, useEffect, useMemo, useState } from "react";

import {
  ApiError,
  ensureBrowserSession,
  loadEpochs,
  loadGenerationGenes,
  loadGenerationSummaries,
  type Epoch,
  type Gene,
  type GenerationSummary
} from "./api";
import { buildDemoGenes, demoEpoch, demoSummaries } from "./demo";
import {
  convergenceOption,
  finiteNumber,
  numericParameterNames,
  objectiveLabel,
  parallelOption,
  parameterDimensions,
  scatterOption
} from "./ga";

const EChart = lazy(() =>
  import("./EChart").then((module) => ({ default: module.EChart }))
);

function displayNumber(value: string | number | null, digits = 4): string {
  const parsed = finiteNumber(value);
  return parsed === null
    ? "—"
    : new Intl.NumberFormat("zh-TW", {
        maximumFractionDigits: digits
      }).format(parsed);
}

function selectBestGene(epoch: Epoch, genes: Gene[]): Gene | null {
  if (!genes.length) {
    return null;
  }
  const minimizeDrawdown = epoch.config_json.objective === "minimize_drawdown";
  return genes.reduce((best, candidate) => {
    const bestValue = finiteNumber(
      minimizeDrawdown ? best.max_drawdown : best.score_total
    );
    const candidateValue = finiteNumber(
      minimizeDrawdown ? candidate.max_drawdown : candidate.score_total
    );
    if (candidateValue === null) {
      return best;
    }
    if (bestValue === null) {
      return candidate;
    }
    return minimizeDrawdown
      ? candidateValue < bestValue
        ? candidate
        : best
      : candidateValue > bestValue
        ? candidate
        : best;
  });
}

function geneIdFromChartData(data: unknown): number | null {
  if (Array.isArray(data)) {
    return typeof data[3] === "number" ? data[3] : null;
  }
  if (data && typeof data === "object" && "geneId" in data) {
    const value = (data as { geneId?: unknown }).geneId;
    return typeof value === "number" ? value : null;
  }
  return null;
}

function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    if (error.status === 401 || error.status === 403) {
      return "目前工作階段沒有讀取研究結果的權限。請從受信任的 Tailscale 入口重新開啟。";
    }
    return `資料服務回覆 ${error.status}：${error.message}`;
  }
  return error instanceof Error ? error.message : "無法讀取 GA 資料";
}

export function App() {
  const demoMode =
    import.meta.env.DEV &&
    new URLSearchParams(window.location.search).get("demo") === "1";
  const [epochs, setEpochs] = useState<Epoch[]>([]);
  const [epochId, setEpochId] = useState("");
  const [summaries, setSummaries] = useState<GenerationSummary[]>([]);
  const [generationIndex, setGenerationIndex] = useState<number | null>(null);
  const [genes, setGenes] = useState<Gene[]>([]);
  const [selectedGeneId, setSelectedGeneId] = useState<number | null>(null);
  const [xParameter, setXParameter] = useState("");
  const [yParameter, setYParameter] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [reloadToken, setReloadToken] = useState(0);

  const epoch = epochs.find((item) => item.id === epochId) ?? null;

  useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
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
        setEpochId((current) =>
          items.some((item) => item.id === current) ? current : (items[0]?.id ?? "")
        );
      })
      .catch((reason) => active && setError(errorMessage(reason)))
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, reloadToken]);

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
    setError(null);
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
      .catch((reason) => active && setError(errorMessage(reason)))
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epoch]);

  useEffect(() => {
    if (!epoch || generationIndex === null) {
      setGenes([]);
      return;
    }
    let active = true;
    setLoading(true);
    setError(null);
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
      .catch((reason) => active && setError(errorMessage(reason)))
      .finally(() => active && setLoading(false));
    return () => {
      active = false;
    };
  }, [demoMode, epoch, generationIndex]);

  const numericParameters = useMemo(
    () => numericParameterNames(genes),
    [genes]
  );
  const dimensions = useMemo(() => parameterDimensions(genes), [genes]);

  useEffect(() => {
    setXParameter((current) =>
      numericParameters.includes(current) ? current : (numericParameters[0] ?? "")
    );
    setYParameter((current) =>
      numericParameters.includes(current) && current !== numericParameters[0]
        ? current
        : (numericParameters[1] ?? numericParameters[0] ?? "")
    );
  }, [numericParameters]);

  const selectedGene =
    genes.find((gene) => gene.id === selectedGeneId) ?? null;
  const rankedGenes = useMemo(() => {
    if (!epoch) {
      return [];
    }
    const minimizeDrawdown = epoch.config_json.objective === "minimize_drawdown";
    return [...genes]
      .sort((left, right) => {
        const leftValue =
          finiteNumber(minimizeDrawdown ? left.max_drawdown : left.score_total) ??
          (minimizeDrawdown
            ? Number.POSITIVE_INFINITY
            : Number.NEGATIVE_INFINITY);
        const rightValue =
          finiteNumber(minimizeDrawdown ? right.max_drawdown : right.score_total) ??
          (minimizeDrawdown
            ? Number.POSITIVE_INFINITY
            : Number.NEGATIVE_INFINITY);
        return minimizeDrawdown ? leftValue - rightValue : rightValue - leftValue;
      })
      .slice(0, 8);
  }, [epoch, genes]);

  const convergence = useMemo(
    () => (epoch ? convergenceOption(epoch, summaries) : {}),
    [epoch, summaries]
  );
  const scatter = useMemo(
    () =>
      xParameter && yParameter
        ? scatterOption(genes, xParameter, yParameter, selectedGeneId)
        : {},
    [genes, selectedGeneId, xParameter, yParameter]
  );
  const parallel = useMemo(
    () => parallelOption(genes, dimensions, selectedGeneId),
    [dimensions, genes, selectedGeneId]
  );

  const chooseGeneFromChart = (data: unknown) => {
    const geneId = geneIdFromChartData(data);
    if (geneId !== null) {
      setSelectedGeneId(geneId);
    }
  };

  const chooseEpoch = (nextEpochId: string) => {
    setSummaries([]);
    setGenerationIndex(null);
    setGenes([]);
    setSelectedGeneId(null);
    setEpochId(nextEpochId);
  };

  return (
    <main className="console-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">FluxTrade / research console</p>
          <h1>參數地形</h1>
        </div>
        <div className="epoch-control">
          <label htmlFor="epoch">演化批次</label>
          <select
            id="epoch"
            value={epochId}
            onChange={(event) => chooseEpoch(event.target.value)}
            disabled={!epochs.length}
          >
            {epochs.map((item) => (
              <option key={item.id} value={item.id}>
                {item.strategy_id} · {item.started_at.slice(0, 10)}
              </option>
            ))}
          </select>
        </div>
      </header>

      {demoMode && (
        <p className="demo-notice">
          本機展示資料 · 正式建置不包含此模式
        </p>
      )}

      {error && (
        <section className="error-panel" role="alert">
          <div>
            <strong>研究資料未載入</strong>
            <p>{error}</p>
          </div>
          <button type="button" onClick={() => setReloadToken((value) => value + 1)}>
            重新讀取
          </button>
        </section>
      )}

      {!error && !loading && !epoch && (
        <section className="empty-panel">
          <strong>尚無演化批次</strong>
          <p>完成一次參數搜尋後，候選地形會出現在這裡。</p>
        </section>
      )}

      {epoch && (
        <Suspense
          fallback={
            <div className="loading-indicator" aria-live="polite">
              <span />
              載入圖表引擎
            </div>
          }
        >
          <section className="run-strip" aria-label="演化批次摘要">
            <div className="run-identity">
              <span className={`status-mark status-${epoch.status}`} />
              <div>
                <strong>{epoch.strategy_id}</strong>
                <span>{epoch.eval_pair} · {epoch.eval_timeframe}</span>
              </div>
            </div>
            <dl>
              <div>
                <dt>目標</dt>
                <dd>{objectiveLabel(epoch)}</dd>
              </div>
              <div>
                <dt>世代</dt>
                <dd>{epoch.generations_run ?? 0}/{epoch.max_generations}</dd>
              </div>
              <div>
                <dt>族群</dt>
                <dd>{epoch.pop_size.toLocaleString("zh-TW")}</dd>
              </div>
              <div>
                <dt>最佳評分</dt>
                <dd>{displayNumber(epoch.best_score)}</dd>
              </div>
            </dl>
          </section>

          <section className="analysis-grid">
            <article className="panel convergence-panel">
              <div className="panel-heading">
                <div>
                  <p className="panel-kicker">evolution trace</p>
                  <h2>收斂軌跡</h2>
                </div>
                <label>
                  顯示世代
                  <select
                    value={generationIndex ?? ""}
                    onChange={(event) =>
                      setGenerationIndex(Number(event.target.value))
                    }
                  >
                    {summaries.map((item) => (
                      <option
                        key={item.generation_index}
                        value={item.generation_index}
                      >
                        {item.generation_index}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
              <EChart
                option={convergence}
                className="chart chart-convergence"
                ariaLabel="各世代評分或回撤的上下界收斂圖"
              />
            </article>

            <article className="panel topology-panel">
              <div className="panel-heading">
                <div>
                  <p className="panel-kicker">population topology</p>
                  <h2>候選地形</h2>
                </div>
                <div className="axis-controls">
                  <label>
                    X
                    <select
                      value={xParameter}
                      onChange={(event) => setXParameter(event.target.value)}
                    >
                      {numericParameters.map((name) => (
                        <option key={name}>{name}</option>
                      ))}
                    </select>
                  </label>
                  <label>
                    Y
                    <select
                      value={yParameter}
                      onChange={(event) => setYParameter(event.target.value)}
                    >
                      {numericParameters.map((name) => (
                        <option key={name}>{name}</option>
                      ))}
                    </select>
                  </label>
                </div>
              </div>
              {numericParameters.length >= 2 ? (
                <EChart
                  option={scatter}
                  className="chart chart-topology"
                  ariaLabel="兩個策略參數與候選評分的散點圖"
                  onDataClick={chooseGeneFromChart}
                />
              ) : (
                <div className="chart-message">
                  這個批次需要至少兩個數值參數才能繪製候選地形。
                </div>
              )}
            </article>

            <aside className="panel selection-panel">
              <div className="panel-heading">
                <div>
                  <p className="panel-kicker">candidate ledger</p>
                  <h2>候選比較</h2>
                </div>
                <span>{genes.length.toLocaleString("zh-TW")} 筆</span>
              </div>
              <ol className="ranking-list">
                {rankedGenes.map((gene, index) => (
                  <li key={gene.id}>
                    <button
                      type="button"
                      className={gene.id === selectedGeneId ? "is-selected" : ""}
                      onClick={() => setSelectedGeneId(gene.id)}
                    >
                      <span className="rank">{String(index + 1).padStart(2, "0")}</span>
                      <span>
                        <strong>{gene.candidate_id}</strong>
                        <small>DD {displayNumber(gene.max_drawdown, 3)}</small>
                      </span>
                      <b>{displayNumber(gene.score_total, 3)}</b>
                    </button>
                  </li>
                ))}
              </ol>
              {selectedGene && (
                <div className="selection-detail">
                  <div>
                    <span>已選候選</span>
                    <strong>{selectedGene.candidate_id}</strong>
                  </div>
                  <dl>
                    {Object.entries(selectedGene.param_pack).map(([name, value]) => (
                      <div key={name}>
                        <dt>{name}</dt>
                        <dd>{String(value)}</dd>
                      </div>
                    ))}
                  </dl>
                  {Object.keys(selectedGene.score_breakdown).length > 0 && (
                    <>
                      <p>評估指標</p>
                      <dl>
                        {Object.entries(selectedGene.score_breakdown).map(
                          ([name, value]) => (
                            <div key={name}>
                              <dt>{name}</dt>
                              <dd>{String(value)}</dd>
                            </div>
                          )
                        )}
                      </dl>
                    </>
                  )}
                </div>
              )}
            </aside>

            <article className="panel parallel-panel">
              <div className="panel-heading">
                <div>
                  <p className="panel-kicker">multi-axis fingerprint</p>
                  <h2>參數指紋</h2>
                </div>
                <span>最多顯示 12 軸</span>
              </div>
              {dimensions.length ? (
                <EChart
                  option={parallel}
                  className="chart chart-parallel"
                  ariaLabel="候選基因的多參數平行座標圖"
                  onDataClick={chooseGeneFromChart}
                />
              ) : (
                <div className="chart-message">此世代沒有可繪製的參數。</div>
              )}
            </article>
          </section>
        </Suspense>
      )}

      {loading && (
        <div className="loading-indicator" aria-live="polite">
          <span />
          讀取研究快照
        </div>
      )}
    </main>
  );
}
