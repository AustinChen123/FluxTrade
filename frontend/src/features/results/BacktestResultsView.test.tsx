// @vitest-environment jsdom

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor
} from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import i18n from "../../shared/i18n";
import { BacktestResultsView } from "./BacktestResultsView";
import { demoBacktestSnapshot } from "./demo";
import type { BacktestResultSnapshot } from "./resultsModel";

const chartState = vi.hoisted(() => ({ option: null as unknown }));
const resultsRead = vi.hoisted(() => ({
  useBacktestResults: vi.fn()
}));

vi.mock("./useBacktestResults", () => ({
  useBacktestResults: resultsRead.useBacktestResults
}));

vi.mock("../../shared/charts/EChart", () => ({
  EChart: ({
    ariaLabel,
    option
  }: {
    ariaLabel: string;
    option: unknown;
  }) => {
    chartState.option = option;
    return <div role="img" aria-label={ariaLabel} />;
  }
}));

function decimalUnits(value: string | null, scale: number): bigint {
  const match = value ? /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(value) : null;
  const fraction = match?.[3] ?? "";
  if (!match || fraction.length > scale) {
    throw new Error(`Expected at most ${scale} decimals, received ${String(value)}`);
  }
  const factor = 10n ** BigInt(scale);
  const magnitude =
    BigInt(match[2]) * factor +
    BigInt(fraction.padEnd(scale, "0") || "0");
  return match[1] === "-" ? -magnitude : magnitude;
}

const decimalCents = (value: string | null) => decimalUnits(value, 2);
const demoTrades = demoBacktestSnapshot.tradePage.items;

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

const zeroTradeSnapshot: BacktestResultSnapshot = {
  jobId: "job-empty",
  strategyId: "strategy-empty",
  productId: "CME:MNQ-CONTINUOUS",
  timeframe: "5m",
  startedAt: "2026-01-01T00:00:00Z",
  endedAt: "2026-01-31T23:59:59Z",
  currency: "USD",
  metrics: {
    netPnl: "0",
    returnPct: "0",
    maxDrawdown: "0",
    sharpe: "0",
    sortino: "0",
    calmar: "0"
  },
  equity: [],
  monthlyReturns: [],
  pnlDistribution: [],
  tradePage: {
    items: [],
    totalCount: 0,
    nextCursor: null
  }
};

describe("BacktestResultsView", () => {
  beforeEach(async () => {
    await i18n.changeLanguage("zh-TW");
    resultsRead.useBacktestResults.mockReturnValue({
      state: { status: "unavailable", selection: null },
      loadMoreIndex: vi.fn().mockResolvedValue(undefined),
      loadMoreTrades: vi.fn().mockResolvedValue({
        items: [],
        totalCount: 0,
        nextCursor: null
      })
    });
  });

  afterEach(() => {
    cleanup();
  });

  it("renders the complete demo result and opens trade inspection", () => {
    const onInspectTrade = vi.fn();
    render(
      <BacktestResultsView
        demoMode
        theme="light"
        onInspectTrade={onInspectTrade}
      />
    );

    expect(
      screen.getByLabelText("回測權益曲線與同步回撤圖")
    ).toBeTruthy();
    expect(resultsRead.useBacktestResults).toHaveBeenCalledWith({
      selectedResultId: null,
      enabled: false
    });
    expect(screen.getByText("job-research-0042")).toBeTruthy();
    expect(screen.getByText(/1\.36/)).toBeTruthy();
    expect(screen.getAllByRole("button", { name: "查看 K 線" })).toHaveLength(
      3
    );
    expect(screen.getAllByText(/16:30/).length).toBeGreaterThan(0);
    const tooltipTimestamp = (
      chartState.option as {
        tooltip: {
          axisPointer: {
            label: { formatter: (params: { value: unknown }) => string };
          };
        };
      }
    ).tooltip.axisPointer.label.formatter;
    expect(
      tooltipTimestamp({ value: demoBacktestSnapshot.equity[0].timestamp })
    ).toContain("15:30");

    fireEvent.click(screen.getAllByRole("button", { name: "查看 K 線" })[0]);
    expect(onInspectTrade).toHaveBeenCalledWith(
      "trade-000184",
      "job-research-0042"
    );
  });

  it("loads the explicit index and sends the chosen result ID to the shell", () => {
    const onSelectResult = vi.fn();
    resultsRead.useBacktestResults.mockReturnValue({
      state: {
        status: "index",
        selection: null,
        nextCursor: "signed-index-cursor",
        items: [{
          jobId: "job-index-0042",
          subjectId: "subject-42",
          datasetId: "dataset-42",
          productId: "CME:MNQ-CONTINUOUS",
          timeframe: "5m",
          startedAt: "2026-01-01T00:00:00.000Z",
          endedAt: "2026-01-31T23:59:59.000Z",
          completedAt: "2026-02-01T00:00:00.000Z",
          resultDigest: "a".repeat(64)
        }]
      },
      loadMoreIndex: vi.fn().mockResolvedValue(undefined),
      loadMoreTrades: vi.fn()
    });

    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        selectedResultId={null}
        onSelectResult={onSelectResult}
      />
    );

    expect(resultsRead.useBacktestResults).toHaveBeenCalledWith({
      selectedResultId: null,
      enabled: true
    });
    fireEvent.click(screen.getByRole("button", { name: /job-index-0042/ }));
    expect(onSelectResult).toHaveBeenCalledWith("job-index-0042");
  });

  it("uses the single shared pager with the selected result cursor", async () => {
    const loadMoreTrades = vi.fn().mockResolvedValue({
      items: [demoTrades[1]],
      totalCount: 2,
      nextCursor: null
    });
    const snapshot = {
      ...zeroTradeSnapshot,
      jobId: "job-detail-0042",
      tradePage: {
        items: [demoTrades[0]],
        totalCount: 2,
        nextCursor: "signed-trade-cursor"
      }
    };
    resultsRead.useBacktestResults.mockReturnValue({
      state: {
        status: "detail",
        selection: snapshot.jobId,
        resultId: snapshot.jobId,
        snapshot
      },
      loadMoreIndex: vi.fn(),
      loadMoreTrades
    });
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        selectedResultId={snapshot.jobId}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "載入更多交易" }));
    await waitFor(() => expect(loadMoreTrades).toHaveBeenCalledWith("signed-trade-cursor"));
    await waitFor(() =>
      expect(
        document.querySelectorAll(".results-trades tbody tr")
      ).toHaveLength(2)
    );
  });

  it("keeps the next index cursor opaque and invokes the existing hook pager", () => {
    const loadMoreIndex = vi.fn().mockResolvedValue(undefined);
    resultsRead.useBacktestResults.mockReturnValue({
      state: { status: "index", selection: null, nextCursor: "signed.cursor", items: [] },
      loadMoreIndex,
      loadMoreTrades: vi.fn()
    });
    render(<BacktestResultsView demoMode={false} theme="light" />);

    fireEvent.click(screen.getByRole("button", { name: "載入更多結果" }));
    expect(loadMoreIndex).toHaveBeenCalledOnce();
  });

  it("shows the pending state without rendering the development fixture", () => {
    resultsRead.useBacktestResults.mockReturnValue({
      state: { status: "pending", selection: "job-pending-0042" },
      loadMoreIndex: vi.fn(),
      loadMoreTrades: vi.fn()
    });
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        selectedResultId="job-pending-0042"
      />
    );

    expect(screen.getByText("載入回測績效")).toBeTruthy();
    expect(screen.queryByText("job-research-0042")).toBeNull();
  });

  it("shows an explicit empty state when the signed index has no results", () => {
    resultsRead.useBacktestResults.mockReturnValue({
      state: { status: "empty", selection: null, items: [] },
      loadMoreIndex: vi.fn(),
      loadMoreTrades: vi.fn()
    });
    render(<BacktestResultsView demoMode={false} theme="light" />);

    expect(screen.getByText("目前沒有已完成的結果")).toBeTruthy();
    expect(screen.queryByRole("table")).toBeNull();
  });

  it.each([
    ["permission-unavailable", "目前工作階段沒有讀取權限"],
    ["unavailable", "結果可能已不可用"],
    ["error", "請返回結果清單，稍後再選擇結果"],
    ["invalid-data", "這筆結果無法安全顯示"]
  ] as const)("renders a fixed %s state without prior financial data", (status, message) => {
    resultsRead.useBacktestResults.mockReturnValue({
      state: { status, selection: "job-failed-0042" },
      loadMoreIndex: vi.fn(),
      loadMoreTrades: vi.fn()
    });
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        selectedResultId="job-failed-0042"
        onSelectResult={vi.fn()}
      />
    );

    expect(screen.getByRole("alert").textContent).toContain(message);
    expect(screen.queryByText("9007199254740993.00")).toBeNull();
    expect(screen.getByRole("button", { name: "選擇其他結果" })).toBeTruthy();
  });

  it("does not start production reads when an explicit snapshot is supplied", () => {
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        selectedResultId="job-provided-0042"
        snapshot={zeroTradeSnapshot}
      />
    );

    expect(resultsRead.useBacktestResults).toHaveBeenCalledWith({
      selectedResultId: "job-provided-0042",
      enabled: false
    });
    expect(screen.getByText("job-empty")).toBeTruthy();
  });

  it("keeps explicit false loading flags compatible with development demo", () => {
    render(
      <BacktestResultsView
        demoMode
        theme="light"
        loading={false}
        loadError={false}
      />
    );

    expect(screen.getByText("job-research-0042")).toBeTruthy();
    expect(resultsRead.useBacktestResults).toHaveBeenCalledWith({
      selectedResultId: null,
      enabled: false
    });
  });

  it("formats production raw ratios as percentages and preserves legacy points", () => {
    const ratio = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          returnPctUnit: "ratio",
          metrics: { ...zeroTradeSnapshot.metrics, returnPct: "0.125" },
          monthlyReturns: [{ month: "2026-01", returnPct: "-0.0000136" }]
        }}
      />
    );

    expect(ratio.container.querySelector(".metric-primary small")?.textContent)
      .toBe("12.50%");
    expect(ratio.container.querySelector(".monthly-grid strong")?.textContent)
      .toBe("-0.0014%");

    ratio.rerender(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          metrics: { ...zeroTradeSnapshot.metrics, returnPct: "1.36" },
          monthlyReturns: [{ month: "2026-01", returnPct: "1.36" }]
        }}
      />
    );
    expect(ratio.container.querySelector(".metric-primary small")?.textContent)
      .toBe("1.36%");
    expect(ratio.container.querySelector(".monthly-grid strong")?.textContent)
      .toBe("1.36%");
  });

  it("keeps authoritative USDT money strings visible without currency coercion", () => {
    const exactPnl = "12345678901234567890.123456789";
    const exactDrawdown = "0.0000000000000000001";
    const exactFee = "0.0000000000000000003";
    const snapshot = {
      ...zeroTradeSnapshot,
      jobId: "job-usdt-0042",
      currency: "USDT",
      metrics: {
        ...zeroTradeSnapshot.metrics,
        netPnl: exactPnl,
        maxDrawdown: exactDrawdown
      },
      tradePage: {
        items: [{
          ...demoTrades[0],
          pnl: exactPnl,
          fee: exactFee
        }],
        totalCount: 1,
        nextCursor: null
      }
    };
    resultsRead.useBacktestResults.mockReturnValue({
      state: {
        status: "detail",
        selection: snapshot.jobId,
        resultId: snapshot.jobId,
        snapshot
      },
      loadMoreIndex: vi.fn(),
      loadMoreTrades: vi.fn()
    });
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        selectedResultId={snapshot.jobId}
      />
    );

    expect(screen.getAllByText(`${exactPnl} USDT`)).toHaveLength(2);
    expect(screen.getByText(`${exactDrawdown} USDT`)).toBeTruthy();
    expect(screen.getByText(`${exactFee} USDT`)).toBeTruthy();
  });

  it("uses the same internally consistent trades as candle inspection", () => {
    expect(demoBacktestSnapshot.tradePage.items).toBe(demoTrades);
    const tradePnl = demoBacktestSnapshot.tradePage.items.reduce(
      (total, trade) => total + decimalCents(trade.pnl),
      0n
    );
    const firstEquity = decimalCents(
      demoBacktestSnapshot.equity[0]?.equity ?? null
    );
    const finalEquity = decimalCents(
      demoBacktestSnapshot.equity.at(-1)?.equity ?? null
    );
    const maxDrawdown = demoBacktestSnapshot.equity.reduce(
      (maximum, sample) => {
        const drawdown = decimalCents(sample.drawdown);
        return drawdown > maximum ? drawdown : maximum;
      },
      0n
    );

    expect(decimalCents(demoBacktestSnapshot.metrics.netPnl)).toBe(tradePnl);
    expect(finalEquity - firstEquity).toBe(tradePnl);
    expect(decimalCents(demoBacktestSnapshot.metrics.maxDrawdown)).toBe(
      maxDrawdown
    );
    expect(
      decimalUnits(
        demoBacktestSnapshot.monthlyReturns[0]?.returnPct ?? null,
        5
      ) * firstEquity
    ).toBe(tradePnl * 100n * 100_000n);
    const lossCount = demoBacktestSnapshot.tradePage.items.filter(
      (trade) => decimalCents(trade.pnl) < 0n
    ).length;
    expect(demoBacktestSnapshot.pnlDistribution).toEqual([
      { lower: null, upper: "0", count: lossCount },
      {
        lower: "0",
        upper: null,
        count: demoBacktestSnapshot.tradePage.items.length - lossCount
      }
    ]);
  });

  it.each(["0x10", "1e2", "", "Infinity"])(
    "rejects non-decimal financial value %j",
    (netPnl) => {
      const view = render(
        <BacktestResultsView
          demoMode={false}
          theme="light"
          snapshot={{
            ...zeroTradeSnapshot,
            metrics: { ...zeroTradeSnapshot.metrics, netPnl }
          }}
        />
      );

      expect(
        view.container.querySelector(".metric-primary strong")?.textContent
      ).toBe("—");
    }
  );

  it("formats financial text without losing Decimal precision", () => {
    const view = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          metrics: {
            ...zeroTradeSnapshot.metrics,
            netPnl: "9007199254740993.00"
          }
        }}
      />
    );

    expect(
      view.container.querySelector(".metric-primary strong")?.textContent
    ).toContain("9,007,199,254,740,993.00");
  });

  it("retries cursor pagination and appends trades without duplicate rows", async () => {
    const onLoadMoreTrades = vi
      .fn()
      .mockRejectedValueOnce(new Error("temporary"))
      .mockResolvedValueOnce({
        items: demoTrades,
        totalCount: 3,
        nextCursor: null
      });
    const view = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        onLoadMoreTrades={onLoadMoreTrades}
        snapshot={{
          ...zeroTradeSnapshot,
          tradePage: {
            items: [demoTrades[0]],
            totalCount: 3,
            nextCursor: "cursor-1"
          }
        }}
      />
    );

    expect(
      view.container.querySelector(
        ".results-metrics > div:last-child strong"
      )?.textContent
    ).toBe("3");
    expect(screen.getByText("已載入 1／3 筆")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "載入更多交易" }));
    expect(
      await screen.findByRole("button", { name: "重試載入交易" })
    ).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "重試載入交易" }));

    await waitFor(() =>
      expect(
        view.container.querySelectorAll(".results-trades tbody tr")
      ).toHaveLength(3)
    );
    expect(screen.getByText("已載入 3／3 筆")).toBeTruthy();
    expect(
      view.container.querySelectorAll(
        ".results-trades tbody td:first-child"
      )
    ).toHaveLength(3);
    expect(onLoadMoreTrades).toHaveBeenNthCalledWith(1, "cursor-1");
    expect(onLoadMoreTrades).toHaveBeenNthCalledWith(2, "cursor-1");
  });

  it("rejects a conflicting duplicate trade from the next page", async () => {
    const onLoadMoreTrades = vi.fn().mockResolvedValue({
      items: [{ ...demoTrades[0], pnl: "999.00" }],
      totalCount: 2,
      nextCursor: null
    });
    const view = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        onLoadMoreTrades={onLoadMoreTrades}
        snapshot={{
          ...zeroTradeSnapshot,
          tradePage: {
            items: [demoTrades[0]],
            totalCount: 2,
            nextCursor: "cursor-1"
          }
        }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "載入更多交易" }));

    expect(
      await screen.findByRole("button", { name: "重試載入交易" })
    ).toBeTruthy();
    expect(
      view.container.querySelectorAll(".results-trades tbody tr")
    ).toHaveLength(1);
  });

  it("ignores a stale trade-page response after the job changes", async () => {
    const pending = deferred<{
      items: typeof demoTrades;
      totalCount: number;
      nextCursor: null;
    }>();
    const onLoadMoreTrades = vi.fn().mockReturnValue(pending.promise);
    const view = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        onLoadMoreTrades={onLoadMoreTrades}
        snapshot={{
          ...zeroTradeSnapshot,
          jobId: "job-one",
          tradePage: {
            items: [demoTrades[0]],
            totalCount: 3,
            nextCursor: "cursor-1"
          }
        }}
      />
    );

    const loadButton = screen.getByRole("button", {
      name: "載入更多交易"
    });
    fireEvent.click(loadButton);
    fireEvent.click(loadButton);
    expect(onLoadMoreTrades).toHaveBeenCalledTimes(1);

    view.rerender(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        onLoadMoreTrades={onLoadMoreTrades}
        snapshot={{
          ...zeroTradeSnapshot,
          jobId: "job-two",
          tradePage: {
            items: [demoTrades[2]],
            totalCount: 1,
            nextCursor: null
          }
        }}
      />
    );
    pending.resolve({
      items: demoTrades,
      totalCount: 3,
      nextCursor: null
    });

    await waitFor(() =>
      expect(
        view.container.querySelector(
          ".results-trades tbody td:first-child"
        )?.textContent
      ).toBe("trade-000186")
    );
    expect(
      view.container.querySelectorAll(".results-trades tbody tr")
    ).toHaveLength(1);
  });

  it("keeps nearby equity ticks distinct and shades monthly returns by magnitude", () => {
    const view = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          equity: [
            {
              timestamp: "2026-01-01T00:00:00Z",
              equity: "100000.00",
              drawdown: "0.00"
            },
            {
              timestamp: "2026-01-01T00:05:00Z",
              equity: "100000.88",
              drawdown: "0.00"
            }
          ],
          monthlyReturns: [
            { month: "2026-01", returnPct: "10" },
            { month: "2026-02", returnPct: "-5" },
            { month: "2026-03", returnPct: "1" }
          ]
        }}
      />
    );
    const equityAxisFormatter = (
      chartState.option as {
        yAxis: Array<{
          axisLabel: { formatter: (value: number) => string };
        }>;
      }
    ).yAxis[0].axisLabel.formatter;
    const months = view.container.querySelectorAll(".monthly-grid > div");

    expect(equityAxisFormatter(100000)).not.toBe(
      equityAxisFormatter(100000.88)
    );
    expect(months[0].getAttribute("style")).toContain(
      "--return-intensity: 36%"
    );
    expect(months[1].getAttribute("style")).toContain(
      "--return-intensity: 22%"
    );
    expect(months[2].getAttribute("style")).toContain(
      "--return-intensity: 11%"
    );
  });

  it("keeps monthly identifiers stable while locale changes", async () => {
    const snapshot = {
      ...zeroTradeSnapshot,
      monthlyReturns: [{ month: "2026-07", returnPct: "1" }]
    };
    const view = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={snapshot}
      />
    );

    expect(screen.getByText("2026年7月")).toBeTruthy();
    await i18n.changeLanguage("en");
    view.rerender(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={snapshot}
      />
    );
    expect(screen.getByText("Jul 2026")).toBeTruthy();
    expect(snapshot.monthlyReturns.map(({ month }) => month)).toEqual([
      "2026-07"
    ]);
  });

  it("renders a Berlin midnight rollover in the results period", () => {
    const view = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          startedAt: "2026-01-15T23:30:00Z",
          endedAt: "2026-01-15T23:59:59Z"
        }}
      />
    );

    expect(
      view.container.querySelector(
        ".results-intro dl > div:last-child dd"
      )?.textContent
    ).toBe("2026/01/16 – 2026/01/16");
  });

  it("fails visibly instead of normalizing malformed snapshot values", () => {
    const view = render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          currency: "INVALID",
          startedAt: "not-a-timestamp",
          endedAt: "not-a-timestamp",
          metrics: { ...zeroTradeSnapshot.metrics, netPnl: "not-a-decimal" },
          equity: [
            {
              timestamp: "not-a-timestamp",
              equity: "100000.00",
              drawdown: "0.00"
            }
          ],
          monthlyReturns: [
            { month: "2026-13", returnPct: "not-a-decimal" }
          ],
          tradePage: {
            items: [
              {
                ...demoTrades[0],
                entryTime: "not-a-timestamp",
                exitTime: "not-a-timestamp"
              }
            ],
            totalCount: 1,
            nextCursor: null
          }
        }}
      />
    );

    expect(
      view.container.querySelector(
        ".results-intro dl > div:last-child dd"
      )?.textContent
    ).toBe("— – —");
    expect(
      view.container.querySelector(".monthly-grid > div")?.className
    ).toBe("");
    expect(
      view.container.querySelector(".monthly-grid strong")?.textContent
    ).toBe("—");
    expect(
      view.container.querySelector(
        ".results-trades tbody td:nth-child(3)"
      )?.textContent
    ).toBe("—");
    expect(
      screen.getByText("權益或回撤樣本格式無效，已停止繪製圖表。")
    ).toBeTruthy();
    expect(screen.queryByLabelText("回測權益曲線與同步回撤圖")).toBeNull();
    expect(screen.queryByText(/2027/)).toBeNull();
  });

  it("fails the whole equity chart closed when one sample is malformed", () => {
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          equity: [
            {
              timestamp: "2026-01-01T00:00:00Z",
              equity: "100000.00",
              drawdown: "0.00"
            },
            {
              timestamp: "not-a-timestamp",
              equity: "100001.00",
              drawdown: "0.00"
            }
          ]
        }}
      />
    );

    expect(
      screen.getByText("權益或回撤樣本格式無效，已停止繪製圖表。")
    ).toBeTruthy();
    expect(screen.queryByLabelText("回測權益曲線與同步回撤圖")).toBeNull();
  });

  it.each([
    [
      "duplicate",
      "2026-01-01T00:00:00Z",
      "2026-01-01T00:00:00Z"
    ],
    [
      "descending",
      "2026-01-01T00:05:00Z",
      "2026-01-01T00:00:00Z"
    ]
  ])("fails closed for %s equity timestamps", (_name, first, second) => {
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          equity: [
            { timestamp: first, equity: "100000.00", drawdown: "0.00" },
            { timestamp: second, equity: "100001.00", drawdown: "0.00" }
          ]
        }}
      />
    );

    expect(screen.getByText("權益或回撤樣本格式無效，已停止繪製圖表。")).toBeTruthy();
    expect(screen.queryByLabelText("回測權益曲線與同步回撤圖")).toBeNull();
  });

  it.each([-1, 1.5, Number.NaN])(
    "fails the distribution closed for invalid count %s",
    (count) => {
      render(
        <BacktestResultsView
          demoMode={false}
          theme="light"
          snapshot={{
            ...zeroTradeSnapshot,
            pnlDistribution: [{ lower: null, upper: "0", count }]
          }}
        />
      );

      expect(
        screen.getByText("交易分布資料格式無效，已停止繪製分布。")
      ).toBeTruthy();
      expect(screen.queryByRole("list")).toBeNull();
    }
  );

  it("fails closed when a partial trade page has no next cursor", () => {
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          tradePage: {
            items: [demoTrades[0]],
            totalCount: 2,
            nextCursor: null
          }
        }}
      />
    );

    expect(screen.getByText("交易頁資料不完整，已停止顯示清冊。")).toBeTruthy();
    expect(screen.queryByRole("table")).toBeNull();
  });

  it("makes an unwired next trade page explicit", () => {
    render(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={{
          ...zeroTradeSnapshot,
          tradePage: {
            items: [demoTrades[0]],
            totalCount: 2,
            nextCursor: "cursor-1"
          }
        }}
      />
    );

    expect(screen.getByRole("alert").textContent).toContain(
      "尚未連接後續交易頁面"
    );
  });

  it("fails closed when production results are unavailable", () => {
    render(<BacktestResultsView demoMode={false} theme="dark" />);

    expect(screen.getByText("回測結果清單暫時無法使用")).toBeTruthy();
    expect(screen.queryByLabelText("回測權益曲線與同步回撤圖")).toBeNull();
  });

  it("keeps loading, error, and zero-trade states explicit", () => {
    resultsRead.useBacktestResults.mockReturnValue({
      state: { status: "pending", selection: null },
      loadMoreIndex: vi.fn(),
      loadMoreTrades: vi.fn()
    });
    const view = render(
      <BacktestResultsView demoMode={false} theme="light" loading />
    );
    expect(screen.getByText("載入回測績效")).toBeTruthy();
    expect(resultsRead.useBacktestResults).toHaveBeenCalledWith({
      selectedResultId: null,
      enabled: false
    });
    expect(resultsRead.useBacktestResults).toHaveBeenCalledWith({
      selectedResultId: null,
      enabled: false
    });

    view.rerender(
      <BacktestResultsView demoMode={false} theme="light" loadError />
    );
    expect(screen.getByText("回測結果未載入")).toBeTruthy();
    expect(screen.queryByLabelText("回測權益曲線與同步回撤圖")).toBeNull();

    view.rerender(
      <BacktestResultsView
        demoMode={false}
        theme="light"
        snapshot={zeroTradeSnapshot}
      />
    );
    expect(
      screen.getByText("這次結果沒有可顯示的權益與回撤樣本。")
    ).toBeTruthy();
    expect(screen.getByText("這次結果沒有月報酬資料。")).toBeTruthy();
    expect(screen.getByText("這次結果沒有交易分布資料。")).toBeTruthy();
    expect(screen.getByText("這次回測沒有已平倉交易。")).toBeTruthy();
  });

  it("switches all result copy to English", async () => {
    await i18n.changeLanguage("en");
    render(<BacktestResultsView demoMode theme="dark" />);

    expect(
      screen.getByRole("heading", { name: "Backtest performance" })
    ).toBeTruthy();
    expect(screen.getByText("Monthly returns")).toBeTruthy();
    expect(screen.getAllByRole("button", { name: "Inspect candles" })).toHaveLength(
      3
    );
  });
});
