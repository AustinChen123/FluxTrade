// @vitest-environment jsdom

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import i18n from "../../shared/i18n";
import type {
  StrategyManagerError,
  StrategyRecord
} from "./strategyCommandState";
import { StrategyManagerView } from "./StrategyManagerView";

const active: StrategyRecord = {
  strategy_id: "active-strategy",
  status: "ACTIVE",
  config: {},
  performance: {},
  last_heartbeat: null,
  uptime_start: null,
  last_error_message: null,
  entered_error_at: null,
  recovered_at: null,
  stopped_at: null,
  version: 3,
  available_commands: ["STOP"]
};

afterEach(cleanup);

describe("StrategyManagerView", () => {
  it("projects authoritative rows and delegates refresh and command actions", async () => {
    await i18n.changeLanguage("zh-TW");
    const refresh = vi.fn().mockResolvedValue(undefined);
    const submit = vi.fn().mockResolvedValue(undefined);
    render(
      <StrategyManagerView
        strategies={[active]}
        loading={false}
        error={null}
        notice=""
        pendingStrategyId={null}
        awaitingStrategies={new Map()}
        locale="zh-TW"
        t={i18n.t}
        refresh={refresh}
        submit={submit}
      />
    );

    expect(screen.getByText("active-strategy")).toBeTruthy();
    expect(screen.getAllByText("—")).toHaveLength(2);
    fireEvent.click(screen.getByRole("button", { name: "重新整理狀態" }));
    fireEvent.click(screen.getByRole("button", { name: "停止" }));
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(submit).toHaveBeenCalledWith(active, "STOP");
  });

  it("renders awaiting and global pending locks without performing browser I/O", async () => {
    await i18n.changeLanguage("zh-TW");
    render(
      <StrategyManagerView
        strategies={[active]}
        loading={false}
        error={null}
        notice=""
        pendingStrategyId="other-strategy"
        awaitingStrategies={new Map([
          ["active-strategy", { status: "ACTIVE", version: 3 }]
        ])}
        locale="zh-TW"
        t={i18n.t}
        refresh={vi.fn().mockResolvedValue(undefined)}
        submit={vi.fn().mockResolvedValue(undefined)}
      />
    );

    expect(
      (screen.getByRole("button", {
        name: "等待狀態更新"
      }) as HTMLButtonElement).disabled
    ).toBe(true);
    expect(
      (screen.getByRole("button", {
        name: "重新整理狀態"
      }) as HTMLButtonElement).disabled
    ).toBe(true);
  });

  it.each([
    [
      { kind: "unknown", detail: { type: "unknown" } },
      "策略命令結果不明",
      "控制面可能已接受命令；在權威狀態更新前不會重新開放操作。"
    ],
    [
      { kind: "command", detail: { type: "step_up" } },
      "策略命令未送出",
      "這項命令需要短效升權。請取得 step-up 權限後重新建立工作階段。"
    ],
    [
      { kind: "load", detail: { type: "unauthorized" } },
      "策略狀態未載入",
      "目前工作階段沒有管理策略的權限。請從受信任的 Tailscale 入口重新開啟。"
    ],
    [
      { kind: "refresh", detail: { type: "service", status: 503 } },
      "命令已接受，但狀態尚未更新",
      "策略控制服務回覆 503。請確認引擎 listener 與控制面狀態。"
    ],
    [
      { kind: "load", detail: { type: "generic" } },
      "策略狀態未載入",
      "控制面目前無法完成要求。確認工作階段與服務狀態後再試一次。"
    ]
  ] as const)("renders classified error %s", async (error, title, body) => {
    await i18n.changeLanguage("zh-TW");
    render(
      <StrategyManagerView
        strategies={[]}
        loading={false}
        error={error as StrategyManagerError}
        notice=""
        pendingStrategyId={null}
        awaitingStrategies={new Map()}
        locale="zh-TW"
        t={i18n.t}
        refresh={vi.fn().mockResolvedValue(undefined)}
        submit={vi.fn().mockResolvedValue(undefined)}
      />
    );

    expect(screen.getByText(title)).toBeTruthy();
    expect(screen.getByText(body)).toBeTruthy();
  });
});
