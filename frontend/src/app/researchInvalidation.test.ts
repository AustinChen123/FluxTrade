// @vitest-environment jsdom

import { createElement, StrictMode } from "react";
import { render, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ResearchInvalidationBridge,
  startResearchInvalidationStream
} from "./researchInvalidation";

function response(
  chunks: (string | Uint8Array)[],
  contentType = "text/event-stream"
) {
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(
          typeof chunk === "string" ? new TextEncoder().encode(chunk) : chunk
        );
      }
      controller.close();
    }
  });
  return new Response(stream, {
    status: 200,
    headers: { "Content-Type": contentType }
  });
}

async function flush() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("Research invalidation transport", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it("refreshes on connection and only on complete valid invalidations", async () => {
    const frame = new TextEncoder().encode(
      "event: invalidate\ndata: {\"schema_version\":1,\"resource\":\"ga_job\",\"identity\":\"工作\",\"revision\":2}\n\n"
    );
    const splitAt = frame.indexOf(0xe5) + 1;
    const fetchMock = vi.fn().mockResolvedValue(response([
      ": heartbeat\n\n",
      frame.slice(0, splitAt),
      frame.slice(splitAt),
      "event: invalidate\ndata: {\"schema_version\":1,\"resource\":\"other\",\"identity\":\"x\",\"revision\":3}\n\n"
    ]));
    vi.stubGlobal("fetch", fetchMock);
    const invalidate = vi.fn();

    const stop = startResearchInvalidationStream(invalidate);
    await flush();

    expect(fetchMock).toHaveBeenCalledWith("/api/v1/events", {
      method: "GET",
      credentials: "include",
      cache: "no-store",
      signal: expect.any(AbortSignal)
    });
    expect(invalidate).toHaveBeenCalledTimes(2);
    stop();
  });

  it("backs off after an invalid frame and resets only after a valid frame", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(response(["event: invalidate\ndata: {}\n\n"]))
      .mockResolvedValueOnce(response([
        "event: invalidate\ndata: {\"schema_version\":1,\"resource\":\"evolution_epoch\",\"identity\":\"epoch-a\",\"revision\":1}\n\n"
      ]))
      .mockResolvedValueOnce(response([]));
    vi.stubGlobal("fetch", fetchMock);
    const invalidate = vi.fn();

    const stop = startResearchInvalidationStream(invalidate);
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(999);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1);
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(invalidate).toHaveBeenCalledTimes(3);
    await vi.advanceTimersByTimeAsync(999);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(1);
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(3);
    stop();
  });

  it("reconnects after open failures and rejects non-event-stream responses", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn()
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValueOnce(response([], "application/json"));
    vi.stubGlobal("fetch", fetchMock);
    const invalidate = vi.fn();

    const stop = startResearchInvalidationStream(invalidate);
    await flush();
    await vi.advanceTimersByTimeAsync(1000);
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(invalidate).toHaveBeenCalledTimes(0);
    stop();
  });

  it("caps reconnect delay at thirty seconds until a valid frame resets it", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn().mockRejectedValue(new Error("offline"));
    vi.stubGlobal("fetch", fetchMock);
    const stop = startResearchInvalidationStream(vi.fn());
    await flush();
    for (const [delay, expectedCalls] of [
      [1000, 2],
      [2000, 3],
      [4000, 4],
      [8000, 5],
      [16000, 6],
      [30000, 7],
      [30000, 8]
    ]) {
      await vi.advanceTimersByTimeAsync(delay - 1);
      expect(fetchMock).toHaveBeenCalledTimes(expectedCalls - 1);
      await vi.advanceTimersByTimeAsync(1);
      await flush();
      expect(fetchMock).toHaveBeenCalledTimes(expectedCalls);
    }
    stop();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not dispatch a queued read after the stream is stopped", async () => {
    let stop = () => {};
    let releaseRead!: () => void;
    const readGate = new Promise<void>((resolve) => { releaseRead = resolve; });
    const frame = new TextEncoder().encode(
      "event: invalidate\ndata: {\"schema_version\":1,\"resource\":\"ga_job\",\"identity\":\"queued\",\"revision\":1}\n\n"
    );
    const cancelled = vi.fn();
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        await readGate;
        controller.enqueue(frame);
        stop();
      },
      cancel: cancelled
    });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(body, {
      status: 200,
      headers: { "Content-Type": "text/event-stream" }
    })));
    const invalidate = vi.fn();

    stop = startResearchInvalidationStream(invalidate);
    await flush();
    expect(invalidate).toHaveBeenCalledTimes(1);
    releaseRead();
    await flush();

    expect(invalidate).toHaveBeenCalledTimes(1);
    expect(cancelled).toHaveBeenCalledTimes(1);
  });

  it("stops dispatching remaining frames when a callback stops the stream", async () => {
    let stop = () => {};
    const frame = "event: invalidate\ndata: {\"schema_version\":1,\"resource\":\"ga_job\",\"identity\":\"batch\",\"revision\":1}\n\n";
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response([frame + frame])));
    const invalidate = vi.fn(() => {
      if (invalidate.mock.calls.length === 2) stop();
    });

    stop = startResearchInvalidationStream(invalidate);
    await flush();

    expect(invalidate).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["invalid UTF-8", new Uint8Array([0xff])],
    ["fragmented invalid UTF-8", new Uint8Array([0xe2, 0x82])]
  ])("rejects %s before reconnecting", async (_name, malformedBytes) => {
    vi.useFakeTimers();
    const encoder = new TextEncoder();
    const prefix = encoder.encode(
      'event: invalidate\ndata: {"schema_version":1,"resource":"ga_job","identity":"'
    );
    const suffix = encoder.encode('","revision":1}\n\n');
    const pieces = [prefix, malformedBytes, suffix];
    let next = 0;
    const cancelled = vi.fn();
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (next < pieces.length) controller.enqueue(pieces[next++]!);
      },
      cancel: cancelled
    });
    const fetchMock = vi.fn().mockResolvedValue(new Response(body, {
      status: 200,
      headers: { "Content-Type": "text/event-stream" }
    }));
    vi.stubGlobal("fetch", fetchMock);
    const invalidate = vi.fn();

    const stop = startResearchInvalidationStream(invalidate);
    await flush();

    expect(invalidate).toHaveBeenCalledTimes(1);
    expect(cancelled).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    stop();
  });

  it("rejects a truncated UTF-8 sequence at stream completion", async () => {
    vi.useFakeTimers();
    const encoder = new TextEncoder();
    const chunks = [
      encoder.encode('event: invalidate\ndata: {"schema_version":1,"resource":"ga_job","identity":"'),
      new Uint8Array([0xe2, 0x82])
    ];
    let next = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (next < chunks.length) {
          controller.enqueue(chunks[next++]!);
        } else {
          controller.close();
        }
      }
    });
    const fetchMock = vi.fn().mockResolvedValue(new Response(body, {
      status: 200,
      headers: { "Content-Type": "text/event-stream" }
    }));
    vi.stubGlobal("fetch", fetchMock);
    const invalidate = vi.fn();

    const stop = startResearchInvalidationStream(invalidate);
    await flush();

    expect(invalidate).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    stop();
  });

  it("keeps the canonical server heartbeat open without reconnecting", async () => {
    vi.useFakeTimers();
    let emitted = false;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (!emitted) {
          emitted = true;
          controller.enqueue(new TextEncoder().encode(": heartbeat\n\n"));
        }
      }
    });
    const fetchMock = vi.fn().mockResolvedValue(new Response(body, {
      status: 200,
      headers: { "Content-Type": "text/event-stream" }
    }));
    vi.stubGlobal("fetch", fetchMock);

    const stop = startResearchInvalidationStream(vi.fn());
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    stop();
  });

  it("reconnects after a noncanonical heartbeat comment", async () => {
    vi.useFakeTimers();
    let emitted = false;
    const firstBody = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (!emitted) {
          emitted = true;
          controller.enqueue(new TextEncoder().encode(": other\n\n"));
        }
      }
    });
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(firstBody, {
        status: 200,
        headers: { "Content-Type": "text/event-stream" }
      }))
      .mockResolvedValueOnce(response([]));
    vi.stubGlobal("fetch", fetchMock);

    const stop = startResearchInvalidationStream(vi.fn());
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(1000);
    await flush();
    expect(fetchMock).toHaveBeenCalledTimes(2);
    stop();
  });

  it("closes the StrictMode probe stream and keeps one mounted stream", async () => {
    const signals: AbortSignal[] = [];
    const fetchMock = vi.fn((_url: string, options: RequestInit) => {
      const signal = options.signal as AbortSignal;
      signals.push(signal);
      return new Promise<Response>((_resolve, reject) => {
        signal.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
      });
    });
    vi.stubGlobal("fetch", fetchMock);
    const { unmount } = render(createElement(
      StrictMode,
      null,
      createElement(ResearchInvalidationBridge, {
        enabled: true,
        onInvalidate: vi.fn()
      })
    ));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(signals[0]?.aborted).toBe(true);
    expect(signals[1]?.aborted).toBe(false);
    unmount();
    expect(signals[1]?.aborted).toBe(true);
  });
});
