import { useEffect } from "react";

const RECONNECT_SECONDS = [1, 2, 4, 8, 16, 30] as const;
const INVALIDATION_RESOURCES = new Set(["ga_job", "evolution_epoch"]);

type Invalidation = Readonly<{
  schema_version: 1;
  resource: "ga_job" | "evolution_epoch";
  identity: string;
  revision: number;
}>;

function parseFrame(frame: string): Invalidation | null | undefined {
  if (frame === ": heartbeat") return null;
  const lines = frame.split("\n");
  if (
    lines.length !== 2 ||
    lines[0] !== "event: invalidate" ||
    !lines[1]?.startsWith("data: ")
  ) return undefined;

  try {
    const value: unknown = JSON.parse(lines[1].slice(6));
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      return undefined;
    }
    const record = value as Record<string, unknown>;
    if (
      Object.keys(record).sort().join(",") !==
        "identity,resource,revision,schema_version" ||
      record.schema_version !== 1 ||
      typeof record.resource !== "string" ||
      !INVALIDATION_RESOURCES.has(record.resource) ||
      typeof record.identity !== "string" ||
      record.identity.length === 0 ||
      record.identity.length > 128 ||
      !/^[^\u0000-\u001f\u007f-\u009f]*$/u.test(record.identity) ||
      !Number.isSafeInteger(record.revision) ||
      (record.revision as number) <= 0
    ) return undefined;
    return record as Invalidation;
  } catch {
    return undefined;
  }
}

export function startResearchInvalidationStream(
  onInvalidate: () => void
): () => void {
  let active = true;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let currentAbort: AbortController | null = null;
  let currentReader: ReadableStreamDefaultReader<Uint8Array> | null = null;
  let reconnectIndex = 0;

  const scheduleReconnect = () => {
    if (!active || reconnectTimer !== null) return;
    const seconds = RECONNECT_SECONDS[
      Math.min(reconnectIndex, RECONNECT_SECONDS.length - 1)
    ];
    reconnectIndex = Math.min(reconnectIndex + 1, RECONNECT_SECONDS.length - 1);
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      void connect();
    }, seconds * 1000);
  };

  const connect = async () => {
    if (!active) return;
    const controller = new AbortController();
    currentAbort = controller;
    try {
      const response = await fetch("/api/v1/events", {
        method: "GET",
        credentials: "include",
        cache: "no-store",
        signal: controller.signal
      });
      if (
        !response.ok ||
        response.headers.get("Content-Type")?.toLowerCase().split(";", 1)[0]?.trim() !== "text/event-stream" ||
        response.body === null
      ) {
        await response.body?.cancel();
        throw new Error("invalid event stream response");
      }
      if (!active) {
        await response.body.cancel();
        return;
      }
      onInvalidate();
      if (!active || controller.signal.aborted) {
        await response.body.cancel();
        return;
      }
      const reader = response.body.getReader();
      currentReader = reader;
      const decoder = new TextDecoder("utf-8", { fatal: true });
      let buffer = "";
      let reconnect = false;
      while (active && !controller.signal.aborted) {
        const { value, done } = await reader.read();
        if (!active || controller.signal.aborted) break;
        if (done) {
          buffer += decoder.decode();
          if (buffer.length > 0) reconnect = true;
          break;
        }
        buffer += decoder.decode(value, { stream: true });
        buffer = buffer.replaceAll("\r\n", "\n");
        let boundary = buffer.indexOf("\n\n");
        while (boundary >= 0) {
          if (!active || controller.signal.aborted) break;
          const frame = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const parsed = parseFrame(frame);
          if (parsed === undefined) {
            reconnect = true;
            break;
          }
          if (parsed !== null) {
            reconnectIndex = 0;
            onInvalidate();
          }
          if (!active || controller.signal.aborted) break;
          boundary = buffer.indexOf("\n\n");
        }
        if (reconnect) break;
      }
      if (reconnect || active) {
        await reader.cancel().catch(() => undefined);
        scheduleReconnect();
      }
    } catch {
      if (currentAbort === controller && currentReader !== null) {
        await currentReader.cancel().catch(() => undefined);
        currentReader = null;
      }
      if (!controller.signal.aborted) scheduleReconnect();
    } finally {
      if (currentReader !== null && currentAbort === controller) {
        currentReader = null;
      }
      if (currentAbort === controller) currentAbort = null;
    }
  };

  void connect();
  return () => {
    active = false;
    currentAbort?.abort();
    void currentReader?.cancel().catch(() => undefined);
    currentReader = null;
    currentAbort = null;
    if (reconnectTimer !== null) clearTimeout(reconnectTimer);
    reconnectTimer = null;
  };
}

export function ResearchInvalidationBridge({
  enabled,
  onInvalidate
}: {
  readonly enabled: boolean;
  readonly onInvalidate: () => void;
}) {
  useEffect(() => {
    if (!enabled) return;
    return startResearchInvalidationStream(onInvalidate);
  }, [enabled, onInvalidate]);
  return null;
}
