import { parseUtcTimestamp } from "./utc";

export const PRESENTATION_TIME_ZONE = "Europe/Berlin";

export function parsePresentationTimestamp(value: unknown): number | null {
  if (typeof value === "string") {
    return parseUtcTimestamp(value);
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return null;
  }
  const clipped = new Date(value).valueOf();
  if (!Number.isFinite(clipped)) {
    return null;
  }
  return Object.is(clipped, -0) ? 0 : clipped;
}

export function formatPresentationTimestamp(
  value: unknown,
  formatter: Intl.DateTimeFormat
): string {
  const timestamp = parsePresentationTimestamp(value);
  return timestamp === null ? "—" : formatter.format(timestamp);
}
