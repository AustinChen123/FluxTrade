export function isDecimalString(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value.trim())
  );
}

export function finiteDecimalNumber(value: unknown): number | null {
  if (!isDecimalString(value)) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
