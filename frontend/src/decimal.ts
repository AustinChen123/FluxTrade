export function isDecimalString(value: string | null): value is string {
  return (
    value !== null &&
    /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value.trim())
  );
}

export function finiteDecimalNumber(value: string | null): number | null {
  if (!isDecimalString(value)) {
    return null;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}
