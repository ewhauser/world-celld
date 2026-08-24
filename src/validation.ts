/** Largest epoch-ms deadline that fits the queue's fixed-width 13-digit keys. */
export const MAX_QUEUE_TIMESTAMP_MS = 9_999_999_999_999;
export const MAX_QUEUE_DELAY_SECONDS = Math.floor(MAX_QUEUE_TIMESTAMP_MS / 1000);

export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export function isNonNegativeSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

export function isPositiveSafeInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) > 0;
}

export function boundedIntegerOption(
  name: string,
  value: unknown,
  fallback: number,
  minimum: number,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  if (value === undefined) return fallback;
  if (!Number.isSafeInteger(value) || (value as number) < minimum || (value as number) > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value as number;
}

/**
 * Parse a deployment/runtime integer setting without accepting whitespace,
 * exponents, fractions, or numeric prefixes such as `5junk`.
 */
export function strictIntegerSetting(
  name: string,
  raw: string | number | undefined,
  fallback: number,
  minimum: number,
  maximum = Number.MAX_SAFE_INTEGER,
): number {
  if (raw === undefined) return fallback;
  const value = typeof raw === 'number' ? raw : /^\d+$/.test(raw) ? Number(raw) : Number.NaN;
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

export function isValidQueueDelaySeconds(value: unknown, minimum: 0 | 1 = 0): value is number {
  return (
    Number.isSafeInteger(value) &&
    (value as number) >= minimum &&
    (value as number) <= MAX_QUEUE_DELAY_SECONDS
  );
}

export function queueDelayDeadline(
  now: number,
  delaySeconds: unknown,
  minimum: 0 | 1 = 0,
  requiredHeadroomMs = 0,
): number | null {
  if (!isNonNegativeSafeInteger(now) || !isValidQueueDelaySeconds(delaySeconds, minimum)) {
    return null;
  }
  return checkedQueueTimestampAdd(now, delaySeconds * 1000, requiredHeadroomMs);
}

/**
 * Add an offset to a queue timestamp while preserving its 13-digit key order.
 * Optional headroom reserves space for a later persisted derivative deadline.
 */
export function checkedQueueTimestampAdd(
  timestampMs: unknown,
  offsetMs: unknown,
  requiredHeadroomMs = 0,
): number | null {
  if (
    !isNonNegativeSafeInteger(timestampMs) ||
    !isNonNegativeSafeInteger(offsetMs) ||
    !isNonNegativeSafeInteger(requiredHeadroomMs)
  ) {
    return null;
  }
  const timestamp = timestampMs;
  const offset = offsetMs;
  if (timestamp > MAX_QUEUE_TIMESTAMP_MS || offset > MAX_QUEUE_TIMESTAMP_MS - timestamp) {
    return null;
  }
  const result = timestamp + offset;
  return requiredHeadroomMs <= MAX_QUEUE_TIMESTAMP_MS - result ? result : null;
}
