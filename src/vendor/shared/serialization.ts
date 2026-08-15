/** Standard Date fields used across all worlds */
export const DATE_FIELDS = new Set([
  'createdAt',
  'updatedAt',
  'startedAt',
  'completedAt',
  'retryAfter',
] as const);

/**
 * JSON reviver that converts ISO date strings to Date objects.
 * Use with JSON.parse(json, dateReviver).
 */
export function dateReviver(key: string, value: unknown): unknown {
  if (
    DATE_FIELDS.has(key as typeof DATE_FIELDS extends Set<infer T> ? T : never) &&
    typeof value === 'string'
  ) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date;
  }
  return value;
}

/**
 * Base64 helpers. Modified vs upstream: implemented over atob/btoa instead of
 * Buffer so this module is safe inside the celld worker bundle (workerd has
 * no Buffer global); atob/btoa are global in both workerd and Node >= 16.
 */
export function b64encode(bytes: Uint8Array): string {
  let bin = '';
  const CHUNK = 0x8000;
  for (let i = 0; i < bytes.length; i += CHUNK) {
    bin += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(bin);
}

export function b64decode(text: string): Uint8Array {
  const bin = atob(text);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) {
    out[i] = bin.charCodeAt(i);
  }
  return out;
}

/**
 * JSON replacer that encodes Uint8Array as a tagged object with base64 data.
 */
export function uint8ArrayReplacer(_key: string, value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return {
      __type: 'Uint8Array',
      data: b64encode(value),
    };
  }
  return value;
}

/**
 * JSON reviver that decodes tagged Uint8Array objects and ISO date strings.
 * Supports both:
 * - New format: { __type: 'Uint8Array', data: '<base64>' }
 * - Legacy format: { __uint8array: true, data: [1, 2, 3] }
 */
export function uint8ArrayReviver(key: string, value: unknown): unknown {
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    // New format: base64-encoded
    if (obj.__type === 'Uint8Array' && typeof obj.data === 'string') {
      return b64decode(obj.data);
    }
    // Legacy format: number array (backwards compat with NATS JetStream data)
    if (obj.__uint8array === true && Array.isArray(obj.data)) {
      return new Uint8Array(obj.data as number[]);
    }
  }
  return dateReviver(key, value);
}

/** Stringify with Uint8Array support. */
export function stringify(value: unknown): string {
  return JSON.stringify(value, uint8ArrayReplacer);
}

/** Parse with Uint8Array and Date support. */
export function parse<T>(text: string): T {
  return JSON.parse(text, uint8ArrayReviver) as T;
}

/** Deep clone using structuredClone */
export function deepClone<T>(value: T): T {
  return structuredClone(value);
}
