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
 * Base64 helpers. Modified vs upstream: implemented without Buffer so this
 * module is safe inside the celld worker bundle (workerd has no Buffer
 * global). Uses the native Uint8Array base64 methods where the runtime has
 * them (Node >= 25, current V8) and falls back to atob/btoa elsewhere.
 */
const nativeToBase64 = (
  Uint8Array.prototype as Uint8Array & { toBase64?: (this: Uint8Array) => string }
).toBase64;
const nativeFromBase64 = (Uint8Array as { fromBase64?: (text: string) => Uint8Array }).fromBase64;

/** Small enough to stay far below engine argument-count limits and fast to spread. */
const FALLBACK_ENCODE_CHUNK = 0x1000;

/** @internal Exported for tests; use b64encode. */
export function b64encodeFallback(bytes: Uint8Array): string {
  let bin = '';
  for (let i = 0; i < bytes.length; i += FALLBACK_ENCODE_CHUNK) {
    bin += String.fromCharCode.apply(
      null,
      bytes.subarray(i, i + FALLBACK_ENCODE_CHUNK) as unknown as number[],
    );
  }
  return btoa(bin);
}

/** @internal Exported for tests; use b64decode. */
export function b64decodeFallback(text: string): Uint8Array {
  const bin = atob(text);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) {
    out[i] = bin.charCodeAt(i);
  }
  return out;
}

export function b64encode(bytes: Uint8Array): string {
  return nativeToBase64 ? nativeToBase64.call(bytes) : b64encodeFallback(bytes);
}

export function b64decode(text: string): Uint8Array {
  return nativeFromBase64 ? nativeFromBase64(text) : b64decodeFallback(text);
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
    if (obj.__type === 'Uint8Array') {
      if (typeof obj.data !== 'string') throw new SyntaxError('Malformed Uint8Array tag');
      try {
        return b64decode(obj.data);
      } catch {
        throw new SyntaxError('Malformed Uint8Array tag');
      }
    }
    // Legacy format: number array (backwards compat with NATS JetStream data)
    if (obj.__uint8array === true) {
      if (
        !Array.isArray(obj.data) ||
        !obj.data.every((byte) => Number.isInteger(byte) && byte >= 0 && byte <= 255)
      ) {
        throw new SyntaxError('Malformed legacy Uint8Array tag');
      }
      return new Uint8Array(obj.data);
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
