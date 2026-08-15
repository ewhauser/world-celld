/**
 * RPC wire codec for the Node client <-> worker router HTTP boundary.
 *
 * Extends the shared tagged-JSON format (`{__type:'Uint8Array', data}` +
 * DATE_FIELDS revival) with explicitly tagged Dates: on Cloudflare the DO RPC
 * boundary is structured clone, which preserves EVERY Date — including ones
 * outside the well-known DATE_FIELDS (e.g. inside applyEvent outcomes and
 * user payloads). The HTTP hop must preserve them too, so Dates travel as
 * `{__type:'Date', iso}`.
 *
 * Runs on both sides of the wire, including inside the celld worker — so no
 * Node built-ins (Buffer): base64 goes through atob/btoa.
 */
import { b64decode, b64encode, dateReviver } from './vendor/shared/serialization.js';

function rpcReplacer(this: unknown, key: string, value: unknown): unknown {
  // JSON.stringify invokes Date.toJSON before the replacer sees the value;
  // recover the original from the holder object.
  const original =
    this && typeof this === 'object' ? (this as Record<string, unknown>)[key] : undefined;
  if (original instanceof Date) {
    return { __type: 'Date', iso: original.toISOString() };
  }
  if (value instanceof Uint8Array) {
    return { __type: 'Uint8Array', data: b64encode(value) };
  }
  return value;
}

function rpcReviver(key: string, value: unknown): unknown {
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    if (obj.__type === 'Date' && typeof obj.iso === 'string') {
      return new Date(obj.iso);
    }
    if (obj.__type === 'Uint8Array' && typeof obj.data === 'string') {
      return b64decode(obj.data);
    }
  }
  // Fallback shared-codec semantics for untagged ISO strings in DATE_FIELDS.
  return dateReviver(key, value);
}

export function rpcStringify(value: unknown): string {
  return JSON.stringify(value, rpcReplacer);
}

export function rpcParse<T>(text: string): T {
  return JSON.parse(text, rpcReviver) as T;
}
