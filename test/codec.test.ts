import { describe, expect, it } from 'vitest';
import { rpcParse, rpcStringify } from '../src/codec.js';

describe('rpc codec', () => {
  it('round-trips Dates in arbitrary positions', () => {
    const value = {
      createdAt: new Date('2026-01-02T03:04:05.678Z'),
      nested: { someRandomField: new Date('1999-12-31T23:59:59.999Z') },
      list: [new Date(0)],
    };
    const out = rpcParse<typeof value>(rpcStringify(value));
    expect(out.createdAt).toBeInstanceOf(Date);
    expect(out.createdAt.toISOString()).toBe('2026-01-02T03:04:05.678Z');
    expect(out.nested.someRandomField).toBeInstanceOf(Date);
    expect(out.nested.someRandomField.toISOString()).toBe('1999-12-31T23:59:59.999Z');
    expect(out.list[0]).toBeInstanceOf(Date);
    expect(out.list[0].getTime()).toBe(0);
  });

  it('round-trips Uint8Array including binary-unsafe bytes', () => {
    const bytes = new Uint8Array([0, 1, 2, 253, 254, 255]);
    const out = rpcParse<{ data: Uint8Array }>(rpcStringify({ data: bytes }));
    expect(out.data).toBeInstanceOf(Uint8Array);
    expect(Array.from(out.data)).toEqual([0, 1, 2, 253, 254, 255]);
  });

  it('round-trips large Uint8Array (chunked base64 path)', () => {
    const bytes = new Uint8Array(200_000).map((_, i) => i % 256);
    const out = rpcParse<{ data: Uint8Array }>(rpcStringify({ data: bytes }));
    expect(out.data.length).toBe(200_000);
    expect(out.data[199_999]).toBe(199_999 % 256);
  });

  it('revives untagged ISO strings in well-known DATE_FIELDS (shared-codec compat)', () => {
    const out = rpcParse<{ createdAt: Date }>('{"createdAt":"2026-01-01T00:00:00.000Z"}');
    expect(out.createdAt).toBeInstanceOf(Date);
  });

  it('round-trips argument arrays with null and nested structures', () => {
    const args = ['wrun_1', { pagination: { limit: 5 } }, null, true, 42];
    expect(rpcParse(rpcStringify(args))).toEqual(args);
  });
});
