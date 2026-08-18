import { describe, expect, it } from 'vitest';
import {
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_READ_BYTES,
  decodeStreamWriteBatch,
  encodeStreamWriteBatch,
  validateStreamReadRequest,
} from '../src/stream-protocol.js';

describe('stream binary protocol', () => {
  it('decodes each write chunk into a compact storage-safe buffer', () => {
    const chunks = [Uint8Array.of(1, 2, 3), Uint8Array.of(4, 5, 6, 7)];
    const encoded = encodeStreamWriteBatch(chunks);
    const decoded = decodeStreamWriteBatch(encoded);

    expect(decoded).toEqual(chunks);
    for (const chunk of decoded) {
      expect(chunk.byteOffset).toBe(0);
      expect(chunk.buffer.byteLength).toBe(chunk.byteLength);
      expect(chunk.buffer).not.toBe(encoded.buffer);
      expect(structuredClone(chunk).buffer.byteLength).toBe(chunk.byteLength);
    }
  });

  it('requires every read budget to fit one maximum-size chunk', () => {
    const request = {
      runId: 'wrun_protocol',
      startIndex: 0,
      maxChunks: 1,
      maxBytes: MAX_STREAM_CHUNK_BYTES,
      waitMs: 0,
    };

    expect(() => validateStreamReadRequest(request)).not.toThrow();
    expect(() =>
      validateStreamReadRequest({ ...request, maxBytes: MAX_STREAM_CHUNK_BYTES - 1 }),
    ).toThrow(`maxBytes must be between ${MAX_STREAM_CHUNK_BYTES} and ${MAX_STREAM_READ_BYTES}`);
  });
});
