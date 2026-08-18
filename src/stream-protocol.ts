/**
 * Hard-cutover stream protocol shared by the client, Worker router, and
 * StreamDO. Chunk payloads use a compact binary envelope on the public HTTP
 * boundary and Uint8Array values on the DO/storage boundary.
 */

export const STREAM_BATCH_CONTENT_TYPE = 'application/vnd.world-celld.stream-batch.v1';

/** One storage.put(entries) call remains below the 128-key platform limit. */
export const MAX_STREAM_WRITE_CHUNKS = 32;
/** A chunk remains below the 2 MiB SQLite-backed DO key/value limit. */
export const MAX_STREAM_CHUNK_BYTES = 1024 * 1024;
/** Keeps serialized RPC bodies well below the 32 MiB Workers RPC limit. */
export const MAX_STREAM_BATCH_BYTES = 8 * 1024 * 1024;
export const MAX_STREAM_READ_CHUNKS = 32;
export const MAX_STREAM_READ_BYTES = 8 * 1024 * 1024;
export const MAX_STREAM_ERROR_BYTES = 16 * 1024;
/** Must remain below the default 30 second fleet request deadline. */
export const MAX_STREAM_LONG_POLL_MS = 20_000;

export type StreamTerminalState = 'open' | 'closed' | 'errored' | 'expired';

export interface StreamErrorData {
  name: string;
  message: string;
}

export interface StreamWriteResult {
  startIndex: number;
  count: number;
  tailIndex: number;
}

export interface StreamReadRequest {
  runId: string;
  startIndex: number;
  maxChunks: number;
  maxBytes: number;
  waitMs: number;
}

export interface StreamReadResult {
  startIndex: number;
  tailIndex: number;
  chunks: Uint8Array[];
  state: StreamTerminalState;
  timedOut: boolean;
  error?: StreamErrorData;
}

const WRITE_MAGIC = 0x57435357; // WCSW
const WRITE_ACK_MAGIC = 0x57435341; // WCSA
const READ_MAGIC = 0x57435352; // WCSR
const PROTOCOL_VERSION = 1;
const WRITE_HEADER_BYTES = 8;
const WRITE_ACK_BYTES = 16;
const READ_HEADER_BYTES = 28;

const STATE_TO_WIRE: Record<StreamTerminalState, number> = {
  open: 0,
  closed: 1,
  errored: 2,
  expired: 3,
};

const WIRE_TO_STATE: readonly StreamTerminalState[] = ['open', 'closed', 'errored', 'expired'];

function protocolError(message: string): Error {
  const error = new Error(`world-celld: invalid stream protocol: ${message}`);
  error.name = 'StreamProtocolError';
  return error;
}

function requireSafeIndex(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value < -1 || value > 0x7fffffff) {
    throw protocolError(`${field} is out of range`);
  }
}

export function validateStreamWriteChunks(chunks: readonly Uint8Array[]): number {
  if (chunks.length === 0) throw protocolError('batch must contain at least one chunk');
  if (chunks.length > MAX_STREAM_WRITE_CHUNKS) {
    throw protocolError(`batch exceeds ${MAX_STREAM_WRITE_CHUNKS} chunks`);
  }

  let totalBytes = 0;
  for (const chunk of chunks) {
    if (!(chunk instanceof Uint8Array)) throw protocolError('chunk is not binary data');
    if (chunk.byteLength > MAX_STREAM_CHUNK_BYTES) {
      throw protocolError(`chunk exceeds ${MAX_STREAM_CHUNK_BYTES} bytes`);
    }
    totalBytes += chunk.byteLength;
    if (totalBytes > MAX_STREAM_BATCH_BYTES) {
      throw protocolError(`batch exceeds ${MAX_STREAM_BATCH_BYTES} bytes`);
    }
  }
  return totalBytes;
}

export function validateStreamReadRequest(request: StreamReadRequest): void {
  if (!request.runId) throw protocolError('runId is required');
  if (!Number.isSafeInteger(request.startIndex) || request.startIndex < 0) {
    throw protocolError('startIndex must be a non-negative safe integer');
  }
  if (
    !Number.isSafeInteger(request.maxChunks) ||
    request.maxChunks < 0 ||
    request.maxChunks > MAX_STREAM_READ_CHUNKS
  ) {
    throw protocolError(`maxChunks must be between 0 and ${MAX_STREAM_READ_CHUNKS}`);
  }
  if (
    !Number.isSafeInteger(request.maxBytes) ||
    request.maxBytes < 1 ||
    request.maxBytes > MAX_STREAM_READ_BYTES
  ) {
    throw protocolError(`maxBytes must be between 1 and ${MAX_STREAM_READ_BYTES}`);
  }
  if (
    !Number.isSafeInteger(request.waitMs) ||
    request.waitMs < 0 ||
    request.waitMs > MAX_STREAM_LONG_POLL_MS
  ) {
    throw protocolError(`waitMs must be between 0 and ${MAX_STREAM_LONG_POLL_MS}`);
  }
}

export function encodeStreamWriteBatch(chunks: readonly Uint8Array[]): Uint8Array {
  const totalBytes = validateStreamWriteChunks(chunks);
  const encoded = new Uint8Array(WRITE_HEADER_BYTES + chunks.length * 4 + totalBytes);
  const view = new DataView(encoded.buffer);
  view.setUint32(0, WRITE_MAGIC);
  view.setUint16(4, PROTOCOL_VERSION);
  view.setUint16(6, chunks.length);

  let offset = WRITE_HEADER_BYTES;
  for (const chunk of chunks) {
    view.setUint32(offset, chunk.byteLength);
    offset += 4;
    encoded.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return encoded;
}

export function decodeStreamWriteBatch(encoded: Uint8Array): Uint8Array[] {
  if (encoded.byteLength < WRITE_HEADER_BYTES) throw protocolError('truncated write header');
  const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  if (view.getUint32(0) !== WRITE_MAGIC) throw protocolError('bad write magic');
  if (view.getUint16(4) !== PROTOCOL_VERSION) throw protocolError('unsupported write version');
  const count = view.getUint16(6);
  const chunks: Uint8Array[] = [];
  let offset = WRITE_HEADER_BYTES;
  for (let index = 0; index < count; index++) {
    if (offset + 4 > encoded.byteLength) throw protocolError('truncated chunk length');
    const length = view.getUint32(offset);
    offset += 4;
    if (offset + length > encoded.byteLength) throw protocolError('truncated chunk data');
    chunks.push(encoded.subarray(offset, offset + length));
    offset += length;
  }
  if (offset !== encoded.byteLength) throw protocolError('trailing write data');
  validateStreamWriteChunks(chunks);
  return chunks;
}

export function encodeStreamWriteResult(result: StreamWriteResult): Uint8Array {
  requireSafeIndex(result.startIndex, 'startIndex');
  requireSafeIndex(result.tailIndex, 'tailIndex');
  if (
    !Number.isSafeInteger(result.count) ||
    result.count < 1 ||
    result.count > MAX_STREAM_WRITE_CHUNKS
  ) {
    throw protocolError('write count is out of range');
  }
  const encoded = new Uint8Array(WRITE_ACK_BYTES);
  const view = new DataView(encoded.buffer);
  view.setUint32(0, WRITE_ACK_MAGIC);
  view.setUint16(4, PROTOCOL_VERSION);
  view.setUint16(6, result.count);
  view.setInt32(8, result.startIndex);
  view.setInt32(12, result.tailIndex);
  return encoded;
}

export function decodeStreamWriteResult(encoded: Uint8Array): StreamWriteResult {
  if (encoded.byteLength !== WRITE_ACK_BYTES) throw protocolError('invalid write response size');
  const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  if (view.getUint32(0) !== WRITE_ACK_MAGIC) throw protocolError('bad write response magic');
  if (view.getUint16(4) !== PROTOCOL_VERSION) {
    throw protocolError('unsupported write response version');
  }
  const count = view.getUint16(6);
  if (count < 1 || count > MAX_STREAM_WRITE_CHUNKS) {
    throw protocolError('write response count is out of range');
  }
  const result = {
    count,
    startIndex: view.getInt32(8),
    tailIndex: view.getInt32(12),
  };
  requireSafeIndex(result.startIndex, 'startIndex');
  requireSafeIndex(result.tailIndex, 'tailIndex');
  if (result.tailIndex !== result.startIndex + result.count - 1) {
    throw protocolError('write response offsets are inconsistent');
  }
  return result;
}

export function encodeStreamReadResult(result: StreamReadResult): Uint8Array {
  requireSafeIndex(result.startIndex, 'startIndex');
  requireSafeIndex(result.tailIndex, 'tailIndex');
  let chunkBytes = 0;
  for (const chunk of result.chunks) chunkBytes += 4 + chunk.byteLength;
  if (result.chunks.length > MAX_STREAM_READ_CHUNKS || chunkBytes > MAX_STREAM_READ_BYTES + 128) {
    throw protocolError('read result exceeds configured bounds');
  }

  const encoder = new TextEncoder();
  const errorName = result.error ? encoder.encode(result.error.name) : new Uint8Array();
  const errorMessage = result.error ? encoder.encode(result.error.message) : new Uint8Array();
  if (errorName.byteLength + errorMessage.byteLength > MAX_STREAM_ERROR_BYTES) {
    throw protocolError(`stream error exceeds ${MAX_STREAM_ERROR_BYTES} bytes`);
  }
  const encoded = new Uint8Array(
    READ_HEADER_BYTES + chunkBytes + errorName.byteLength + errorMessage.byteLength,
  );
  const view = new DataView(encoded.buffer);
  view.setUint32(0, READ_MAGIC);
  view.setUint8(4, PROTOCOL_VERSION);
  const state = STATE_TO_WIRE[result.state];
  if (state === undefined) throw protocolError('unknown stream state');
  view.setUint8(5, state);
  view.setUint8(6, result.timedOut ? 1 : 0);
  view.setUint8(7, 0);
  view.setInt32(8, result.startIndex);
  view.setInt32(12, result.tailIndex);
  view.setUint32(16, result.chunks.length);
  view.setUint32(20, errorName.byteLength);
  view.setUint32(24, errorMessage.byteLength);

  let offset = READ_HEADER_BYTES;
  for (const chunk of result.chunks) {
    view.setUint32(offset, chunk.byteLength);
    offset += 4;
    encoded.set(chunk, offset);
    offset += chunk.byteLength;
  }
  encoded.set(errorName, offset);
  offset += errorName.byteLength;
  encoded.set(errorMessage, offset);
  return encoded;
}

export function decodeStreamReadResult(encoded: Uint8Array): StreamReadResult {
  if (encoded.byteLength < READ_HEADER_BYTES) throw protocolError('truncated read header');
  const view = new DataView(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  if (view.getUint32(0) !== READ_MAGIC) throw protocolError('bad read magic');
  if (view.getUint8(4) !== PROTOCOL_VERSION) throw protocolError('unsupported read version');
  const state = WIRE_TO_STATE[view.getUint8(5)];
  if (!state) throw protocolError('unknown stream state');
  const startIndex = view.getInt32(8);
  const tailIndex = view.getInt32(12);
  requireSafeIndex(startIndex, 'startIndex');
  requireSafeIndex(tailIndex, 'tailIndex');
  const count = view.getUint32(16);
  const errorNameLength = view.getUint32(20);
  const errorMessageLength = view.getUint32(24);
  if (count > MAX_STREAM_READ_CHUNKS) throw protocolError('read chunk count exceeds limit');
  if (errorNameLength + errorMessageLength > MAX_STREAM_ERROR_BYTES) {
    throw protocolError('read error exceeds limit');
  }

  const chunks: Uint8Array[] = [];
  let offset = READ_HEADER_BYTES;
  let totalBytes = 0;
  for (let index = 0; index < count; index++) {
    if (offset + 4 > encoded.byteLength) throw protocolError('truncated read chunk length');
    const length = view.getUint32(offset);
    offset += 4;
    if (offset + length > encoded.byteLength) throw protocolError('truncated read chunk data');
    totalBytes += length;
    if (totalBytes > MAX_STREAM_READ_BYTES) throw protocolError('read bytes exceed limit');
    chunks.push(encoded.subarray(offset, offset + length));
    offset += length;
  }

  if (offset + errorNameLength + errorMessageLength !== encoded.byteLength) {
    throw protocolError('invalid read error payload');
  }
  const decoder = new TextDecoder();
  const error =
    errorNameLength > 0 || errorMessageLength > 0
      ? {
          name: decoder.decode(encoded.subarray(offset, offset + errorNameLength)),
          message: decoder.decode(encoded.subarray(offset + errorNameLength)),
        }
      : undefined;

  return {
    startIndex,
    tailIndex,
    chunks,
    state,
    timedOut: (view.getUint8(6) & 1) !== 0,
    error,
  };
}

export function normalizeStreamError(error: unknown): StreamErrorData {
  let normalized: StreamErrorData;
  if (error instanceof Error) {
    normalized = { name: error.name || 'Error', message: error.message };
  } else if (typeof error === 'string') {
    normalized = { name: 'Error', message: error };
  } else if (
    error &&
    typeof error === 'object' &&
    typeof (error as { name?: unknown }).name === 'string' &&
    typeof (error as { message?: unknown }).message === 'string'
  ) {
    normalized = {
      name: (error as StreamErrorData).name,
      message: (error as StreamErrorData).message,
    };
  } else {
    normalized = { name: 'Error', message: String(error) };
  }

  // Bound persisted terminal metadata as well as its binary response frame.
  normalized.name = normalized.name.slice(0, 1024);
  normalized.message = normalized.message.slice(0, 4096);
  while (
    new TextEncoder().encode(normalized.name).byteLength +
      new TextEncoder().encode(normalized.message).byteLength >
    MAX_STREAM_ERROR_BYTES
  ) {
    normalized.message = normalized.message.slice(0, Math.max(0, normalized.message.length - 256));
    if (normalized.message.length === 0) {
      normalized.name = normalized.name.slice(0, Math.max(0, normalized.name.length - 256));
    }
  }
  return normalized;
}
