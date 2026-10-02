import {
  MAX_STREAM_BATCH_BYTES,
  MAX_STREAM_READ_BYTES,
  MAX_STREAM_READ_CHUNKS,
  MAX_STREAM_WRITE_CHUNKS,
  NEGOTIATED_STREAM_CHUNKS,
  STREAM_BATCH_CONTENT_TYPE,
  STREAM_CHUNK_LIMIT_HEADER,
  isChunkLimitRejection,
  decodeStreamReadResult,
  decodeStreamWriteResult,
  encodeStreamWriteBatch,
  type StreamReadRequest,
  type StreamReadResult,
  type StreamWriteResult,
} from '../stream-protocol.js';
import { FleetTransportError, reconstructError, type WireError } from './errors.js';
import { resolveFleetTimeoutMs, type RpcTransport } from './rpc-client.js';

const RETRYABLE_STATUSES = new Set([502, 503, 504]);
const READ_ATTEMPTS = 3;
const MAX_WRITE_RESPONSE_BYTES = 64;
const MAX_READ_RESPONSE_BYTES = MAX_STREAM_READ_BYTES + 64 * 1024;
const MAX_ERROR_RESPONSE_BYTES = 64 * 1024;

/**
 * Per-request chunk limit the fleet has advertised, per transport. Absent
 * until a worker advertises one, so a client never sends an older worker more
 * than the baseline it accepts; a response without the header drops back.
 */
const advertisedChunkLimits = new WeakMap<RpcTransport, number>();

export function streamChunkLimit(transport: RpcTransport): number {
  return advertisedChunkLimits.get(transport) ?? MAX_STREAM_WRITE_CHUNKS;
}

export function resetStreamChunkLimit(transport: RpcTransport): void {
  advertisedChunkLimits.delete(transport);
}

function recordStreamChunkLimit(transport: RpcTransport, response: Response): void {
  const advertised = Number(response.headers.get(STREAM_CHUNK_LIMIT_HEADER));
  if (Number.isSafeInteger(advertised) && advertised > MAX_STREAM_WRITE_CHUNKS) {
    advertisedChunkLimits.set(transport, Math.min(advertised, NEGOTIATED_STREAM_CHUNKS));
  } else {
    advertisedChunkLimits.delete(transport);
  }
}

function delayMs(ms: number, signal?: AbortSignal): Promise<void> {
  if (!signal) return new Promise((resolve) => setTimeout(resolve, ms));
  if (signal.aborted) return Promise.reject(aborted(signal));
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener('abort', onAbort);
      reject(aborted(signal));
    };
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

function requestSignal(timeoutMs: number, signal?: AbortSignal): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

function aborted(signal: AbortSignal): unknown {
  return signal.reason ?? new DOMException('The operation was aborted', 'AbortError');
}

async function readBoundedResponse(response: Response, maxBytes: number): Promise<Uint8Array> {
  const contentLength = Number(response.headers.get('content-length') ?? '0');
  if (contentLength > maxBytes) {
    await response.body?.cancel('stream response exceeds configured limit').catch(() => undefined);
    throw new FleetTransportError(`world-celld: stream response exceeds ${maxBytes} bytes`);
  }
  if (!response.body) return new Uint8Array();

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > maxBytes) {
        await reader.cancel('stream response exceeds configured limit').catch(() => undefined);
        throw new FleetTransportError(`world-celld: stream response exceeds ${maxBytes} bytes`);
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

async function errorFromResponse(response: Response): Promise<Error> {
  const text = new TextDecoder().decode(
    await readBoundedResponse(response, MAX_ERROR_RESPONSE_BYTES),
  );
  let wire: WireError | undefined;
  try {
    wire = (JSON.parse(text) as { error?: WireError }).error;
  } catch {
    wire = { message: text || `HTTP ${response.status}` };
  }
  return reconstructError(wire, response.status);
}

function streamUrl(transport: RpcTransport, name: string): string {
  return `${transport.fleetUrl.replace(/\/$/, '')}/v1/streams/${encodeURIComponent(name)}/chunks`;
}

export async function writeStreamChunks(
  transport: RpcTransport,
  name: string,
  runId: string,
  chunks: Uint8Array[],
): Promise<StreamWriteResult> {
  const timeoutMs = resolveFleetTimeoutMs(transport);
  const body = encodeStreamWriteBatch(chunks);
  if (body.byteLength > MAX_STREAM_BATCH_BYTES + 4 * NEGOTIATED_STREAM_CHUNKS + 1024) {
    throw new Error('world-celld: encoded stream batch exceeds configured limit');
  }
  const url = new URL(streamUrl(transport, name));
  url.searchParams.set('runId', runId);
  const doFetch = transport.fetchImpl ?? fetch;

  let response: Response;
  try {
    response = await doFetch(url, {
      method: 'POST',
      headers: {
        'content-type': STREAM_BATCH_CONTENT_TYPE,
        authorization: `Bearer ${transport.secret}`,
      },
      body,
      signal: requestSignal(timeoutMs),
    });
  } catch (error) {
    throw new FleetTransportError(`world-celld: fleet unreachable at ${url.href}`, error);
  }

  if (!response.ok) throw await errorFromResponse(response);
  recordStreamChunkLimit(transport, response);
  try {
    const result = decodeStreamWriteResult(
      await readBoundedResponse(response, MAX_WRITE_RESPONSE_BYTES),
    );
    if (result.count !== chunks.length) {
      throw new Error('Stream write acknowledgement does not match the requested chunk count');
    }
    return result;
  } catch (error) {
    if (error instanceof FleetTransportError) throw error;
    throw new FleetTransportError(`world-celld: malformed stream response from ${url.href}`, error);
  }
}

export async function readStreamChunks(
  transport: RpcTransport,
  name: string,
  request: StreamReadRequest,
  signal?: AbortSignal,
): Promise<StreamReadResult> {
  const timeoutMs = resolveFleetTimeoutMs(transport);
  const url = new URL(streamUrl(transport, name));
  url.searchParams.set('runId', request.runId);
  url.searchParams.set('startIndex', String(request.startIndex));
  url.searchParams.set('maxChunks', String(request.maxChunks));
  url.searchParams.set('maxBytes', String(request.maxBytes));
  url.searchParams.set('waitMs', String(request.waitMs));
  const doFetch = transport.fetchImpl ?? fetch;
  let lastError: unknown;
  let maxChunks = request.maxChunks;

  for (let attempt = 1; attempt <= READ_ATTEMPTS; attempt++) {
    if (signal?.aborted) throw aborted(signal);
    if (attempt > 1) await delayMs(100 * attempt + Math.random() * 200, signal);
    let response: Response;
    try {
      response = await doFetch(url, {
        method: 'GET',
        headers: { authorization: `Bearer ${transport.secret}` },
        signal: requestSignal(timeoutMs, signal),
      });
    } catch (error) {
      if (signal?.aborted) throw aborted(signal);
      lastError = new FleetTransportError(`world-celld: fleet unreachable at ${url.href}`, error);
      continue;
    }

    if (signal?.aborted) {
      await response.body?.cancel(aborted(signal)).catch(() => undefined);
      throw aborted(signal);
    }

    if (response.ok) {
      recordStreamChunkLimit(transport, response);
      try {
        const result = decodeStreamReadResult(
          await readBoundedResponse(response, MAX_READ_RESPONSE_BYTES),
        );
        // A structurally valid frame can still be a stale or partial reply.
        // Do not expose output or advance its offset until it matches this
        // request's range and bounds. Reads can safely retry the same offset.
        if (
          result.startIndex !== request.startIndex ||
          result.chunks.length > maxChunks ||
          result.chunks.reduce((bytes, chunk) => bytes + chunk.byteLength, 0) > request.maxBytes ||
          (result.chunks.length > 0 &&
            result.startIndex + result.chunks.length - 1 > result.tailIndex) ||
          (maxChunks > 0 &&
            result.chunks.length === 0 &&
            result.startIndex <= result.tailIndex &&
            result.state !== 'expired')
        ) {
          throw new Error('Stream read response does not match the requested range and bounds');
        }
        return result;
      } catch (error) {
        if (signal?.aborted) throw aborted(signal);
        lastError =
          error instanceof FleetTransportError
            ? error
            : new FleetTransportError(
                `world-celld: malformed stream response from ${url.href}`,
                error,
              );
        if (attempt < READ_ATTEMPTS) continue;
        throw lastError;
      }
    }

    let error: Error;
    try {
      error = await errorFromResponse(response);
    } catch (cause) {
      if (signal?.aborted) throw aborted(signal);
      error =
        cause instanceof FleetTransportError
          ? cause
          : new FleetTransportError(
              `world-celld: malformed stream error response from ${url.href}`,
              cause,
            );
    }
    if (RETRYABLE_STATUSES.has(response.status) && attempt < READ_ATTEMPTS) {
      lastError = error;
      continue;
    }
    if (
      request.maxChunks > MAX_STREAM_READ_CHUNKS &&
      isChunkLimitRejection(error) &&
      attempt < READ_ATTEMPTS
    ) {
      // An older worker behind the same fleet URL; reads are safe to repeat.
      resetStreamChunkLimit(transport);
      maxChunks = MAX_STREAM_READ_CHUNKS;
      url.searchParams.set('maxChunks', String(MAX_STREAM_READ_CHUNKS));
      lastError = error;
      continue;
    }
    throw error;
  }

  throw lastError instanceof Error
    ? lastError
    : new FleetTransportError('world-celld: stream read failed', lastError);
}
