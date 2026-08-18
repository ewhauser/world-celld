import {
  MAX_STREAM_BATCH_BYTES,
  MAX_STREAM_READ_BYTES,
  STREAM_BATCH_CONTENT_TYPE,
  decodeStreamReadResult,
  decodeStreamWriteResult,
  encodeStreamWriteBatch,
  type StreamReadRequest,
  type StreamReadResult,
  type StreamWriteResult,
} from '../stream-protocol.js';
import { FleetTransportError, reconstructError, type WireError } from './errors.js';
import type { RpcTransport } from './rpc-client.js';

const RETRYABLE_STATUSES = new Set([502, 503, 504]);
const READ_ATTEMPTS = 3;
const DEFAULT_TIMEOUT_MS = 30_000;
const MAX_WRITE_RESPONSE_BYTES = 64;
const MAX_READ_RESPONSE_BYTES = MAX_STREAM_READ_BYTES + 64 * 1024;
const MAX_ERROR_RESPONSE_BYTES = 64 * 1024;

function delayMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function requestSignal(timeoutMs: number, signal?: AbortSignal): AbortSignal {
  const timeout = AbortSignal.timeout(Math.max(1, timeoutMs));
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
  const body = encodeStreamWriteBatch(chunks);
  if (body.byteLength > MAX_STREAM_BATCH_BYTES + 1024) {
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
      signal: requestSignal(transport.timeoutMs ?? DEFAULT_TIMEOUT_MS),
    });
  } catch (error) {
    throw new FleetTransportError(`world-celld: fleet unreachable at ${url.href}`, error);
  }

  if (!response.ok) throw await errorFromResponse(response);
  try {
    return decodeStreamWriteResult(await readBoundedResponse(response, MAX_WRITE_RESPONSE_BYTES));
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
  const url = new URL(streamUrl(transport, name));
  url.searchParams.set('runId', request.runId);
  url.searchParams.set('startIndex', String(request.startIndex));
  url.searchParams.set('maxChunks', String(request.maxChunks));
  url.searchParams.set('maxBytes', String(request.maxBytes));
  url.searchParams.set('waitMs', String(request.waitMs));
  const doFetch = transport.fetchImpl ?? fetch;
  let lastError: unknown;

  for (let attempt = 1; attempt <= READ_ATTEMPTS; attempt++) {
    if (signal?.aborted) throw aborted(signal);
    if (attempt > 1) await delayMs(100 * attempt + Math.random() * 200);
    let response: Response;
    try {
      response = await doFetch(url, {
        method: 'GET',
        headers: { authorization: `Bearer ${transport.secret}` },
        signal: requestSignal(transport.timeoutMs ?? DEFAULT_TIMEOUT_MS, signal),
      });
    } catch (error) {
      if (signal?.aborted) throw aborted(signal);
      lastError = new FleetTransportError(`world-celld: fleet unreachable at ${url.href}`, error);
      continue;
    }

    if (response.ok) {
      try {
        return decodeStreamReadResult(await readBoundedResponse(response, MAX_READ_RESPONSE_BYTES));
      } catch (error) {
        lastError = new FleetTransportError(
          `world-celld: malformed stream response from ${url.href}`,
          error,
        );
        if (attempt < READ_ATTEMPTS) continue;
        throw lastError;
      }
    }

    const error = await errorFromResponse(response);
    if (RETRYABLE_STATUSES.has(response.status) && attempt < READ_ATTEMPTS) {
      lastError = error;
      continue;
    }
    throw error;
  }

  throw lastError instanceof Error
    ? lastError
    : new FleetTransportError('world-celld: stream read failed', lastError);
}
