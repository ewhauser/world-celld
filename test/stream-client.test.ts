import { afterEach, describe, expect, it, vi } from 'vitest';
import { MAX_FLEET_RPC_TIMEOUT_MS } from '../src/lifecycle.js';
import { readStreamChunks, writeStreamChunks } from '../src/remote/stream-client.js';
import {
  MAX_STREAM_READ_BYTES,
  encodeStreamReadResult,
  encodeStreamWriteResult,
} from '../src/stream-protocol.js';

const READ_REQUEST = {
  runId: 'wrun_stream',
  startIndex: 0,
  maxChunks: 4,
  maxBytes: 1024 * 1024,
  waitMs: 0,
};
const OVERSIZED_WRITE_RESPONSE_BYTES = 1024;
const OVERSIZED_READ_RESPONSE_BYTES = MAX_STREAM_READ_BYTES + 1024 * 1024;
const OVERSIZED_ERROR_RESPONSE_BYTES = 1024 * 1024;

function binaryResponse(body: Uint8Array, init?: ResponseInit): Response {
  return new Response(body, init);
}

function writeSuccess(): Response {
  return binaryResponse(encodeStreamWriteResult({ startIndex: 0, count: 1, tailIndex: 0 }), {
    status: 200,
  });
}

function readSuccess(): Response {
  return binaryResponse(
    encodeStreamReadResult({
      startIndex: 0,
      tailIndex: 0,
      chunks: [Uint8Array.of(1, 2, 3)],
      state: 'open',
      timedOut: false,
    }),
    { status: 200 },
  );
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('remote binary stream writes', () => {
  it('surfaces transport failure without retrying a commit-ambiguous write', async () => {
    const committed = new Error('response lost after commit');
    const fetchImpl = vi.fn<typeof fetch>(async () => {
      throw committed;
    });

    const outcome = await writeStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      'wrun_stream',
      [Uint8Array.of(1)],
    ).catch((error) => error);

    expect(outcome).toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('fleet unreachable'),
      cause: committed,
    });
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it('surfaces retryable HTTP status without blindly retrying the write', async () => {
    const fetchImpl = vi
      .fn<typeof fetch>(async () => writeSuccess())
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            error: { name: 'FleetUnavailableError', message: 'try later', status: 503 },
          }),
          { status: 503 },
        ),
      );

    const outcome = await writeStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      'wrun_stream',
      [Uint8Array.of(1)],
    ).catch((error) => error);

    expect(outcome).toMatchObject({
      name: 'FleetUnavailableError',
      message: 'try later',
      status: 503,
    });
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it('surfaces a malformed successful write response without retrying', async () => {
    const fetchImpl = vi
      .fn<typeof fetch>(async () => writeSuccess())
      .mockResolvedValueOnce(binaryResponse(Uint8Array.of(1, 2, 3), { status: 200 }));

    await expect(
      writeStreamChunks(
        { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
        'stream',
        'wrun_stream',
        [Uint8Array.of(1)],
      ),
    ).rejects.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('malformed stream response'),
    });
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it('bounds a successful write response without retrying', async () => {
    const fetchImpl = vi
      .fn<typeof fetch>(async () => writeSuccess())
      .mockResolvedValueOnce(
        binaryResponse(new Uint8Array(OVERSIZED_WRITE_RESPONSE_BYTES), { status: 200 }),
      );

    await expect(
      writeStreamChunks(
        { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
        'stream',
        'wrun_stream',
        [Uint8Array.of(1)],
      ),
    ).rejects.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('stream response exceeds'),
    });
    expect(fetchImpl).toHaveBeenCalledOnce();
  });
});

describe('remote binary stream reads', () => {
  it.each([
    ['transport rejection', () => Promise.reject(new TypeError('connection reset'))],
    ['HTTP 502', () => Promise.resolve(new Response('bad gateway', { status: 502 }))],
    ['HTTP 503', () => Promise.resolve(new Response('unavailable', { status: 503 }))],
    ['HTTP 504', () => Promise.resolve(new Response('gateway timeout', { status: 504 }))],
    [
      'malformed success',
      () => Promise.resolve(binaryResponse(Uint8Array.of(1, 2, 3), { status: 200 })),
    ],
  ])('retries a transient %s and succeeds later', async (_name, firstAttempt) => {
    vi.useFakeTimers();
    const fetchImpl = vi
      .fn<typeof fetch>(async () => readSuccess())
      .mockImplementationOnce(firstAttempt);

    const result = readStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      READ_REQUEST,
    );
    await vi.runAllTimersAsync();

    await expect(result).resolves.toMatchObject({
      chunks: [Uint8Array.of(1, 2, 3)],
      state: 'open',
    });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it('surfaces the final transport rejection after read exhaustion', async () => {
    vi.useFakeTimers();
    const failures = [new Error('first'), new Error('second'), new Error('final')];
    let attempt = 0;
    const fetchImpl = vi.fn<typeof fetch>(async () => {
      throw failures[attempt++];
    });

    const outcome = readStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      READ_REQUEST,
    ).catch((error) => error);
    await vi.runAllTimersAsync();

    await expect(outcome).resolves.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('fleet unreachable'),
      cause: failures[2],
    });
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it('surfaces the final retryable status after read exhaustion', async () => {
    vi.useFakeTimers();
    const statuses = [502, 503, 504];
    let attempt = 0;
    const fetchImpl = vi.fn<typeof fetch>(async () => {
      const status = statuses[attempt++];
      return new Response(
        JSON.stringify({
          error: { name: `Fleet${status}Error`, message: `failure ${status}`, status },
        }),
        { status },
      );
    });

    const outcome = readStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      READ_REQUEST,
    ).catch((error) => error);
    await vi.runAllTimersAsync();

    await expect(outcome).resolves.toMatchObject({
      name: 'Fleet504Error',
      message: 'failure 504',
      status: 504,
    });
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it('surfaces malformed success after read retry exhaustion', async () => {
    vi.useFakeTimers();
    const fetchImpl = vi.fn<typeof fetch>(async () =>
      binaryResponse(Uint8Array.of(1, 2, 3), { status: 200 }),
    );

    const outcome = readStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      READ_REQUEST,
    ).catch((error) => error);
    await vi.runAllTimersAsync();

    await expect(outcome).resolves.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('malformed stream response'),
    });
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });

  it('honors a caller abort while a read request is in flight', async () => {
    const controller = new AbortController();
    const abortReason = new DOMException('caller stopped', 'AbortError');
    const fetchImpl = vi.fn<typeof fetch>(
      async (_input: RequestInfo | URL, init?: RequestInit) =>
        await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), {
            once: true,
          });
        }),
    );

    const result = readStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      READ_REQUEST,
      controller.signal,
    );
    controller.abort(abortReason);

    await expect(result).rejects.toBe(abortReason);
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it('honors a caller abort during retry backoff without another attempt', async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    const abortReason = new DOMException('caller stopped', 'AbortError');
    const fetchImpl = vi.fn<typeof fetch>(async () => {
      throw new TypeError('connection reset');
    });

    const result = readStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      READ_REQUEST,
      controller.signal,
    );
    await Promise.resolve();
    await Promise.resolve();
    expect(fetchImpl).toHaveBeenCalledOnce();
    let immediate: unknown = 'still-backing-off';
    const observed = result.then(
      () => (immediate = 'resolved'),
      (error) => (immediate = error),
    );
    controller.abort(abortReason);
    for (let turn = 0; turn < 5; turn++) await Promise.resolve();
    if (immediate === 'still-backing-off') {
      await vi.runAllTimersAsync();
      await observed;
    }

    expect(immediate).toBe(abortReason);
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it('retries an oversized transient error response and succeeds later', async () => {
    vi.useFakeTimers();
    const fetchImpl = vi
      .fn<typeof fetch>(async () => readSuccess())
      .mockResolvedValueOnce(
        binaryResponse(new Uint8Array(OVERSIZED_ERROR_RESPONSE_BYTES), { status: 503 }),
      );

    const outcome = readStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      READ_REQUEST,
    ).catch((error) => error);
    await vi.runAllTimersAsync();

    await expect(outcome).resolves.toMatchObject({ state: 'open' });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it('bounds non-retryable error response bodies', async () => {
    const fetchImpl = vi.fn<typeof fetch>(async () =>
      binaryResponse(new Uint8Array(OVERSIZED_ERROR_RESPONSE_BYTES), { status: 400 }),
    );

    await expect(
      readStreamChunks(
        { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
        'stream',
        READ_REQUEST,
      ),
    ).rejects.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('stream response exceeds'),
    });
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it('bounds successful read responses across all retry attempts', async () => {
    vi.useFakeTimers();
    const fetchImpl = vi.fn<typeof fetch>(async () =>
      binaryResponse(Uint8Array.of(1), {
        status: 200,
        headers: { 'content-length': String(OVERSIZED_READ_RESPONSE_BYTES) },
      }),
    );

    const outcome = readStreamChunks(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'stream',
      READ_REQUEST,
    ).catch((error) => error);
    await vi.runAllTimersAsync();

    await expect(outcome).resolves.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('stream response exceeds'),
    });
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });
});

describe('remote binary stream timeout validation', () => {
  it.each([0, MAX_FLEET_RPC_TIMEOUT_MS + 1, 1.5, Number.NaN])(
    'rejects invalid timeout %s before a stream write or read',
    async (timeoutMs) => {
      const fetchImpl = vi.fn<typeof fetch>(async () => readSuccess());
      const transport = {
        fleetUrl: 'http://fleet.test',
        secret: 'secret',
        fetchImpl,
        timeoutMs,
      };

      await expect(
        writeStreamChunks(transport, 'stream', 'wrun_stream', [Uint8Array.of(1)]),
      ).rejects.toThrow(
        `world-celld: fleet RPC timeout must be between 1 and ${MAX_FLEET_RPC_TIMEOUT_MS}`,
      );
      await expect(readStreamChunks(transport, 'stream', READ_REQUEST)).rejects.toThrow(
        `world-celld: fleet RPC timeout must be between 1 and ${MAX_FLEET_RPC_TIMEOUT_MS}`,
      );
      expect(fetchImpl).not.toHaveBeenCalled();
    },
  );
});
