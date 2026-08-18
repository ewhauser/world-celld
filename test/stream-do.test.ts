import { describe, expect, it } from 'vitest';
import {
  MAX_STREAM_BATCH_BYTES,
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_READ_BYTES,
  MAX_STREAM_WRITE_CHUNKS,
  type StreamReadRequest,
} from '../src/stream-protocol.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { StreamDO } from '../src/worker/durable-objects/StreamDO.js';

const RUN_ID = 'wrun_stream_do';

function setup(name = 'stream:test') {
  const fleet = new FakeFleet({ streams: StreamDO as never });
  const get = () => fleet.namespace('streams').get({ toString: () => name }) as StreamDO;
  return { fleet, get, name };
}

function readRequest(overrides: Partial<StreamReadRequest> = {}): StreamReadRequest {
  return {
    runId: RUN_ID,
    startIndex: 0,
    maxChunks: 32,
    maxBytes: MAX_STREAM_READ_BYTES,
    waitMs: 0,
    ...overrides,
  };
}

describe('StreamDO binary batch and long-poll protocol', () => {
  it('round-trips an immediate ordered binary batch with contiguous offsets', async () => {
    const { get } = setup();
    const chunks = [
      new Uint8Array([0, 1, 2, 255]),
      new TextEncoder().encode('middle'),
      new Uint8Array([9, 8, 7]),
    ];

    await expect(get().writeChunks(RUN_ID, chunks)).resolves.toEqual({
      startIndex: 0,
      count: 3,
      tailIndex: 2,
    });
    const result = await get().readChunks(readRequest());

    expect(result).toMatchObject({
      startIndex: 0,
      tailIndex: 2,
      state: 'open',
      timedOut: false,
    });
    expect(result.chunks.map((chunk) => Array.from(chunk))).toEqual(
      chunks.map((chunk) => Array.from(chunk)),
    );
  });

  it('rejects an empty protocol batch and accepts the maximum count', async () => {
    const { get } = setup();
    await expect(get().writeChunks(RUN_ID, [])).rejects.toThrow(/at least one chunk/);

    const chunks = Array.from({ length: MAX_STREAM_WRITE_CHUNKS }, (_, index) =>
      Uint8Array.of(index),
    );
    await expect(get().writeChunks(RUN_ID, chunks)).resolves.toMatchObject({
      startIndex: 0,
      count: MAX_STREAM_WRITE_CHUNKS,
      tailIndex: MAX_STREAM_WRITE_CHUNKS - 1,
    });
    await expect(get().writeChunks(RUN_ID, [...chunks, Uint8Array.of(33)])).rejects.toThrow(
      /exceeds 32 chunks/,
    );
  });

  it('enforces the total batch byte limit', async () => {
    const { get } = setup();
    const oneMiB = new Uint8Array(1024 * 1024);
    await expect(
      get().writeChunks(
        RUN_ID,
        Array.from({ length: MAX_STREAM_BATCH_BYTES / oneMiB.byteLength }, () => oneMiB),
      ),
    ).resolves.toMatchObject({ count: 8 });
    await expect(
      get().writeChunks(
        RUN_ID,
        Array.from({ length: 9 }, () => oneMiB),
      ),
    ).rejects.toThrow(/batch exceeds/);
  });

  it('compacts direct-RPC subarrays before structured-clone storage', async () => {
    const { fleet, get, name } = setup();
    const backing = new Uint8Array(MAX_STREAM_CHUNK_BYTES + 64);
    const chunk = backing.subarray(32, 32 + MAX_STREAM_CHUNK_BYTES);

    await get().writeChunks(RUN_ID, [chunk]);

    const stored = fleet.cell('streams', name).storage.data.get('chunk:000000000000') as Uint8Array;
    expect(stored.byteLength).toBe(MAX_STREAM_CHUNK_BYTES);
    expect(stored.byteOffset).toBe(0);
    expect(stored.buffer.byteLength).toBe(MAX_STREAM_CHUNK_BYTES);
  });

  it('batch-fetches 32 small chunks without over-reading the byte budget', async () => {
    const { fleet, get, name } = setup();
    const chunks = Array.from({ length: MAX_STREAM_WRITE_CHUNKS }, (_, index) =>
      new Uint8Array(MAX_STREAM_READ_BYTES / MAX_STREAM_WRITE_CHUNKS).fill(index),
    );
    await get().writeChunks(RUN_ID, chunks);
    const storage = fleet.cell('streams', name).storage;
    storage.resetOperationCounts();

    const result = await get().readChunks(readRequest());

    expect(result.chunks).toHaveLength(MAX_STREAM_WRITE_CHUNKS);
    expect(result.chunks.map((chunk) => chunk[0])).toEqual(
      Array.from({ length: MAX_STREAM_WRITE_CHUNKS }, (_, index) => index),
    );
    expect(storage.operationCounts).toMatchObject({
      get: 0,
      getMany: 2,
      list: 0,
    });
  });

  it('fetches only the payload prefix selected by the byte budget', async () => {
    const { fleet, get, name } = setup();
    const chunkBytes = MAX_STREAM_CHUNK_BYTES / 8;
    await get().writeChunks(
      RUN_ID,
      Array.from({ length: 16 }, (_, index) => new Uint8Array(chunkBytes).fill(index)),
    );
    const storage = fleet.cell('streams', name).storage;
    storage.resetOperationCounts();

    const result = await get().readChunks(
      readRequest({ maxBytes: MAX_STREAM_CHUNK_BYTES, maxChunks: 16 }),
    );

    expect(result.chunks).toHaveLength(8);
    expect(storage.getManyCalls).toHaveLength(2);
    expect(storage.getManyCalls[0]).toHaveLength(16);
    expect(storage.getManyCalls[0].every((key) => key.startsWith('chunk-size:'))).toBe(true);
    expect(storage.getManyCalls[1]).toHaveLength(8);
    expect(storage.getManyCalls[1].every((key) => key.startsWith('chunk:'))).toBe(true);
  });

  it('times out one bounded idle read without polling storage', async () => {
    const { fleet, get, name } = setup();
    // Warm immutable metadata once, then isolate the idle-path operation count.
    await get().readChunks(readRequest());
    const storage = fleet.cell('streams', name).storage;
    storage.resetOperationCounts();

    const startedAt = performance.now();
    const result = await get().readChunks(readRequest({ waitMs: 30 }));
    const elapsedMs = performance.now() - startedAt;

    expect(result).toMatchObject({ chunks: [], state: 'open', timedOut: true });
    expect(elapsedMs).toBeGreaterThanOrEqual(20);
    expect(storage.operationCounts).toMatchObject({ get: 0, list: 0 });
  });

  it('wakes concurrent readers promptly on one batch write', async () => {
    const { get } = setup();
    const readers = Array.from({ length: 4 }, () =>
      get().readChunks(readRequest({ waitMs: 1_000 })),
    );
    await new Promise((resolve) => setTimeout(resolve, 10));

    await get().writeChunks(RUN_ID, [new TextEncoder().encode('wake')]);
    const results = await Promise.all(readers);

    expect(results).toHaveLength(4);
    for (const result of results) {
      expect(new TextDecoder().decode(result.chunks[0])).toBe('wake');
      expect(result.timedOut).toBe(false);
    }
  });

  it('wakes on close and preserves the first terminal state', async () => {
    const { get } = setup();
    const pending = get().readChunks(readRequest({ waitMs: 1_000 }));
    await new Promise((resolve) => setTimeout(resolve, 10));
    await get().closeStream(RUN_ID);

    await expect(pending).resolves.toMatchObject({ state: 'closed', chunks: [] });
    await get().failStream(RUN_ID, 'late failure');
    await expect(get().readChunks(readRequest())).resolves.toMatchObject({ state: 'closed' });
    await expect(get().writeChunks(RUN_ID, [Uint8Array.of(1)])).rejects.toThrow(/closed/);
  });

  it('wakes on a persisted stream error and rejects later writes', async () => {
    const { get } = setup();
    const pending = get().readChunks(readRequest({ waitMs: 1_000 }));
    await new Promise((resolve) => setTimeout(resolve, 10));
    await get().failStream(RUN_ID, { name: 'ProducerError', message: 'producer failed' });

    await expect(pending).resolves.toMatchObject({
      state: 'errored',
      error: { name: 'ProducerError', message: 'producer failed' },
    });
    await expect(get().writeChunks(RUN_ID, [Uint8Array.of(1)])).rejects.toThrow(/errored/);
  });

  it('serializes concurrent writers without collisions or reordering inside a batch', async () => {
    const { get } = setup();
    const first = [Uint8Array.of(1), Uint8Array.of(2), Uint8Array.of(3)];
    const second = [Uint8Array.of(4), Uint8Array.of(5)];

    const [firstResult, secondResult] = await Promise.all([
      get().writeChunks(RUN_ID, first),
      get().writeChunks(RUN_ID, second),
    ]);
    expect(firstResult).toMatchObject({ startIndex: 0, tailIndex: 2 });
    expect(secondResult).toMatchObject({ startIndex: 3, tailIndex: 4 });
    const result = await get().readChunks(readRequest());
    expect(result.chunks.map((chunk) => chunk[0])).toEqual([1, 2, 3, 4, 5]);
  });

  it('loads persisted offsets and terminal state after a DO restart', async () => {
    const { fleet, get, name } = setup();
    await get().writeChunks(RUN_ID, [Uint8Array.of(1), Uint8Array.of(2)]);
    fleet.restartCell('streams', name);
    await expect(get().writeChunks(RUN_ID, [Uint8Array.of(3)])).resolves.toMatchObject({
      startIndex: 2,
      tailIndex: 2,
    });
    await get().closeStream(RUN_ID);
    fleet.restartCell('streams', name);

    await expect(get().readChunks(readRequest())).resolves.toMatchObject({
      tailIndex: 2,
      state: 'closed',
    });
  });

  it('removes an aborted waiter and remains usable', async () => {
    const { get } = setup();
    const controller = new AbortController();
    const pending = get().readChunks(readRequest({ waitMs: 1_000 }), controller.signal);
    await new Promise((resolve) => setTimeout(resolve, 10));
    controller.abort(new DOMException('test abort', 'AbortError'));

    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    expect((get() as unknown as { waiters: Set<unknown> }).waiters.size).toBe(0);
    await get().writeChunks(RUN_ID, [Uint8Array.of(7)]);
    await expect(get().readChunks(readRequest())).resolves.toMatchObject({ tailIndex: 0 });
  });

  it('fences expiry before bounded deletion and wakes pending readers', async () => {
    const { fleet, get, name } = setup();
    await get().writeChunks(RUN_ID, [Uint8Array.of(1), Uint8Array.of(2)]);
    const pending = get().readChunks(readRequest({ startIndex: 2, waitMs: 1_000 }));
    await new Promise((resolve) => setTimeout(resolve, 10));

    await expect(get().expireStream(RUN_ID, Date.now())).resolves.toEqual({
      deleted: true,
      chunks: 2,
    });
    await expect(pending).resolves.toMatchObject({ state: 'expired', chunks: [] });
    expect(Array.from(fleet.cell('streams', name).storage.data.keys())).toEqual(['meta']);
    await expect(get().expireStream(RUN_ID, Date.now())).resolves.toEqual({
      deleted: true,
      chunks: 2,
    });
  });

  it('coalesces a maximum write into one metadata read and one batch put', async () => {
    const { fleet, get, name } = setup();
    const storage = fleet.cell('streams', name).storage;
    storage.resetOperationCounts();

    await get().writeChunks(
      RUN_ID,
      Array.from({ length: MAX_STREAM_WRITE_CHUNKS }, (_, index) => Uint8Array.of(index)),
    );

    expect(storage.operationCounts).toMatchObject({
      transaction: 1,
      get: 1,
      putMany: 1,
      put: 0,
    });
  });
});
