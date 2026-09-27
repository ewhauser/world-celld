import { describe, expect, it } from 'vitest';
import {
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_READ_BYTES,
  type StreamReadRequest,
} from '../src/stream-protocol.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { StreamDO } from '../src/worker/durable-objects/StreamDO.js';

const RUN_ID = 'wrun_segments';

/** A stream created in segment layout, as a writer that creates segment streams leaves it. */
function segmentStream(name = 'stream:segments') {
  const fleet = new FakeFleet({ streams: StreamDO as never });
  const cell = fleet.cell('streams', name);
  cell.storage.data.set('meta', { count: 0, state: 'open', layout: 2 });
  const stream = fleet.namespace('streams').get({ toString: () => name }) as StreamDO;
  const keys = (prefix: string) =>
    Array.from(cell.storage.data.keys()).filter((key) => key.startsWith(prefix));
  return { fleet, cell, stream, keys };
}

function read(overrides: Partial<StreamReadRequest> = {}): StreamReadRequest {
  return {
    runId: RUN_ID,
    startIndex: 0,
    maxChunks: 512,
    maxBytes: MAX_STREAM_READ_BYTES,
    waitMs: 0,
    ...overrides,
  };
}

/** Chunk i holds its index in its first two bytes. */
function chunk(index: number, size = 8): Uint8Array {
  const bytes = new Uint8Array(size);
  bytes[0] = index & 0xff;
  bytes[1] = index >> 8;
  return bytes;
}

const indexOf = (value: Uint8Array) => value[0] + (value[1] << 8);

function range(from: number, to: number): number[] {
  return Array.from({ length: to - from }, (_, offset) => from + offset);
}

describe('StreamDO segment layout', () => {
  it('appends each write as segment rows and keeps the layout', async () => {
    const { cell, stream, keys } = segmentStream();
    const batches = [5, 1, 300];
    let next = 0;
    for (const size of batches) {
      await stream.writeChunks(
        RUN_ID,
        Array.from({ length: size }, () => chunk(next++)),
      );
    }
    expect(keys('chunk')).toEqual([]);
    expect(keys('seg:')).toEqual(['seg:000000000000', 'seg:000000000005', 'seg:000000000006']);
    expect(keys('segsize:')).toHaveLength(3);
    expect(cell.storage.data.get('segsize:000000000006')).toEqual({ count: 300, bytes: 2400 });
    expect(cell.storage.data.get('meta')).toMatchObject({ count: 306, layout: 2 });

    await stream.closeStream(RUN_ID);
    expect(cell.storage.data.get('meta')).toMatchObject({ state: 'closed', layout: 2 });
  });

  it('splits a write into segments of at most 1 MiB of payload', async () => {
    const { stream, keys } = segmentStream();
    const size = 600 * 1024;
    await stream.writeChunks(RUN_ID, [chunk(0, size), chunk(1, size), chunk(2, size)]);
    expect(keys('seg:')).toEqual(['seg:000000000000', 'seg:000000000001', 'seg:000000000002']);
    const result = await stream.readChunks(read({ maxBytes: MAX_STREAM_READ_BYTES }));
    expect(result.chunks.map(indexOf)).toEqual([0, 1, 2]);
    expect(result.chunks.every((value) => value.byteLength === size)).toBe(true);
  });

  it('reads aligned, mid-segment, and bounded ranges exactly', async () => {
    const { stream } = segmentStream();
    let next = 0;
    for (const size of [10, 10, 10]) {
      await stream.writeChunks(
        RUN_ID,
        Array.from({ length: size }, () => chunk(next++)),
      );
    }

    const all = await stream.readChunks(read());
    expect(all.chunks.map(indexOf)).toEqual(range(0, 30));
    expect(all.tailIndex).toBe(29);
    expect((await stream.readChunks(read({ startIndex: 10 }))).chunks.map(indexOf)).toEqual(
      range(10, 30),
    );
    expect((await stream.readChunks(read({ startIndex: 13 }))).chunks.map(indexOf)).toEqual(
      range(13, 30),
    );
    expect(
      (await stream.readChunks(read({ startIndex: 7, maxChunks: 5 }))).chunks.map(indexOf),
    ).toEqual(range(7, 12));
    expect((await stream.readChunks(read({ startIndex: 29 }))).chunks.map(indexOf)).toEqual([29]);
    expect((await stream.readChunks(read({ startIndex: 30 }))).chunks).toEqual([]);
  });

  it('trims a read to the byte budget chunk by chunk', async () => {
    const { stream } = segmentStream();
    const size = 400 * 1024;
    await stream.writeChunks(RUN_ID, [chunk(0, size), chunk(1, size)]);
    await stream.writeChunks(RUN_ID, [chunk(2, size), chunk(3, size)]);
    const bounded = await stream.readChunks(read({ maxBytes: MAX_STREAM_CHUNK_BYTES }));
    expect(bounded.chunks.map(indexOf)).toEqual([0, 1]);
    const next = await stream.readChunks(read({ startIndex: 1, maxBytes: MAX_STREAM_CHUNK_BYTES }));
    expect(next.chunks.map(indexOf)).toEqual([1, 2]);
  });

  it('wakes a waiting reader when a segment is appended', async () => {
    const { stream } = segmentStream();
    const waiting = stream.readChunks(read({ waitMs: 5_000 }));
    await stream.writeChunks(RUN_ID, [chunk(0), chunk(1)]);
    expect((await waiting).chunks.map(indexOf)).toEqual([0, 1]);
  });

  it('expires segments in bounded pages without loading payloads', async () => {
    const { cell, stream, keys } = segmentStream();
    let next = 0;
    for (const size of [100, 100, 100, 5]) {
      await stream.writeChunks(
        RUN_ID,
        Array.from({ length: size }, () => chunk(next++)),
      );
    }
    cell.storage.listCalls.length = 0;
    const first = await stream.expireStream(RUN_ID, 123, { limit: 256 });
    expect(first).toEqual({ deleted: true, chunks: 200, bytes: 1600, done: false });
    expect(
      cell.storage.listCalls
        .filter((call) => call.options.prefix === 'seg:')
        .every((call) => call.options.limit === 1),
    ).toBe(true);
    expect(await stream.expireStream(RUN_ID, 123, { limit: 256 })).toEqual({
      deleted: false,
      chunks: 105,
      bytes: 840,
      done: true,
    });
    expect(keys('seg')).toEqual([]);
    expect(cell.storage.data.get('meta')).toMatchObject({
      state: 'expired',
      payloadDeleted: true,
      layout: 2,
    });
    expect(await stream.expireStream(RUN_ID, 123)).toEqual({
      deleted: false,
      chunks: 0,
      bytes: 0,
      done: true,
    });
  });

  it('makes progress on a segment larger than the page and removes an orphan segment', async () => {
    const { cell, stream, keys } = segmentStream();
    await stream.writeChunks(
      RUN_ID,
      Array.from({ length: 300 }, (_, index) => chunk(index)),
    );
    await stream.writeChunks(RUN_ID, [chunk(300)]);
    cell.storage.data.delete('segsize:000000000300');

    expect(await stream.expireStream(RUN_ID, 123, { limit: 64 })).toEqual({
      deleted: true,
      chunks: 300,
      bytes: 2400,
      done: false,
    });
    const orphan = await stream.expireStream(RUN_ID, 123, { limit: 64 });
    expect(orphan).toMatchObject({ chunks: 1, done: true });
    expect(keys('seg')).toEqual([]);
  });

  it('creates new streams in segment layout', async () => {
    const fleet = new FakeFleet({ streams: StreamDO as never });
    const stream = fleet.namespace('streams').get({ toString: () => 'stream:new' }) as StreamDO;
    await stream.writeChunks(RUN_ID, [chunk(0), chunk(1)]);
    const storage = fleet.cell('streams', 'stream:new').storage;
    expect(storage.data.get('meta')).toMatchObject({ count: 2, layout: 2 });
    expect(Array.from(storage.data.keys()).toSorted()).toEqual([
      'meta',
      'seg:000000000000',
      'segsize:000000000000',
    ]);
  });

  it('reads a written batch back with one list and one multi-get', async () => {
    const { cell, stream } = segmentStream();
    await stream.writeChunks(
      RUN_ID,
      Array.from({ length: 32 }, (_, index) => chunk(index)),
    );
    cell.storage.resetOperationCounts();
    const result = await stream.readChunks(read());
    expect(result.chunks.map(indexOf)).toEqual(range(0, 32));
    expect(cell.storage.operationCounts).toMatchObject({ get: 0, getMany: 1, list: 1 });
  });

  it('keeps writing per-chunk rows for a stream that already uses them', async () => {
    const fleet = new FakeFleet({ streams: StreamDO as never });
    fleet.cell('streams', 'stream:rows').storage.data.set('meta', { count: 0, state: 'open' });
    const stream = fleet.namespace('streams').get({ toString: () => 'stream:rows' }) as StreamDO;
    await stream.writeChunks(RUN_ID, [chunk(0), chunk(1)]);
    const keys = Array.from(fleet.cell('streams', 'stream:rows').storage.data.keys());
    expect(keys.filter((key) => key.startsWith('seg'))).toEqual([]);
    expect(keys.filter((key) => key.startsWith('chunk:'))).toHaveLength(2);
    expect(fleet.cell('streams', 'stream:rows').storage.data.get('meta')).not.toHaveProperty(
      'layout',
    );
  });
});
