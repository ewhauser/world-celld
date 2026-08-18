import { describe, expect, it } from 'vitest';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { StreamDO } from '../src/worker/durable-objects/StreamDO.js';

const readRequest = {
  runId: 'wrun_large',
  startIndex: 299,
  maxChunks: 1,
  maxBytes: 1024 * 1024,
  waitMs: 0,
};

function countKeys(storage: { data: Map<string, unknown> }, prefix: string): number {
  return Array.from(storage.data.keys()).filter((key) => key.startsWith(prefix)).length;
}

describe('StreamDO paged KV cleanup', () => {
  it('bounds each chunk page, resumes to completion, and rolls back an interrupted page', async () => {
    const fleet = new FakeFleet({ streams: StreamDO });
    const stream = fleet.namespace('streams').get({ toString: () => 'stream:large' }) as StreamDO;
    const storage = fleet.cell('streams', 'stream:large').storage;
    const chunk = new Uint8Array(1024);
    for (let offset = 0; offset < 300; offset += 32) {
      await stream.writeChunks(
        'wrun_large',
        Array.from({ length: Math.min(32, 300 - offset) }, () => chunk),
      );
    }

    storage.listCalls.length = 0;
    storage.resetOperationCounts();
    storage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key === 'meta',
      new Error('injected crash after staged deletes'),
    );
    await expect(stream.expireStream('wrun_large', 123)).rejects.toThrow(
      'injected crash after staged deletes',
    );
    expect(countKeys(storage, 'chunk:')).toBe(300);
    expect(countKeys(storage, 'chunk-size:')).toBe(300);
    expect(await stream.readChunks(readRequest)).toMatchObject({
      tailIndex: 299,
      state: 'open',
      chunks: [chunk],
    });

    storage.listCalls.length = 0;
    storage.resetOperationCounts();
    expect(await stream.expireStream('wrun_large', 123)).toEqual({
      deleted: true,
      chunks: 64,
      bytes: 64 * 1024,
      done: false,
    });
    expect(countKeys(storage, 'chunk:')).toBe(236);
    for (let page = 0; page < 3; page++) {
      expect(await stream.expireStream('wrun_large', 123)).toEqual({
        deleted: false,
        chunks: 64,
        bytes: 64 * 1024,
        done: false,
      });
    }
    expect(await stream.expireStream('wrun_large', 123)).toEqual({
      deleted: false,
      chunks: 44,
      bytes: 44 * 1024,
      done: true,
    });
    expect(countKeys(storage, 'chunk:')).toBe(0);
    expect(countKeys(storage, 'chunk-size:')).toBe(0);
    expect(await stream.readChunks(readRequest)).toMatchObject({
      tailIndex: -1,
      state: 'expired',
      chunks: [],
    });

    expect(storage.listCalls).toHaveLength(5);
    expect(
      storage.listCalls.every(
        (call) =>
          call.transactional &&
          call.options.prefix === 'chunk-size:' &&
          call.options.limit === 65 &&
          call.resultSize <= 65,
      ),
    ).toBe(true);
    expect(storage.operationCounts.deleteMany).toBe(5);
  });

  it('bounds a page by payload bytes without reading chunk values', async () => {
    const fleet = new FakeFleet({ streams: StreamDO });
    const stream = fleet.namespace('streams').get({ toString: () => 'stream:bytes' }) as StreamDO;
    const storage = fleet.cell('streams', 'stream:bytes').storage;
    await stream.writeChunks(
      'wrun_bytes',
      Array.from({ length: 4 }, () => new Uint8Array(1024)),
    );
    storage.listCalls.length = 0;
    storage.resetOperationCounts();

    expect(await stream.expireStream('wrun_bytes', 123, { limit: 128, byteLimit: 2500 })).toEqual({
      deleted: true,
      chunks: 2,
      bytes: 2048,
      done: false,
    });
    expect(countKeys(storage, 'chunk:')).toBe(2);
    expect(countKeys(storage, 'chunk-size:')).toBe(2);
    expect(storage.getManyCalls).toEqual([]);
    expect(storage.listCalls).toEqual([
      {
        options: { prefix: 'chunk-size:', limit: 65 },
        resultSize: 4,
        transactional: true,
      },
    ]);
  });

  it('pages the registry and counts completed streams idempotently', async () => {
    const fleet = new FakeFleet({ streams: StreamDO });
    const registry = fleet.namespace('streams').get({
      toString: () => 'run-streams:wrun_registry',
    }) as StreamDO;
    for (let index = 0; index < 40; index++) {
      await registry.registerStream('wrun_registry', `stream-${String(index).padStart(2, '0')}`);
    }

    const first = await registry.expireRegistry('wrun_registry', 123, { limit: 16 });
    expect(first.streams).toHaveLength(16);
    expect(await registry.finalizeRegistry('wrun_registry', first.streams)).toEqual({
      deleted: 16,
      done: false,
    });
    expect(await registry.finalizeRegistry('wrun_registry', first.streams)).toEqual({
      deleted: 16,
      done: false,
    });

    const second = await registry.expireRegistry('wrun_registry', 123, { limit: 16 });
    expect(await registry.finalizeRegistry('wrun_registry', second.streams)).toEqual({
      deleted: 32,
      done: false,
    });
    const third = await registry.expireRegistry('wrun_registry', 123, { limit: 16 });
    expect(third.streams).toHaveLength(8);
    expect(await registry.finalizeRegistry('wrun_registry', third.streams)).toEqual({
      deleted: 40,
      done: true,
    });
  });
});
