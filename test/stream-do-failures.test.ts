import { describe, expect, it } from 'vitest';
import {
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_READ_BYTES,
  type StreamReadRequest,
} from '../src/stream-protocol.js';
import { FakeFleet, type FakeStorage } from '../src/testing/fake-cell.js';
import { StreamDO } from '../src/worker/durable-objects/StreamDO.js';

const RUN_ID = 'wrun_stream_owner';
const OTHER_RUN_ID = 'wrun_stream_other';
const META_KEY = 'meta';
const CHUNK_KEY = 'chunk:000000000000';
const CHUNK_SIZE_KEY = 'chunk-size:000000000000';

function setup(name = 'stream:failure') {
  const fleet = new FakeFleet({ streams: StreamDO as never });
  const get = () => fleet.namespace('streams').get({ toString: () => name }) as StreamDO;
  const storage = fleet.cell('streams', name).storage;
  return { fleet, get, name, storage };
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

function snapshot(storage: FakeStorage): Map<string, unknown> {
  return structuredClone(storage.data);
}

describe('StreamDO persisted-state failure handling', () => {
  it('treats absent metadata as a never-written stream without persisting partial state', async () => {
    const { get, storage } = setup();

    await expect(get().readChunks(readRequest())).resolves.toMatchObject({
      chunks: [],
      state: 'open',
      tailIndex: -1,
    });
    expect(storage.data).toEqual(new Map());
  });

  it('rejects malformed metadata deterministically without poisoning the instance cache', async () => {
    const invalidMetadata: Array<[string, unknown]> = [
      ['stored null', null],
      ['stored false', false],
      ['stored zero', 0],
      ['stored empty string', ''],
      ['non-object', 'invalid'],
      ['missing count', { state: 'open' }],
      ['missing state', { count: 0 }],
      ['negative count', { count: -1, state: 'open' }],
      ['fractional count', { count: 0.5, state: 'open' }],
      ['overflowed count', { count: 0x80000000, state: 'open' }],
      ['invalid state', { count: 0, state: 'unknown' }],
      ['invalid owner', { count: 0, state: 'open', ownerRunId: 42 }],
      ['non-boolean payloadDeleted', { count: 0, state: 'expired', payloadDeleted: 'true' }],
    ];

    for (const [label, invalid] of invalidMetadata) {
      const { get, storage } = setup(`stream:invalid-meta:${label}`);
      storage.data.set(META_KEY, invalid);

      await expect(get().readChunks(readRequest())).rejects.toThrow(
        'Invalid persisted stream metadata',
      );

      storage.data.set(META_KEY, { count: 0, state: 'open', ownerRunId: RUN_ID });
      await expect(get().readChunks(readRequest())).resolves.toMatchObject({
        chunks: [],
        state: 'open',
        tailIndex: -1,
      });
    }
  });

  it('rejects missing, malformed, and state-inconsistent error payloads', async () => {
    const invalidErrors: Array<[string, string, unknown]> = [
      ['missing errored-state error', 'errored', undefined],
      ['empty errored-state error', 'errored', {}],
      ['non-string errored-state name', 'errored', { name: 42, message: 'failure' }],
      ['non-string errored-state message', 'errored', { name: 'Error', message: null }],
      ['non-string closed-state name', 'closed', { name: 42, message: 'failure' }],
      ['non-string closed-state message', 'closed', { name: 'Error', message: null }],
      ['unexpected valid closed-state error', 'closed', { name: 'Error', message: 'failure' }],
      ['unexpected valid open-state error', 'open', { name: 'Error', message: 'failure' }],
      ['unexpected valid expired-state error', 'expired', { name: 'Error', message: 'failure' }],
    ];

    for (const [label, state, error] of invalidErrors) {
      const { get, storage } = setup(`stream:invalid-error:${label}`);
      storage.data.set(META_KEY, {
        count: 0,
        state,
        ownerRunId: RUN_ID,
        ...(state === 'expired' ? { payloadDeleted: true } : {}),
        ...(error === undefined ? {} : { error }),
      });

      await expect(get().readChunks(readRequest())).rejects.toThrow(
        'Invalid persisted stream error metadata',
      );
    }
  });

  it('never treats stored falsy metadata as absent on mutating paths', async () => {
    const operations: Array<[string, (stream: StreamDO) => Promise<unknown>]> = [
      ['write', (stream) => stream.writeChunks(RUN_ID, [Uint8Array.of(1)])],
      ['close', (stream) => stream.closeStream(RUN_ID)],
      ['fail', (stream) => stream.failStream(RUN_ID, 'failure')],
      ['expire', (stream) => stream.expireStream(RUN_ID, 123)],
    ];

    for (const [label, operation] of operations) {
      const { get, storage } = setup(`stream:falsy-meta:${label}`);
      storage.data.set(META_KEY, null);
      const before = snapshot(storage);

      await expect(operation(get())).rejects.toThrow('Invalid persisted stream metadata');
      expect(storage.data).toEqual(before);
    }
  });

  it('rejects missing and invalid chunk sizes, then retries after repair', async () => {
    const invalidSizes: Array<[string, unknown]> = [
      ['missing', undefined],
      ['non-number', '3'],
      ['negative', -1],
      ['fractional', 1.5],
      ['oversized', MAX_STREAM_CHUNK_BYTES + 1],
    ];

    for (const [label, invalid] of invalidSizes) {
      const { get, storage } = setup(`stream:invalid-size:${label}`);
      const chunk = Uint8Array.of(1, 2, 3);
      await get().writeChunks(RUN_ID, [chunk]);
      if (invalid === undefined) storage.data.delete(CHUNK_SIZE_KEY);
      else storage.data.set(CHUNK_SIZE_KEY, invalid);

      await expect(get().readChunks(readRequest())).rejects.toThrow(
        'Invalid persisted stream chunk size at index 0',
      );

      storage.data.set(CHUNK_SIZE_KEY, chunk.byteLength);
      await expect(get().readChunks(readRequest())).resolves.toMatchObject({
        chunks: [chunk],
        tailIndex: 0,
      });
    }
  });

  it('rejects missing, non-binary, and length-mismatched chunk payloads, then retries', async () => {
    const invalidPayloads: Array<[string, unknown]> = [
      ['missing', undefined],
      ['non-Uint8Array', 'abc'],
      ['length mismatch', Uint8Array.of(1, 2)],
    ];

    for (const [label, invalid] of invalidPayloads) {
      const { get, storage } = setup(`stream:invalid-payload:${label}`);
      const chunk = Uint8Array.of(1, 2, 3);
      await get().writeChunks(RUN_ID, [chunk]);
      if (invalid === undefined) storage.data.delete(CHUNK_KEY);
      else storage.data.set(CHUNK_KEY, invalid);

      await expect(get().readChunks(readRequest())).rejects.toThrow(
        'Invalid persisted stream chunk at index 0',
      );

      storage.data.set(CHUNK_KEY, chunk);
      await expect(get().readChunks(readRequest())).resolves.toMatchObject({
        chunks: [chunk],
        tailIndex: 0,
      });
    }
  });

  it('rolls back expiry when chunk-size keys are malformed or outside the offset range', async () => {
    const malformedKeys = [
      'chunk-size:00000000000x',
      'chunk-size:0000000000000',
      'chunk-size:002147483648',
    ];

    for (const malformedKey of malformedKeys) {
      const { get, storage } = setup(`stream:malformed-key:${malformedKey}`);
      await get().writeChunks(RUN_ID, [Uint8Array.of(1)]);
      storage.data.set(malformedKey, 1);
      const before = snapshot(storage);

      await expect(get().expireStream(RUN_ID, 123)).rejects.toThrow(
        `Invalid persisted stream chunk size key "${malformedKey}"`,
      );
      expect(storage.data).toEqual(before);

      storage.data.delete(malformedKey);
      await expect(get().expireStream(RUN_ID, 123)).resolves.toMatchObject({ done: true });
      expect(Array.from(storage.data.keys())).toEqual([META_KEY]);
    }
  });

  it('rolls back expiry on an invalid stored size and succeeds after repair', async () => {
    const { get, storage } = setup();
    await get().writeChunks(RUN_ID, [Uint8Array.of(1)]);
    storage.data.set(CHUNK_SIZE_KEY, '1');
    const before = snapshot(storage);

    await expect(get().expireStream(RUN_ID, 123)).rejects.toThrow(
      'Invalid persisted stream chunk size at index 0',
    );
    expect(storage.data).toEqual(before);

    storage.data.set(CHUNK_SIZE_KEY, 1);
    await expect(get().expireStream(RUN_ID, 123)).resolves.toMatchObject({ done: true });
    expect(Array.from(storage.data.keys())).toEqual([META_KEY]);
  });

  it('cleans orphan payloads despite missing and inconsistent size keys', async () => {
    const { get, storage } = setup();
    await get().writeChunks(RUN_ID, [Uint8Array.of(1), Uint8Array.of(2, 3)]);
    storage.data.delete(CHUNK_SIZE_KEY);
    // Keep the size-key count equal to meta.count so cleanup must compare
    // actual remaining payload keys rather than trusting matching counts.
    storage.data.set('chunk-size:000000000002', 1);

    await expect(get().expireStream(RUN_ID, 123)).resolves.toEqual({
      deleted: true,
      chunks: 2,
      bytes: 3,
      done: false,
    });
    expect(storage.data.has(CHUNK_KEY)).toBe(true);

    await expect(get().expireStream(RUN_ID, 123)).resolves.toEqual({
      deleted: false,
      chunks: 1,
      bytes: 1,
      done: true,
    });
    expect(Array.from(storage.data.keys())).toEqual([META_KEY]);
    expect(storage.data.get(META_KEY)).toMatchObject({
      count: 0,
      state: 'expired',
      payloadDeleted: true,
    });
  });

  it('resumes historically completed cleanup when payloadDeleted still has orphan payloads', async () => {
    const { fleet, get, name, storage } = setup('stream:historical-orphans');
    storage.data.set(META_KEY, {
      count: 0,
      state: 'expired',
      ownerRunId: RUN_ID,
      expiredAt: 123,
      expiredChunkCount: 2,
      payloadDeleted: true,
    });
    storage.data.set(CHUNK_KEY, Uint8Array.of(1));
    storage.data.set('chunk:000000000001', Uint8Array.of(2, 3));
    fleet.restartCell('streams', name);

    await expect(get().expireStream(RUN_ID, 123)).resolves.toEqual({
      deleted: false,
      chunks: 1,
      bytes: 1,
      done: false,
    });
    expect(storage.data.get(META_KEY)).toMatchObject({ payloadDeleted: false });
    expect(Array.from(storage.data.keys()).filter((key) => key.startsWith('chunk:'))).toEqual([
      'chunk:000000000001',
    ]);

    fleet.restartCell('streams', name);
    await expect(get().expireStream(RUN_ID, 123)).resolves.toEqual({
      deleted: false,
      chunks: 1,
      bytes: 2,
      done: true,
    });
    expect(Array.from(storage.data.keys())).toEqual([META_KEY]);
    expect(
      storage.operationCalls
        .filter((call) => call.operation === 'delete')
        .map((call) => call.keys.length),
    ).toEqual([2, 2]);

    const completed = snapshot(storage);
    storage.resetOperationCounts();
    fleet.restartCell('streams', name);
    await expect(get().expireStream(RUN_ID, 123)).resolves.toEqual({
      deleted: false,
      chunks: 0,
      bytes: 0,
      done: true,
    });
    expect(storage.data).toEqual(completed);
    expect(storage.operationCounts).toMatchObject({ put: 0, putMany: 0, delete: 0, deleteMany: 0 });
    expect(storage.operationCounts.list).toBe(2);
  });

  it('rejects owner mismatches on every stream path without mutation', async () => {
    const { get, storage } = setup();
    await get().writeChunks(RUN_ID, [Uint8Array.of(1)]);
    const before = snapshot(storage);
    const operations: Array<() => Promise<unknown>> = [
      () => get().readChunks(readRequest({ runId: OTHER_RUN_ID })),
      () => get().writeChunks(OTHER_RUN_ID, [Uint8Array.of(2)]),
      () => get().closeStream(OTHER_RUN_ID),
      () => get().failStream(OTHER_RUN_ID, 'failure'),
      () => get().expireStream(OTHER_RUN_ID, 123),
    ];

    for (const operation of operations) {
      await expect(operation()).rejects.toThrow(`Stream is owned by workflow run "${RUN_ID}"`);
      expect(storage.data).toEqual(before);
    }
  });

  it('rejects registry owner mismatches across registration and expiry without mutation', async () => {
    const { get, storage } = setup('run-streams:wrun_stream_owner');
    await get().registerStream(RUN_ID, 'stream-a');
    const registered = snapshot(storage);

    await expect(get().registerStream(OTHER_RUN_ID, 'stream-b')).rejects.toThrow(
      `Stream registry is owned by workflow run "${RUN_ID}"`,
    );
    await expect(get().expireRegistry(OTHER_RUN_ID, 123)).rejects.toThrow(
      `Stream registry is owned by workflow run "${RUN_ID}"`,
    );
    expect(storage.data).toEqual(registered);

    const { streams } = await get().expireRegistry(RUN_ID, 123);
    const expired = snapshot(storage);
    await expect(get().finalizeRegistry(OTHER_RUN_ID, streams)).rejects.toThrow(
      `Stream registry for workflow run "${OTHER_RUN_ID}" is not expired`,
    );
    expect(storage.data).toEqual(expired);
  });

  it('rejects persisted offset overflow and out-of-wire-range read offsets without mutation', async () => {
    const { get, storage } = setup();
    storage.data.set(META_KEY, {
      count: 0x7fffffff,
      state: 'open',
      ownerRunId: RUN_ID,
    });
    const before = snapshot(storage);

    await expect(get().writeChunks(RUN_ID, [Uint8Array.of(1)])).rejects.toThrow(
      'Stream offset limit exceeded',
    );
    await expect(get().readChunks(readRequest({ startIndex: 0x80000000 }))).rejects.toThrow(
      'startIndex must be between 0 and 2147483647',
    );
    expect(storage.data).toEqual(before);
  });

  it('retries shared metadata loads after storage failure in the same instance and after restart', async () => {
    const { fleet, get, name, storage } = setup();
    storage.data.set(META_KEY, { count: 0, state: 'closed', ownerRunId: RUN_ID });
    storage.failNextRead(
      (read) => read.operation === 'get' && read.keys?.length === 1 && read.keys[0] === META_KEY,
      new Error('injected meta read failure'),
    );

    const concurrent = await Promise.allSettled([
      get().readChunks(readRequest()),
      get().readChunks(readRequest()),
    ]);
    expect(concurrent).toHaveLength(2);
    for (const result of concurrent) {
      expect(result).toMatchObject({
        status: 'rejected',
        reason: expect.objectContaining({ message: 'injected meta read failure' }),
      });
    }
    await expect(get().readChunks(readRequest())).resolves.toMatchObject({ state: 'closed' });

    storage.failNextRead(
      (read) => read.operation === 'get' && read.keys?.[0] === META_KEY,
      new Error('injected restarted meta read failure'),
    );
    fleet.restartCell('streams', name);
    await expect(get().readChunks(readRequest())).rejects.toThrow(
      'injected restarted meta read failure',
    );
    fleet.restartCell('streams', name);
    await expect(get().readChunks(readRequest())).resolves.toMatchObject({ state: 'closed' });
  });

  it('retries chunk index and payload reads without cache poisoning or mutation', async () => {
    const { get, storage } = setup();
    const chunk = Uint8Array.of(1, 2, 3);
    await get().writeChunks(RUN_ID, [chunk]);
    const before = snapshot(storage);

    storage.failNextRead(
      (read) => read.operation === 'get' && read.keys?.[0]?.startsWith('chunk-size:') === true,
      new Error('injected chunk-size read failure'),
    );
    await expect(get().readChunks(readRequest())).rejects.toThrow(
      'injected chunk-size read failure',
    );
    await expect(get().readChunks(readRequest())).resolves.toMatchObject({ chunks: [chunk] });

    storage.failNextRead((read) => {
      const key = read.keys?.[0];
      return (
        read.operation === 'get' &&
        key?.startsWith('chunk:') === true &&
        !key.startsWith('chunk-size:')
      );
    }, new Error('injected chunk payload read failure'));
    await expect(get().readChunks(readRequest())).rejects.toThrow(
      'injected chunk payload read failure',
    );
    await expect(get().readChunks(readRequest())).resolves.toMatchObject({ chunks: [chunk] });
    expect(storage.data).toEqual(before);
  });

  it('rolls back failed transactional reads and writes, then retries at offset zero', async () => {
    const readFailure = setup('stream:transaction-read-failure');
    readFailure.storage.failNextRead(
      (read) => read.operation === 'get' && read.keys?.[0] === META_KEY,
      new Error('injected transaction read failure'),
    );

    await expect(readFailure.get().writeChunks(RUN_ID, [Uint8Array.of(1)])).rejects.toThrow(
      'injected transaction read failure',
    );
    expect(readFailure.storage.data).toEqual(new Map());
    await expect(readFailure.get().writeChunks(RUN_ID, [Uint8Array.of(1)])).resolves.toMatchObject({
      startIndex: 0,
      tailIndex: 0,
    });

    const writeFailure = setup('stream:transaction-write-failure');
    writeFailure.storage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key === CHUNK_SIZE_KEY,
      new Error('injected transaction write failure'),
    );

    await expect(writeFailure.get().writeChunks(RUN_ID, [Uint8Array.of(1)])).rejects.toThrow(
      'injected transaction write failure',
    );
    expect(writeFailure.storage.data).toEqual(new Map());
    await expect(writeFailure.get().writeChunks(RUN_ID, [Uint8Array.of(1)])).resolves.toMatchObject(
      { startIndex: 0, tailIndex: 0 },
    );
  });

  it('rolls back an expiry list failure and remains cleanly retryable', async () => {
    const { get, storage } = setup();
    await get().writeChunks(RUN_ID, [Uint8Array.of(1)]);
    const before = snapshot(storage);
    storage.failNextRead(
      (read) => read.operation === 'list' && read.options?.prefix === 'chunk-size:',
      new Error('injected expiry list failure'),
    );

    await expect(get().expireStream(RUN_ID, 123)).rejects.toThrow('injected expiry list failure');
    expect(storage.data).toEqual(before);
    await expect(get().expireStream(RUN_ID, 123)).resolves.toMatchObject({ done: true });
    expect(Array.from(storage.data.keys())).toEqual([META_KEY]);
  });
});
