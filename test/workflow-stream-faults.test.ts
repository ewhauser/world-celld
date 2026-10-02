import { afterEach, describe, expect, it, vi } from 'vitest';
import { readStreamChunks, writeStreamChunks } from '../src/remote/stream-client.js';
import { createStreamer } from '../src/streamer.js';
import {
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_READ_BYTES,
  encodeStreamReadResult,
  encodeStreamWriteResult,
  type StreamReadRequest,
  type StreamReadResult,
} from '../src/stream-protocol.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { StreamDO } from '../src/worker/durable-objects/StreamDO.js';

const RUN_ID = 'wrun_stream_faults';
const NAME = 'workflow-output';
const CELL_NAME = `stream:${NAME}`;

function setup() {
  const fleet = new FakeFleet({ streams: StreamDO as never });
  const env = { WORKFLOW_STREAMS: fleet.namespace('streams') as never };
  const streamer = createStreamer({ env });
  const get = () => fleet.cell('streams', CELL_NAME).instance as StreamDO;
  const storage = fleet.cell('streams', CELL_NAME).storage;
  return { fleet, streamer, get, storage };
}

function read(overrides: Partial<StreamReadRequest> = {}): StreamReadRequest {
  return {
    runId: RUN_ID,
    startIndex: 0,
    maxChunks: 32,
    maxBytes: MAX_STREAM_READ_BYTES,
    waitMs: 0,
    ...overrides,
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('workflow output stream fault recovery', () => {
  it('rejects a missing trailing segment index after restart instead of returning a truncated closed page', async () => {
    const { fleet, streamer, storage } = setup();
    await streamer.writeChunksToStream(NAME, RUN_ID, ['first', 'second']);
    await streamer.writeToStream(NAME, RUN_ID, 'last');
    await streamer.closeStream(NAME, RUN_ID);
    const sizeKey = 'segsize:000000000002';
    const size = storage.data.get(sizeKey);
    storage.data.delete(sizeKey);
    const damaged = structuredClone(storage.data);
    fleet.restartCell('streams', CELL_NAME);

    await expect(streamer.getStreamChunks(NAME, RUN_ID)).rejects.toThrow(
      'Missing persisted stream segment at index 2',
    );
    expect(storage.data).toEqual(damaged);

    storage.data.set(sizeKey, size);
    const page = await streamer.getStreamChunks(NAME, RUN_ID);
    expect(page.data.map(({ data }) => new TextDecoder().decode(data))).toEqual([
      'first',
      'second',
      'last',
    ]);
    expect(page).toMatchObject({ done: true, hasMore: false, cursor: null });
  });

  it('errors a resumed reader when the segment containing its offset no longer covers it', async () => {
    const { fleet, streamer, storage } = setup();
    await streamer.writeChunksToStream(NAME, RUN_ID, ['first', 'second']);
    await streamer.writeToStream(NAME, RUN_ID, 'last');
    await streamer.closeStream(NAME, RUN_ID);
    storage.data.delete('segsize:000000000002');
    fleet.restartCell('streams', CELL_NAME);

    const reader = (await streamer.readFromStream(NAME, RUN_ID, 2)).getReader();
    await expect(reader.read()).rejects.toThrow('Missing persisted stream segment at index 2');
  });

  it('rejects overlapping segment indexes without returning duplicated or skipped workflow output', async () => {
    const { get, storage } = setup();
    await get().writeChunks(RUN_ID, [Uint8Array.of(1), Uint8Array.of(2)]);
    await get().writeChunks(RUN_ID, [Uint8Array.of(3)]);
    storage.data.set('segsize:000000000000', { count: 3, bytes: 2 });

    await expect(get().readChunks(read())).rejects.toThrow(
      'Overlapping persisted stream segment at index 2',
    );
    storage.data.set('segsize:000000000000', { count: 2, bytes: 2 });
    await expect(get().readChunks(read())).resolves.toMatchObject({
      chunks: [Uint8Array.of(1), Uint8Array.of(2), Uint8Array.of(3)],
    });
  });

  it.each([false, true])(
    'preserves committed output after a failed append and resumes contiguous offsets (restart=%s)',
    async (restart) => {
      const { fleet, get, storage } = setup();
      const first = [Uint8Array.of(1), Uint8Array.of(2)];
      const next = [Uint8Array.of(3), Uint8Array.of(4)];
      await get().writeChunks(RUN_ID, first);
      const before = structuredClone(storage.data);
      storage.failNextMutation(
        (mutation) => mutation.operation === 'put' && mutation.key === 'segsize:000000000002',
        new Error('segment index write failed'),
      );

      await expect(get().writeChunks(RUN_ID, next)).rejects.toThrow('segment index write failed');
      expect(storage.data).toEqual(before);
      await expect(get().readChunks(read())).resolves.toMatchObject({
        chunks: first,
        tailIndex: 1,
        state: 'open',
      });

      if (restart) fleet.restartCell('streams', CELL_NAME);
      await expect(get().writeChunks(RUN_ID, next)).resolves.toEqual({
        startIndex: 2,
        count: 2,
        tailIndex: 3,
      });
      fleet.restartCell('streams', CELL_NAME);
      await expect(get().readChunks(read())).resolves.toMatchObject({
        chunks: [...first, ...next],
        tailIndex: 3,
      });
    },
  );

  it.each(['closed', 'errored'] as const)(
    'does not wake a reader or poison terminal state when persisting %s fails',
    async (state) => {
      const { fleet, get, storage } = setup();
      await get().writeChunks(RUN_ID, [Uint8Array.of(1)]);
      const before = structuredClone(storage.data);
      const finish = () =>
        state === 'closed'
          ? get().closeStream(RUN_ID)
          : get().failStream(RUN_ID, { name: 'ProducerError', message: 'producer failed' });
      let settled = false;
      const waiting = get().readChunks(read({ startIndex: 1, waitMs: 5_000 }));
      void waiting.then(() => (settled = true));
      storage.failNextMutation(
        (mutation) =>
          mutation.operation === 'put' &&
          mutation.key === 'meta' &&
          (mutation.value as { state?: string }).state === state,
        new Error('terminal metadata write failed'),
      );

      await expect(finish()).rejects.toThrow('terminal metadata write failed');
      expect(storage.data).toEqual(before);
      expect(settled).toBe(false);
      await expect(get().readChunks(read({ maxChunks: 0 }))).resolves.toMatchObject({
        state: 'open',
        tailIndex: 0,
      });

      await finish();
      await expect(waiting).resolves.toMatchObject({ state, chunks: [], timedOut: false });
      fleet.restartCell('streams', CELL_NAME);
      await expect(get().readChunks(read())).resolves.toMatchObject({
        state,
        chunks: [Uint8Array.of(1)],
      });
      await expect(get().writeChunks(RUN_ID, [Uint8Array.of(2)])).rejects.toThrow(
        state === 'closed'
          ? 'Cannot write to a closed stream'
          : 'Cannot write to an errored stream',
      );
    },
  );

  it('rolls back every storage batch of a legacy append when a later put fails', async () => {
    const { fleet, get, storage } = setup();
    storage.data.set('meta', { count: 0, state: 'open', ownerRunId: RUN_ID });
    const chunks = Array.from({ length: 128 }, (_, index) => Uint8Array.of(index));
    const before = structuredClone(storage.data);
    storage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key === 'chunk-size:000000000064',
      new Error('later legacy payload batch failed'),
    );

    await expect(get().writeChunks(RUN_ID, chunks)).rejects.toThrow(
      'later legacy payload batch failed',
    );
    expect(storage.data).toEqual(before);
    expect(
      storage.operationCalls.some(
        (call) => call.operation === 'put' && call.transactional && call.keys.length === 128,
      ),
    ).toBe(true);

    fleet.restartCell('streams', CELL_NAME);
    await expect(get().writeChunks(RUN_ID, chunks)).resolves.toEqual({
      startIndex: 0,
      count: 128,
      tailIndex: 127,
    });
    fleet.restartCell('streams', CELL_NAME);
    const result = await get().readChunks(read({ maxChunks: 128 }));
    expect(result.chunks.map((chunk) => chunk[0])).toEqual(chunks.map((chunk) => chunk[0]));
  });

  it('rolls back a segmented expiry after payload deletion, then resumes bounded cleanup across restart', async () => {
    const { fleet, get, storage } = setup();
    for (const chunk of [1, 2, 3]) await get().writeChunks(RUN_ID, [Uint8Array.of(chunk)]);
    const before = structuredClone(storage.data);
    storage.failNextMutation(
      (mutation) =>
        mutation.operation === 'put' &&
        mutation.key === 'meta' &&
        (mutation.value as { state?: string }).state === 'expired',
      new Error('expiry fence write failed'),
    );

    await expect(get().expireStream(RUN_ID, 123, { limit: 1 })).rejects.toThrow(
      'expiry fence write failed',
    );
    expect(storage.data).toEqual(before);
    await expect(get().readChunks(read())).resolves.toMatchObject({
      state: 'open',
      chunks: [Uint8Array.of(1), Uint8Array.of(2), Uint8Array.of(3)],
    });

    for (let page = 0; page < 3; page++) {
      fleet.restartCell('streams', CELL_NAME);
      await expect(get().expireStream(RUN_ID, 123, { limit: 1 })).resolves.toEqual({
        deleted: page === 0,
        chunks: 1,
        bytes: 1,
        done: page === 2,
      });
      await expect(get().writeChunks(RUN_ID, [Uint8Array.of(4)])).rejects.toThrow(/expired/);
      await expect(get().readChunks(read())).resolves.toMatchObject({
        state: 'expired',
        chunks: [],
      });
    }
    expect(Array.from(storage.data.keys())).toEqual(['meta']);
  });
});

describe('workflow output remote acknowledgement faults', () => {
  it('rejects a partial write acknowledgement without replaying an ambiguous append', async () => {
    const fetchImpl = vi.fn<typeof fetch>(
      async () => new Response(encodeStreamWriteResult({ startIndex: 0, count: 1, tailIndex: 0 })),
    );

    await expect(
      writeStreamChunks(
        { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
        NAME,
        RUN_ID,
        [Uint8Array.of(1), Uint8Array.of(2)],
      ),
    ).rejects.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('malformed stream response'),
    });
    expect(fetchImpl).toHaveBeenCalledOnce();
  });

  it.each([
    ['stale read offset', { startIndex: 0, tailIndex: 2, chunks: [Uint8Array.of(1)] }],
    ['empty page before the durable tail', { startIndex: 2, tailIndex: 2, chunks: [] }],
    ['chunks beyond the durable tail', { startIndex: 2, tailIndex: 1, chunks: [Uint8Array.of(1)] }],
    [
      'excess chunk count',
      { startIndex: 2, tailIndex: 3, chunks: [Uint8Array.of(1), Uint8Array.of(2)] },
    ],
    [
      'excess page bytes',
      { startIndex: 2, tailIndex: 2, chunks: [new Uint8Array(MAX_STREAM_CHUNK_BYTES + 1)] },
    ],
  ] as Array<[string, Pick<StreamReadResult, 'startIndex' | 'tailIndex' | 'chunks'>]>)(
    'retries %s without exposing corrupt output or advancing the cursor',
    async (_label, damaged) => {
      vi.useFakeTimers();
      const success = {
        startIndex: 2,
        tailIndex: 2,
        chunks: [Uint8Array.of(3)],
        state: 'closed' as const,
        timedOut: false,
      };
      const fetchImpl = vi.fn<typeof fetch>(
        async () => new Response(encodeStreamReadResult(success)),
      );
      fetchImpl.mockResolvedValueOnce(
        new Response(encodeStreamReadResult({ ...damaged, state: 'closed', timedOut: false })),
      );
      const outcome = readStreamChunks(
        { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
        NAME,
        read({ startIndex: 2, maxChunks: 1, maxBytes: MAX_STREAM_CHUNK_BYTES }),
      );
      await vi.runAllTimersAsync();

      const result = await outcome;
      expect(result).toMatchObject({
        startIndex: 2,
        tailIndex: 2,
        state: 'closed',
        timedOut: false,
      });
      expect(result.chunks.map((chunk) => ({ length: chunk.byteLength, first: chunk[0] }))).toEqual(
        [{ length: 1, first: 3 }],
      );
      expect(fetchImpl).toHaveBeenCalledTimes(2);
      for (const [url] of fetchImpl.mock.calls) {
        expect(new URL(url instanceof Request ? url.url : url).searchParams.get('startIndex')).toBe(
          '2',
        );
      }
    },
  );
});
