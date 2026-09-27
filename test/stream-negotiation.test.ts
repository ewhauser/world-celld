import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import {
  MAX_STREAM_READ_CHUNKS,
  MAX_STREAM_WRITE_CHUNKS,
  NEGOTIATED_STREAM_CHUNKS,
  STREAM_CHUNK_LIMIT_HEADER,
} from '../src/stream-protocol.js';
import { createStreamer } from '../src/streamer.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';

const SECRET = 'stream-negotiation-secret';

function legacyRejection(message: string): Response {
  return Response.json({ error: { name: 'StreamProtocolError', message } }, { status: 400 });
}

/**
 * Wraps the fleet so it behaves like a worker from before negotiation when
 * `legacy.enabled`: no advertised limit and the old 32-chunk validation.
 */
function fleetFetch(legacy: { enabled: boolean }) {
  const writes: number[] = [];
  const reads: number[] = [];
  const doFetch: typeof fetch = async (input, init) => {
    const url = new URL(input instanceof Request ? input.url : String(input));
    const isStream = url.pathname.startsWith('/v1/streams/');
    if (isStream && init?.method === 'POST' && init.body instanceof Uint8Array) {
      const count = new DataView(init.body.buffer, init.body.byteOffset).getUint16(6);
      writes.push(count);
      if (legacy.enabled && count > MAX_STREAM_WRITE_CHUNKS) {
        return legacyRejection(
          `world-celld: invalid stream protocol: batch exceeds ${MAX_STREAM_WRITE_CHUNKS} chunks`,
        );
      }
    }
    if (isStream && init?.method === 'GET') {
      const maxChunks = Number(url.searchParams.get('maxChunks'));
      reads.push(maxChunks);
      if (legacy.enabled && maxChunks > MAX_STREAM_READ_CHUNKS) {
        return legacyRejection(
          `maxChunks must be an integer between 0 and ${MAX_STREAM_READ_CHUNKS}`,
        );
      }
    }
    const response = await fetch(input, init);
    if (!legacy.enabled || !isStream) return response;
    const headers = new Headers(response.headers);
    headers.delete(STREAM_CHUNK_LIMIT_HEADER);
    return new Response(response.body, { status: response.status, headers });
  };
  return { doFetch, writes, reads };
}

function chunksOf(count: number): Uint8Array[] {
  return Array.from({ length: count }, (_, index) => Uint8Array.of(index % 256, index >> 8));
}

async function readAll(
  streamer: ReturnType<typeof createStreamer>,
  name: string,
  runId: string,
): Promise<number[]> {
  const indexes: number[] = [];
  const reader = (await streamer.readFromStream(name, runId)).getReader();
  for (;;) {
    const { done, value } = await reader.read();
    if (done) return indexes;
    indexes.push(value[0] + (value[1] << 8));
  }
}

describe('stream chunk limit negotiation', () => {
  let harness: Harness;

  beforeAll(async () => {
    harness = await startHarness({ secret: SECRET });
  });

  afterAll(async () => {
    await harness.close();
  });

  function streamerFor(doFetch: typeof fetch) {
    const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET, fetchImpl: doFetch });
    return createStreamer({ env: { WORKFLOW_STREAMS: env.WORKFLOW_STREAMS } });
  }

  it('sends negotiated batches once the worker advertises its limit', async () => {
    const fleet = fleetFetch({ enabled: false });
    const streamer = streamerFor(fleet.doFetch);
    await streamer.writeChunksToStream('negotiated', 'wrun_negotiated', chunksOf(1000));
    await streamer.closeStream('negotiated', 'wrun_negotiated');

    expect(fleet.writes).toEqual([MAX_STREAM_WRITE_CHUNKS, NEGOTIATED_STREAM_CHUNKS, 456]);
    const page = await streamer.getStreamChunks('negotiated', 'wrun_negotiated', { limit: 1000 });
    expect(page.data).toHaveLength(NEGOTIATED_STREAM_CHUNKS);
    expect(await readAll(streamer, 'negotiated', 'wrun_negotiated')).toEqual(
      Array.from({ length: 1000 }, (_, index) => index),
    );
  });

  it('stays at the baseline with a worker that does not advertise', async () => {
    const fleet = fleetFetch({ enabled: true });
    const streamer = streamerFor(fleet.doFetch);
    await streamer.writeChunksToStream('legacy', 'wrun_legacy', chunksOf(100));
    await streamer.closeStream('legacy', 'wrun_legacy');

    expect(fleet.writes).toEqual([32, 32, 32, 4]);
    expect(await readAll(streamer, 'legacy', 'wrun_legacy')).toEqual(
      Array.from({ length: 100 }, (_, index) => index),
    );
    expect(fleet.reads.every((maxChunks) => maxChunks <= MAX_STREAM_READ_CHUNKS)).toBe(true);
  });

  it('falls back without loss when the worker stops accepting negotiated batches', async () => {
    const legacy = { enabled: false };
    const fleet = fleetFetch(legacy);
    const streamer = streamerFor(fleet.doFetch);
    await streamer.writeChunksToStream('rollback', 'wrun_rollback', chunksOf(40));
    expect(fleet.writes).toEqual([32, 8]);

    // The worker is rolled back after it advertised the negotiated limit.
    legacy.enabled = true;
    const later = chunksOf(140).slice(40);
    await streamer.writeChunksToStream('rollback', 'wrun_rollback', later);
    await streamer.closeStream('rollback', 'wrun_rollback');

    expect(fleet.writes.slice(2)).toEqual([100, 32, 32, 32, 4]);
    expect(await readAll(streamer, 'rollback', 'wrun_rollback')).toEqual(
      Array.from({ length: 140 }, (_, index) => index),
    );
  });

  it('retries a negotiated read at the baseline after a rollback', async () => {
    const legacy = { enabled: false };
    const fleet = fleetFetch(legacy);
    const streamer = streamerFor(fleet.doFetch);
    await streamer.writeChunksToStream('rollback-read', 'wrun_rollback_read', chunksOf(80));
    await streamer.closeStream('rollback-read', 'wrun_rollback_read');

    legacy.enabled = true;
    const page = await streamer.getStreamChunks('rollback-read', 'wrun_rollback_read', {
      limit: 1000,
    });
    expect(page.data).toHaveLength(MAX_STREAM_READ_CHUNKS);
    expect(fleet.reads.slice(-2)).toEqual([NEGOTIATED_STREAM_CHUNKS, MAX_STREAM_READ_CHUNKS]);
  });
});
