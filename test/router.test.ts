/**
 * Wire-protocol tests: real router + real DO classes (on fake cells) behind
 * node:http, driven by the real remote client. Everything except celld.
 */
import { WorkflowRunNotFoundError } from '@workflow/errors';
import { createConnection } from 'node:net';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { rpcStringify } from '../src/codec.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import {
  MAX_STREAM_WRITE_CHUNKS,
  STREAM_BATCH_CONTENT_TYPE,
  decodeStreamWriteBatch,
} from '../src/stream-protocol.js';
import { createStorage } from '../src/storage.js';
import { createStreamer } from '../src/streamer.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';
import { createRouter } from '../src/worker/router.js';

const SECRET = 'test-secret';

let harness: Harness;

beforeAll(async () => {
  harness = await startHarness({ secret: SECRET });
});

afterAll(async () => {
  await harness.close();
});

function rpc(path: string, body: unknown, token?: string): Promise<Response> {
  return fetch(`${harness.url}${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: rpcStringify(body),
  });
}

describe('test harness lifecycle', () => {
  it('closes with an incomplete client connection still open', async () => {
    const isolated = await startHarness({ secret: SECRET });
    const target = new URL(isolated.url);
    const socket = createConnection({ host: target.hostname, port: Number(target.port) });
    let close: Promise<void> | undefined;
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
      await new Promise<void>((resolve, reject) => {
        socket.once('connect', resolve);
        socket.once('error', reject);
      });
      socket.write(`GET /v1/health HTTP/1.1\r\nHost: ${target.host}\r\n`);

      close = isolated.close();
      await expect(
        Promise.race([
          close,
          new Promise<never>((_, reject) => {
            timeout = setTimeout(() => reject(new Error('harness close timed out')), 1_000);
          }),
        ]),
      ).resolves.toBeUndefined();
    } finally {
      if (timeout) clearTimeout(timeout);
      socket.destroy();
      await (close ?? isolated.close());
    }
  });
});

describe('router auth and shape', () => {
  it('serves /v1/health unauthenticated', async () => {
    const res = await fetch(`${harness.url}/v1/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.ok).toBe(true);
    expect(body.name).toBe('world-celld');
    expect(body.specVersion).toBeTypeOf('number');
  });

  it('rejects rpc without a token', async () => {
    const res = await rpc('/v1/rpc/runs/wrun_x/getRun', []);
    expect(res.status).toBe(401);
  });

  it('rejects rpc with a wrong token', async () => {
    const res = await rpc('/v1/rpc/runs/wrun_x/getRun', [], 'wrong');
    expect(res.status).toBe(401);
  });

  it('fails closed with 503 when no secret is configured', async () => {
    const noSecret = await startHarness({});
    try {
      const res = await fetch(`${noSecret.url}/v1/rpc/runs/wrun_x/getRun`, {
        method: 'POST',
        headers: { authorization: 'Bearer anything' },
        body: '[]',
      });
      expect(res.status).toBe(503);
    } finally {
      await noSecret.close();
    }
  });

  it('404s unknown bindings and non-whitelisted methods', async () => {
    expect((await rpc('/v1/rpc/nope/x/getRun', [], SECRET)).status).toBe(404);
    expect((await rpc('/v1/rpc/runs/wrun_x/fetch', [], SECRET)).status).toBe(404);
    expect((await rpc('/v1/rpc/runs/wrun_x/alarm', [], SECRET)).status).toBe(404);
    expect((await rpc('/v1/rpc/runs/wrun_x/constructor', [], SECRET)).status).toBe(404);
    expect((await rpc('/v1/rpc/streams/stream%3Ax/writeChunk', [], SECRET)).status).toBe(404);
  });

  it('authenticates and validates the hard-cutover binary stream route', async () => {
    const path = '/v1/streams/stream%3Ax/chunks?runId=wrun_x';
    expect((await fetch(`${harness.url}${path}`)).status).toBe(401);
    const malformed = await fetch(`${harness.url}${path}`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${SECRET}`,
        'content-type': STREAM_BATCH_CONTENT_TYPE,
      },
      body: new Uint8Array(),
    });
    expect(malformed.status).toBe(400);
  });

  it('400s malformed bodies', async () => {
    const res = await rpc('/v1/rpc/runs/wrun_x/getRun', { not: 'an array' }, SECRET);
    expect(res.status).toBe(400);
  });

  it('413s an oversized RPC body even when content-length is absent', async () => {
    const router = createRouter({
      WORKFLOW_DB: harness.fleet.namespace('runs'),
      WORKFLOW_STREAMS: harness.fleet.namespace('streams'),
      WORKFLOW_INDEX: harness.fleet.namespace('index'),
      WORKFLOW_QUEUE: harness.fleet.namespace('queue'),
      WORLD_SECRET: SECRET,
    });
    const request = new Request('http://world.test/v1/rpc/runs/wrun_x/getRun', {
      method: 'POST',
      headers: {
        authorization: `Bearer ${SECRET}`,
        'content-type': 'application/json',
      },
      body: ' '.repeat(32 * 1024 * 1024 + 1),
    });
    expect(request.headers.has('content-length')).toBe(false);

    const response = await router(request);
    expect(response.status).toBe(413);
  });
});

describe('full stack: vendored storage over the wire', () => {
  const transport = () => createRemoteEnv({ fleetUrl: harness.url, secret: SECRET });

  it('atomically publishes hook indexes over the wire', async () => {
    const env = transport();
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-test',
    });
    const created = await storage.events.create(null, {
      eventType: 'run_created',
      eventData: { deploymentId: 'wire-test', workflowName: 'wire-hooks', input: [] },
    });

    await storage.events.create(created.run.runId, {
      eventType: 'hook_created',
      correlationId: 'wire-hook',
      eventData: { token: 'wire-hook-token' },
    });

    await expect(storage.hooks.getByToken('wire-hook-token')).resolves.toMatchObject({
      runId: created.run.runId,
      hookId: 'wire-hook',
    });
  });

  it('creates a run via applyEvent and reads it back with Dates intact', async () => {
    const env = transport();
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-test',
    });

    const result = await storage.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'wire-test',
        workflowName: 'wire-workflow',
        input: [new Uint8Array([1, 2, 3])],
      },
    });

    const runId = result.run.runId;
    expect(runId).toMatch(/^wrun_/);
    expect(result.run.createdAt).toBeInstanceOf(Date);

    const run = await storage.runs.get(runId);
    expect(run.status).toBe('pending');
    expect(run.createdAt).toBeInstanceOf(Date);
    expect(run.updatedAt).toBeInstanceOf(Date);
    expect('input' in run).toBe(true);
    if (!('input' in run)) throw new Error('expected run input');
    expect(run.input?.[0]).toBeInstanceOf(Uint8Array);

    const listed = await storage.runs.list({ workflowName: 'wire-workflow' });
    expect(listed.data.some((r) => r.runId === runId)).toBe(true);
  });

  it('lists runs with N+1 public RPCs, bounded run fanout, and value-bearing index pages', async () => {
    let publicRpcs = 0;
    let activeRunReads = 0;
    let maxActiveRunReads = 0;
    const paths = new Map<string, number>();
    const countedFetch: typeof fetch = async (input, init) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      publicRpcs++;
      paths.set(url.pathname, (paths.get(url.pathname) ?? 0) + 1);

      const isRunRead = url.pathname.endsWith('/getRun');
      if (isRunRead) {
        activeRunReads++;
        maxActiveRunReads = Math.max(maxActiveRunReads, activeRunReads);
        // Make overlap deterministic so the assertion distinguishes serial,
        // bounded, and unbounded implementations.
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
      try {
        return await fetch(input, init);
      } finally {
        if (isRunRead) activeRunReads--;
      }
    };
    const env = createRemoteEnv({
      fleetUrl: harness.url,
      secret: SECRET,
      fetchImpl: countedFetch,
    });
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-test',
    });
    const runIds: string[] = [];
    for (let i = 0; i < 20; i++) {
      const created = await storage.events.create(null, {
        eventType: 'run_created',
        eventData: {
          deploymentId: 'wire-test',
          workflowName: 'wire-list-fanout',
          input: [i],
        },
      });
      runIds.push(created.run.runId);
    }

    let indexLists = 0;
    let indexGets = 0;
    let runStorageGets = 0;
    const restorers: Array<() => void> = [];
    const indexStorage = harness.fleet.cell('index', 'index').storage;
    const originalIndexList = indexStorage.list.bind(indexStorage);
    const originalIndexGet = indexStorage.get.bind(indexStorage);
    indexStorage.list = async <T>(options) => {
      indexLists++;
      return originalIndexList<T>(options);
    };
    indexStorage.get = async <T>(key: string) => {
      indexGets++;
      return originalIndexGet<T>(key);
    };
    restorers.push(() => {
      indexStorage.list = originalIndexList;
      indexStorage.get = originalIndexGet;
    });
    for (const runId of runIds) {
      const runStorage = harness.fleet.cell('runs', runId).storage;
      const originalGet = runStorage.get.bind(runStorage);
      runStorage.get = async <T>(key: string) => {
        runStorageGets++;
        return originalGet<T>(key);
      };
      restorers.push(() => {
        runStorage.get = originalGet;
      });
    }

    try {
      // Reproduce the legacy 2N+1 protocol against the same cells so the
      // before/after counts include identical router and storage layers.
      publicRpcs = 0;
      paths.clear();
      const legacyPage = await env.WORKFLOW_INDEX.list({
        prefix: 'run:wire-list-fanout:',
        limit: 50,
      });
      for (const key of legacyPage.keys) {
        const raw = await env.WORKFLOW_INDEX.get(key.name);
        if (!raw) continue;
        const { runId } = JSON.parse(raw) as { runId: string };
        await env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(runId)).getRun();
      }
      expect(publicRpcs).toBe(41);
      expect(maxActiveRunReads).toBe(1);
      expect(indexLists).toBe(1);
      expect(indexGets).toBe(20);
      expect(runStorageGets).toBe(60);

      publicRpcs = 0;
      maxActiveRunReads = 0;
      paths.clear();
      indexLists = 0;
      indexGets = 0;
      runStorageGets = 0;
      const listed = await storage.runs.list({
        workflowName: 'wire-list-fanout',
        pagination: { limit: 20 },
      });

      expect(listed.data).toHaveLength(20);
      expect(publicRpcs).toBe(21);
      expect(paths.get('/v1/rpc/index/index/list')).toBe(1);
      expect(paths.get('/v1/rpc/index/index/get')).toBeUndefined();
      expect(Array.from(paths.entries()).filter(([path]) => path.endsWith('/getRun'))).toHaveLength(
        20,
      );
      expect(maxActiveRunReads).toBe(8);
      expect(indexLists).toBe(1);
      expect(indexGets).toBe(0);
      expect(runStorageGets).toBe(60);

      await Promise.all(
        runIds.map((runId) =>
          storage.events.create(runId, {
            eventType: 'run_completed',
            eventData: { output: [] },
          }),
        ),
      );
      publicRpcs = 0;
      maxActiveRunReads = 0;
      paths.clear();
      indexLists = 0;
      indexGets = 0;
      runStorageGets = 0;

      const pending = await storage.runs.list({
        workflowName: 'wire-list-fanout',
        status: 'pending',
        pagination: { limit: 20 },
      });
      expect(pending.data).toEqual([]);
      expect(publicRpcs).toBe(1);
      expect(paths.get('/v1/rpc/index/index/list')).toBe(1);
      expect(maxActiveRunReads).toBe(0);
      expect(indexLists).toBe(1);
      expect(indexGets).toBe(0);
      expect(runStorageGets).toBe(0);
    } finally {
      for (const restore of restorers) restore();
    }
  });

  it('reconstructs typed errors across the wire', async () => {
    const env = transport();
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-test',
    });

    const error = await storage.runs.get('wrun_does_not_exist').catch((e) => e);
    expect(WorkflowRunNotFoundError.is(error)).toBe(true);
  });

  it('streams chunks through StreamDO over the wire', async () => {
    const env = transport();
    const streamer = createStreamer({ env: { WORKFLOW_STREAMS: env.WORKFLOW_STREAMS } });

    await streamer.writeToStream('wire-stream', 'wrun_s1', 'hello ');
    await streamer.writeToStream('wire-stream', 'wrun_s1', new Uint8Array([0xf0, 0x9f]));
    await streamer.closeStream('wire-stream', 'wrun_s1');

    const info = await streamer.getStreamInfo('wire-stream', 'wrun_s1');
    expect(info).toEqual({ tailIndex: 1, done: true });

    const chunks = await streamer.getStreamChunks('wire-stream', 'wrun_s1', {});
    expect(chunks.data).toHaveLength(2);
    expect(chunks.data[1].data).toBeInstanceOf(Uint8Array);
    expect(Array.from(chunks.data[1].data)).toEqual([0xf0, 0x9f]);

    expect(await streamer.listStreamsByRunId('wrun_s1')).toEqual(['wire-stream']);

    // A write to a closed stream surfaces the DO's thrown error.
    await expect(streamer.writeToStream('wire-stream', 'wrun_s1', 'nope')).rejects.toThrow(
      /closed stream/,
    );
  });

  it('uses two public calls for a cold maximum batch and binary payloads end-to-end', async () => {
    const calls: Array<{ url: string; contentType: string | null; body?: Uint8Array }> = [];
    const countingFetch: typeof fetch = async (input, init) => {
      calls.push({
        url: input instanceof Request ? input.url : input instanceof URL ? input.href : input,
        contentType: new Headers(init?.headers).get('content-type'),
        body: init?.body instanceof Uint8Array ? new Uint8Array(init.body) : undefined,
      });
      return await fetch(input, init);
    };
    const env = createRemoteEnv({
      fleetUrl: harness.url,
      secret: SECRET,
      fetchImpl: countingFetch,
    });
    const streamer = createStreamer({ env: { WORKFLOW_STREAMS: env.WORKFLOW_STREAMS } });
    const name = `wire-batch-${Date.now()}`;
    const runId = `wrun_batch_${Date.now()}`;
    const chunks = Array.from({ length: MAX_STREAM_WRITE_CHUNKS }, (_, index) =>
      Uint8Array.of(0, index, 255),
    );

    await streamer.streams.writeMulti!(runId, name, chunks);

    expect(calls).toHaveLength(2);
    expect(calls[0].url).toContain('/v1/rpc/streams/');
    expect(calls[1].url).toContain('/v1/streams/');
    expect(calls[1].contentType).toBe(STREAM_BATCH_CONTENT_TYPE);
    expect(calls[1].body).toBeInstanceOf(Uint8Array);
    expect(decodeStreamWriteBatch(calls[1].body!)).toEqual(chunks);

    calls.length = 0;
    const streamStorage = harness.fleet.cell('streams', `stream:${name}`).storage;
    streamStorage.resetOperationCounts();
    await streamer.streams.writeMulti!(runId, name, chunks);

    expect(calls).toHaveLength(1);
    expect(calls[0].url).toContain('/v1/streams/');
    expect(streamStorage.operationCounts).toMatchObject({
      transaction: 1,
      get: 1,
      putMany: 1,
      put: 0,
    });

    calls.length = 0;
    const page = await streamer.getStreamChunks(name, runId, { limit: 32 });
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toContain('/v1/streams/');
    expect(page.data).toHaveLength(32);
    expect(page.data.map((chunk) => Array.from(chunk.data))).toEqual(
      chunks.map((chunk) => Array.from(chunk)),
    );
  });

  it('holds one idle public read and wakes it on close without storage polling', async () => {
    const calls: string[] = [];
    const countingFetch: typeof fetch = async (input, init) => {
      calls.push(input instanceof Request ? input.url : input instanceof URL ? input.href : input);
      return await fetch(input, init);
    };
    const env = createRemoteEnv({
      fleetUrl: harness.url,
      secret: SECRET,
      fetchImpl: countingFetch,
    });
    const streamer = createStreamer({
      env: { WORKFLOW_STREAMS: env.WORKFLOW_STREAMS },
      streamLongPollMs: 200,
    });
    const name = `wire-idle-${Date.now()}`;
    const runId = `wrun_idle_${Date.now()}`;
    const stream = await streamer.streams.get(runId, name);
    const reader = stream.getReader();
    const pending = reader.read();

    await new Promise((resolve) => setTimeout(resolve, 30));
    const readCalls = () => calls.filter((url) => url.includes('/v1/streams/')).length;
    expect(readCalls()).toBe(1);
    const streamStorage = harness.fleet.cell('streams', `stream:${name}`).storage;
    streamStorage.resetOperationCounts();

    await new Promise((resolve) => setTimeout(resolve, 40));
    expect(readCalls()).toBe(1);
    expect(streamStorage.operationCounts).toMatchObject({ get: 0, list: 0 });

    await streamer.closeStream(name, runId);
    await expect(pending).resolves.toMatchObject({ done: true });
  });
});
