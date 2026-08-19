/**
 * Wire-protocol tests: real router + real DO classes (on fake cells) behind
 * node:http, driven by the real remote client. Everything except celld.
 */
import { HookNotFoundError, WorkflowRunNotFoundError } from '@workflow/errors';
import { createConnection } from 'node:net';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { rpcParse, rpcStringify } from '../src/codec.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import { allRunCatalogShardNames, hookIdShardName, runCatalogShardName } from '../src/indexes.js';
import {
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_READ_BYTES,
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
    expect((await rpc('/v1/index/runs/list', [])).status).toBe(401);
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
    expect((await rpc('/v1/rpc/run-catalog/x/upsertRun', [], SECRET)).status).toBe(404);
    expect((await rpc('/v1/rpc/hook-tokens/x/finalize', [], SECRET)).status).toBe(404);
    expect((await rpc('/v1/index/runs/nope', [], SECRET)).status).toBe(404);
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

    const undersizedRead = await fetch(
      `${harness.url}${path}&startIndex=0&maxChunks=1&maxBytes=${MAX_STREAM_CHUNK_BYTES - 1}&waitMs=0`,
      { headers: { authorization: `Bearer ${SECRET}` } },
    );
    expect(undersizedRead.status).toBe(400);
    await expect(undersizedRead.json()).resolves.toMatchObject({
      error: { name: 'StreamProtocolError' },
    });
  });

  it('400s malformed bodies', async () => {
    const res = await rpc('/v1/rpc/runs/wrun_x/getRun', { not: 'an array' }, SECRET);
    expect(res.status).toBe(400);
    expect((await rpc('/v1/index/runs/list', { not: 'an array' }, SECRET)).status).toBe(400);
  });

  it('413s an oversized RPC body even when content-length is absent', async () => {
    const router = createRouter({
      WORKFLOW_DB: harness.fleet.namespace('runs'),
      WORKFLOW_STREAMS: harness.fleet.namespace('streams'),
      WORKFLOW_RUN_CATALOG: harness.fleet.namespace('run-catalog'),
      WORKFLOW_RUN_FENCES: harness.fleet.namespace('run-fences'),
      WORKFLOW_HOOK_TOKENS: harness.fleet.namespace('hook-tokens'),
      WORKFLOW_HOOK_IDS: harness.fleet.namespace('hook-ids'),
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

  it('creates a correlated event with one public RPC and no unused index mutations', async () => {
    let publicRpcs = 0;
    const paths = new Map<string, number>();
    const countedFetch: typeof fetch = async (input, init) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      publicRpcs++;
      paths.set(url.pathname, (paths.get(url.pathname) ?? 0) + 1);
      return fetch(input, init);
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
    const created = await storage.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'wire-test',
        workflowName: 'wire-correlation-fanout',
        input: [],
      },
    });

    publicRpcs = 0;
    paths.clear();
    const runStorage = harness.fleet.cell('runs', created.run.runId).storage;
    const catalogStorage = harness.fleet.cell(
      'run-catalog',
      runCatalogShardName(created.run.runId),
    ).storage;
    const runMutations: string[] = [];
    const catalogMutations: string[] = [];
    const originalRunMutation = runStorage.maybeFailMutation.bind(runStorage);
    const originalCatalogMutation = catalogStorage.maybeFailMutation.bind(catalogStorage);
    runStorage.maybeFailMutation = (mutation) => {
      runMutations.push(`${mutation.operation}:${mutation.key}`);
      originalRunMutation(mutation);
    };
    catalogStorage.maybeFailMutation = (mutation) => {
      catalogMutations.push(`${mutation.operation}:${mutation.key}`);
      originalCatalogMutation(mutation);
    };

    try {
      await storage.events.create(created.run.runId, {
        eventType: 'step_created',
        correlationId: 'wire-step',
        eventData: { stepName: 'wire-step', input: [] },
      });
    } finally {
      runStorage.maybeFailMutation = originalRunMutation;
      catalogStorage.maybeFailMutation = originalCatalogMutation;
    }

    expect(publicRpcs).toBe(1);
    expect(paths).toEqual(new Map([[`/v1/rpc/runs/${created.run.runId}/applyEvent`, 1]]));
    expect(runMutations).toHaveLength(4);
    expect(runMutations).not.toContainEqual(expect.stringContaining('retention:index:'));
    expect(catalogMutations).toEqual([]);

    publicRpcs = 0;
    paths.clear();
    const correlated = await storage.events.listByCorrelationId({
      runId: created.run.runId,
      correlationId: 'wire-step',
      pagination: { sortOrder: 'asc' },
    });
    expect(correlated.data).toHaveLength(1);
    expect(correlated.data[0]).toMatchObject({
      runId: created.run.runId,
      correlationId: 'wire-step',
      eventType: 'step_created',
    });
    expect(publicRpcs).toBe(1);
    expect(paths).toEqual(new Map([[`/v1/rpc/runs/${created.run.runId}/listEvents`, 1]]));
  });

  it('commits terminal catalog metadata and its per-run fence through one public RPC', async () => {
    let publicRpcs = 0;
    const paths = new Map<string, number>();
    const countedFetch: typeof fetch = async (input, init) => {
      const url = new URL(input instanceof Request ? input.url : String(input));
      publicRpcs++;
      paths.set(url.pathname, (paths.get(url.pathname) ?? 0) + 1);
      return fetch(input, init);
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
    const created = await storage.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'wire-test',
        workflowName: 'wire-terminal-hook-batch',
        input: [],
      },
    });
    for (let hook = 0; hook < 3; hook++) {
      await storage.events.create(created.run.runId, {
        eventType: 'hook_created',
        correlationId: `hook-${hook}`,
        eventData: { token: `terminal-token-${hook}` },
      });
    }

    publicRpcs = 0;
    paths.clear();
    await storage.events.create(created.run.runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });

    expect(publicRpcs).toBe(2);
    expect(paths).toEqual(
      new Map([
        [`/v1/rpc/runs/${created.run.runId}/applyEvent`, 1],
        ['/v1/index/runs/commit', 1],
      ]),
    );
  });

  it('merges every catalog shard behind one public call with bounded run fanout', async () => {
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

    let catalogLists = 0;
    let runStorageGets = 0;
    const restorers: Array<() => void> = [];
    const runStorages = runIds.map((runId) => harness.fleet.cell('runs', runId).storage);
    const resetRunStorageCalls = () => {
      for (const runStorage of runStorages) runStorage.operationCalls.length = 0;
    };
    const countRunStorageGets = () =>
      runStorages.reduce(
        (total, runStorage) =>
          total + runStorage.operationCalls.filter((call) => call.operation === 'get').length,
        0,
      );
    for (const name of allRunCatalogShardNames()) {
      const catalogStorage = harness.fleet.cell('run-catalog', name).storage;
      const originalList = catalogStorage.list.bind(catalogStorage);
      catalogStorage.list = async <T>(options) => {
        catalogLists++;
        return originalList<T>(options);
      };
      restorers.push(() => {
        catalogStorage.list = originalList;
      });
    }
    try {
      publicRpcs = 0;
      maxActiveRunReads = 0;
      paths.clear();
      catalogLists = 0;
      runStorageGets = 0;
      resetRunStorageCalls();
      const listed = await storage.runs.list({
        workflowName: 'wire-list-fanout',
        pagination: { limit: 20 },
      });

      expect(listed.data).toHaveLength(20);
      expect(publicRpcs).toBe(1 + 20);
      expect(paths.get('/v1/index/runs/list')).toBe(1);
      expect(Array.from(paths.entries()).filter(([path]) => path.endsWith('/getRun'))).toHaveLength(
        20,
      );
      expect(maxActiveRunReads).toBe(8);
      expect(catalogLists).toBe(16);
      runStorageGets = countRunStorageGets();
      expect(runStorageGets).toBe(20);

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
      catalogLists = 0;
      runStorageGets = 0;
      resetRunStorageCalls();

      const pending = await storage.runs.list({
        workflowName: 'wire-list-fanout',
        status: 'pending',
        pagination: { limit: 20 },
      });
      expect(pending.data).toEqual([]);
      expect(publicRpcs).toBe(1);
      expect(paths).toEqual(new Map([['/v1/index/runs/list', 1]]));
      expect(maxActiveRunReads).toBe(0);
      expect(catalogLists).toBe(16);
      runStorageGets = countRunStorageGets();
      expect(runStorageGets).toBe(0);
    } finally {
      for (const restore of restorers) restore();
    }
  });

  it('repairs a run catalog commit after the authoritative run transaction succeeds', async () => {
    const env = transport();
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-index-repair',
    });
    const runId = 'wrun_catalog_failure_repair';
    const catalogStorage = harness.fleet.cell('run-catalog', runCatalogShardName(runId)).storage;
    catalogStorage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key.startsWith('run:'),
      new Error('injected catalog commit failure'),
    );

    const request = {
      eventType: 'run_created' as const,
      eventData: {
        deploymentId: 'wire-index-repair',
        workflowName: 'wire-catalog-repair',
        input: [],
      },
    };
    await expect(storage.events.create(runId, request)).rejects.toThrow(
      /injected catalog commit failure/,
    );
    await expect(storage.runs.get(runId)).resolves.toMatchObject({ runId });
    await expect(storage.runs.list({ workflowName: 'wire-catalog-repair' })).resolves.toMatchObject(
      { data: [] },
    );

    await expect(storage.events.create(runId, request)).resolves.toMatchObject({
      run: { runId },
    });
    const listed = await storage.runs.list({ workflowName: 'wire-catalog-repair' });
    expect(listed.data.map((run) => run.runId)).toEqual([runId]);
  });

  it('repairs a partial hook publication without duplicating ownership or events', async () => {
    const env = transport();
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-hook-repair',
    });
    const runId = 'wrun_hook_publish_repair';
    await storage.events.create(runId, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'wire-hook-repair',
        workflowName: 'wire-hook-repair',
        input: [],
      },
    });
    const hookId = 'hook-publication-repair';
    const token = 'token-publication-repair';
    const idStorage = harness.fleet.cell('hook-ids', hookIdShardName(hookId)).storage;
    idStorage.failNextMutation(
      (mutation) =>
        mutation.operation === 'put' && mutation.key === `hookid:${encodeURIComponent(hookId)}`,
      new Error('injected hook id publication failure'),
    );
    const request = {
      eventType: 'hook_created' as const,
      correlationId: hookId,
      eventData: { token },
    };

    await expect(storage.events.create(runId, request)).rejects.toThrow(
      /injected hook id publication failure/,
    );
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId, hookId });
    await expect(storage.hooks.get(hookId)).rejects.toSatisfy((error) =>
      HookNotFoundError.is(error),
    );

    await expect(storage.events.create(runId, request)).resolves.toMatchObject({
      hook: { runId, hookId, token },
    });
    await expect(storage.hooks.get(hookId)).resolves.toMatchObject({ runId, hookId, token });
    const events = await storage.events.list({ runId, pagination: { sortOrder: 'asc' } });
    expect(events.data.filter((event) => event.eventType === 'hook_created')).toHaveLength(1);
  });

  it('keeps token ownership after a commit-then-lost applyEvent response', async () => {
    const normalEnv = transport();
    const normalStorage = createStorage({
      env: {
        WORKFLOW_DB: normalEnv.WORKFLOW_DB,
        WORKFLOW_INDEX: normalEnv.WORKFLOW_INDEX,
      },
      deploymentId: 'wire-lost-response',
    });
    const ownerRunId = 'wrun_lost_response_owner';
    const contenderRunId = 'wrun_lost_response_contender';
    for (const runId of [ownerRunId, contenderRunId]) {
      await normalStorage.events.create(runId, {
        eventType: 'run_created',
        eventData: {
          deploymentId: 'wire-lost-response',
          workflowName: 'wire-lost-response',
          input: [],
        },
      });
    }

    let loseApplyResponse = true;
    const ambiguousEnv = createRemoteEnv({
      fleetUrl: harness.url,
      secret: SECRET,
      fetchImpl: async (input, init) => {
        const response = await fetch(input, init);
        const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
        if (loseApplyResponse && path === `/v1/rpc/runs/${ownerRunId}/applyEvent`) {
          loseApplyResponse = false;
          throw new Error('injected lost applyEvent response');
        }
        return response;
      },
    });
    const ambiguousStorage = createStorage({
      env: {
        WORKFLOW_DB: ambiguousEnv.WORKFLOW_DB,
        WORKFLOW_INDEX: ambiguousEnv.WORKFLOW_INDEX,
      },
      deploymentId: 'wire-lost-response',
    });
    const token = 'lost-response-token';
    const ownerHookId = 'lost-response-owner-hook';
    const ownerRequest = {
      eventType: 'hook_created' as const,
      correlationId: ownerHookId,
      eventData: { token },
    };
    await expect(ambiguousStorage.events.create(ownerRunId, ownerRequest)).rejects.toThrow(
      /fleet unreachable/,
    );

    const contender = await normalStorage.events.create(contenderRunId, {
      eventType: 'hook_created',
      correlationId: 'lost-response-contender-hook',
      eventData: { token },
    });
    expect(contender.event?.eventType).toBe('hook_conflict');
    await expect(normalStorage.hooks.getByToken(token)).rejects.toSatisfy((error) =>
      HookNotFoundError.is(error),
    );

    await normalStorage.events.create(ownerRunId, ownerRequest);
    await expect(normalStorage.hooks.getByToken(token)).resolves.toMatchObject({
      runId: ownerRunId,
      hookId: ownerHookId,
    });
    const ownerEvents = await normalStorage.events.list({
      runId: ownerRunId,
      pagination: { sortOrder: 'asc' },
    });
    expect(ownerEvents.data.filter((event) => event.eventType === 'hook_created')).toHaveLength(1);
  });

  it('releases token and hook-ID claims when applyEvent never reaches the RunDO', async () => {
    const normalEnv = transport();
    const normalStorage = createStorage({
      env: {
        WORKFLOW_DB: normalEnv.WORKFLOW_DB,
        WORKFLOW_INDEX: normalEnv.WORKFLOW_INDEX,
      },
      deploymentId: 'wire-pre-dispatch',
    });
    const ownerRunId = 'wrun_pre_dispatch_owner';
    const contenderRunId = 'wrun_pre_dispatch_contender';
    for (const runId of [ownerRunId, contenderRunId]) {
      await normalStorage.events.create(runId, {
        eventType: 'run_created',
        eventData: {
          deploymentId: 'wire-pre-dispatch',
          workflowName: 'wire-pre-dispatch',
          input: [],
        },
      });
    }

    let rejectBeforeDispatch = true;
    let delayedApplyRequest: Request | undefined;
    const ambiguousEnv = createRemoteEnv({
      fleetUrl: harness.url,
      secret: SECRET,
      fetchImpl: async (input, init) => {
        const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
        if (rejectBeforeDispatch && path === `/v1/rpc/runs/${ownerRunId}/applyEvent`) {
          rejectBeforeDispatch = false;
          delayedApplyRequest = new Request(input, { ...init, signal: undefined });
          throw new Error('injected pre-dispatch failure');
        }
        return fetch(input, init);
      },
    });
    const ambiguousStorage = createStorage({
      env: {
        WORKFLOW_DB: ambiguousEnv.WORKFLOW_DB,
        WORKFLOW_INDEX: ambiguousEnv.WORKFLOW_INDEX,
      },
      deploymentId: 'wire-pre-dispatch',
    });
    const token = 'pre-dispatch-token';
    await expect(
      ambiguousStorage.events.create(ownerRunId, {
        eventType: 'hook_created',
        correlationId: 'pre-dispatch-owner-hook',
        eventData: { token },
      }),
    ).rejects.toThrow(/fleet unreachable/);

    await expect(
      normalStorage.events.create(contenderRunId, {
        eventType: 'hook_created',
        correlationId: 'pre-dispatch-contender-hook',
        eventData: { token },
      }),
    ).resolves.toMatchObject({
      hook: { runId: contenderRunId, hookId: 'pre-dispatch-contender-hook', token },
    });
    await expect(normalStorage.hooks.getByToken(token)).resolves.toMatchObject({
      runId: contenderRunId,
    });

    if (!delayedApplyRequest) throw new Error('expected intercepted applyEvent request');
    const delayedResponse = await fetch(delayedApplyRequest);
    expect(delayedResponse.ok).toBe(true);
    expect(rpcParse(await delayedResponse.text())).toMatchObject({
      ok: false,
      code: 'HOOK_CLAIM_CANCELLED',
    });
    const ownerEvents = await normalStorage.events.list({
      runId: ownerRunId,
      pagination: { sortOrder: 'asc' },
    });
    expect(ownerEvents.data.filter((event) => event.eventType === 'hook_created')).toHaveLength(0);
  });

  it('rejects a hook-ID collision without publishing the contender token', async () => {
    const env = transport();
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-id-collision',
    });
    const firstRunId = 'wrun_id_collision_first';
    const secondRunId = 'wrun_id_collision_second';
    for (const runId of [firstRunId, secondRunId]) {
      await storage.events.create(runId, {
        eventType: 'run_created',
        eventData: {
          deploymentId: 'wire-id-collision',
          workflowName: 'wire-id-collision',
          input: [],
        },
      });
    }
    const hookId = 'wire-shared-hook-id';
    await storage.events.create(firstRunId, {
      eventType: 'hook_created',
      correlationId: hookId,
      eventData: { token: 'wire-first-id-token' },
    });

    const collision = await storage.events.create(secondRunId, {
      eventType: 'hook_created',
      correlationId: hookId,
      eventData: { token: 'wire-second-id-token' },
    });
    expect(collision.event?.eventType).toBe('hook_conflict');
    await expect(storage.hooks.getByToken('wire-second-id-token')).rejects.toSatisfy((error) =>
      HookNotFoundError.is(error),
    );
    await expect(storage.hooks.get(hookId)).resolves.toMatchObject({ runId: firstRunId });

    await expect(
      storage.events.create(secondRunId, {
        eventType: 'hook_created',
        correlationId: 'wire-replacement-hook-id',
        eventData: { token: 'wire-second-id-token' },
      }),
    ).resolves.toMatchObject({ hook: { runId: secondRunId } });
  });

  it('repairs a partial hook deletion from its durable release marker', async () => {
    const env = transport();
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-hook-delete-repair',
    });
    const runId = 'wrun_hook_delete_repair';
    const hookId = 'hook-delete-repair';
    const token = 'token-delete-repair';
    await storage.events.create(runId, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'wire-hook-delete-repair',
        workflowName: 'wire-hook-delete-repair',
        input: [],
      },
    });
    await storage.events.create(runId, {
      eventType: 'hook_created',
      correlationId: hookId,
      eventData: { token },
    });
    const idStorage = harness.fleet.cell('hook-ids', hookIdShardName(hookId)).storage;
    idStorage.failNextMutation(
      (mutation) =>
        mutation.operation === 'delete' && mutation.key === `hookid:${encodeURIComponent(hookId)}`,
      new Error('injected hook id deletion failure'),
    );

    const disposal = { eventType: 'hook_disposed' as const, correlationId: hookId };
    await expect(storage.events.create(runId, disposal)).rejects.toThrow(
      /injected hook id deletion failure/,
    );
    await expect(storage.events.create(runId, disposal)).resolves.toBeDefined();
    await expect(storage.hooks.getByToken(token)).rejects.toSatisfy((error) =>
      HookNotFoundError.is(error),
    );
    await expect(storage.hooks.get(hookId)).rejects.toSatisfy((error) =>
      HookNotFoundError.is(error),
    );
    const events = await storage.events.list({ runId, pagination: { sortOrder: 'asc' } });
    expect(events.data.filter((event) => event.eventType === 'hook_disposed')).toHaveLength(1);
  });

  it('converges concurrent token claims and concurrent resumes without duplicates', async () => {
    const env = transport();
    const storage = createStorage({
      env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
      deploymentId: 'wire-hook-concurrency',
    });
    const runIds = ['wrun_hook_concurrency_a', 'wrun_hook_concurrency_b'];
    await Promise.all(
      runIds.map((runId) =>
        storage.events.create(runId, {
          eventType: 'run_created',
          eventData: {
            deploymentId: 'wire-hook-concurrency',
            workflowName: 'wire-hook-concurrency',
            input: [],
          },
        }),
      ),
    );
    const token = 'concurrent-shared-token';
    const creations = await Promise.all(
      runIds.map((runId, index) =>
        storage.events.create(runId, {
          eventType: 'hook_created',
          correlationId: `concurrent-hook-${index}`,
          eventData: { token },
        }),
      ),
    );
    const owner = await storage.hooks.getByToken(token);
    expect(runIds).toContain(owner.runId);
    expect(creations.filter((result) => result.hook !== undefined)).toHaveLength(1);
    expect(creations.filter((result) => result.event?.eventType === 'hook_conflict')).toHaveLength(
      1,
    );

    const received = {
      eventType: 'hook_received' as const,
      correlationId: owner.hookId,
      eventData: { payload: ['once'] },
    };
    const params = { resumeId: 'concurrent-resume', resumePayloadDigest: 'digest' };
    const resumed = await Promise.all([
      storage.events.create(owner.runId, received, params),
      storage.events.create(owner.runId, received, params),
    ]);
    expect(resumed[0].event?.eventId).toBe(resumed[1].event?.eventId);
    const events = await storage.events.list({
      runId: owner.runId,
      pagination: { sortOrder: 'asc' },
    });
    expect(events.data.filter((event) => event.eventType === 'hook_received')).toHaveLength(1);
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

    const streamStorage = harness.fleet.cell('streams', `stream:${name}`).storage;
    const storedPayloads = Array.from(streamStorage.data.entries())
      .filter(([key]) => key.startsWith('chunk:'))
      .map(([, value]) => value as Uint8Array);
    expect(storedPayloads).toHaveLength(MAX_STREAM_WRITE_CHUNKS);
    for (const chunk of storedPayloads) {
      expect(chunk.byteOffset).toBe(0);
      expect(chunk.buffer.byteLength).toBe(chunk.byteLength);
    }

    calls.length = 0;
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
    streamStorage.resetOperationCounts();
    const page = await streamer.getStreamChunks(name, runId, { limit: 32 });
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toContain('/v1/streams/');
    expect(page.data).toHaveLength(32);
    expect(page.data.map((chunk) => Array.from(chunk.data))).toEqual(
      chunks.map((chunk) => Array.from(chunk)),
    );
    expect(streamStorage.operationCounts).toMatchObject({
      get: 0,
      getMany: 2,
      list: 0,
    });
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

  it('removes the DO waiter when an HTTP long-poll client disconnects', async () => {
    const name = `stream:wire-abort-${Date.now()}`;
    const runId = `wrun_abort_${Date.now()}`;
    const controller = new AbortController();
    const url = new URL(`${harness.url}/v1/streams/${encodeURIComponent(name)}/chunks`);
    url.searchParams.set('runId', runId);
    url.searchParams.set('startIndex', '0');
    url.searchParams.set('maxChunks', '32');
    url.searchParams.set('maxBytes', String(MAX_STREAM_READ_BYTES));
    url.searchParams.set('waitMs', '1000');

    const pending = fetch(url, {
      headers: { authorization: `Bearer ${SECRET}` },
      signal: controller.signal,
    });
    const waiterCount = () =>
      (
        harness.fleet.cell('streams', name).instance as unknown as {
          waiters: Set<unknown>;
        }
      ).waiters.size;
    await vi.waitFor(() => expect(waiterCount()).toBe(1));

    controller.abort(new DOMException('test disconnect', 'AbortError'));
    await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    await vi.waitFor(() => expect(waiterCount()).toBe(0));
  });
});
