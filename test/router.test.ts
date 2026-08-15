/**
 * Wire-protocol tests: real router + real DO classes (on fake cells) behind
 * node:http, driven by the real remote client. Everything except celld.
 */
import { WorkflowRunNotFoundError } from '@workflow/errors';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { rpcStringify } from '../src/codec.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
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

    const runId = result.run!.runId;
    expect(runId).toMatch(/^wrun_/);
    expect(result.run!.createdAt).toBeInstanceOf(Date);

    const run = await storage.runs.get(runId);
    expect(run.status).toBe('pending');
    expect(run.createdAt).toBeInstanceOf(Date);
    expect(run.updatedAt).toBeInstanceOf(Date);
    if ('input' in run) {
      expect(run.input?.[0]).toBeInstanceOf(Uint8Array);
    }

    const listed = await storage.runs.list({ workflowName: 'wire-workflow' });
    expect(listed.data.some((r) => r.runId === runId)).toBe(true);
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
});
