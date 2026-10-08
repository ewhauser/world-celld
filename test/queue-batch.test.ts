import { afterAll, beforeAll, expect, test, vi } from 'vitest';
import { createQueue } from '../src/queue.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';
import { createRouter, type WorkerEnv } from '../src/worker/router.js';

const SECRET = 'queue-batch-test';
let harness: Harness;

beforeAll(async () => {
  harness = await startHarness({ secret: SECRET });
});

afterAll(async () => {
  await harness.close();
});

test('reports a failed native chunk per entry and permits retrying its key', async () => {
  const previousMode = process.env.CELLD_QUEUE_MODE;
  process.env.CELLD_QUEUE_MODE = 'native';
  const send = vi.fn<WorkerEnv['WORKFLOW_QUEUE']['send']>().mockResolvedValue(undefined);
  const sendBatch = vi
    .fn<NonNullable<WorkerEnv['WORKFLOW_QUEUE']['sendBatch']>>()
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(new Error('broker unavailable'));
  const router = createRouter({
    WORKFLOW_DB: harness.fleet.namespace('runs'),
    WORKFLOW_STREAMS: harness.fleet.namespace('streams'),
    WORKFLOW_RUN_CATALOG: harness.fleet.namespace('run-catalog'),
    WORKFLOW_HOOK_TOKENS: harness.fleet.namespace('hook-tokens'),
    WORKFLOW_HOOK_IDS: harness.fleet.namespace('hook-ids'),
    WORKFLOW_QUEUE: { send, sendBatch },
    WORLD_SECRET: SECRET,
  });
  try {
    const env = createRemoteEnv({
      fleetUrl: 'https://world.internal',
      secret: SECRET,
      fetchImpl: async (input, init) => router(new Request(input, init)),
    });
    const queue = createQueue({
      env: { WORKFLOW_QUEUE: env.WORKFLOW_QUEUE },
      deploymentId: 'batch-test',
    });
    const messages = Array.from({ length: 101 }, (_, index) => ({
      message: { __healthCheck: true as const, correlationId: `chunk-${index}` },
      opts: { idempotencyKey: `chunk-${index}` },
    }));
    const results = await queue.queueBatch!('__wkf_workflow_batch', messages);
    expect(sendBatch.mock.calls.map(([entries]) => entries.length)).toEqual([100, 1]);
    expect(results.slice(0, 100).every((result) => result.error === undefined)).toBe(true);
    expect(results[100]).toMatchObject({
      messageId: null,
      error: 'broker unavailable',
      retryable: true,
    });
    const retry = await queue.queue(
      '__wkf_workflow_batch',
      messages[100].message,
      messages[100].opts,
    );
    expect(retry.messageId).toMatch(/^msg_/);
    expect(send).toHaveBeenCalledOnce();
  } finally {
    if (previousMode === undefined) delete process.env.CELLD_QUEUE_MODE;
    else process.env.CELLD_QUEUE_MODE = previousMode;
  }
});

test('returns a permanent per-entry result for invalid payloads', async () => {
  const previousMode = process.env.CELLD_QUEUE_MODE;
  process.env.CELLD_QUEUE_MODE = 'native';
  try {
    const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET });
    const queue = createQueue({
      env: { WORKFLOW_QUEUE: env.WORKFLOW_QUEUE },
      deploymentId: 'batch-test',
    });
    const results = await queue.queueBatch!('__wkf_workflow_batch', [
      { message: {} as Parameters<typeof queue.queue>[1] },
      { message: { __healthCheck: true, correlationId: 'valid' } },
    ]);
    expect(results[0]).toMatchObject({ messageId: null, retryable: false });
    expect(results[1]?.error).toBeUndefined();
  } finally {
    if (previousMode === undefined) delete process.env.CELLD_QUEUE_MODE;
    else process.env.CELLD_QUEUE_MODE = previousMode;
  }
});

test('splits native batches at the broker byte limit', async () => {
  const previousMode = process.env.CELLD_QUEUE_MODE;
  process.env.CELLD_QUEUE_MODE = 'native';
  try {
    const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET });
    const queue = createQueue({
      env: { WORKFLOW_QUEUE: env.WORKFLOW_QUEUE },
      deploymentId: 'batch-test',
    });
    const before = harness.queueBatchCalls.length;
    const results = await queue.queueBatch!(
      '__wkf_workflow_batch',
      Array.from({ length: 60 }, (_, index) => ({
        message: { __healthCheck: true as const, correlationId: `${index}${'x'.repeat(5000)}` },
      })),
    );
    expect(results.every((result) => result.error === undefined)).toBe(true);
    const calls = harness.queueBatchCalls.slice(before);
    expect(calls.length).toBeGreaterThan(1);
    expect(calls.reduce((sum, count) => sum + count, 0)).toBe(60);
    expect(calls.every((count) => count <= 100)).toBe(true);
  } finally {
    if (previousMode === undefined) delete process.env.CELLD_QUEUE_MODE;
    else process.env.CELLD_QUEUE_MODE = previousMode;
  }
});

test('preserves per-message delays in one native batch', async () => {
  const previousMode = process.env.CELLD_QUEUE_MODE;
  process.env.CELLD_QUEUE_MODE = 'native';
  try {
    const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET });
    const queue = createQueue({
      env: { WORKFLOW_QUEUE: env.WORKFLOW_QUEUE },
      deploymentId: 'batch-test',
    });
    const before = harness.queuePublications.length;
    const results = await queue.queueBatch!('__wkf_workflow_batch', [
      { message: { __healthCheck: true, correlationId: 'immediate' } },
      {
        message: { __healthCheck: true, correlationId: 'delayed' },
        opts: { delaySeconds: 42 },
      },
    ]);
    expect(results.every((result) => result.error === undefined)).toBe(true);
    const publications = harness.queuePublications.slice(before);
    expect(publications[0].delaySeconds).toBe(0);
    expect(publications[1].delaySeconds).toBeGreaterThan(0);
    expect(publications[1].delaySeconds).toBeLessThanOrEqual(42);
  } finally {
    if (previousMode === undefined) delete process.env.CELLD_QUEUE_MODE;
    else process.env.CELLD_QUEUE_MODE = previousMode;
  }
});

test('falls back to single-send routes during a worker rollout', async () => {
  const previousMode = process.env.CELLD_QUEUE_MODE;
  process.env.CELLD_QUEUE_MODE = 'native';
  let requests = 0;
  try {
    const env = createRemoteEnv({
      fleetUrl: harness.url,
      secret: SECRET,
      fetchImpl: async (input, init) => {
        requests++;
        const url =
          typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
        if (new URL(url).pathname === '/v1/queue/send-batch') {
          return Response.json(
            { error: { name: 'NotFound', message: 'unknown route' } },
            { status: 404 },
          );
        }
        return fetch(input, init);
      },
    });
    const queue = createQueue({
      env: { WORKFLOW_QUEUE: env.WORKFLOW_QUEUE },
      deploymentId: 'batch-test',
    });
    const results = await queue.queueBatch!('__wkf_workflow_batch', [
      { message: { __healthCheck: true, correlationId: 'old-worker-a' } },
      { message: { __healthCheck: true, correlationId: 'old-worker-b' } },
    ]);
    expect(results.every((result) => result.error === undefined)).toBe(true);
    expect(requests).toBe(3);
  } finally {
    if (previousMode === undefined) delete process.env.CELLD_QUEUE_MODE;
    else process.env.CELLD_QUEUE_MODE = previousMode;
  }
});
