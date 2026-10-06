import { EntityConflictError } from '@workflow/errors';
import { afterAll, beforeAll, expect, test } from 'vitest';
import { createQueue } from '../src/queue.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import { createStorage } from '../src/storage.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';

const SECRET = 'world-v8-feature-evaluation';
let harness: Harness;

beforeAll(async () => {
  harness = await startHarness({ secret: SECRET });
});

afterAll(async () => {
  await harness.close();
});

function storage() {
  const env = createRemoteEnv({ fleetUrl: harness.url, secret: SECRET });
  return createStorage({
    env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
    deploymentId: 'feature-evaluation',
  });
}

async function createRun(runId: string) {
  await storage().events.create(runId, {
    eventType: 'run_created',
    eventData: { deploymentId: 'feature-evaluation', workflowName: 'evaluation', input: [] },
  });
}

test('hook resume claim converges concurrent writes across a cell restart', async () => {
  const runId = 'wrun_feature_eval_resume';
  const writer = storage();
  await createRun(runId);
  await writer.events.create(runId, {
    eventType: 'hook_created',
    correlationId: 'hook-eval',
    eventData: { token: 'token-eval' },
  });
  const received = {
    eventType: 'hook_received' as const,
    correlationId: 'hook-eval',
    eventData: { token: 'token-eval', payload: new Uint8Array([1, 2, 3]) },
  };
  const params = { resumeId: 'resume-eval', resumePayloadDigest: 'payload-eval' };
  const results = await Promise.all(
    Array.from({ length: 32 }, () => writer.events.create(runId, received, params)),
  );
  expect(new Set(results.map((result) => result.event?.eventId)).size).toBe(1);
  harness.fleet.restartCell('runs', runId);
  expect((await writer.events.create(runId, received, params)).event?.eventId).toBe(
    results[0].event?.eventId,
  );
  await expect(
    writer.events.create(runId, received, { ...params, resumePayloadDigest: 'changed' }),
  ).rejects.toSatisfy((error) => EntityConflictError.is(error));
  const events = await writer.events.list({ runId, pagination: { sortOrder: 'asc' } });
  const receivedEvents = events.data.filter((event) => event.eventType === 'hook_received');
  expect(receivedEvents).toHaveLength(1);
  expect(receivedEvents[0]?.resumeId).toBe(params.resumeId);
});

test('a force request cannot take a token from another run', async () => {
  const writer = storage();
  const ownerRunId = 'wrun_feature_eval_owner';
  const contenderRunId = 'wrun_feature_eval_contender';
  await Promise.all([createRun(ownerRunId), createRun(contenderRunId)]);
  await writer.events.create(ownerRunId, {
    eventType: 'hook_created',
    correlationId: 'owner-hook',
    eventData: { token: 'held-token' },
  });
  const result = await writer.events.create(contenderRunId, {
    eventType: 'hook_created',
    correlationId: 'contender-hook',
    eventData: { token: 'held-token', force: true },
  });
  expect(result.event?.eventType).toBe('hook_conflict');
  expect((await writer.hooks.getByToken('held-token')).runId).toBe(ownerRunId);
  const events = await writer.events.list({ runId: ownerRunId });
  expect(events.data.some((event) => event.eventType === 'hook_disposed')).toBe(false);
});

test('current queue fan-out sends one public RPC per message', async () => {
  const previousMode = process.env.CELLD_QUEUE_MODE;
  process.env.CELLD_QUEUE_MODE = 'native';
  let publicRpcs = 0;
  try {
    const env = createRemoteEnv({
      fleetUrl: harness.url,
      secret: SECRET,
      fetchImpl: async (input, init) => {
        publicRpcs++;
        return fetch(input, init);
      },
    });
    const queue = createQueue({
      env: { WORKFLOW_QUEUE: env.WORKFLOW_QUEUE },
      deploymentId: 'feature-evaluation',
      baseUrl: 'http://127.0.0.1:9',
    });
    const count = 64;
    const started = performance.now();
    const results = await Promise.all(
      Array.from({ length: count }, (_, index) =>
        queue.queue('__wkf_workflow_evaluation', {
          __healthCheck: true,
          correlationId: `fanout-${index}`,
        }),
      ),
    );
    expect(results.every((result) => result.messageId !== null)).toBe(true);
    expect(publicRpcs).toBe(count);
    console.log(
      `WORLD_V8_QUEUE_FANOUT ${JSON.stringify({ count, publicRpcs, elapsedMs: performance.now() - started })}`,
    );
  } finally {
    if (previousMode === undefined) delete process.env.CELLD_QUEUE_MODE;
    else process.env.CELLD_QUEUE_MODE = previousMode;
  }
});
