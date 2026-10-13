import { slotToEventId, type BatchEventRequest } from '@workflow/world';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import { createStorage } from '../src/storage.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';

const secret = 'batch-contract-secret';
const runId = 'wrun_batch_contract';
let harness: Harness;

function storage(fetchImpl?: typeof fetch, enableEventBatching = true) {
  const env = createRemoteEnv({
    fleetUrl: harness.url,
    secret,
    ...(fetchImpl ? { fetchImpl } : {}),
  });
  return createStorage({
    env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
    deploymentId: 'batch-test',
    enableEventBatching,
  });
}

const create = (id: string): BatchEventRequest => ({
  event: {
    eventType: 'step_created',
    correlationId: id,
    eventData: { stepName: id, input: { payload: 'x'.repeat(1024) } },
  },
});
const start = (id: string): BatchEventRequest => ({
  event: {
    eventType: 'step_started',
    correlationId: id,
    eventData: { stepName: id, ownerMessageId: 'delivery-1' },
  },
});

beforeAll(async () => {
  harness = await startHarness({ secret });
  const s = storage();
  await s.events.create(runId, {
    eventType: 'run_created',
    eventData: {
      deploymentId: 'batch-test',
      workflowName: 'batch-test',
      input: [],
    },
  });
  await s.events.create(runId, { eventType: 'run_started' });
});
afterAll(async () => {
  await harness.close();
});

describe('events.createBatch', () => {
  it('is absent by default for clients talking to older workers', () => {
    expect('createBatch' in storage(undefined, false).events).toBe(false);
  });

  it('commits ordered pairs and returns input-aligned conflicts on duplicate delivery', async () => {
    const s = storage();
    const batch = [create('batch-a'), start('batch-a'), create('batch-b'), start('batch-b')];
    const first = await s.events.createBatch!(runId, batch);
    expect(first.results.map((result) => result.status)).toEqual([200, 200, 200, 200]);
    expect(first.results.map((result) => result.event?.eventId)).toEqual(
      [3, 4, 5, 6].map(slotToEventId),
    );
    expect(first.results[1].step).toMatchObject({ status: 'running', attempt: 1 });
    expect(first.results[1].event?.eventData).toMatchObject({ ownerMessageId: 'delivery-1' });

    harness.fleet.restartCell('runs', runId);
    const duplicate = await s.events.createBatch!(runId, batch);
    expect(duplicate.results.map((result) => result.status)).toEqual([409, 409, 409, 409]);
    expect((await s.steps.get(runId, 'batch-a')).attempt).toBe(1);
    expect((await s.events.list({ runId })).data.map((event) => event.eventId)).toEqual(
      [1, 2, 3, 4, 5, 6].map(slotToEventId),
    );
  });

  it('does not retry a commit-ambiguous ownership claim after a lost response', async () => {
    let calls = 0;
    const loseResponse: typeof fetch = async (input, init) => {
      const response = await fetch(input, init);
      if (
        new URL(input instanceof Request ? input.url : String(input)).pathname.endsWith(
          '/applyEventBatch',
        )
      ) {
        calls++;
        throw new Error('lost batch response');
      }
      return response;
    };
    await expect(
      storage(loseResponse).events.createBatch!(runId, [create('batch-lost'), start('batch-lost')]),
    ).rejects.toThrow('fleet unreachable');
    expect(calls).toBe(1);
    expect((await storage().steps.get(runId, 'batch-lost')).attempt).toBe(1);
    expect(
      (
        await storage().events.createBatch!(runId, [create('batch-lost'), start('batch-lost')])
      ).results.map((result) => result.status),
    ).toEqual([409, 409]);
  });

  it('rejects invalid envelopes before mutation and keeps slots dense after conflicts', async () => {
    const s = storage();
    const prior = (await s.events.list({ runId })).data.length;
    await expect(
      s.events.createBatch!(runId, [
        create('batch-invalid'),
        {
          event: {
            eventType: 'run_created',
            eventData: {
              deploymentId: 'batch-test',
              workflowName: 'batch-test',
              input: [],
            },
          },
        } as BatchEventRequest,
      ]),
    ).rejects.toThrow('does not support');
    expect((await s.events.list({ runId })).data).toHaveLength(prior);
    const mixed = await s.events.createBatch!(runId, [
      create('batch-a'),
      create('batch-after-conflict'),
    ]);
    expect(mixed.results.map((result) => result.status)).toEqual([409, 200]);
    const events = (await s.events.list({ runId })).data;
    expect(events.map((event) => event.eventId)).toEqual(
      Array.from({ length: events.length }, (_, index) => slotToEventId(index + 1)),
    );
  });

  it('rolls back every entry on a commit fault, then replays densely after restart', async () => {
    const s = storage();
    const cell = harness.fleet.cell('runs', runId);
    const prior = (await s.events.list({ runId })).data.length;
    cell.storage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key === 'event_sequence',
      new Error('injected commit fault'),
    );
    await expect(
      s.events.createBatch!(runId, [create('batch-fault'), start('batch-fault')]),
    ).rejects.toThrow('injected commit fault');
    expect((await s.events.list({ runId })).data).toHaveLength(prior);
    harness.fleet.restartCell('runs', runId);
    const replay = await s.events.createBatch!(runId, [
      create('batch-fault'),
      start('batch-fault'),
    ]);
    expect(replay.results.map((result) => result.event?.eventId)).toEqual(
      [prior + 1, prior + 2].map(slotToEventId),
    );
    expect((await s.steps.get(runId, 'batch-fault')).attempt).toBe(1);
  });

  it("supports the fold's wait creation and fences terminal runs", async () => {
    const s = storage();
    const wait = {
      event: {
        eventType: 'wait_created' as const,
        correlationId: 'batch-wait',
        eventData: { resumeAt: new Date(Date.now() + 1_000) },
      },
    };
    const first = await s.events.createBatch!(runId, [wait]);
    expect(first.results[0]).toMatchObject({ status: 200, wait: { status: 'waiting' } });
    expect((await s.events.createBatch!(runId, [wait])).results[0].status).toBe(409);

    const terminalRun = 'wrun_batch_terminal';
    await s.events.create(terminalRun, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'batch-test',
        workflowName: 'batch-test',
        input: [],
      },
    });
    await s.events.create(terminalRun, { eventType: 'run_completed', eventData: { output: null } });
    const rejected = await s.events.createBatch!(terminalRun, [create('late-step')]);
    expect(rejected.results[0].status).toBe(409);
    expect((await s.events.list({ runId: terminalRun })).data).toHaveLength(2);
  });
});
