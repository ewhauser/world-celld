import type { CreateEventRequest, Event, Step } from '@workflow/world';
import { slotToEventId } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import type { ApplyEventRequest } from '../src/apply-event.js';
import { CLEANUP_RECORD_KEY, TERMINAL_CLEANUP_KEY } from '../src/retention.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import { createStorage } from '../src/storage.js';
import { FakeFleet, type FakeStorageMutation } from '../src/testing/fake-cell.js';
import { startHarness } from '../src/testing/http-harness.js';
import { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';

const START_TIME = new Date('2026-10-01T18:00:00.000Z').getTime();
const RUN_ID = 'wrun_event_faults';
const RUN_INPUT = {
  deploymentId: 'event-faults',
  workflowName: 'event-faults',
  input: new Uint8Array([0, 127, 255]),
};
const LAZY_STEP: CreateEventRequest = {
  eventType: 'step_started',
  correlationId: 'fault-step',
  eventData: { stepName: 'fault-step', input: new Uint8Array([7, 8, 9]) },
};

function setup() {
  let fleet: FakeFleet;
  fleet = new FakeFleet({ runs: WorkflowRunDO }, { clock: () => fleet.now }, START_TIME);
  const storage = fleet.cell('runs', RUN_ID).storage;
  const run = () => fleet.cell('runs', RUN_ID).instance as WorkflowRunDO;
  const apply = (
    data: ApplyEventRequest['data'],
    options?: Omit<ApplyEventRequest, 'runId' | 'data'>,
  ) => run().applyEvent({ runId: RUN_ID, data, ...options });
  const create = () => apply({ eventType: 'run_created', eventData: RUN_INPUT });
  const events = () =>
    [...storage.data.entries()]
      .filter(([key]) => key.startsWith('event:'))
      .map(([, event]) => event as Event)
      .toSorted((a, b) => a.eventId.localeCompare(b.eventId));
  return { fleet, storage, run, apply, create, events };
}

const putKey = (key: string) => (mutation: FakeStorageMutation) =>
  mutation.operation === 'put' && mutation.key === key;

describe('workflow event transaction failures', () => {
  it.each([
    ['bootstrap event append', putKey(`event:${slotToEventId(2)}`)],
    ['sequence persistence', putKey('event_sequence')],
    ['queue claim scope persistence', putKey('queue-claim-scope')],
  ])(
    'rolls back resilient start when %s fails, then retries densely after restart',
    async (_name, fault) => {
      const { fleet, storage, apply, events } = setup();
      storage.failNextMutation(fault, new Error('bootstrap fault'));
      const start = { eventType: 'run_started' as const, eventData: RUN_INPUT };

      await expect(apply(start)).rejects.toThrow('bootstrap fault');
      expect(storage.data.size).toBe(0);
      expect(storage.alarmAt).toBeNull();

      fleet.restartCell('runs', RUN_ID);
      await expect(apply(start)).resolves.toMatchObject({ ok: true, run: { status: 'running' } });
      expect(events().map((event) => [event.eventId, event.eventType])).toEqual([
        [slotToEventId(1), 'run_created'],
        [slotToEventId(2), 'run_started'],
      ]);
      const replay = await apply(start);
      expect(replay).toMatchObject({ ok: true, events: events(), hasMore: false });
      expect(replay).not.toHaveProperty('event');
      expect(storage.data.get('event_sequence')).toBe(2);
    },
  );

  it('rolls back bootstrap when the response preload fails after both events were staged', async () => {
    const { storage, apply, events } = setup();
    storage.failNextRead(
      (read) => read.operation === 'list' && read.options?.prefix === 'event:',
      new Error('preload read fault'),
    );
    const start = { eventType: 'run_started' as const, eventData: RUN_INPUT };
    await expect(apply(start)).rejects.toThrow('preload read fault');
    expect(storage.data.size).toBe(0);
    await expect(apply(start)).resolves.toMatchObject({ ok: true });
    expect(events()).toHaveLength(2);
    expect(storage.data.get('event_sequence')).toBe(2);
  });

  it.each([
    ['creation index', (mutation: FakeStorageMutation) => mutation.key.startsWith('stepcreated:')],
    ['step-created event', putKey(`event:${slotToEventId(2)}`)],
    ['step-started event', putKey(`event:${slotToEventId(3)}`)],
    ['sequence persistence', putKey('event_sequence')],
  ])('rolls back lazy step creation when %s fails', async (_name, fault) => {
    const { fleet, storage, apply, create, events } = setup();
    await create();
    const before = structuredClone(storage.data);
    storage.failNextMutation(fault, new Error('lazy step fault'));

    await expect(apply(LAZY_STEP)).rejects.toThrow('lazy step fault');
    expect(storage.data).toEqual(before);
    fleet.restartCell('runs', RUN_ID);
    await expect(apply(LAZY_STEP)).resolves.toMatchObject({
      ok: true,
      stepCreated: true,
      step: { status: 'running', attempt: 1 },
    });
    expect(events().map((event) => event.eventId)).toEqual([
      slotToEventId(1),
      slotToEventId(2),
      slotToEventId(3),
    ]);
    expect(await apply(LAZY_STEP)).toMatchObject({ ok: false, code: 'ENTITY_CONFLICT' });
    expect(storage.data.get('event_sequence')).toBe(3);
  });

  it.each([CLEANUP_RECORD_KEY, TERMINAL_CLEANUP_KEY])(
    'rolls back completion and alarms when the %s record fails',
    async (key) => {
      const { fleet, storage, apply, create } = setup();
      await create();
      await apply({ eventType: 'run_started' });
      await apply({
        eventType: 'hook_created',
        correlationId: 'retained-hook',
        eventData: { token: 'retained-token' },
      });
      await apply({
        eventType: 'wait_created',
        correlationId: 'retained-wait',
        eventData: { resumeAt: new Date(START_TIME + 10_000) },
      });
      const before = structuredClone(storage.data);
      const alarmBefore = storage.alarmAt;
      const completion = {
        eventType: 'run_completed' as const,
        eventData: { output: new Uint8Array([1]) },
      };
      const options = { cleanup: { retentionMs: 60_000 } };
      storage.failNextMutation(putKey(key), new Error('retention bookkeeping fault'));

      await expect(apply(completion, options)).rejects.toThrow('retention bookkeeping fault');
      expect(storage.data).toEqual(before);
      expect(storage.alarmAt).toBe(alarmBefore);
      fleet.restartCell('runs', RUN_ID);
      await expect(apply(completion, options)).resolves.toMatchObject({
        ok: true,
        event: { eventId: slotToEventId(5) },
        run: { status: 'completed' },
      });
      expect(storage.data.get(CLEANUP_RECORD_KEY)).toMatchObject({ phase: 'retained' });
      expect(storage.data.get(TERMINAL_CLEANUP_KEY)).toMatchObject({ phase: 'hooks' });
      expect(storage.alarmAt).toBe(START_TIME + 1);
      expect(storage.data.has('hook:retained-hook')).toBe(true);
      expect(storage.data.has('wait:retained-wait')).toBe(true);
    },
  );

  it('rolls back an appended resume if its durable deduplication claim fails', async () => {
    const { fleet, storage, apply, create, events } = setup();
    await create();
    await apply({ eventType: 'run_started' });
    await apply({
      eventType: 'hook_created',
      correlationId: 'resume-hook',
      eventData: { token: 'resume-token' },
    });
    const before = structuredClone(storage.data);
    const resume = {
      eventType: 'hook_received' as const,
      correlationId: 'resume-hook',
      eventData: { payload: new Uint8Array([3, 2, 1]) },
    };
    const options = {
      params: {
        resumeId: 'resume-once',
        resumePayloadDigest: 'digest',
        preloadEvents: true as const,
      },
    };
    storage.failNextMutation(putKey('hookresume:resume-once'), new Error('resume claim fault'));
    await expect(apply(resume, options)).rejects.toThrow('resume claim fault');
    expect(storage.data).toEqual(before);

    fleet.restartCell('runs', RUN_ID);
    const resumed = await apply(resume, options);
    expect(resumed).toMatchObject({
      ok: true,
      event: { eventId: slotToEventId(4) },
      hasMore: false,
    });
    expect(await apply(resume, options)).toEqual(resumed);
    expect(events().filter((event) => event.eventType === 'hook_received')).toHaveLength(1);
    expect(storage.data.get('event_sequence')).toBe(4);
  });

  it.each(['hookreleased:dispose-hook', `event:${slotToEventId(3)}`, 'event_sequence'])(
    'restores a disposed hook when the %s write fails',
    async (key) => {
      const { fleet, storage, apply, create, events } = setup();
      await create();
      await apply({
        eventType: 'hook_created',
        correlationId: 'dispose-hook',
        eventData: { token: 'dispose-token' },
      });
      const before = structuredClone(storage.data);
      storage.failNextMutation(putKey(key), new Error('dispose fault'));
      const disposal = { eventType: 'hook_disposed' as const, correlationId: 'dispose-hook' };
      await expect(apply(disposal)).rejects.toThrow('dispose fault');
      expect(storage.data).toEqual(before);

      fleet.restartCell('runs', RUN_ID);
      const disposed = await apply(disposal);
      expect(disposed).toMatchObject({
        ok: true,
        event: { eventId: slotToEventId(3) },
        releasedHooks: [{ hookId: 'dispose-hook', token: 'dispose-token' }],
      });
      const replay = await apply(disposal);
      expect(replay).toMatchObject({
        ok: true,
        releasedHooks: [{ hookId: 'dispose-hook', token: 'dispose-token' }],
      });
      expect(replay).not.toHaveProperty('event');
      expect(events().filter((event) => event.eventType === 'hook_disposed')).toHaveLength(1);
      expect(storage.data.has('hook:dispose-hook')).toBe(false);
      expect([...storage.data.keys()].some((entry) => entry.startsWith('hookcreated:'))).toBe(
        false,
      );
    },
  );
});

describe('workflow fault state correctness', () => {
  it('preserves serialized step errors across the public HTTP create, get, and list boundaries', async () => {
    const secret = 'workflow-error-boundary';
    const harness = await startHarness({ secret, virtualClock: true });
    try {
      const env = createRemoteEnv({ fleetUrl: harness.url, secret });
      const storage = createStorage({ env, deploymentId: 'event-faults' });
      for (const eventType of ['step_failed', 'step_retrying'] as const) {
        for (const error of [new Uint8Array([0, 1, 127, 128, 255]), null]) {
          const created = await storage.events.create(null, {
            eventType: 'run_created',
            eventData: RUN_INPUT,
          });
          const runId = created.run.runId;
          await storage.events.create(runId, LAZY_STEP);
          const failed = await storage.events.create(runId, {
            eventType,
            correlationId: 'fault-step',
            eventData: { error },
          });
          expect(failed.step?.error).toEqual(error);
          expect(failed.event?.eventData).toMatchObject({ error });
          harness.fleet.restartCell('runs', runId);
          expect((await storage.steps.get(runId, 'fault-step')).error).toEqual(error);
          expect((await storage.steps.list({ runId })).data[0]?.error).toEqual(error);
          const retryErrors: unknown[] = [];
          if (eventType === 'step_retrying') {
            const started = await storage.events.create(runId, {
              eventType: 'step_started',
              correlationId: 'fault-step',
            });
            retryErrors.push(started.step.error);
            const completed = await storage.events.create(runId, {
              eventType: 'step_completed',
              correlationId: 'fault-step',
              eventData: { result: new Uint8Array([1]) },
            });
            retryErrors.push(completed.step.error);
          }
          expect(retryErrors).toEqual(eventType === 'step_retrying' ? [error, error] : []);
        }
      }
    } finally {
      await harness.close();
    }
  });

  it('keeps retry deadlines and attempt counts consistent across failed writes and restart', async () => {
    const { fleet, storage, apply, create, events } = setup();
    await create();
    await apply(LAZY_STEP);
    const beforeRetry = structuredClone(storage.data);
    const retryAfter = new Date(START_TIME + 1_501);
    const retry = {
      eventType: 'step_retrying' as const,
      correlationId: 'fault-step',
      eventData: { error: new Uint8Array([4, 5]), retryAfter },
    };
    storage.failNextMutation(putKey(`event:${slotToEventId(4)}`), new Error('retry state fault'));
    await expect(apply(retry)).rejects.toThrow('retry state fault');
    expect(storage.data).toEqual(beforeRetry);
    await expect(apply(retry)).resolves.toMatchObject({
      ok: true,
      step: { status: 'pending', attempt: 1, retryAfter },
    });
    fleet.restartCell('runs', RUN_ID);
    const start = { eventType: 'step_started' as const, correlationId: 'fault-step' };
    const beforeEarlyStart = structuredClone(storage.data);
    expect(await apply(start)).toMatchObject({
      ok: false,
      code: 'TOO_EARLY',
      retryAfterSeconds: 2,
    });
    expect(storage.data).toEqual(beforeEarlyStart);
    fleet.advance(1_500);
    expect(await apply(start)).toMatchObject({
      ok: false,
      code: 'TOO_EARLY',
      retryAfterSeconds: 1,
    });
    expect(storage.data).toEqual(beforeEarlyStart);
    fleet.advance(1);

    storage.failNextMutation(putKey(`event:${slotToEventId(5)}`), new Error('retry start fault'));
    await expect(apply(start)).rejects.toThrow('retry start fault');
    expect(storage.data).toEqual(beforeEarlyStart);
    const started = await apply(start);
    expect(started).toMatchObject({
      ok: true,
      event: { eventId: slotToEventId(5) },
      step: { status: 'running', attempt: 2, startedAt: new Date(START_TIME) },
    });
    if (!started.ok) throw new Error('expected successful retry');
    expect(started.step?.retryAfter).toBeUndefined();
    expect(events().map((event) => event.eventId)).toEqual(
      Array.from({ length: 5 }, (_, index) => slotToEventId(index + 1)),
    );
  });

  it('serializes competing terminal transitions without appending the losing event', async () => {
    const { storage, apply, create, events } = setup();
    await create();
    await apply({ eventType: 'run_started' });
    const outcomes = await Promise.all([
      apply({ eventType: 'run_completed', eventData: { output: new Uint8Array([1]) } }),
      apply({ eventType: 'run_failed', eventData: { error: { message: 'competing failure' } } }),
    ]);
    expect(outcomes.filter((outcome) => outcome.ok)).toHaveLength(1);
    expect(outcomes.filter((outcome) => !outcome.ok)).toEqual([
      expect.objectContaining({ ok: false, code: 'ENTITY_CONFLICT' }),
    ]);
    expect(events()).toHaveLength(3);
    expect(storage.data.get('event_sequence')).toBe(3);
  });

  it.each(['step_failed', 'step_retrying'] as const)(
    'preserves the serialized error on %s in both entity and event after restart',
    async (eventType) => {
      const { fleet, storage, apply, create, run } = setup();
      await create();
      await apply(LAZY_STEP);
      // Includes bytes that would be lost by stringification or Error-shaped projection.
      const error = new Uint8Array([0, 1, 127, 128, 254, 255]);
      const outcome = await apply({ eventType, correlationId: 'fault-step', eventData: { error } });
      expect(outcome).toMatchObject({ ok: true, step: { error }, event: { eventData: { error } } });
      fleet.restartCell('runs', RUN_ID);
      expect(await run().getStep('fault-step')).toMatchObject({ ok: true, value: { error } });
      expect((storage.data.get('step:fault-step') as Step).error).toBeInstanceOf(Uint8Array);
    },
  );

  it('preserves legacy thrown values without projecting them onto an Error shape', async () => {
    for (const error of [
      null,
      42,
      'retry text',
      { cause: { reason: 'network' }, message: 'retry' },
    ]) {
      const { apply, create } = setup();
      await create();
      await apply(LAZY_STEP);
      expect(
        await apply({
          eventType: 'step_failed',
          correlationId: 'fault-step',
          eventData: { error },
        }),
      ).toMatchObject({ ok: true, step: { error } });
    }
  });

  it('rejects a changed hook token during orphan index repair while accepting the original token', async () => {
    const { storage, apply, create } = setup();
    await create();
    await apply({
      eventType: 'hook_created',
      correlationId: 'orphan-hook',
      eventData: { token: 'original-token' },
    });
    const before = structuredClone(storage.data);
    const changed = await apply(
      {
        eventType: 'hook_created',
        correlationId: 'orphan-hook',
        eventData: { token: 'different-token' },
      },
      { tokenHolder: null },
    );
    expect(changed).toMatchObject({ ok: false, code: 'ENTITY_CONFLICT' });
    expect(storage.data).toEqual(before);
    expect(
      await apply(
        {
          eventType: 'hook_created',
          correlationId: 'orphan-hook',
          eventData: { token: 'original-token' },
        },
        { tokenHolder: null },
      ),
    ).toMatchObject({ ok: true, hookToIndex: { token: 'original-token' } });
    expect(storage.data).toEqual(before);
  });
});
