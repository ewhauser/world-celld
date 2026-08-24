/**
 * Negative contract coverage for the complete apply-event path:
 * storage adapter -> remote client -> HTTP router -> WorkflowRunDO transaction.
 */
import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  RunNotSupportedError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import { slotToEventId, SPEC_VERSION_CURRENT } from '@workflow/world';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { ApplyEventOutcome } from '../src/apply-event.js';
import { rpcParse, rpcStringify } from '../src/codec.js';
import { hookIdShardName, hookTokenShardName } from '../src/indexes.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import { createStorage } from '../src/storage.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';

const SECRET = 'apply-event-negative-secret';

type ContractError = Error & {
  code?: string;
  status?: number;
  retryAfter?: number;
  runSpecVersion?: number;
  worldSpecVersion?: number;
};

let harness: Harness;

beforeAll(async () => {
  harness = await startHarness({ secret: SECRET, virtualClock: true });
});

afterAll(async () => {
  await harness.close();
});

function remoteEnv(fetchImpl?: typeof fetch) {
  return createRemoteEnv({
    fleetUrl: harness.url,
    secret: SECRET,
    ...(fetchImpl ? { fetchImpl } : {}),
  });
}

function storage(fetchImpl?: typeof fetch) {
  const env = remoteEnv(fetchImpl);
  return createStorage({
    env: { WORKFLOW_DB: env.WORKFLOW_DB, WORKFLOW_INDEX: env.WORKFLOW_INDEX },
    deploymentId: 'apply-event-negative',
  });
}

async function createRun(runId: string) {
  const result = await storage().events.create(runId, {
    eventType: 'run_created',
    eventData: {
      deploymentId: 'apply-event-negative',
      workflowName: 'apply-event-negative',
      input: [],
    },
  });
  return result.run;
}

function rpc(path: string, body: unknown): Promise<Response> {
  return fetch(`${harness.url}${path}`, {
    method: 'POST',
    headers: {
      authorization: `Bearer ${SECRET}`,
      'content-type': 'application/json',
    },
    body: rpcStringify(body),
  });
}

async function captureError(promise: Promise<unknown>): Promise<ContractError> {
  return await promise.then(
    () => {
      throw new Error('expected operation to reject');
    },
    (error: unknown) => error as ContractError,
  );
}

describe('negative apply-event contract', () => {
  it('rejects malformed schemas and run identity mismatches before dispatch or sequence allocation', async () => {
    const runId = 'wrun_negative_schema';
    await createRun(runId);
    const cell = harness.fleet.cell('runs', runId);
    const before = structuredClone(cell.storage.data);
    const beforeAlarm = cell.storage.alarmAt;

    for (const request of [
      { data: { eventType: 'run_started' } },
      { runId: '', data: { eventType: 'run_started' } },
      { runId: 42, data: { eventType: 'run_started' } },
      { runId, data: { eventType: 'run_started' }, params: { eventCount: '1' } },
    ]) {
      const response = await rpc(`/v1/rpc/runs/${runId}/applyEvent`, [request]);
      expect(response.status).toBe(400);
      await expect(response.json()).resolves.toMatchObject({ error: { name: 'BadRequest' } });
      expect(cell.storage.data).toEqual(before);
    }

    const malformedEvents: unknown[] = [
      { eventType: 'run_exploded', eventData: {} },
      { eventType: 'run_completed' },
      {
        eventType: 'step_created',
        correlationId: 'bad-step',
        eventData: { stepName: 42, input: [] },
      },
      {
        eventType: 'hook_created',
        correlationId: 'bad-hook',
        eventData: { token: 42 },
      },
      {
        eventType: 'wait_created',
        correlationId: 'bad-wait',
        eventData: { resumeAt: 'not-a-date' },
      },
      { eventType: 'run_cancelled', eventData: { cancelReason: 42 } },
    ];

    for (const data of malformedEvents) {
      const response = await rpc(`/v1/rpc/runs/${runId}/applyEvent`, [{ runId, data }]);
      expect(response.status).toBe(400);
      await expect(response.json()).resolves.toMatchObject({ error: { name: 'BadRequest' } });
      expect(cell.storage.data).toEqual(before);
      expect(cell.storage.alarmAt).toBe(beforeAlarm);
    }

    const malformedTag = await fetch(`${harness.url}/v1/rpc/runs/${runId}/applyEvent`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${SECRET}`,
        'content-type': 'application/json',
      },
      body: JSON.stringify([
        {
          runId,
          data: {
            eventType: 'step_created',
            correlationId: 'bad-tag',
            eventData: {
              stepName: 'bad-tag',
              input: { __type: 'Date', iso: 42 },
            },
          },
        },
      ]),
    });
    expect(malformedTag.status).toBe(400);
    await expect(malformedTag.json()).resolves.toMatchObject({
      error: { name: 'BadRequest', message: 'malformed rpc body' },
    });

    const mismatch = await captureError(
      remoteEnv()
        .WORKFLOW_DB.get(remoteEnv().WORKFLOW_DB.idFromName(runId))
        .applyEvent({
          runId: 'wrun_wrong_identity',
          data: { eventType: 'run_started' },
        }),
    );
    expect(mismatch).toMatchObject({ name: 'BadRequest', status: 400 });
    expect(mismatch.message).toContain('does not match');
    expect(harness.fleet.cell('runs', 'wrun_wrong_identity').storage.data.size).toBe(0);
    expect(cell.storage.data).toEqual(before);

    harness.fleet.restartCell('runs', runId);
    const validResponse = await rpc(`/v1/rpc/runs/${runId}/applyEvent`, [
      {
        runId,
        ignoredEnvelopeField: true,
        data: {
          eventType: 'step_created',
          correlationId: 'valid-after-rejection',
          ignoredEventField: true,
          eventData: {
            stepName: 'valid-after-rejection',
            input: [],
            ignoredEventDataField: true,
          },
        },
      },
    ]);
    expect(validResponse.status).toBe(200);
    const valid = rpcParse<ApplyEventOutcome>(await validResponse.text());
    expect(valid).toMatchObject({
      ok: true,
      event: { eventId: slotToEventId(2), eventType: 'step_created' },
      step: { stepId: 'valid-after-rejection' },
    });
    if (!valid.ok || !valid.event || !valid.step) throw new Error('expected valid event outcome');
    expect(valid.event).not.toHaveProperty('ignoredEventField');
    expect(valid.event.eventData).not.toHaveProperty('ignoredEventDataField');
    expect(valid.step).not.toHaveProperty('ignoredEventDataField');
    expect(cell.storage.data.get('event_sequence')).toBe(2);
  });

  it('validates canonical event requests before hook-index admission', async () => {
    const runId = 'wrun_negative_preindex_validation';
    await createRun(runId);
    let publicCalls = 0;
    const countedFetch: typeof fetch = async (input, init) => {
      publicCalls += 1;
      return await fetch(input, init);
    };
    const eventStorage = storage(countedFetch);

    await expect(
      eventStorage.events.create(runId, {
        eventType: 'hook_created',
        correlationId: 'malformed-before-index',
        eventData: { token: 'malformed-before-index-token', isWebhook: 'yes' },
      } as never),
    ).rejects.toMatchObject({ name: 'ZodError' });

    expect(publicCalls).toBe(0);
    expect(harness.fleet.cell('runs', runId).storage.data.get('event_sequence')).toBe(1);
    expect(
      harness.fleet.cell('hook-tokens', hookTokenShardName('malformed-before-index-token')).storage
        .data.size,
    ).toBe(0);
    expect(
      harness.fleet.cell('hook-ids', hookIdShardName('malformed-before-index')).storage.data.size,
    ).toBe(0);
  });

  it('preserves typed domain codes and details through HTTP outcomes and the storage client', async () => {
    const runId = 'wrun_negative_domains';
    await createRun(runId);
    const env = remoteEnv();
    const runStub = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(runId));
    const missingStep = {
      eventType: 'step_completed' as const,
      correlationId: 'missing-step',
      eventData: { result: [] },
    };

    await expect(runStub.applyEvent({ runId, data: missingStep })).resolves.toEqual({
      ok: false,
      code: 'STEP_NOT_FOUND',
      message: 'Step "missing-step" not found',
    });

    const eventStorage = storage();
    const stepError = await captureError(eventStorage.events.create(runId, missingStep));
    expect(stepError).toMatchObject({
      name: 'WorkflowWorldError',
      message: 'Step "missing-step" not found',
      status: 404,
      code: 'STEP_NOT_FOUND',
    });
    expect(WorkflowWorldError.is(stepError)).toBe(true);

    const waitError = await captureError(
      eventStorage.events.create(runId, {
        eventType: 'wait_completed',
        correlationId: 'missing-wait',
      }),
    );
    expect(waitError).toMatchObject({
      name: 'WorkflowWorldError',
      message: 'Wait "missing-wait" not found',
      status: 404,
      code: 'WAIT_NOT_FOUND',
    });

    const hookError = await captureError(
      eventStorage.events.create(runId, {
        eventType: 'hook_received',
        correlationId: 'missing-hook',
        eventData: { payload: [] },
      }),
    );
    expect(HookNotFoundError.is(hookError)).toBe(true);
    expect(hookError.code).toBe('HOOK_NOT_FOUND');

    const missingRunError = await captureError(
      eventStorage.events.create('wrun_negative_missing', { eventType: 'run_started' }),
    );
    expect(WorkflowRunNotFoundError.is(missingRunError)).toBe(true);
    expect(missingRunError.code).toBe('RUN_NOT_FOUND');

    expect(harness.fleet.cell('runs', runId).storage.data.get('event_sequence')).toBe(1);
    const createdStep = await eventStorage.events.create(runId, {
      eventType: 'step_created',
      correlationId: 'duplicate-step',
      eventData: { stepName: 'duplicate-step', input: [] },
    });
    expect(createdStep.event?.eventId).toBe(slotToEventId(2));
    const duplicateError = await captureError(
      eventStorage.events.create(runId, {
        eventType: 'step_created',
        correlationId: 'duplicate-step',
        eventData: { stepName: 'duplicate-step', input: [] },
      }),
    );
    expect(EntityConflictError.is(duplicateError)).toBe(true);
    expect(duplicateError.code).toBe('ENTITY_CONFLICT');

    const unsupportedRunId = 'wrun_negative_unsupported';
    await createRun(unsupportedRunId);
    const unsupported = await captureError(
      eventStorage.events.create(unsupportedRunId, {
        eventType: 'run_started',
        specVersion: SPEC_VERSION_CURRENT + 1,
      }),
    );
    expect(RunNotSupportedError.is(unsupported)).toBe(true);
    expect(unsupported).toMatchObject({
      code: 'RUN_NOT_SUPPORTED',
      runSpecVersion: SPEC_VERSION_CURRENT + 1,
      worldSpecVersion: SPEC_VERSION_CURRENT,
    });

    const retryRunId = 'wrun_negative_too_early';
    await createRun(retryRunId);
    await eventStorage.events.create(retryRunId, {
      eventType: 'step_created',
      correlationId: 'retry-step',
      eventData: { stepName: 'retry-step', input: [] },
    });
    await eventStorage.events.create(retryRunId, {
      eventType: 'step_started',
      correlationId: 'retry-step',
    });
    await eventStorage.events.create(retryRunId, {
      eventType: 'step_retrying',
      correlationId: 'retry-step',
      eventData: { error: ['retry'], retryAfter: new Date(harness.fleet.now + 5_000) },
    });
    const tooEarly = await captureError(
      eventStorage.events.create(retryRunId, {
        eventType: 'step_started',
        correlationId: 'retry-step',
      }),
    );
    expect(TooEarlyError.is(tooEarly)).toBe(true);
    expect(tooEarly).toMatchObject({ code: 'TOO_EARLY', retryAfter: 5 });
    expect(harness.fleet.cell('runs', retryRunId).storage.data.get('event_sequence')).toBe(4);
    harness.fleet.advance(5_000);
    const validRetry = await eventStorage.events.create(retryRunId, {
      eventType: 'step_started',
      correlationId: 'retry-step',
    });
    expect(validRetry.event?.eventId).toBe(slotToEventId(5));

    await eventStorage.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });
    const terminal = await captureError(
      eventStorage.events.create(runId, {
        eventType: 'run_completed',
        eventData: { output: [] },
      }),
    );
    expect(EntityConflictError.is(terminal)).toBe(true);
    expect(terminal.code).toBe('ENTITY_CONFLICT');
    const expired = await captureError(
      eventStorage.events.create(runId, { eventType: 'run_started' }),
    );
    expect(RunExpiredError.is(expired)).toBe(true);
    expect(expired.code).toBe('RUN_EXPIRED');
  });

  it('rolls back staged hook state, resolves cancellation races, and retries densely after restart', async () => {
    const runId = 'wrun_negative_hook_rollback';
    await createRun(runId);
    const eventStorage = storage();
    const hookId = 'rollback-hook';
    const token = 'rollback-token';
    const runCell = harness.fleet.cell('runs', runId);
    const beforeAlarm = runCell.storage.alarmAt;
    runCell.storage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key.startsWith('event:'),
      new Error('injected final event write failure'),
    );

    await expect(
      eventStorage.events.create(runId, {
        eventType: 'hook_created',
        correlationId: hookId,
        eventData: { token },
      }),
    ).rejects.toThrow('injected final event write failure');

    expect(runCell.storage.data.get('event_sequence')).toBe(1);
    expect(runCell.storage.data.has(`hook:${hookId}`)).toBe(false);
    expect([...runCell.storage.data.keys()].some((key) => key.startsWith('hookcreated:'))).toBe(
      false,
    );
    expect([...runCell.storage.data.keys()].some((key) => key.startsWith('hookevent:'))).toBe(
      false,
    );
    expect(runCell.storage.alarmAt).toBe(beforeAlarm);
    expect(harness.fleet.cell('hook-tokens', hookTokenShardName(token)).storage.data.size).toBe(0);
    expect(harness.fleet.cell('hook-ids', hookIdShardName(hookId)).storage.data.size).toBe(0);

    harness.fleet.restartCell('runs', runId);
    const retry = await eventStorage.events.create(runId, {
      eventType: 'hook_created',
      correlationId: hookId,
      eventData: { token },
    });
    expect(retry.event?.eventId).toBe(slotToEventId(2));
    await expect(eventStorage.hooks.getByToken(token)).resolves.toMatchObject({ runId, hookId });

    const cancellationRunId = 'wrun_negative_hook_cancellation';
    await createRun(cancellationRunId);
    const cancellationHookId = 'cancelled-hook';
    const cancellationToken = 'cancelled-token';
    const owner = { runId: cancellationRunId, hookId: cancellationHookId };
    const env = remoteEnv();
    const admission = await env.WORKFLOW_INDEX.reserveHook(cancellationToken, owner);
    if (!admission.admitted) throw new Error('expected initial hook admission');
    const cancellationRun = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(cancellationRunId));
    await expect(
      cancellationRun.resolveHookTokenClaim({
        hookId: cancellationHookId,
        token: cancellationToken,
        claimId: admission.reservation.claimId,
      }),
    ).resolves.toEqual({ committed: false });

    let applyAttempts = 0;
    const countedFetch: typeof fetch = async (input, init) => {
      const path = new URL(input instanceof Request ? input.url : String(input)).pathname;
      if (path.endsWith(`/${cancellationRunId}/applyEvent`)) applyAttempts += 1;
      return await fetch(input, init);
    };
    const cancellationStorage = storage(countedFetch);
    const cancelled = await captureError(
      cancellationStorage.events.create(cancellationRunId, {
        eventType: 'hook_created',
        correlationId: cancellationHookId,
        eventData: { token: cancellationToken },
      }),
    );
    expect(cancelled).toMatchObject({
      name: 'WorkflowWorldError',
      status: 503,
      code: 'HOOK_CLAIM_CANCELLED',
    });
    expect(applyAttempts).toBe(1);
    expect(
      harness.fleet.cell('hook-tokens', hookTokenShardName(cancellationToken)).storage.data.size,
    ).toBe(0);
    expect(
      harness.fleet.cell('hook-ids', hookIdShardName(cancellationHookId)).storage.data.size,
    ).toBe(0);
    expect(
      [...harness.fleet.cell('runs', cancellationRunId).storage.data.keys()].some((key) =>
        key.startsWith('hook:'),
      ),
    ).toBe(false);

    const cancellationRetry = await cancellationStorage.events.create(cancellationRunId, {
      eventType: 'hook_created',
      correlationId: cancellationHookId,
      eventData: { token: cancellationToken },
    });
    expect(cancellationRetry.event?.eventId).toBe(slotToEventId(2));
    await expect(cancellationStorage.hooks.getByToken(cancellationToken)).resolves.toMatchObject({
      runId: cancellationRunId,
      hookId: cancellationHookId,
    });
  });
});
