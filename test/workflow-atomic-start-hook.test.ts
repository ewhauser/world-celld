import { HookConflictError } from '@workflow/errors';
import { startHookId, type RunCreatedEventRequest } from '@workflow/world';
import { atomicStartHookConformance } from '@workflow/world/test-support/atomic-start-hook-conformance.js';
import { ulid } from 'ulid';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createCelldWorld, createWorld, type CelldWorld } from '../src/index.js';
import { HOOK_CLAIM_LEASE_MS } from '../src/lifecycle.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';
import type { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';

const secret = 'atomic-start-test';
let harness: Harness;
let world: CelldWorld;
let second: CelldWorld;
let clock: number;

beforeEach(async () => {
  harness = await startHarness({ secret, virtualClock: true });
  clock = harness.fleet.now;
  const config = { fleetUrl: harness.url, secret };
  [world, second] = await Promise.all([createWorld(config), createWorld(config)]);
});
afterEach(async () => {
  vi.restoreAllMocks();
  await harness.close();
});

const newRunId = () => `wrun_${ulid(harness.fleet.now)}`;
const create = (token: string): RunCreatedEventRequest => ({
  eventType: 'run_created',
  eventData: {
    deploymentId: 'eve',
    workflowName: 'start-session',
    input: [],
    startHook: { token },
  },
});

atomicStartHookConformance({
  name: 'celld HTTP storage',
  storage: () => world,
  capabilities: () => world.capabilities,
  newRunId,
  secondEvents: () => second.events,
  advanceClock: (ms) => {
    harness.fleet.now = clock + ms;
  },
  retentionReadsAdvanceClock: true,
});

describe('celld atomic admission recovery', () => {
  it('recovers a committed start when the worker loses the response before publication', async () => {
    const runId = newRunId();
    const token = 'eve:inbox:v1:lost-response';
    const cell = harness.fleet.cell('runs', runId).instance as WorkflowRunDO;
    const apply = cell.applyStartHookEvent.bind(cell);
    vi.spyOn(cell, 'applyStartHookEvent').mockImplementationOnce(async (request) => {
      await apply(request);
      throw new Error('lost after commit');
    });
    await expect(world.events.create(runId, create(token))).rejects.toThrow('lost after commit');

    await expect(second.hooks.getByToken(token)).resolves.toMatchObject({ runId });
    await expect(second.hooks.get(startHookId(runId))).resolves.toMatchObject({ runId });
    await expect(second.events.create(runId, create(token))).resolves.toMatchObject({
      run: { runId, startHook: { hookId: startHookId(runId) } },
    });
    const loser = newRunId();
    const error = await second.events
      .create(loser, create(token))
      .catch((failure: unknown) => failure);
    expect(HookConflictError.is(error)).toBe(true);
    expect(error).toMatchObject({ conflictingRunId: runId });
    await expect(second.runs.get(loser)).rejects.toMatchObject({
      name: 'WorkflowRunNotFoundError',
    });
    expect((await second.events.list({ runId })).data.map((e) => e.eventType)).toEqual([
      'run_created',
    ]);
  });

  it('keeps a retained token when the run ends before its start indexes publish', async () => {
    const runId = newRunId();
    const token = 'eve:inbox:v1:cancel-before-publication';
    const request = create(token);
    request.eventData.startHook = { token, tokenRetentionUntil: new Date(clock + 60_000) };
    const cell = harness.fleet.cell('runs', runId).instance as WorkflowRunDO;
    const apply = cell.applyStartHookEvent.bind(cell);
    vi.spyOn(cell, 'applyStartHookEvent').mockImplementationOnce(async (event) => {
      await apply(event);
      throw new Error('lost after commit');
    });
    await expect(world.events.create(runId, request)).rejects.toThrow('lost after commit');
    await world.events.create(runId, { eventType: 'run_cancelled' });
    harness.fleet.advance(2);
    await harness.fleet.fireDueAlarms();
    await expect(second.hooks.getByToken(token)).resolves.toMatchObject({ runId });
    const loser = newRunId();
    await expect(second.events.create(loser, create(token))).rejects.toMatchObject({
      name: 'HookConflictError',
      conflictingRunId: runId,
    });
    harness.fleet.advance(60_000);
    await harness.fleet.fireDueAlarms();
    await expect(second.events.create(newRunId(), create(token))).resolves.toMatchObject({
      run: { startHook: { token } },
    });
  });

  it('fences a stalled writer before reassigning its expired reservation', async () => {
    const runId = newRunId();
    const token = 'eve:inbox:v1:expired-claim';
    const index = createRemoteEnv({ fleetUrl: harness.url, secret }, true).WORKFLOW_INDEX;
    const admitted = await index.reserveStartHook!(token, { runId, hookId: startHookId(runId) });
    if (!admitted.admitted) throw new Error('expected a fresh reservation');
    await expect(world.hooks.getByToken(token)).rejects.toMatchObject({
      name: 'HookNotFoundError',
    });
    const winner = newRunId();
    const pending = await index.reserveStartHook!(token, {
      runId: winner,
      hookId: startHookId(winner),
    });
    expect(pending).toMatchObject({ admitted: false, pending: true });
    harness.fleet.advance(HOOK_CLAIM_LEASE_MS + 1);
    await expect(second.events.create(winner, create(token))).resolves.toMatchObject({
      run: { runId: winner },
    });

    const stalled = harness.fleet.cell('runs', runId).instance as WorkflowRunDO;
    await expect(
      stalled.applyStartHookEvent({
        runId,
        data: create(token),
        hookClaimId: admitted.reservation.claimId,
      }),
    ).resolves.toMatchObject({ ok: false, code: 'HOOK_CLAIM_CANCELLED' });
    await expect(world.runs.get(runId)).rejects.toMatchObject({ name: 'WorkflowRunNotFoundError' });
    await expect(world.hooks.getByToken(token)).resolves.toMatchObject({ runId: winner });
  });

  it('connects to an older worker without advertising or silently dropping atomic start Hooks', async () => {
    const fetch = globalThis.fetch.bind(globalThis);
    vi.spyOn(globalThis, 'fetch').mockImplementation((input, init) =>
      (input instanceof Request ? input.url : input.toString()).endsWith(
        '/v1/index/hooks/start-capability',
      )
        ? Promise.resolve(new Response(null, { status: 404 }))
        : fetch(input, init),
    );
    const older = await createWorld({ fleetUrl: harness.url, secret });
    expect(older.capabilities?.atomicStartHook?.active).toBe(false);
    const runId = newRunId();
    await expect(
      older.events.create(runId, create('no-old-worker-capability')),
    ).rejects.toMatchObject({
      code: 'START_HOOK_ADMISSION_DISABLED',
    });
    await expect(
      older.events.create(runId, {
        eventType: 'run_created',
        eventData: { deploymentId: 'eve', workflowName: 'ordinary', input: [] },
      }),
    ).resolves.toMatchObject({ run: { runId } });
  });

  it('does not admit an atomic start on a synchronous, unprobed HTTP adapter', async () => {
    const unprobed = createCelldWorld({ fleetUrl: harness.url, secret });
    expect(unprobed.capabilities?.atomicStartHook?.active).toBe(false);
    const runId = newRunId();
    await expect(unprobed.events.create(runId, create('no-capability'))).rejects.toMatchObject({
      code: 'START_HOOK_ADMISSION_DISABLED',
    });
    await expect(world.runs.get(runId)).rejects.toMatchObject({ name: 'WorkflowRunNotFoundError' });
  });
});
