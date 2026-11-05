import { HookForceClaimedError, HookNotFoundError } from '@workflow/errors';
import type { Event } from '@workflow/world';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createCelldWorld } from '../src/index.js';
import { createWorkflowIndex, hookTokenShardName } from '../src/indexes.js';
import { HOOK_TAKEOVER_SETTLE_MS } from '../src/lifecycle.js';
import { createStorage } from '../src/storage.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { startHarness } from '../src/testing/http-harness.js';
import { HookIdDO } from '../src/worker/durable-objects/HookIdDO.js';
import { HookTokenDO } from '../src/worker/durable-objects/HookTokenDO.js';
import { RunCatalogDO } from '../src/worker/durable-objects/RunCatalogDO.js';
import { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';

const token = 'force-claim-token';
const RETENTION_MS = 60 * 60 * 1000;

function forceClaimedBy(event: Event | undefined) {
  return event?.eventType === 'hook_disposed' ? event.eventData?.forceClaimedBy : undefined;
}

function isHookNotFound(error: unknown) {
  return HookNotFoundError.is(error);
}

describe('forced hook claims and hook token retention', () => {
  let fleet: FakeFleet;
  let storage: ReturnType<typeof createStorage>;
  let indexes: ReturnType<typeof createWorkflowIndex>;

  beforeEach(() => {
    const env: Record<string, unknown> = {};
    fleet = new FakeFleet(
      {
        runs: WorkflowRunDO,
        'run-catalog': RunCatalogDO,
        'hook-tokens': HookTokenDO,
        'hook-ids': HookIdDO,
      },
      env,
    );
    indexes = createWorkflowIndex({
      runCatalog: fleet.namespace('run-catalog') as never,
      hookTokens: fleet.namespace('hook-tokens') as never,
      hookIds: fleet.namespace('hook-ids') as never,
    });
    Object.assign(env, {
      clock: () => fleet.now,
      WORKFLOW_DB: fleet.namespace('runs'),
      WORKFLOW_RUN_CATALOG: fleet.namespace('run-catalog'),
      WORKFLOW_HOOK_TOKENS: fleet.namespace('hook-tokens'),
      WORKFLOW_HOOK_IDS: fleet.namespace('hook-ids'),
    });
    storage = createStorage({
      env: { WORKFLOW_DB: fleet.namespace('runs') as never, WORKFLOW_INDEX: indexes },
      deploymentId: 'force-claim',
    });
  });

  afterEach(() => vi.restoreAllMocks());

  async function startRun(runId: string) {
    await storage.events.create(runId, {
      eventType: 'run_created',
      eventData: { deploymentId: 'force-claim', workflowName: 'force-claim', input: [] },
    });
    return runId;
  }

  function createHook(
    runId: string,
    hookId: string,
    eventData: { force?: boolean; tokenRetentionUntil?: Date } = {},
  ) {
    return storage.events.create(runId, {
      eventType: 'hook_created',
      correlationId: hookId,
      eventData: { token, ...eventData },
    });
  }

  function deliver(runId: string, hookId: string, payload: string) {
    return storage.events.create(runId, {
      eventType: 'hook_received',
      correlationId: hookId,
      eventData: { payload },
    });
  }

  /** resumeHook's redirect loop: follow HookForceClaimedError to the token's owner. */
  async function deliverFollowing(runId: string, hookId: string, payload: string) {
    let target = { runId, hookId };
    for (let redirects = 0; ; redirects++) {
      try {
        await deliver(target.runId, target.hookId, payload);
        return { target, redirects };
      } catch (error) {
        if (!HookForceClaimedError.is(error) || redirects >= 4) throw error;
        target = { runId: error.claimedByRunId, hookId: error.claimedByHookId! };
      }
    }
  }

  async function events(runId: string): Promise<Event[]> {
    return (await storage.events.list({ runId, pagination: { limit: 100 } })).data;
  }

  async function ofType(runId: string, eventType: Event['eventType']) {
    return (await events(runId)).filter((event) => event.eventType === eventType);
  }

  async function finishRun(runId: string) {
    await storage.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });
    fleet.advance(1);
    await fleet.fireDueAlarms();
  }

  function tokenShard() {
    return fleet.cell('hook-tokens', hookTokenShardName(token));
  }

  function runCell(runId: string) {
    return fleet.cell('runs', runId).instance as WorkflowRunDO;
  }

  it('disposes the victim before the token moves, and refuses a delivery that resolved it', async () => {
    const victim = await startRun('wrun_force_victim');
    const claimer = await startRun('wrun_force_claimer');
    await createHook(victim, 'victim-hook');
    const resolved = await storage.hooks.getByToken(token);
    expect(resolved).toMatchObject({ runId: victim, hookId: 'victim-hook' });

    const victimCell = runCell(victim);
    const forceDispose = victimCell.forceDisposeHook.bind(victimCell);
    let inFlight: Promise<unknown> | undefined;
    vi.spyOn(victimCell, 'forceDisposeHook').mockImplementationOnce(async (request) => {
      const result = await forceDispose(request);
      // Between disposal and re-pointing: the claimer is journaled, the victim
      // is disposed, and the token still resolves to the victim.
      expect(await ofType(claimer, 'hook_created')).toHaveLength(1);
      expect(forceClaimedBy((await ofType(victim, 'hook_disposed'))[0])).toEqual({
        runId: claimer,
        hookId: 'claimer-hook',
      });
      await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: victim });
      inFlight = deliver(resolved.runId, resolved.hookId, 'in-flight').catch((error) => error);
      expect(await inFlight).toBeInstanceOf(HookForceClaimedError);
      return result;
    });

    const created = await createHook(claimer, 'claimer-hook', { force: true });
    expect(inFlight).toBeDefined();
    expect(created.hook?.claimedFrom).toEqual({
      runId: victim,
      hookId: 'victim-hook',
      workflowName: 'force-claim',
      deploymentId: 'force-claim',
      runSpecVersion: 8,
    });
    expect(created.event?.eventData).toMatchObject({
      force: true,
      forceClaimedFrom: { runId: victim, hookId: 'victim-hook' },
    });
    const refused = (await inFlight) as HookForceClaimedError;
    expect(refused).toMatchObject({
      token,
      claimedByRunId: claimer,
      claimedByHookId: 'claimer-hook',
    });

    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({
      runId: claimer,
      hookId: 'claimer-hook',
      claimedFrom: { runId: victim, hookId: 'victim-hook' },
    });
    await expect(storage.hooks.get('claimer-hook')).resolves.toMatchObject({ runId: claimer });
    await expect(storage.hooks.get('victim-hook')).rejects.toSatisfy(isHookNotFound);
    expect(await ofType(victim, 'hook_disposed')).toHaveLength(1);
    expect(await ofType(victim, 'hook_received')).toHaveLength(0);
    await expect(deliverFollowing(victim, 'victim-hook', 'retry')).resolves.toMatchObject({
      target: { runId: claimer, hookId: 'claimer-hook' },
      redirects: 1,
    });
    expect(await ofType(claimer, 'hook_received')).toHaveLength(1);
    expect(tokenShard().storage.data.has(`takeover:${token}`)).toBe(false);
  });

  it('lands a delivery racing a takeover in exactly one owner', async () => {
    const victim = await startRun('wrun_race_victim');
    const claimer = await startRun('wrun_race_claimer');
    await createHook(victim, 'victim-hook');
    const [, delivered] = await Promise.all([
      createHook(claimer, 'claimer-hook', { force: true }),
      deliverFollowing(victim, 'victim-hook', 'racing'),
    ]);
    const received = [
      ...(await ofType(victim, 'hook_received')),
      ...(await ofType(claimer, 'hook_received')),
    ];
    expect(received).toHaveLength(1);
    expect(received[0].runId).toBe(delivered.target.runId);
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: claimer });
  });

  it('converges concurrent forcers on one owner with a consistent takeover chain', async () => {
    const victim = await startRun('wrun_converge_victim');
    await createHook(victim, 'victim-hook');
    const forcers = await Promise.all(
      ['a', 'b', 'c', 'd'].map((name) => startRun(`wrun_converge_${name}`)),
    );
    const results = await Promise.allSettled(
      forcers.map((runId) => createHook(runId, `${runId}-hook`, { force: true })),
    );
    expect(results.map((result) => result.status)).toEqual(forcers.map(() => 'fulfilled'));

    const owner = await storage.hooks.getByToken(token);
    expect(forcers).toContain(owner.runId);
    const live = [];
    for (const runId of [victim, ...forcers]) {
      live.push(
        ...(await storage.hooks.list({ runId })).data.filter((hook) => hook.token === token),
      );
    }
    expect(live.map((hook) => hook.runId)).toEqual([owner.runId]);

    // Every disposal names a creation that names it back, and the chain from
    // the victim reaches the owner through every forcer.
    const takenFrom = new Map<string, string>();
    for (const runId of [victim, ...forcers]) {
      const disposals = await ofType(runId, 'hook_disposed');
      expect(disposals).toHaveLength(runId === owner.runId ? 0 : 1);
      if (runId === owner.runId) continue;
      const by = forceClaimedBy(disposals[0]);
      expect(by).toBeDefined();
      const [creation] = await ofType(by!.runId, 'hook_created');
      expect(creation.eventData).toMatchObject({ forceClaimedFrom: { runId } });
      takenFrom.set(runId, by!.runId);
    }
    const chain = [victim];
    while (takenFrom.has(chain.at(-1)!)) chain.push(takenFrom.get(chain.at(-1)!)!);
    expect(chain.at(-1)).toBe(owner.runId);
    expect(new Set(chain)).toEqual(new Set([victim, ...forcers]));

    await expect(deliverFollowing(victim, 'victim-hook', 'late')).resolves.toMatchObject({
      target: { runId: owner.runId },
    });
    expect(tokenShard().storage.data.has(`takeover:${token}`)).toBe(false);
  });

  it('refuses to take a token from a running victim below the force-claim spec version', async () => {
    const victim = await startRun('wrun_old_victim');
    const claimer = await startRun('wrun_old_claimer');
    await createHook(victim, 'victim-hook');
    const victimStorage = fleet.cell('runs', victim).storage;
    victimStorage.data.set('run', {
      ...(victimStorage.data.get('run') as object),
      specVersion: 7,
    });

    const created = await createHook(claimer, 'claimer-hook', { force: true });
    expect(created.event).toMatchObject({
      eventType: 'hook_conflict',
      eventData: {
        token,
        conflictingRunId: victim,
        forceRefusedReason: 'victim-spec-version',
      },
    });
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: victim });
    expect(fleet.cell('runs', victim).storage.data.has('hookforced:victim-hook')).toBe(false);
    expect(tokenShard().storage.data.has(`takeover:${token}`)).toBe(false);
  });

  it('lets a run take over its own hook', async () => {
    const runId = await startRun('wrun_self_takeover');
    await createHook(runId, 'first-hook');
    const created = await createHook(runId, 'second-hook', { force: true });
    expect(created.hook?.claimedFrom).toMatchObject({ runId, hookId: 'first-hook' });
    const [disposal] = await ofType(runId, 'hook_disposed');
    expect(disposal).toMatchObject({ correlationId: 'first-hook' });
    expect(forceClaimedBy(disposal)).toEqual({ runId, hookId: 'second-hook' });
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({
      hookId: 'second-hook',
    });
    await expect(deliver(runId, 'first-hook', 'stale')).rejects.toMatchObject({
      claimedByRunId: runId,
      claimedByHookId: 'second-hook',
    });
    await expect(deliver(runId, 'second-hook', 'fresh')).resolves.toBeDefined();
  });

  it('holds a retained token after its run ends, refusing unforced creation until expiry', async () => {
    const retained = await startRun('wrun_retained');
    const until = new Date(fleet.now + RETENTION_MS);
    await createHook(retained, 'retained-hook', { tokenRetentionUntil: until });
    await finishRun(retained);
    expect(tokenShard().storage.data.has(`retained:${token}`)).toBe(true);

    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({
      runId: retained,
      hookId: 'retained-hook',
    });
    await expect(deliver(retained, 'retained-hook', 'late')).rejects.toSatisfy(isHookNotFound);
    const competitor = await startRun('wrun_retained_competitor');
    const blocked = await createHook(competitor, 'blocked-hook');
    expect(blocked.event).toMatchObject({
      eventType: 'hook_conflict',
      eventData: { conflictingRunId: retained },
    });

    fleet.advance(RETENTION_MS);
    await expect(storage.hooks.getByToken(token)).rejects.toSatisfy(isHookNotFound);
    await fleet.fireDueAlarms();
    expect([...tokenShard().storage.data.keys()].filter((key) => key.includes(token))).toEqual([]);

    const successor = await startRun('wrun_retained_successor');
    const created = await createHook(successor, 'successor-hook');
    expect(created.event?.eventType).toBe('hook_created');
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: successor });
  });

  it('frees an expired retained token on admission when its compaction alarm has not run', async () => {
    const retained = await startRun('wrun_retained_lazy');
    await createHook(retained, 'retained-hook', {
      tokenRetentionUntil: new Date(fleet.now + RETENTION_MS),
    });
    await finishRun(retained);
    fleet.advance(RETENTION_MS);
    const successor = await startRun('wrun_retained_lazy_successor');
    const created = await createHook(successor, 'successor-hook');
    expect(created.event?.eventType).toBe('hook_created');
    expect(tokenShard().storage.data.has(`retained:${token}`)).toBe(false);
    await fleet.fireDueAlarms();
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: successor });
  });

  it('releases a retained token at once when the hook is disposed', async () => {
    const runId = await startRun('wrun_retained_disposed');
    await createHook(runId, 'retained-hook', {
      tokenRetentionUntil: new Date(fleet.now + RETENTION_MS),
    });
    await storage.events.create(runId, {
      eventType: 'hook_disposed',
      correlationId: 'retained-hook',
    });
    const successor = await startRun('wrun_retained_disposed_successor');
    const created = await createHook(successor, 'successor-hook');
    expect(created.event?.eventType).toBe('hook_created');
  });

  it('takes a finished run’s retained token over silently', async () => {
    const retained = await startRun('wrun_retained_victim');
    await createHook(retained, 'retained-hook', {
      tokenRetentionUntil: new Date(fleet.now + RETENTION_MS),
    });
    await finishRun(retained);
    const logBefore = await events(retained);

    const claimer = await startRun('wrun_retained_claimer');
    const created = await createHook(claimer, 'claimer-hook', { force: true });
    expect(created.hook?.claimedFrom).toEqual({ runId: retained, hookId: 'retained-hook' });
    expect(await events(retained)).toEqual(logBefore);
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: claimer });
    expect(tokenShard().storage.data.has(`retained:${token}`)).toBe(false);
  });

  it('claims a finished run’s token without a takeover once retention has passed', async () => {
    const finished = await startRun('wrun_unretained_victim');
    await createHook(finished, 'finished-hook');
    await storage.events.create(finished, {
      eventType: 'run_completed',
      eventData: { output: [] },
    });
    // Terminal cleanup has not released the record yet.
    const claimer = await startRun('wrun_unretained_claimer');
    const created = await createHook(claimer, 'claimer-hook', { force: true });
    expect(created.event?.eventType).toBe('hook_created');
    expect(created.hook?.claimedFrom).toBeUndefined();
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: claimer });
  });

  it.each(['after', 'before'] as const)(
    'settles a takeover whose creation RPC fails %s committing',
    async (commit) => {
      const victim = await startRun(`wrun_ambiguous_${commit}_victim`);
      const claimer = await startRun(`wrun_ambiguous_${commit}_claimer`);
      await createHook(victim, 'victim-hook');
      const claimerCell = runCell(claimer);
      const applyEvent = claimerCell.applyEvent.bind(claimerCell);
      vi.spyOn(claimerCell, 'applyEvent').mockImplementationOnce(async (request) => {
        if (commit === 'after') await applyEvent(request);
        throw new Error('connection reset');
      });

      await expect(createHook(claimer, 'claimer-hook', { force: true })).rejects.toThrow(
        'connection reset',
      );
      expect(tokenShard().storage.data.has(`takeover:${token}`)).toBe(false);
      const owner = commit === 'after' ? claimer : victim;
      expect(await ofType(victim, 'hook_disposed')).toHaveLength(commit === 'after' ? 1 : 0);
      await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: owner });
      await expect(deliverFollowing(victim, 'victim-hook', 'probe')).resolves.toMatchObject({
        target: { runId: owner },
      });

      const retried = await createHook(claimer, 'claimer-hook', { force: true });
      expect(retried.hook).toMatchObject({ runId: claimer, hookId: 'claimer-hook' });
      expect(await ofType(claimer, 'hook_created')).toHaveLength(1);
      expect(await ofType(victim, 'hook_disposed')).toHaveLength(1);
      await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: claimer });
    },
  );

  it('rolls a journaled takeover forward from the alarm when its client stops', async () => {
    const victim = await startRun('wrun_stopped_victim');
    const claimer = await startRun('wrun_stopped_claimer');
    await createHook(victim, 'victim-hook');
    const owner = { runId: claimer, hookId: 'claimer-hook' };
    const admission = await indexes.forceReserveHook(token, owner);
    if (!admission.admitted) throw new Error('expected a takeover');
    const outcome = await runCell(claimer).applyEvent({
      runId: claimer,
      data: {
        eventType: 'hook_created',
        correlationId: 'claimer-hook',
        eventData: { token, force: true },
      },
      tokenHolder: null,
      hookClaimId: admission.reservation.claimId,
      claimedFrom: admission.claimedFrom,
    });
    expect(outcome.ok).toBe(true);
    // The client stops here; until settled the victim still holds the token.
    await expect(deliver(victim, 'victim-hook', 'before-settle')).resolves.toBeDefined();

    fleet.advance(HOOK_TAKEOVER_SETTLE_MS);
    await fleet.fireDueAlarms();
    expect(forceClaimedBy((await ofType(victim, 'hook_disposed'))[0])).toEqual(owner);
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject(owner);
    await expect(deliver(victim, 'victim-hook', 'after-settle')).rejects.toMatchObject({
      claimedByRunId: claimer,
    });
  });

  it('fences an unjournaled takeover that another forcer settles, so its creation is refused', async () => {
    const victim = await startRun('wrun_fenced_victim');
    const slow = await startRun('wrun_fenced_slow');
    const fast = await startRun('wrun_fenced_fast');
    await createHook(victim, 'victim-hook');
    const slowOwner = { runId: slow, hookId: 'slow-hook' };
    const admission = await indexes.forceReserveHook(token, slowOwner);
    if (!admission.admitted) throw new Error('expected a takeover');

    await createHook(fast, 'fast-hook', { force: true });
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: fast });
    expect(forceClaimedBy((await ofType(victim, 'hook_disposed'))[0])).toEqual({
      runId: fast,
      hookId: 'fast-hook',
    });

    const late = await runCell(slow).applyEvent({
      runId: slow,
      data: {
        eventType: 'hook_created',
        correlationId: 'slow-hook',
        eventData: { token, force: true },
      },
      tokenHolder: null,
      hookClaimId: admission.reservation.claimId,
      claimedFrom: admission.claimedFrom,
    });
    expect(late).toMatchObject({ ok: false, code: 'HOOK_CLAIM_CANCELLED' });
    expect(await ofType(slow, 'hook_created')).toHaveLength(0);

    // The runtime's retry is re-admitted and takes the token from the winner.
    const retried = await createHook(slow, 'slow-hook', { force: true });
    expect(retried.hook?.claimedFrom).toMatchObject({ runId: fast, hookId: 'fast-hook' });
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject(slowOwner);
  });

  it('abandons a takeover whose client never applied it, leaving the victim in place', async () => {
    const victim = await startRun('wrun_abandoned_victim');
    const claimer = await startRun('wrun_abandoned_claimer');
    await createHook(victim, 'victim-hook');
    const admission = await indexes.forceReserveHook(token, {
      runId: claimer,
      hookId: 'claimer-hook',
    });
    if (!admission.admitted) throw new Error('expected a takeover');
    fleet.advance(HOOK_TAKEOVER_SETTLE_MS);
    await fleet.fireDueAlarms();
    expect(tokenShard().storage.data.has(`takeover:${token}`)).toBe(false);
    expect(await ofType(victim, 'hook_disposed')).toHaveLength(0);
    await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: victim });
    await expect(deliver(victim, 'victim-hook', 'kept')).resolves.toBeDefined();
  });

  /** A pending takeover of a token whose victim then disposes its hook. */
  async function takeoverOfReleasedToken(name: string) {
    const victim = await startRun(`wrun_released_${name}_victim`);
    const claimer = await startRun(`wrun_released_${name}_claimer`);
    const competitor = await startRun(`wrun_released_${name}_competitor`);
    await createHook(victim, 'victim-hook');
    const admission = await indexes.forceReserveHook(token, {
      runId: claimer,
      hookId: 'claimer-hook',
    });
    if (!admission.admitted) throw new Error('expected a takeover');
    const applyClaimer = () =>
      runCell(claimer).applyEvent({
        runId: claimer,
        data: {
          eventType: 'hook_created',
          correlationId: 'claimer-hook',
          eventData: { token, force: true },
        },
        tokenHolder: null,
        hookClaimId: admission.reservation.claimId,
        claimedFrom: admission.claimedFrom,
      });
    const releaseVictim = async () => {
      await storage.events.create(victim, {
        eventType: 'hook_disposed',
        correlationId: 'victim-hook',
      });
      expect(tokenShard().storage.data.has(`takeover:${token}`)).toBe(true);
    };
    /** After the alarm runs, exactly `winner` holds a live hook on the token. */
    const expectSoleOwner = async (winner: string) => {
      expect(tokenShard().storage.data.has(`takeover:${token}`)).toBe(false);
      fleet.advance(HOOK_TAKEOVER_SETTLE_MS);
      await fleet.fireDueAlarms();
      await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId: winner });
      const live = [];
      for (const runId of [victim, claimer, competitor]) {
        live.push(
          ...(await storage.hooks.list({ runId })).data.filter((hook) => hook.token === token),
        );
      }
      expect(live.map((hook) => hook.runId)).toEqual([winner]);
    };
    return { claimer, competitor, applyClaimer, releaseVictim, expectSoleOwner };
  }

  it('rolls a journaled takeover forward before an unforced creation takes the released token', async () => {
    const { claimer, competitor, applyClaimer, releaseVictim, expectSoleOwner } =
      await takeoverOfReleasedToken('journaled');
    expect((await applyClaimer()).ok).toBe(true);
    await releaseVictim();

    const created = await createHook(competitor, 'competitor-hook');
    expect(created.event).toMatchObject({
      eventType: 'hook_conflict',
      eventData: { conflictingRunId: claimer },
    });
    await expectSoleOwner(claimer);
  });

  it('fences an unjournaled takeover before an unforced creation takes the released token', async () => {
    const { competitor, applyClaimer, releaseVictim, expectSoleOwner } =
      await takeoverOfReleasedToken('unjournaled');
    await releaseVictim();

    const created = await createHook(competitor, 'competitor-hook');
    expect(created.event?.eventType).toBe('hook_created');
    await expect(applyClaimer()).resolves.toMatchObject({
      ok: false,
      code: 'HOOK_CLAIM_CANCELLED',
    });
    await expectSoleOwner(competitor);
  });

  it('re-admits a fenced forcer with a fresh wait budget instead of preempting at once', async () => {
    const victim = await startRun('wrun_budget_victim');
    const claimer = await startRun('wrun_budget_claimer');
    await createHook(victim, 'victim-hook');

    const forceReserve = indexes.forceReserveHook.bind(indexes);
    const preempts: boolean[] = [];
    let pendingAnswers = 6;
    vi.spyOn(indexes, 'forceReserveHook').mockImplementation(async (tok, owner, options) => {
      preempts.push(options?.preempt === true);
      // Another creation is in flight until this forcer has waited it out once.
      if (pendingAnswers-- > 0) {
        return { admitted: false, holder: { runId: victim, hookId: 'victim-hook' }, pending: true };
      }
      return forceReserve(tok, owner, options);
    });
    // A concurrent forcer fences this creation once after it was admitted.
    vi.spyOn(runCell(claimer), 'applyEvent').mockImplementationOnce(async () => ({
      ok: false,
      code: 'HOOK_CLAIM_CANCELLED',
      message: 'fenced by a concurrent forcer',
    }));

    const created = await createHook(claimer, 'claimer-hook', { force: true });
    expect(created.hook).toMatchObject({ runId: claimer, hookId: 'claimer-hook' });
    // Six pending answers, the preempting admission that was fenced, then a
    // re-admission that waits again before it would preempt.
    expect(preempts).toEqual([false, false, false, false, false, false, true, false]);
  });

  it('still refuses a delivery with HookForceClaimedError when settling the takeover fails', async () => {
    const victim = await startRun('wrun_settle_fails_victim');
    const claimer = await startRun('wrun_settle_fails_claimer');
    await createHook(victim, 'victim-hook');
    await createHook(claimer, 'claimer-hook', { force: true });

    vi.spyOn(indexes, 'settleHookTakeover').mockRejectedValue(new Error('shard unavailable'));
    const refused = await deliver(victim, 'victim-hook', 'stale').catch((error) => error);
    expect(HookForceClaimedError.is(refused)).toBe(true);
    expect(refused).toMatchObject({ claimedByRunId: claimer, claimedByHookId: 'claimer-hook' });
  });
});

describe('forced hook claims over the fleet HTTP boundary', () => {
  it('advertises both capabilities and takes over, redirects and retains through the router', async () => {
    const secret = 'force-claim-secret';
    const harness = await startHarness({ secret, virtualClock: true });
    try {
      const world = createCelldWorld({
        fleetUrl: harness.url,
        secret,
        deploymentId: 'force-claim',
      });
      expect(world.capabilities).toMatchObject({
        hookForceClaim: true,
        hookRetention: { active: true },
      });
      const start = async () =>
        (
          await world.events.create(null, {
            eventType: 'run_created',
            eventData: { deploymentId: 'force-claim', workflowName: 'force-claim', input: [] },
          })
        ).run.runId;
      const victim = await start();
      const claimer = await start();
      const until = new Date(harness.fleet.now + RETENTION_MS);
      await world.events.create(victim, {
        eventType: 'hook_created',
        correlationId: 'victim-hook',
        eventData: { token },
      });
      const created = await world.events.create(claimer, {
        eventType: 'hook_created',
        correlationId: 'claimer-hook',
        eventData: { token, force: true, tokenRetentionUntil: until },
      });
      expect(created.hook?.claimedFrom).toMatchObject({ runId: victim, hookId: 'victim-hook' });
      const refused = await world.events
        .create(victim, {
          eventType: 'hook_received',
          correlationId: 'victim-hook',
          eventData: { payload: 'stale' },
        })
        .catch((error) => error);
      expect(HookForceClaimedError.is(refused)).toBe(true);
      expect(refused).toMatchObject({ claimedByRunId: claimer, claimedByHookId: 'claimer-hook' });

      await world.events.create(claimer, { eventType: 'run_completed', eventData: { output: [] } });
      harness.fleet.advance(1);
      await harness.fleet.fireDueAlarms();
      await expect(world.hooks.getByToken(token)).resolves.toMatchObject({
        runId: claimer,
        tokenRetentionUntil: until,
      });
      const successor = await start();
      const blocked = await world.events.create(successor, {
        eventType: 'hook_created',
        correlationId: 'successor-hook',
        eventData: { token },
      });
      expect(blocked.event?.eventType).toBe('hook_conflict');
    } finally {
      await harness.close();
    }
  });
});
