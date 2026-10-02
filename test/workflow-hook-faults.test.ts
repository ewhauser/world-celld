import { HookNotFoundError } from '@workflow/errors';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createWorkflowIndex, hookIdShardName, hookTokenShardName } from '../src/indexes.js';
import { HOOK_CLAIM_LEASE_MS, LIFECYCLE_COMPACTION_RETRY_MS } from '../src/lifecycle.js';
import { createStorage } from '../src/storage.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { HookIdDO } from '../src/worker/durable-objects/HookIdDO.js';
import { HookTokenDO } from '../src/worker/durable-objects/HookTokenDO.js';
import { RunCatalogDO } from '../src/worker/durable-objects/RunCatalogDO.js';
import { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';

describe('workflow hook publication fault recovery', () => {
  let fleet: FakeFleet;
  let storage: ReturnType<typeof createStorage>;
  let indexes: ReturnType<typeof createWorkflowIndex>;
  const runId = 'wrun_hook_faults';
  const hookId = 'hook-faults';
  const token = 'hook-fault-token';
  const request = {
    eventType: 'hook_created' as const,
    correlationId: hookId,
    eventData: { token },
  };

  beforeEach(async () => {
    const env: Record<string, unknown> = {};
    fleet = new FakeFleet(
      {
        runs: WorkflowRunDO,
        'run-catalog': RunCatalogDO,
        'hook-tokens': HookTokenDO as never,
        'hook-ids': HookIdDO as never,
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
      deploymentId: 'hook-faults',
    });
    await storage.events.create(runId, {
      eventType: 'run_created',
      eventData: { deploymentId: 'hook-faults', workflowName: 'hook-faults', input: [] },
    });
  });

  afterEach(() => vi.restoreAllMocks());

  async function createCompetitor() {
    const competitor = 'wrun_hook_competitor';
    await storage.events.create(competitor, {
      eventType: 'run_created',
      eventData: { deploymentId: 'hook-faults', workflowName: 'hook-faults', input: [] },
    });
    return competitor;
  }

  function expirePublication(domain: 'token' | 'id') {
    if (domain === 'token') {
      const cell = fleet.cell('hook-tokens', hookTokenShardName(token)).instance as HookTokenDO;
      const finalize = cell.finalize.bind(cell);
      vi.spyOn(cell, 'finalize').mockImplementationOnce(async (...args) => {
        fleet.advance(HOOK_CLAIM_LEASE_MS);
        return await finalize(...args);
      });
    } else {
      const cell = fleet.cell('hook-ids', hookIdShardName(hookId)).instance as HookIdDO;
      const publish = cell.publish.bind(cell);
      vi.spyOn(cell, 'publish').mockImplementationOnce(async (...args) => {
        fleet.advance(HOOK_CLAIM_LEASE_MS);
        return await publish(...args);
      });
    }
  }

  it.each(['token', 'id'] as const)(
    'protects a committed hook from competing %s ownership after publication expires',
    async (domain) => {
      expirePublication(domain);
      await expect(storage.events.create(runId, request)).rejects.toThrow(/reservation expired/);
      const originalEvents = (await storage.events.list({ runId })).data;
      const competitor = await createCompetitor();
      const competingToken = domain === 'token' ? token : 'competing-token';
      const competingHookId = domain === 'id' ? hookId : 'competing-hook';
      const conflict = await storage.events.create(competitor, {
        eventType: 'hook_created',
        correlationId: competingHookId,
        eventData: { token: competingToken },
      });
      expect(conflict.event?.eventType).toBe('hook_conflict');
      expect((await storage.hooks.list({ runId: competitor })).data).toEqual([]);
      fleet.restartCell('runs', runId);
      await expect(storage.events.create(runId, request)).resolves.toMatchObject({
        hook: { runId, hookId, token },
      });
      await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({ runId, hookId });
      await expect(storage.hooks.get(hookId)).resolves.toMatchObject({ runId, token });
      expect((await storage.events.list({ runId })).data).toEqual(originalEvents);
    },
  );

  it('renews committed unpublished claims through repeated alarm compaction and restart', async () => {
    const owner = { runId, hookId };
    const admission = await indexes.reserveHook(token, owner);
    if (!admission.admitted) throw new Error('expected original reservation');
    const run = fleet.cell('runs', runId).instance as WorkflowRunDO;
    await run.applyEvent({
      runId,
      data: request,
      tokenHolder: null,
      hookClaimId: admission.reservation.claimId,
    });
    for (let iteration = 0; iteration < 3; iteration++) {
      fleet.advance(HOOK_CLAIM_LEASE_MS);
      fleet.restartCell('hook-tokens', hookTokenShardName(token));
      fleet.restartCell('hook-ids', hookIdShardName(hookId));
      await fleet.fireDueAlarms();
      expect(
        fleet.cell('hook-tokens', hookTokenShardName(token)).storage.data.get(`claim:${token}`),
      ).toMatchObject({
        owner,
        claimId: admission.reservation.tokenClaimId,
        expiresAt: fleet.now + HOOK_CLAIM_LEASE_MS,
      });
      expect(
        fleet.cell('hook-ids', hookIdShardName(hookId)).storage.data.get(`claim:${hookId}`),
      ).toMatchObject({
        owner,
        claimId: admission.reservation.hookIdClaimId,
        expiresAt: fleet.now + HOOK_CLAIM_LEASE_MS,
      });
    }
    const competitor = await createCompetitor();
    expect(
      await indexes.reserveHook(token, { runId: competitor, hookId: 'competitor-hook' }),
    ).toMatchObject({ admitted: false, holder: owner });
    await expect(storage.events.create(runId, request)).resolves.toMatchObject({
      hook: { runId, hookId, token },
    });
  });

  it.each(['paired', 'id-only'] as const)(
    'fences a delayed authoritative creation before reclaiming an expired %s reservation',
    async (form) => {
      const owner = { runId, hookId };
      let eventClaimId: string;
      if (form === 'paired') {
        const admission = await indexes.reserveHook(token, owner);
        if (!admission.admitted) throw new Error('expected original reservation');
        eventClaimId = admission.reservation.claimId;
      } else {
        const ids = fleet.cell('hook-ids', hookIdShardName(hookId)).instance as HookIdDO;
        await ids.reserve(hookId, owner, 'id-only-claim');
        eventClaimId = '-:id-only-claim';
      }
      fleet.advance(HOOK_CLAIM_LEASE_MS);
      const competitor = await createCompetitor();
      const next = await indexes.reserveHook(token, { runId: competitor, hookId });
      expect(next.admitted).toBe(true);
      const run = fleet.cell('runs', runId).instance as WorkflowRunDO;
      expect(
        await run.applyEvent({
          runId,
          data: request,
          tokenHolder: null,
          hookClaimId: eventClaimId,
        }),
      ).toMatchObject({ ok: false, code: 'HOOK_CLAIM_CANCELLED' });
      expect((await storage.hooks.list({ runId })).data).toEqual([]);
      expect((await storage.events.list({ runId })).data.map((event) => event.eventType)).toEqual([
        'run_created',
      ]);
    },
  );

  it.each(['token', 'id'] as const)(
    'keeps expired %s claims when authoritative resolution is unavailable',
    async (domain) => {
      const owner = { runId, hookId };
      const admission = await indexes.reserveHook(token, owner);
      if (!admission.admitted) throw new Error('expected original reservation');
      fleet.advance(HOOK_CLAIM_LEASE_MS);
      const run = fleet.cell('runs', runId).instance as WorkflowRunDO;
      const resolver = vi
        .spyOn(run, 'resolveExpiredHookClaim')
        .mockRejectedValue(new Error('resolution unavailable'));
      const binding = domain === 'token' ? 'hook-tokens' : 'hook-ids';
      const shard = domain === 'token' ? hookTokenShardName(token) : hookIdShardName(hookId);
      const cell = fleet.cell(binding, shard);
      const before = structuredClone(cell.storage.data);
      const competitor = await createCompetitor();
      await expect(
        indexes.reserveHook(domain === 'token' ? token : 'new-token', {
          runId: competitor,
          hookId: domain === 'id' ? hookId : 'new-hook',
        }),
      ).rejects.toThrow('resolution unavailable');
      expect(cell.storage.data).toEqual(before);
      await fleet.fireDueAlarms();
      expect(cell.storage.data).toEqual(before);
      expect(cell.storage.alarmAt).toBe(fleet.now + LIFECYCLE_COMPACTION_RETRY_MS);
      resolver.mockRestore();
      fleet.advance(LIFECYCLE_COMPACTION_RETRY_MS);
      await fleet.fireDueAlarms();
      expect([...cell.storage.data.keys()].some((key) => key.startsWith('claim:'))).toBe(false);
    },
  );

  it.each(['terminal', 'disposed'] as const)(
    'releases unpublished ownership after its authoritative hook is %s',
    async (lifecycle) => {
      const owner = { runId, hookId };
      const admission = await indexes.reserveHook(token, owner);
      if (!admission.admitted) throw new Error('expected original reservation');
      const run = fleet.cell('runs', runId).instance as WorkflowRunDO;
      await run.applyEvent({
        runId,
        data: request,
        tokenHolder: null,
        hookClaimId: admission.reservation.claimId,
      });
      const finish =
        lifecycle === 'terminal'
          ? { eventType: 'run_completed' as const, eventData: { output: [] } }
          : { eventType: 'hook_disposed' as const, correlationId: hookId };
      await run.applyEvent({ runId, data: finish });
      const previousEvents = (await storage.events.list({ runId })).data;
      fleet.advance(HOOK_CLAIM_LEASE_MS);
      const competitor = await createCompetitor();
      const replacement = await indexes.reserveHook(token, { runId: competitor, hookId });
      expect(replacement.admitted).toBe(true);
      expect(
        (
          await run.applyEvent({
            runId,
            data: request,
            tokenHolder: null,
            hookClaimId: admission.reservation.claimId,
          })
        ).ok,
      ).toBe(false);
      expect((await storage.events.list({ runId })).data).toEqual(previousEvents);
    },
  );

  it.each(['token', 'id'] as const)(
    'surfaces an expired %s publication claim and repairs it on replay after restart',
    async (domain) => {
      const tokenName = hookTokenShardName(token);
      const idName = hookIdShardName(hookId);
      if (domain === 'token') {
        const cell = fleet.cell('hook-tokens', tokenName).instance as HookTokenDO;
        const finalize = cell.finalize.bind(cell);
        vi.spyOn(cell, 'finalize').mockImplementationOnce(async (...args) => {
          fleet.advance(HOOK_CLAIM_LEASE_MS);
          return await finalize(...args);
        });
      } else {
        const cell = fleet.cell('hook-ids', idName).instance as HookIdDO;
        const publish = cell.publish.bind(cell);
        vi.spyOn(cell, 'publish').mockImplementationOnce(async (...args) => {
          fleet.advance(HOOK_CLAIM_LEASE_MS);
          return await publish(...args);
        });
      }

      await expect(storage.events.create(runId, request)).rejects.toThrow(/reservation expired/);
      const beforeReplay = await storage.events.list({ runId });
      expect(beforeReplay.data.map((event) => event.eventType)).toEqual([
        'run_created',
        'hook_created',
      ]);
      await expect(storage.hooks.get(hookId)).rejects.toSatisfy((error) =>
        HookNotFoundError.is(error),
      );
      const indexedToken = await storage.hooks.getByToken(token).catch((error) => {
        if (!HookNotFoundError.is(error)) throw error;
        return null;
      });
      expect(indexedToken?.runId).toBe(domain === 'token' ? undefined : runId);
      expect(indexedToken?.hookId).toBe(domain === 'token' ? undefined : hookId);

      fleet.restartCell('runs', runId);
      fleet.restartCell('hook-tokens', tokenName);
      fleet.restartCell('hook-ids', idName);
      await expect(storage.events.create(runId, request)).resolves.toMatchObject({
        hook: { runId, hookId, token },
      });
      await expect(storage.hooks.get(hookId)).resolves.toMatchObject({ runId, hookId, token });
      await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({
        runId,
        hookId,
        token,
      });
      expect((await storage.events.list({ runId })).data).toEqual(beforeReplay.data);
    },
  );

  it.each(['token', 'id'] as const)(
    'repairs interrupted %s index disposal without releasing a replacement token owner',
    async (domain) => {
      await storage.events.create(runId, request);
      const binding = domain === 'token' ? 'hook-tokens' : 'hook-ids';
      const shard = domain === 'token' ? hookTokenShardName(token) : hookIdShardName(hookId);
      const key = domain === 'token' ? `hook:${token}` : `hookid:${hookId}`;
      const fault = fleet.cell(binding, shard).storage;
      fault.failNextMutation(
        (mutation) => mutation.operation === 'delete' && mutation.key === key,
        new Error('hook disposal index unavailable'),
      );
      const dispose = { eventType: 'hook_disposed' as const, correlationId: hookId };

      await expect(storage.events.create(runId, dispose)).rejects.toThrow(
        'hook disposal index unavailable',
      );
      expect((await storage.hooks.list({ runId })).data).toEqual([]);
      const committedEvents = (await storage.events.list({ runId })).data;
      expect(committedEvents.map((event) => event.eventType)).toEqual([
        'run_created',
        'hook_created',
        'hook_disposed',
      ]);

      fleet.restartCell('runs', runId);
      fleet.restartCell(binding, shard);
      const replacementRunId = 'wrun_hook_replacement';
      const replacementHookId = 'replacement-hook';
      await storage.events.create(replacementRunId, {
        eventType: 'run_created',
        eventData: { deploymentId: 'hook-faults', workflowName: 'hook-faults', input: [] },
      });
      const replace = () =>
        storage.events.create(replacementRunId, {
          eventType: 'hook_created',
          correlationId: replacementHookId,
          eventData: { token },
        });
      // When only ID deletion failed, the token is already available. A late
      // replay of the old disposal must leave that new owner's token intact.
      if (domain === 'id') await replace();
      await storage.events.create(runId, dispose);
      if (domain === 'token') await replace();

      await expect(storage.hooks.get(hookId)).rejects.toSatisfy((error) =>
        HookNotFoundError.is(error),
      );
      await expect(storage.hooks.getByToken(token)).resolves.toMatchObject({
        runId: replacementRunId,
        hookId: replacementHookId,
      });
      await expect(storage.hooks.get(replacementHookId)).resolves.toMatchObject({
        runId: replacementRunId,
        token,
      });
      expect((await storage.events.list({ runId })).data).toEqual(committedEvents);
    },
  );
});
