import type { Hook, WorkflowRun } from '@workflow/world';
import { beforeEach, describe, expect, it } from 'vitest';
import {
  HOOK_ID_SHARDS,
  HOOK_TOKEN_SHARDS,
  RUN_CATALOG_SHARDS,
  allRunCatalogShardNames,
  createWorkflowIndex,
  hookIdShardName,
  hookTokenShardName,
  runCatalogShardName,
  stableIndexHash,
  type WorkflowIndex,
} from '../src/indexes.js';
import { globalRunIndexKey, workflowRunIndexKey } from '../src/retention.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import { stringify } from '../src/vendor/shared/index.js';
import { HookIdDO } from '../src/worker/durable-objects/HookIdDO.js';
import { HookTokenDO } from '../src/worker/durable-objects/HookTokenDO.js';
import { RunCatalogDO } from '../src/worker/durable-objects/RunCatalogDO.js';
import {
  CATALOG_FENCE_GRACE_MS,
  HOOK_CLAIM_LEASE_MS,
  MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS,
} from '../src/lifecycle.js';
import type { RunLifecycleStatus } from '../src/retention.js';

class TestRunLifecycleDO {
  constructor(private ctx: { storage: DurableObjectStorage }) {}

  async getLifecycleStatus(): Promise<RunLifecycleStatus> {
    return (await this.ctx.storage.get<RunLifecycleStatus>('status')) ?? 'active';
  }
}

function run(sequence: number, workflowName = 'routing-workflow'): WorkflowRun {
  const createdAt = new Date(Date.UTC(2026, 0, 1, 0, 0, sequence));
  return {
    runId: `wrun_${String(sequence).padStart(4, '0')}`,
    workflowName,
    createdAt,
    updatedAt: createdAt,
    deploymentId: 'index-tests',
    specVersion: 6,
    status: 'pending',
    input: [],
    executionContext: {},
    attributes: {},
  };
}

function hook(runId: string, hookId: string, token: string): Hook {
  return {
    runId,
    hookId,
    token,
    ownerId: '',
    projectId: '',
    environment: '',
    createdAt: new Date('2026-01-01T00:00:00.000Z'),
    specVersion: 6,
    isWebhook: false,
  };
}

function expectDistribution(
  route: (value: string) => string,
  prefix: string,
  cardinality: number,
  minimum: number,
  maximum: number,
): void {
  const counts = new Map<string, number>();
  for (let index = 0; index < 10_000; index++) {
    const name = route(`${prefix}${index}`);
    counts.set(name, (counts.get(name) ?? 0) + 1);
  }
  expect(counts).toHaveLength(cardinality);
  const distribution = [...counts.values()];
  expect(Math.min(...distribution)).toBeGreaterThan(minimum);
  expect(Math.max(...distribution)).toBeLessThan(maximum);
}

describe('sharded workflow indexes', () => {
  let fleet: FakeFleet;
  let indexes: WorkflowIndex;

  beforeEach(() => {
    const cellEnv: Record<string, unknown> = {};
    fleet = new FakeFleet(
      {
        'run-catalog': RunCatalogDO,
        runs: TestRunLifecycleDO as never,
        'hook-tokens': HookTokenDO as never,
        'hook-ids': HookIdDO as never,
      },
      cellEnv,
    );
    cellEnv.clock = () => fleet.now;
    const bindings = {
      runCatalog: fleet.namespace('run-catalog'),
      hookTokens: fleet.namespace('hook-tokens'),
      hookIds: fleet.namespace('hook-ids'),
    };
    Object.assign(cellEnv, {
      WORKFLOW_RUN_CATALOG: bindings.runCatalog,
      WORKFLOW_DB: fleet.namespace('runs'),
      WORKFLOW_HOOK_TOKENS: bindings.hookTokens,
      WORKFLOW_HOOK_IDS: bindings.hookIds,
    });
    indexes = createWorkflowIndex(bindings);
  });

  it('routes deterministically with the documented fixed shard cardinalities', () => {
    expect(stableIndexHash('same')).toBe(0xcd0c4a3b);
    expect(stableIndexHash('café/東京')).toBe(0xb1a7fdc8);
    expect(runCatalogShardName('wrun_same')).toBe('run-catalog:v1:0e');
    expect(hookTokenShardName('token_same')).toBe('hook-token:v1:11');
    expect(hookIdShardName('hook_same')).toBe('hook-id:v1:19');
    expect(new Set(allRunCatalogShardNames())).toHaveLength(RUN_CATALOG_SHARDS);

    expectDistribution(runCatalogShardName, 'wrun_distribution_', RUN_CATALOG_SHARDS, 500, 750);
    expectDistribution(hookTokenShardName, 'token_distribution_', HOOK_TOKEN_SHARDS, 250, 375);
    expectDistribution(hookIdShardName, 'hook_distribution_', HOOK_ID_SHARDS, 250, 375);
  });

  it('commits both run keys in one shard transaction and distributes distinct runs', async () => {
    const runs = Array.from({ length: 128 }, (_, index) => run(index));
    await Promise.all(
      runs.map((value) =>
        indexes.commitRun(
          value,
          JSON.stringify({ runId: value.runId }),
          fleet.now + MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS,
        ),
      ),
    );

    const occupied = new Set(runs.map((value) => runCatalogShardName(value.runId)));
    expect(occupied.size).toBe(RUN_CATALOG_SHARDS);
    for (const value of runs) {
      const storage = fleet.cell('run-catalog', runCatalogShardName(value.runId)).storage;
      expect(storage.data.get(workflowRunIndexKey(value))).toBe(
        JSON.stringify({ runId: value.runId }),
      );
      expect(storage.data.get(globalRunIndexKey(value))).toBe(
        JSON.stringify({ runId: value.runId }),
      );
    }
    const transactionCount = [...occupied].reduce(
      (total, name) => total + fleet.cell('run-catalog', name).storage.operationCounts.transaction,
      0,
    );
    expect(transactionCount).toBe(128);
  });

  it('merges shard pages without gaps in forward and reverse order', async () => {
    const runs = Array.from({ length: 75 }, (_, index) => run(index));
    await Promise.all(
      runs.map((value) =>
        indexes.commitRun(
          value,
          JSON.stringify({ runId: value.runId }),
          fleet.now + MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS,
        ),
      ),
    );

    const collect = async (reverse: boolean) => {
      const names: string[] = [];
      let cursor: string | undefined;
      for (;;) {
        const page = await indexes.listRuns({
          prefix: 'run:routing-workflow:',
          cursor,
          reverse,
          limit: 7,
        });
        names.push(...page.keys.map((entry) => entry.name));
        if (page.list_complete) break;
        expect(page.cursor).toBe(page.keys.at(-1)?.name);
        cursor = page.cursor;
      }
      return names;
    };

    const expected = runs.map(workflowRunIndexKey).toSorted();
    expect(await collect(false)).toEqual(expected);
    expect(await collect(true)).toEqual(expected.toReversed());
  });

  it('merges pages with the same UTF-8 ordering used by storage cursors', async () => {
    const createdAt = new Date('2026-01-01T00:00:00.000Z');
    const values = ['B', 'a'].map((runId) => {
      const value = run(0, 'utf8-order');
      value.runId = runId;
      value.createdAt = createdAt;
      value.updatedAt = createdAt;
      return value;
    });
    expect(new Set(values.map((value) => runCatalogShardName(value.runId)))).toHaveLength(2);
    await Promise.all(
      values.map((value) =>
        indexes.commitRun(
          value,
          JSON.stringify({ runId: value.runId }),
          fleet.now + MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS,
        ),
      ),
    );

    const collect = async (reverse: boolean) => {
      const runIds: string[] = [];
      let cursor: string | undefined;
      for (;;) {
        const page = await indexes.listRuns({
          prefix: 'run:utf8-order:',
          limit: 1,
          cursor,
          reverse,
        });
        runIds.push(
          ...page.keys.map(({ value }) => (JSON.parse(value) as { runId: string }).runId),
        );
        if (page.list_complete) return runIds;
        cursor = page.cursor;
      }
    };

    expect(await collect(false)).toEqual(['B', 'a']);
    expect(await collect(true)).toEqual(['a', 'B']);
  });

  it('serializes concurrent ownership claims for the same token', async () => {
    const owners = Array.from({ length: 32 }, (_, index) => ({
      runId: `wrun_claim_${index}`,
      hookId: `hook_claim_${index}`,
    }));
    const results = await Promise.all(
      owners.map((owner) => indexes.reserveHook('shared-token', owner)),
    );
    const winners = results.flatMap((result, index) => (result.admitted ? [owners[index]] : []));
    expect(winners).toHaveLength(1);
    expect(results.filter((result) => !result.admitted).map((result) => result.holder)).toEqual(
      Array.from({ length: owners.length - 1 }, () => winners[0]),
    );
  });

  it('rejects a hook-ID collision before publishing the contender token', async () => {
    const first = hook('wrun_id_owner', 'shared-hook-id', 'first-token');
    const firstOwner = { runId: first.runId, hookId: first.hookId };
    const firstAdmission = await indexes.reserveHook(first.token, firstOwner);
    if (!firstAdmission.admitted) throw new Error('expected first hook admission');
    await indexes.finalizeHookIndexes(
      first.token,
      first.hookId,
      stringify(first),
      firstOwner,
      firstAdmission.reservation,
    );

    const contender = hook('wrun_id_contender', first.hookId, 'second-token');
    const contenderOwner = { runId: contender.runId, hookId: contender.hookId };
    expect(await indexes.reserveHook(contender.token, contenderOwner)).toEqual({
      admitted: false,
      holder: firstOwner,
    });
    expect(await indexes.getHookByToken(contender.token)).toBeNull();
    expect(await indexes.getHookById(first.hookId)).toBe(stringify(first));

    const replacement = hook(contender.runId, 'different-hook-id', contender.token);
    expect(
      await indexes.reserveHook(replacement.token, {
        runId: replacement.runId,
        hookId: replacement.hookId,
      }),
    ).toMatchObject({ admitted: true });
  });

  it('releases the token claim when hook-ID admission fails before dispatch', async () => {
    const owner = { runId: 'wrun_id_admission_failure', hookId: 'failed-hook-id' };
    const idStorage = fleet.cell('hook-ids', hookIdShardName(owner.hookId)).storage;
    idStorage.failNextMutation(
      (mutation) =>
        mutation.operation === 'put' &&
        mutation.key === `claim:${encodeURIComponent(owner.hookId)}`,
      new Error('injected hook-ID admission failure'),
    );

    await expect(indexes.reserveHook('released-after-id-failure', owner)).rejects.toThrow(
      /injected hook-ID admission failure/,
    );
    await expect(
      indexes.reserveHook('released-after-id-failure', {
        runId: 'wrun_id_admission_contender',
        hookId: 'different-hook-id',
      }),
    ).resolves.toMatchObject({ admitted: true });
  });

  it('rejects stale exact claims without disturbing a newer same-owner reservation', async () => {
    const value = hook('wrun_exact_claim', 'exact-claim-hook', 'exact-claim-token');
    const owner = { runId: value.runId, hookId: value.hookId };
    const first = await indexes.reserveHook(value.token, owner);
    if (!first.admitted) throw new Error('expected first admission');
    await indexes.releaseHookReservation(value.token, owner, first.reservation);

    const second = await indexes.reserveHook(value.token, owner);
    if (!second.admitted) throw new Error('expected second admission');
    expect(second.reservation.claimId).not.toBe(first.reservation.claimId);
    await expect(
      indexes.finalizeHookIndexes(
        value.token,
        value.hookId,
        stringify(value),
        owner,
        first.reservation,
      ),
    ).rejects.toThrow(/reserved by another hook/);

    await expect(
      indexes.finalizeHookIndexes(
        value.token,
        value.hookId,
        stringify(value),
        owner,
        second.reservation,
      ),
    ).resolves.toBeUndefined();
    expect(await indexes.getHookByToken(value.token)).toBe(stringify(value));
  });

  it('publishes both hook lookup domains and hides them immediately after a run fence', async () => {
    const value = hook('wrun_hook_fence', 'hook-fence', 'token-fence');
    const owner = { runId: value.runId, hookId: value.hookId };
    const serialized = stringify(value);
    const admission = await indexes.reserveHook(value.token, owner);
    expect(admission).toMatchObject({ admitted: true });
    if (!admission.admitted) throw new Error('expected hook admission');
    await indexes.finalizeHookIndexes(
      value.token,
      value.hookId,
      serialized,
      owner,
      admission.reservation,
    );
    expect(await indexes.getHookByToken(value.token)).toBe(serialized);
    expect(await indexes.getHookById(value.hookId)).toBe(serialized);

    fleet.cell('runs', value.runId).storage.data.set('status', 'terminal');
    expect(await indexes.getHookByToken(value.token)).toBeNull();
    expect(await indexes.getHookById(value.hookId)).toBeNull();

    await indexes.finalizeHookIndexes(value.token, value.hookId, serialized, owner);
    expect(await indexes.getHookByToken(value.token)).toBeNull();
    expect(await indexes.getHookById(value.hookId)).toBeNull();
  });

  it('fences hook deletion against delayed finalization and permits token reuse by another owner', async () => {
    const first = hook('wrun_first', 'hook-first', 'reusable-token');
    const firstOwner = { runId: first.runId, hookId: first.hookId };
    const firstAdmission = await indexes.reserveHook(first.token, firstOwner);
    if (!firstAdmission.admitted) throw new Error('expected first hook admission');
    await indexes.finalizeHookIndexes(
      first.token,
      first.hookId,
      stringify(first),
      firstOwner,
      firstAdmission.reservation,
    );
    await indexes.releaseHookIndexes({
      runId: first.runId,
      hooks: [{ hookId: first.hookId, token: first.token }],
    });
    await expect(
      indexes.finalizeHookIndexes(
        first.token,
        first.hookId,
        stringify(first),
        firstOwner,
        firstAdmission.reservation,
      ),
    ).rejects.toThrow(/no active reservation/);
    expect(await indexes.getHookByToken(first.token)).toBeNull();

    await indexes.releaseHookIndexes({
      runId: first.runId,
      hooks: [{ hookId: first.hookId, token: first.token }],
    });
    expect(
      Array.from(fleet.cell('hook-tokens', hookTokenShardName(first.token)).storage.data.keys()),
    ).not.toContain(`runfence:${first.runId}`);
    expect(
      Array.from(fleet.cell('hook-ids', hookIdShardName(first.hookId)).storage.data.keys()),
    ).not.toContain(`runfence:${first.runId}`);

    const second = hook('wrun_second', 'hook-second', first.token);
    const secondOwner = { runId: second.runId, hookId: second.hookId };
    const secondAdmission = await indexes.reserveHook(second.token, secondOwner);
    expect(secondAdmission).toMatchObject({ admitted: true });
    if (!secondAdmission.admitted) throw new Error('expected second hook admission');
    await indexes.finalizeHookIndexes(
      second.token,
      second.hookId,
      stringify(second),
      secondOwner,
      secondAdmission.reservation,
    );
    expect(await indexes.getHookByToken(second.token)).toBe(stringify(second));
  });

  it('fences catalog expiry before deletion so delayed commits cannot resurrect a run', async () => {
    const overlong = run(998, 'overlong-publication');
    await expect(
      indexes.commitRun(
        overlong,
        JSON.stringify({ runId: overlong.runId }),
        fleet.now + MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS + 1,
      ),
    ).resolves.toEqual({ stored: false });

    const value = run(999, 'expiry-workflow');
    const metadata = JSON.stringify({ runId: value.runId });
    const publicationExpiresAt = fleet.now + MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS;
    await indexes.commitRun(value, metadata, publicationExpiresAt);
    expect(
      await indexes.expireRun({
        runId: value.runId,
        keys: [workflowRunIndexKey(value), globalRunIndexKey(value)],
        hooks: [],
        expiredAt: 123,
      }),
    ).toEqual({ deleted: 2 });
    expect(await indexes.commitRun(value, metadata, publicationExpiresAt)).toEqual({
      stored: false,
    });
    fleet.advance(CATALOG_FENCE_GRACE_MS);
    await fleet.fireDueAlarms();
    const catalogStorage = fleet.cell('run-catalog', runCatalogShardName(value.runId)).storage;
    expect(catalogStorage.data.has(`expired:${value.runId}`)).toBe(false);
    expect(await indexes.commitRun(value, metadata, publicationExpiresAt)).toEqual({
      stored: false,
    });
    const page = await indexes.listRuns({ prefix: 'run:expiry-workflow:' });
    expect(page.keys).toEqual([]);
  });

  it('compare-deletes a rejected global candidate without fencing later publication', async () => {
    const value = run(997, 'stale-retention-candidate');
    const key = globalRunIndexKey(value);
    const staleMetadata = JSON.stringify({ runId: value.runId, status: 'pending' });
    const currentMetadata = JSON.stringify({ runId: value.runId, status: 'running' });
    const publicationExpiresAt = fleet.now + MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS;
    await indexes.commitRun(value, staleMetadata, publicationExpiresAt);
    await indexes.commitRun(value, currentMetadata, publicationExpiresAt);
    const catalog = fleet.namespace('run-catalog').get({
      toString: () => runCatalogShardName(value.runId),
    }) as RunCatalogDO;

    await expect(catalog.deleteStaleGlobalRun(value.runId, key, staleMetadata)).resolves.toEqual({
      deleted: false,
    });
    expect(fleet.cell('run-catalog', runCatalogShardName(value.runId)).storage.data.get(key)).toBe(
      currentMetadata,
    );

    await expect(catalog.deleteStaleGlobalRun(value.runId, key, currentMetadata)).resolves.toEqual({
      deleted: true,
    });
    expect(await indexes.commitRun(value, currentMetadata, publicationExpiresAt)).toEqual({
      stored: true,
    });
  });

  it('compacts abandoned exact claims after the protocol lease and survives restart', async () => {
    const owner = { runId: 'wrun_abandoned_claim', hookId: 'hook-abandoned-claim' };
    const token = 'token-abandoned-claim';
    const value = hook(owner.runId, owner.hookId, token);
    const admission = await indexes.reserveHook(token, owner);
    expect(admission).toMatchObject({ admitted: true });
    if (!admission.admitted) throw new Error('expected admission');

    const tokenName = hookTokenShardName(token);
    const idName = hookIdShardName(owner.hookId);
    fleet.restartCell('hook-tokens', tokenName);
    fleet.restartCell('hook-ids', idName);
    fleet.advance(HOOK_CLAIM_LEASE_MS);
    await fleet.fireDueAlarms();

    expect(
      Array.from(fleet.cell('hook-tokens', tokenName).storage.data.keys()).filter((key) =>
        key.startsWith('claim'),
      ),
    ).toEqual([]);
    expect(
      Array.from(fleet.cell('hook-ids', idName).storage.data.keys()).filter((key) =>
        key.startsWith('claim'),
      ),
    ).toEqual([]);
    fleet.cell('runs', owner.runId).storage.data.set('status', 'expired');
    await expect(
      indexes.finalizeHookIndexes(
        token,
        owner.hookId,
        stringify(value),
        owner,
        admission.reservation,
      ),
    ).resolves.toBeUndefined();
    await expect(indexes.getHookByToken(token)).resolves.toBeNull();
  });

  it('pages catalog compaction so many expired runs leave no derivative markers', async () => {
    const shardName = 'run-catalog:v1:compaction-test';
    const catalog = fleet.namespace('run-catalog').get({
      toString: () => shardName,
    }) as RunCatalogDO;
    for (let index = 0; index < 300; index++) {
      const runId = `wrun_catalog_compaction_${index}`;
      await catalog.expireRun(runId, [`run:${runId}`, `runall:${runId}`], fleet.now);
    }

    const catalogStorage = fleet.cell('run-catalog', shardName).storage;
    expect(
      Array.from(catalogStorage.data.keys()).filter((key) => key.startsWith('expired:')),
    ).toHaveLength(300);
    fleet.advance(CATALOG_FENCE_GRACE_MS);
    for (let page = 0; page < 3; page++) {
      await fleet.fireDueAlarms();
      fleet.advance(1);
    }
    expect(
      Array.from(catalogStorage.data.keys()).filter(
        (key) => key.startsWith('expired:') || key.startsWith('expiry-gc:'),
      ),
    ).toEqual([]);
  });
});
