import { describe, expect, it } from 'vitest';
import { FakeFleet } from '../src/testing/fake-cell.js';
import {
  RUN_QUEUE_CLAIM_SCOPE_KEY,
  WorkflowRunDO,
} from '../src/worker/durable-objects/WorkflowRunDO.js';

const CLAIM = 'claim:5:queue:key-1';

function setup(options: { marked: boolean }) {
  let fleet!: FakeFleet;
  fleet = new FakeFleet({ runs: WorkflowRunDO }, { clock: () => fleet.now });
  const runId = 'wrun_run_claims';
  const cell = fleet.cell('runs', runId);
  if (options.marked) cell.storage.data.set(RUN_QUEUE_CLAIM_SCOPE_KEY, 'run');
  const run = fleet.namespace('runs').get({ toString: () => runId }) as WorkflowRunDO;
  const claimKeys = () =>
    Array.from(cell.storage.data.keys()).filter((key) => key.startsWith('queue-claim:'));
  return { fleet, run, cell, claimKeys };
}

describe('run-scoped Queue claims', () => {
  it('leaves a run created before run-scoped claims on claim cells, writing nothing', async () => {
    const { run, cell, claimKeys } = setup({ marked: false });
    const writesBefore = cell.storage.operationCounts.put + cell.storage.operationCounts.putMany;
    await expect(
      run.reserveRunQueueMessage({ claimName: CLAIM, messageId: 'msg_a', expiresAt: 10_000 }),
    ).resolves.toEqual({ ok: true, scope: 'cell' });
    expect(cell.storage.operationCounts.put + cell.storage.operationCounts.putMany).toBe(
      writesBefore,
    );
    expect(claimKeys()).toEqual([]);
  });

  it('reserves one message per claim name until completion or expiry', async () => {
    const { fleet, run, claimKeys } = setup({ marked: true });
    const reserve = (messageId: string, expiresAt = fleet.now + 1_000) =>
      run.reserveRunQueueMessage({ claimName: CLAIM, messageId, expiresAt });

    await expect(reserve('msg_a')).resolves.toEqual({
      ok: true,
      scope: 'run',
      admitted: true,
      messageId: 'msg_a',
    });
    await expect(reserve('msg_b')).resolves.toEqual({
      ok: true,
      scope: 'run',
      admitted: false,
      messageId: 'msg_a',
    });
    await expect(reserve('msg_a')).resolves.toMatchObject({ admitted: true });
    await expect(
      run.reserveRunQueueMessage({
        claimName: 'claim:5:queue:key-2',
        messageId: 'msg_c',
        expiresAt: fleet.now + 1_000,
      }),
    ).resolves.toMatchObject({ admitted: true });

    await run.completeRunQueueMessage({ claimName: CLAIM, messageId: 'msg_a' });
    await expect(reserve('msg_b')).resolves.toMatchObject({ admitted: true, messageId: 'msg_b' });
    fleet.advance(1_001);
    await expect(reserve('msg_d')).resolves.toMatchObject({ admitted: true, messageId: 'msg_d' });
    expect(claimKeys()).toHaveLength(2);
  });

  it('claims, holds until the retry deadline, releases, and completes', async () => {
    const { fleet, run, claimKeys } = setup({ marked: true });
    const claim = (messageId: string) =>
      run.claimRunQueueMessage({ claimName: CLAIM, messageId, staleMs: 1_000 });

    await expect(claim('msg_a')).resolves.toEqual({ expired: false, claimed: true });
    await expect(claim('msg_a')).resolves.toEqual({
      expired: false,
      claimed: false,
      retryAt: fleet.now + 1_000,
    });
    await expect(claim('msg_b')).resolves.toEqual({ expired: false, claimed: false });

    await expect(
      run.holdRunQueueMessage({
        claimName: CLAIM,
        messageId: 'msg_a',
        retryAt: fleet.now + 50,
        expiresAt: fleet.now + 500,
      }),
    ).resolves.toEqual({ held: true });
    await expect(claim('msg_a')).resolves.toMatchObject({
      claimed: false,
      retryAt: fleet.now + 50,
    });
    fleet.advance(50);
    await expect(claim('msg_a')).resolves.toMatchObject({ claimed: true });

    await run.releaseRunQueueMessage({ claimName: CLAIM, messageId: 'msg_b' });
    await expect(claim('msg_b')).resolves.toMatchObject({ claimed: false });
    await run.releaseRunQueueMessage({ claimName: CLAIM, messageId: 'msg_a' });
    expect(claimKeys()).toEqual([]);
    await expect(claim('msg_b')).resolves.toMatchObject({ claimed: true });
    await run.completeRunQueueMessage({ claimName: CLAIM, messageId: 'msg_b' });
    expect(claimKeys()).toEqual([]);
  });

  it('keeps a reservation when its in-flight claim is released', async () => {
    const { fleet, run } = setup({ marked: true });
    await run.reserveRunQueueMessage({
      claimName: CLAIM,
      messageId: 'msg_a',
      expiresAt: fleet.now + 10_000,
    });
    await run.claimRunQueueMessage({ claimName: CLAIM, messageId: 'msg_a', staleMs: 1_000 });
    await run.releaseRunQueueMessage({ claimName: CLAIM, messageId: 'msg_a' });
    await expect(
      run.reserveRunQueueMessage({
        claimName: CLAIM,
        messageId: 'msg_b',
        expiresAt: fleet.now + 10_000,
      }),
    ).resolves.toMatchObject({ admitted: false, messageId: 'msg_a' });
  });

  it('rejects claim names that are not idempotency claim names', async () => {
    const { run } = setup({ marked: true });
    await expect(
      run.reserveRunQueueMessage({ claimName: 'run', messageId: 'msg_a', expiresAt: 1 }),
    ).rejects.toThrow(TypeError);
    await expect(
      run.claimRunQueueMessage({ claimName: 'queue-claim-scope', messageId: 'msg_a', staleMs: 1 }),
    ).rejects.toThrow(TypeError);
  });
});
