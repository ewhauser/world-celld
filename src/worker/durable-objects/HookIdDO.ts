import type { HookTokenOwner } from '../../config.js';
import { DurableObject } from '../do-base.js';
import {
  indexKey,
  ownerFromSerializedHook,
  runIsActive,
  sameOwner,
  shardRunFenceKey,
  type HookIndexEnv,
} from './hook-index-shared.js';

const MAX_RELEASE_BATCH = 64;

interface HookClaim {
  owner: HookTokenOwner;
  claimId: string;
  reservedAt: number;
}

function recordKey(hookId: string): string {
  return indexKey('hookid', hookId);
}

function claimKey(hookId: string): string {
  return indexKey('claim', hookId);
}

/** Hook-ID ownership and lookup shard; every hook ID has one transaction domain. */
export class HookIdDO extends DurableObject<HookIndexEnv> {
  async get(hookId: string): Promise<string | null> {
    const raw = await this.ctx.storage.get<string>(recordKey(hookId));
    if (!raw) return null;
    const owner = ownerFromSerializedHook(raw);
    return (await runIsActive(this.env, owner.runId)) ? raw : null;
  }

  async reserve(
    hookId: string,
    owner: HookTokenOwner,
    proposedClaimId: string,
  ): Promise<{ claimed: true; claimId: string } | { claimed: false; holder: HookTokenOwner }> {
    return await this.ctx.storage.transaction(async (txn) => {
      const record = recordKey(hookId);
      const claim = claimKey(hookId);
      const values = await txn.get<string | HookClaim>([record, claim]);
      const indexed = values.get(record) as string | undefined;
      if (indexed !== undefined) {
        return { claimed: false, holder: ownerFromSerializedHook(indexed) };
      }
      const existing = values.get(claim) as HookClaim | undefined;
      if (existing !== undefined && !sameOwner(existing.owner, owner)) {
        return { claimed: false, holder: existing.owner };
      }
      if (existing === undefined || existing.claimId !== proposedClaimId) {
        await txn.put(claim, {
          owner,
          claimId: proposedClaimId,
          reservedAt: Date.now(),
        } satisfies HookClaim);
        return { claimed: true, claimId: proposedClaimId };
      }
      return { claimed: true, claimId: existing.claimId };
    });
  }

  async publish(
    hookId: string,
    serializedHook: string,
    owner: HookTokenOwner,
    claimId?: string,
  ): Promise<{ stored: boolean }> {
    if (!(await runIsActive(this.env, owner.runId))) {
      await this.releaseBatch([{ hookId, owner }], Date.now(), true);
      return { stored: false };
    }
    return await this.ctx.storage.transaction(async (txn) => {
      const record = recordKey(hookId);
      const claim = claimKey(hookId);
      const runFence = shardRunFenceKey(owner.runId);
      const values = await txn.get<string | HookClaim | number>([record, claim, runFence]);
      if (values.has(runFence)) {
        const existingClaim = values.get(claim) as HookClaim | undefined;
        if (existingClaim && sameOwner(existingClaim.owner, owner)) await txn.delete(claim);
        return { stored: false };
      }
      const indexed = values.get(record) as string | undefined;
      if (indexed !== undefined && !sameOwner(ownerFromSerializedHook(indexed), owner)) {
        throw new Error(`Hook id ${hookId} is owned by another hook`);
      }
      const existingClaim = values.get(claim) as HookClaim | undefined;
      if (
        existingClaim !== undefined &&
        (!sameOwner(existingClaim.owner, owner) || existingClaim.claimId !== claimId)
      ) {
        throw new Error(`Hook id ${hookId} is reserved by another hook`);
      }
      if (indexed === undefined && existingClaim === undefined) {
        throw new Error(`Hook id ${hookId} has no active reservation`);
      }
      await txn.put(record, serializedHook);
      if (existingClaim !== undefined) await txn.delete(claim);
      return { stored: true };
    });
  }

  async releaseClaim(hookId: string, owner: HookTokenOwner, claimId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const key = claimKey(hookId);
      const claim = await txn.get<HookClaim>(key);
      if (claim && sameOwner(claim.owner, owner) && claim.claimId === claimId) {
        await txn.delete(key);
      }
    });
  }

  async releaseBatch(
    hooks: Array<{ hookId: string; owner: HookTokenOwner }>,
    fencedAt: number,
    runFenced: boolean,
  ): Promise<{ deleted: number }> {
    if (hooks.length > MAX_RELEASE_BATCH) throw new Error('Hook id release batch is too large');
    if (hooks.length === 0) return { deleted: 0 };
    return await this.ctx.storage.transaction(async (txn) => {
      const keys = hooks.flatMap(({ hookId }) => [recordKey(hookId), claimKey(hookId)]);
      const values = await txn.get<string | HookClaim>(keys);
      const deletes: string[] = [];
      for (const { hookId, owner } of hooks) {
        const record = recordKey(hookId);
        const claim = claimKey(hookId);
        const indexed = values.get(record) as string | undefined;
        const existingClaim = values.get(claim) as HookClaim | undefined;
        if (indexed !== undefined && sameOwner(ownerFromSerializedHook(indexed), owner)) {
          deletes.push(record);
        }
        if (existingClaim !== undefined && sameOwner(existingClaim.owner, owner)) {
          deletes.push(claim);
        }
      }
      if (runFenced) {
        await txn.put(shardRunFenceKey(hooks[0].owner.runId), fencedAt);
      }
      return { deleted: deletes.length === 0 ? 0 : await txn.delete(deletes) };
    });
  }
}
