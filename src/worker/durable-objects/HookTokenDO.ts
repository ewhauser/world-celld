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

interface HookClaim {
  owner: HookTokenOwner;
  claimId: string;
  reservedAt: number;
}

const MAX_RELEASE_BATCH = 64;

function recordKey(token: string): string {
  return indexKey('hook', token);
}

function claimKey(token: string): string {
  return indexKey('claim', token);
}

/** Token-ownership shard. A token hashes to exactly one shard and transaction domain. */
export class HookTokenDO extends DurableObject<HookIndexEnv> {
  async get(token: string): Promise<string | null> {
    const raw = await this.ctx.storage.get<string>(recordKey(token));
    if (!raw) return null;
    const owner = ownerFromSerializedHook(raw);
    return (await runIsActive(this.env, owner.runId)) ? raw : null;
  }

  async reserve(
    token: string,
    owner: HookTokenOwner,
    proposedClaimId: string,
  ): Promise<{ claimed: true; claimId: string } | { claimed: false; holder: HookTokenOwner }> {
    return await this.ctx.storage.transaction(async (txn) => {
      const record = recordKey(token);
      const claim = claimKey(token);
      const values = await txn.get<string | HookClaim>([record, claim]);
      const indexed = values.get(record) as string | undefined;
      if (indexed !== undefined) {
        return { claimed: false, holder: ownerFromSerializedHook(indexed) };
      }
      const existing = values.get(claim) as HookClaim | undefined;
      if (existing !== undefined && !sameOwner(existing.owner, owner)) {
        return { claimed: false, holder: existing.owner };
      }
      if (existing === undefined) {
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

  async finalize(
    token: string,
    serializedHook: string,
    owner: HookTokenOwner,
    claimId?: string,
  ): Promise<{ stored: boolean }> {
    if (!(await runIsActive(this.env, owner.runId))) {
      await this.releaseBatch([{ token, owner }], Date.now(), true);
      return { stored: false };
    }
    return await this.ctx.storage.transaction(async (txn) => {
      const record = recordKey(token);
      const claim = claimKey(token);
      const runFence = shardRunFenceKey(owner.runId);
      const values = await txn.get<string | HookClaim | number>([record, claim, runFence]);
      if (values.has(runFence)) {
        const existingClaim = values.get(claim) as HookClaim | undefined;
        if (existingClaim && sameOwner(existingClaim.owner, owner)) await txn.delete(claim);
        return { stored: false };
      }
      const indexed = values.get(record) as string | undefined;
      if (indexed !== undefined && !sameOwner(ownerFromSerializedHook(indexed), owner)) {
        throw new Error(`Hook token ${token} is owned by another hook`);
      }
      const existingClaim = values.get(claim) as HookClaim | undefined;
      if (
        existingClaim !== undefined &&
        (!sameOwner(existingClaim.owner, owner) || existingClaim.claimId !== claimId)
      ) {
        throw new Error(`Hook token ${token} is reserved by another hook`);
      }
      if (indexed === undefined && existingClaim === undefined) {
        throw new Error(`Hook token ${token} has no active reservation`);
      }
      await txn.put(record, serializedHook);
      if (existingClaim !== undefined) await txn.delete(claim);
      return { stored: true };
    });
  }

  async releaseClaim(token: string, owner: HookTokenOwner, claimId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const key = claimKey(token);
      const claim = await txn.get<HookClaim>(key);
      if (claim && sameOwner(claim.owner, owner) && claim.claimId === claimId) {
        await txn.delete(key);
      }
    });
  }

  async releaseBatch(
    hooks: Array<{ token: string; owner: HookTokenOwner }>,
    fencedAt: number,
    runFenced: boolean,
  ): Promise<{ deleted: number }> {
    if (hooks.length > MAX_RELEASE_BATCH) throw new Error('Hook token release batch is too large');
    if (hooks.length === 0) return { deleted: 0 };
    return await this.ctx.storage.transaction(async (txn) => {
      const keys = hooks.flatMap(({ token }) => [recordKey(token), claimKey(token)]);
      const values = await txn.get<string | HookClaim>(keys);
      const deletes: string[] = [];
      for (const { token, owner } of hooks) {
        const record = recordKey(token);
        const claim = claimKey(token);
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
