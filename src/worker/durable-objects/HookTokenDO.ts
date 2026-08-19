import type { HookTokenOwner } from '../../config.js';
import { DurableObject } from '../do-base.js';
import {
  indexKey,
  ownerFromSerializedHook,
  runIsActive,
  sameOwner,
  type HookIndexEnv,
} from './hook-index-shared.js';
import {
  HOOK_CLAIM_LEASE_MS,
  LIFECYCLE_COMPACTION_BATCH,
  LIFECYCLE_COMPACTION_RETRY_MS,
} from '../../lifecycle.js';

interface HookClaim {
  owner: HookTokenOwner;
  claimId: string;
  reservedAt: number;
  expiresAt: number;
  deadlineKey: string;
}

const MAX_RELEASE_BATCH = 64;
const CLAIM_DEADLINE_PREFIX = 'claim-deadline:';

interface ClaimDeadline {
  token: string;
  claimId: string;
  expiresAt: number;
}

function pad(ms: number): string {
  return String(Math.max(0, Math.floor(ms))).padStart(13, '0');
}

function recordKey(token: string): string {
  return indexKey('hook', token);
}

function claimKey(token: string): string {
  return indexKey('claim', token);
}

function claimDeadlineKey(token: string, expiresAt: number): string {
  return `${CLAIM_DEADLINE_PREFIX}${pad(expiresAt)}:${encodeURIComponent(token)}`;
}

/** Token-ownership shard. A token hashes to exactly one shard and transaction domain. */
export class HookTokenDO extends DurableObject<HookIndexEnv> {
  private now(): number {
    return typeof this.env.clock === 'function' ? this.env.clock() : Date.now();
  }

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
      let existing = values.get(claim) as HookClaim | undefined;
      const now = this.now();
      if (existing && existing.expiresAt <= now) {
        await txn.delete([claim, existing.deadlineKey]);
        existing = undefined;
      }
      if (existing !== undefined && !sameOwner(existing.owner, owner)) {
        return { claimed: false, holder: existing.owner };
      }
      if (existing === undefined) {
        const expiresAt = now + HOOK_CLAIM_LEASE_MS;
        const deadlineKey = claimDeadlineKey(token, expiresAt);
        await txn.put(claim, {
          owner,
          claimId: proposedClaimId,
          reservedAt: now,
          expiresAt,
          deadlineKey,
        } satisfies HookClaim);
        await txn.put<ClaimDeadline>(deadlineKey, { token, claimId: proposedClaimId, expiresAt });
        await this.armAtMost(txn, expiresAt);
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
      await this.releaseBatch([{ token, owner }]);
      return { stored: false };
    }
    return await this.ctx.storage.transaction(async (txn) => {
      const record = recordKey(token);
      const claim = claimKey(token);
      const values = await txn.get<string | HookClaim>([record, claim]);
      const indexed = values.get(record) as string | undefined;
      if (indexed !== undefined && !sameOwner(ownerFromSerializedHook(indexed), owner)) {
        throw new Error(`Hook token ${token} is owned by another hook`);
      }
      const existingClaim = values.get(claim) as HookClaim | undefined;
      if (existingClaim && existingClaim.expiresAt <= this.now()) {
        await txn.delete([claim, existingClaim.deadlineKey]);
        return { stored: false };
      }
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
      if (existingClaim !== undefined) await txn.delete([claim, existingClaim.deadlineKey]);
      return { stored: true };
    });
  }

  async releaseClaim(token: string, owner: HookTokenOwner, claimId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const key = claimKey(token);
      const claim = await txn.get<HookClaim>(key);
      if (claim && sameOwner(claim.owner, owner) && claim.claimId === claimId) {
        await txn.delete([key, claim.deadlineKey]);
      }
    });
  }

  async releaseBatch(
    hooks: Array<{ token: string; owner: HookTokenOwner }>,
  ): Promise<{ deleted: number }> {
    if (hooks.length > MAX_RELEASE_BATCH) throw new Error('Hook token release batch is too large');
    if (hooks.length === 0) return { deleted: 0 };
    return await this.ctx.storage.transaction(async (txn) => {
      const keys = hooks.flatMap(({ token }) => [recordKey(token), claimKey(token)]);
      const values = await txn.get<string | HookClaim>(keys);
      const deletes: string[] = [];
      const deadlineDeletes: string[] = [];
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
          deadlineDeletes.push(existingClaim.deadlineKey);
        }
      }
      const deleted = deletes.length === 0 ? 0 : await txn.delete(deletes);
      if (deadlineDeletes.length > 0) await txn.delete(deadlineDeletes);
      return { deleted };
    });
  }

  async alarm(): Promise<void> {
    try {
      await this.compactClaims();
    } catch {
      await this.ctx.storage.setAlarm(this.now() + LIFECYCLE_COMPACTION_RETRY_MS);
    }
  }

  private async armAtMost(storage: DurableObjectTransaction, at: number): Promise<void> {
    const current = await storage.getAlarm();
    if (current === null || at < current) await storage.setAlarm(at);
  }

  private async compactClaims(): Promise<void> {
    const now = this.now();
    await this.ctx.storage.transaction(async (txn) => {
      const due = await txn.list<ClaimDeadline>({
        prefix: CLAIM_DEADLINE_PREFIX,
        end: `${CLAIM_DEADLINE_PREFIX}${pad(now + 1)}`,
        limit: LIFECYCLE_COMPACTION_BATCH + 1,
      });
      const page = Array.from(due).slice(0, LIFECYCLE_COMPACTION_BATCH);
      const claims = await txn.get<HookClaim>(page.map(([, item]) => claimKey(item.token)));
      const deletes = page.flatMap(([deadline, item]) => {
        const key = claimKey(item.token);
        const claim = claims.get(key);
        return claim?.claimId === item.claimId && claim.expiresAt <= now
          ? [deadline, key]
          : [deadline];
      });
      for (let offset = 0; offset < deletes.length; offset += LIFECYCLE_COMPACTION_BATCH) {
        await txn.delete(deletes.slice(offset, offset + LIFECYCLE_COMPACTION_BATCH));
      }
      if (due.size > LIFECYCLE_COMPACTION_BATCH) {
        await txn.setAlarm(now + 1);
        return;
      }
      const next = await txn.list<ClaimDeadline>({ prefix: CLAIM_DEADLINE_PREFIX, limit: 1 });
      const nextAt = next.values().next().value?.expiresAt;
      if (nextAt === undefined) await txn.deleteAlarm();
      else await txn.setAlarm(Math.max(now + 1, nextAt));
    });
  }
}
