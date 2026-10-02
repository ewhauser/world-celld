import type { HookTokenOwner } from '../../config.js';
import { DurableObject } from '../do-base.js';
import {
  indexKey,
  ownerFromSerializedHook,
  resolveExpiredHookClaim,
  runIsActive,
  sameOwner,
  type HookIndexEnv,
} from './hook-index-shared.js';
import {
  HOOK_CLAIM_LEASE_MS,
  LIFECYCLE_COMPACTION_BATCH,
  LIFECYCLE_COMPACTION_RETRY_MS,
} from '../../lifecycle.js';

const MAX_RELEASE_BATCH = 64;
const CLAIM_DEADLINE_PREFIX = 'claim-deadline:';

interface HookClaim {
  owner: HookTokenOwner;
  claimId: string;
  reservedAt: number;
  expiresAt: number;
  deadlineKey: string;
}

interface ClaimDeadline {
  hookId: string;
  claimId: string;
  expiresAt: number;
}

function pad(ms: number): string {
  return String(Math.max(0, Math.floor(ms))).padStart(13, '0');
}

function recordKey(hookId: string): string {
  return indexKey('hookid', hookId);
}

function claimKey(hookId: string): string {
  return indexKey('claim', hookId);
}

function claimDeadlineKey(hookId: string, expiresAt: number): string {
  return `${CLAIM_DEADLINE_PREFIX}${pad(expiresAt)}:${encodeURIComponent(hookId)}`;
}

/** Hook-ID ownership and lookup shard; every hook ID has one transaction domain. */
export class HookIdDO extends DurableObject<HookIndexEnv> {
  private now(): number {
    return typeof this.env.clock === 'function' ? this.env.clock() : Date.now();
  }

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
    for (;;) {
      const observed = await this.ctx.storage.get<HookClaim>(claimKey(hookId));
      const expired = observed && observed.expiresAt <= this.now() ? observed : undefined;
      const committed = expired
        ? await resolveExpiredHookClaim(this.env, expired.owner, expired.claimId)
        : undefined;
      const result = await this.ctx.storage.transaction(async (txn) => {
        const record = recordKey(hookId);
        const claim = claimKey(hookId);
        const values = await txn.get<string | HookClaim>([record, claim]);
        const indexed = values.get(record) as string | undefined;
        if (indexed !== undefined) {
          return { claimed: false as const, holder: ownerFromSerializedHook(indexed) };
        }
        let existing = values.get(claim) as HookClaim | undefined;
        const now = this.now();
        if (existing && existing.expiresAt <= now) {
          if (existing.claimId !== expired?.claimId || existing.expiresAt !== expired.expiresAt) {
            return null;
          }
          if (committed) {
            await this.renewClaim(txn, hookId, existing, now);
            return sameOwner(existing.owner, owner)
              ? { claimed: true as const, claimId: existing.claimId }
              : { claimed: false as const, holder: existing.owner };
          }
          await txn.delete([claim, existing.deadlineKey]);
          existing = undefined;
        }
        if (existing !== undefined && !sameOwner(existing.owner, owner)) {
          return { claimed: false as const, holder: existing.owner };
        }
        if (existing === undefined || existing.claimId !== proposedClaimId) {
          if (existing) await txn.delete(existing.deadlineKey);
          const expiresAt = now + HOOK_CLAIM_LEASE_MS;
          const deadlineKey = claimDeadlineKey(hookId, expiresAt);
          await txn.put(claim, {
            owner,
            claimId: proposedClaimId,
            reservedAt: now,
            expiresAt,
            deadlineKey,
          } satisfies HookClaim);
          await txn.put<ClaimDeadline>(deadlineKey, {
            hookId,
            claimId: proposedClaimId,
            expiresAt,
          });
          await this.armAtMost(txn, expiresAt);
          return { claimed: true as const, claimId: proposedClaimId };
        }
        return { claimed: true as const, claimId: existing.claimId };
      });
      if (result) return result;
    }
  }

  async publish(
    hookId: string,
    serializedHook: string,
    owner: HookTokenOwner,
    claimId?: string,
  ): Promise<{ stored: boolean }> {
    if (!(await runIsActive(this.env, owner.runId))) {
      await this.releaseBatch([{ hookId, owner }]);
      return { stored: false };
    }
    return await this.ctx.storage.transaction(async (txn) => {
      const record = recordKey(hookId);
      const claim = claimKey(hookId);
      const values = await txn.get<string | HookClaim>([record, claim]);
      const indexed = values.get(record) as string | undefined;
      if (indexed !== undefined && !sameOwner(ownerFromSerializedHook(indexed), owner)) {
        throw new Error(`Hook id ${hookId} is owned by another hook`);
      }
      const existingClaim = values.get(claim) as HookClaim | undefined;
      if (existingClaim && existingClaim.expiresAt <= this.now()) {
        // Token publication may already have succeeded. Surface this failure
        // so replay reacquires the claim and repairs the missing ID lookup.
        throw new Error(`Hook id ${hookId} reservation expired`);
      }
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
      if (existingClaim !== undefined) await txn.delete([claim, existingClaim.deadlineKey]);
      return { stored: true };
    });
  }

  async releaseClaim(hookId: string, owner: HookTokenOwner, claimId: string): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const key = claimKey(hookId);
      const claim = await txn.get<HookClaim>(key);
      if (claim && sameOwner(claim.owner, owner) && claim.claimId === claimId) {
        await txn.delete([key, claim.deadlineKey]);
      }
    });
  }

  async releaseBatch(
    hooks: Array<{ hookId: string; owner: HookTokenOwner }>,
  ): Promise<{ deleted: number }> {
    if (hooks.length > MAX_RELEASE_BATCH) throw new Error('Hook id release batch is too large');
    if (hooks.length === 0) return { deleted: 0 };
    return await this.ctx.storage.transaction(async (txn) => {
      const keys = hooks.flatMap(({ hookId }) => [recordKey(hookId), claimKey(hookId)]);
      const values = await txn.get<string | HookClaim>(keys);
      const deletes: string[] = [];
      const deadlineDeletes: string[] = [];
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

  private async renewClaim(
    txn: DurableObjectTransaction,
    hookId: string,
    claim: HookClaim,
    now: number,
  ): Promise<void> {
    const expiresAt = now + HOOK_CLAIM_LEASE_MS;
    const deadlineKey = claimDeadlineKey(hookId, expiresAt);
    await txn.delete(claim.deadlineKey);
    await txn.put(claimKey(hookId), { ...claim, expiresAt, deadlineKey });
    await txn.put<ClaimDeadline>(deadlineKey, { hookId, claimId: claim.claimId, expiresAt });
    await this.armAtMost(txn, expiresAt);
  }

  private async compactClaims(): Promise<void> {
    const now = this.now();
    const due = await this.ctx.storage.list<ClaimDeadline>({
      prefix: CLAIM_DEADLINE_PREFIX,
      end: `${CLAIM_DEADLINE_PREFIX}${pad(now + 1)}`,
      limit: LIFECYCLE_COMPACTION_BATCH + 1,
    });
    const page = Array.from(due).slice(0, LIFECYCLE_COMPACTION_BATCH);
    const observed = await this.ctx.storage.get<HookClaim>(
      page.map(([, item]) => claimKey(item.hookId)),
    );
    const committed = new Map<string, boolean>();
    await Promise.all(
      page.map(async ([, item]) => {
        const claim = observed.get(claimKey(item.hookId));
        if (!claim || claim.claimId !== item.claimId || claim.expiresAt > now) return;
        const hook = await resolveExpiredHookClaim(this.env, claim.owner, claim.claimId);
        committed.set(claimKey(item.hookId), hook !== null);
      }),
    );
    await this.ctx.storage.transaction(async (txn) => {
      const claims = await txn.get<HookClaim>(page.map(([, item]) => claimKey(item.hookId)));
      const deletes: string[] = [];
      for (const [deadline, item] of page) {
        const key = claimKey(item.hookId);
        const claim = claims.get(key);
        const snapshot = observed.get(key);
        if (claim?.claimId !== item.claimId || claim.expiresAt > now) {
          deletes.push(deadline);
        } else if (claim.claimId === snapshot?.claimId && claim.expiresAt === snapshot.expiresAt) {
          if (committed.get(key)) await this.renewClaim(txn, item.hookId, claim, now);
          else deletes.push(deadline, key);
        }
      }
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
