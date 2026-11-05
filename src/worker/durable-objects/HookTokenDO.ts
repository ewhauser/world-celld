import type { Hook, HookClaimedFrom } from '@workflow/world';
import { SPEC_VERSION_LEGACY, SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM } from '@workflow/world';
import type { HookTokenOwner } from '../../config.js';
import type { HookForceClaimResult } from '../../indexes.js';
import { parse, stringify } from '../../vendor/shared/index.js';
import { DurableObject } from '../do-base.js';
import {
  confirmRepublished,
  indexKey,
  ownerFromSerializedHook,
  publishCommittedClaim,
  resolveExpiredHookClaim,
  retentionAhead,
  runCell,
  runIsActive,
  sameOwner,
  tokenRetentionUntil,
  type HookIndexEnv,
  warnRepublished,
} from './hook-index-shared.js';
import {
  HOOK_CLAIM_LEASE_MS,
  HOOK_TAKEOVER_SETTLE_MS,
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

/** A token kept past its run's end under `tokenRetentionUntil`. */
interface RetainedToken {
  owner: HookTokenOwner;
  until: number;
  deadlineKey: string;
}

/**
 * A forced creation taking the token from `victim`. The record keeps naming
 * the victim until the claimer's hook_created is journaled and the victim is
 * disposed; only then is it re-pointed.
 */
interface HookTakeover {
  claimer: HookTokenOwner;
  claimId: string;
  victim: HookTokenOwner;
  victimRunning: boolean;
  claimedFrom: HookClaimedFrom;
  expiresAt: number;
  deadlineKey: string;
}

const MAX_RELEASE_BATCH = 64;
const CLAIM_DEADLINE_PREFIX = 'claim-deadline:';
const RETAINED_DEADLINE_PREFIX = 'retained-deadline:';
const TAKEOVER_DEADLINE_PREFIX = 'takeover-deadline:';
const DEADLINE_PREFIXES = [
  CLAIM_DEADLINE_PREFIX,
  RETAINED_DEADLINE_PREFIX,
  TAKEOVER_DEADLINE_PREFIX,
] as const;

interface ClaimDeadline {
  token: string;
  claimId: string;
  expiresAt: number;
}

interface TokenDeadline {
  token: string;
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

function retainedKey(token: string): string {
  return indexKey('retained', token);
}

function takeoverKey(token: string): string {
  return indexKey('takeover', token);
}

function deadlineKey(prefix: string, token: string, at: number): string {
  return `${prefix}${pad(at)}:${encodeURIComponent(token)}`;
}

function deadlineAt(prefix: string, key: string): number {
  return Number(key.slice(prefix.length, prefix.length + 13));
}

type TokenValue = string | HookClaim | RetainedToken | HookTakeover;

/** Token-ownership shard. A token hashes to exactly one shard and transaction domain. */
export class HookTokenDO extends DurableObject<HookIndexEnv> {
  private now(): number {
    return typeof this.env.clock === 'function' ? this.env.clock() : Date.now();
  }

  async get(token: string): Promise<string | null> {
    // A claim with no record can belong to a committed hook whose client
    // stopped before finalizing; publish it rather than report it missing.
    const raw =
      (await this.ctx.storage.get<string>(recordKey(token))) ??
      (await publishCommittedClaim(
        this.ctx.storage,
        this.env,
        { record: recordKey(token), claim: claimKey(token) },
        token,
      ));
    if (!raw) return null;
    if (retentionAhead(parse<Hook>(raw), this.now())) return raw;
    const owner = ownerFromSerializedHook(raw);
    return (await runIsActive(this.env, owner.runId)) ? raw : null;
  }

  async reserve(
    token: string,
    owner: HookTokenOwner,
    proposedClaimId: string,
  ): Promise<{ claimed: true; claimId: string } | { claimed: false; holder: HookTokenOwner }> {
    for (;;) {
      const observed = await this.ctx.storage.get<HookClaim>(claimKey(token));
      const expired = observed && observed.expiresAt <= this.now() ? observed : undefined;
      const committed = expired
        ? await resolveExpiredHookClaim(this.env, expired.owner, expired.claimId, token)
        : undefined;
      const result = await this.ctx.storage.transaction(async (txn) => {
        const record = recordKey(token);
        const claim = claimKey(token);
        const values = await txn.get<TokenValue>([
          record,
          claim,
          retainedKey(token),
          takeoverKey(token),
        ]);
        const now = this.now();
        const indexed = values.get(record) as string | undefined;
        if (indexed !== undefined && !(await this.freeExpiredRetention(txn, token, values, now))) {
          return { claimed: false as const, holder: ownerFromSerializedHook(indexed) };
        }
        // The victim released the token while a takeover was pending. Settle
        // the takeover before claiming, or its roll-forward would later
        // overwrite this claim's record and leave two live owners.
        if (values.has(takeoverKey(token))) return 'settle' as const;
        let existing = values.get(claim) as HookClaim | undefined;
        if (existing && existing.expiresAt <= now) {
          if (existing.claimId !== expired?.claimId || existing.expiresAt !== expired.expiresAt) {
            return null;
          }
          if (committed) {
            await this.renewClaim(txn, token, existing, now);
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
        if (existing === undefined) {
          const expiresAt = now + HOOK_CLAIM_LEASE_MS;
          const deadline = deadlineKey(CLAIM_DEADLINE_PREFIX, token, expiresAt);
          await txn.put(claim, {
            owner,
            claimId: proposedClaimId,
            reservedAt: now,
            expiresAt,
            deadlineKey: deadline,
          } satisfies HookClaim);
          await txn.put<ClaimDeadline>(deadline, { token, claimId: proposedClaimId, expiresAt });
          await this.armAtMost(txn, expiresAt);
          return { claimed: true as const, claimId: proposedClaimId };
        }
        return { claimed: true as const, claimId: existing.claimId };
      });
      if (result === 'settle') {
        await this.settleTakeover(token);
        continue;
      }
      if (result) return result;
    }
  }

  /**
   * Reserve for `experimental_force`. A live holder is not a conflict: the
   * token is taken over through a durable intent that `settleTakeover`
   * finishes once the claimer's hook_created is journaled. Another hook's
   * creation still in flight is answered `pending` so the caller waits for
   * it; with `preempt` it is fenced instead, so it cannot commit behind us.
   */
  async forceReserve(
    token: string,
    owner: HookTokenOwner,
    proposedClaimId: string,
    preempt = false,
  ): Promise<HookForceClaimResult> {
    for (;;) {
      const values = await this.ctx.storage.get<TokenValue>([
        recordKey(token),
        claimKey(token),
        takeoverKey(token),
      ]);
      const pending = values.get(takeoverKey(token)) as HookTakeover | undefined;
      if (pending) {
        if (sameOwner(pending.claimer, owner)) {
          return {
            claimed: true,
            claimId: pending.claimId,
            takeover: true,
            claimedFrom: pending.claimedFrom,
          };
        }
        if (!preempt && pending.expiresAt > this.now()) {
          return { claimed: false, holder: pending.claimer, pending: true };
        }
        await this.settleTakeover(token);
        continue;
      }

      const indexed = values.get(recordKey(token)) as string | undefined;
      if (indexed === undefined) {
        const claim = values.get(claimKey(token)) as HookClaim | undefined;
        if (claim && !sameOwner(claim.owner, owner)) {
          if (!preempt && claim.expiresAt > this.now()) {
            return { claimed: false, holder: claim.owner, pending: true };
          }
          await this.preemptClaim(token, claim);
          continue;
        }
        const result = await this.reserve(token, owner, proposedClaimId);
        if (result.claimed || sameOwner(result.holder, owner)) return result;
        continue;
      }

      const victimHook = parse<Hook>(indexed);
      const victim = { runId: victimHook.runId, hookId: victimHook.hookId };
      if (sameOwner(victim, owner)) return { claimed: false, holder: owner };
      const described = await runCell(this.env, victim.runId).describeForceClaimVictim();
      const victimRunning = described.lifecycle === 'active';
      if (
        victimRunning &&
        (described.specVersion ?? SPEC_VERSION_LEGACY) < SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM
      ) {
        return { claimed: false, holder: victim, forceRefusedReason: 'victim-spec-version' };
      }
      // A finished run's token is only taken over while retention holds it;
      // otherwise it is simply free.
      const takeOver = victimRunning || retentionAhead(victimHook, this.now());
      const claimedFrom: HookClaimedFrom =
        described.lifecycle === 'active'
          ? {
              ...victim,
              workflowName: described.workflowName,
              deploymentId: described.deploymentId,
              runSpecVersion: described.specVersion,
            }
          : victim;
      const intent = await this.ctx.storage.transaction(async (txn) => {
        const current = await txn.get<TokenValue>([
          recordKey(token),
          retainedKey(token),
          takeoverKey(token),
        ]);
        if (current.get(recordKey(token)) !== indexed || current.has(takeoverKey(token))) {
          return null;
        }
        if (!takeOver) {
          const retained = current.get(retainedKey(token)) as RetainedToken | undefined;
          await txn.delete([
            recordKey(token),
            ...(retained ? [retainedKey(token), retained.deadlineKey] : []),
          ]);
          return null;
        }
        const expiresAt = this.now() + HOOK_TAKEOVER_SETTLE_MS;
        const takeover: HookTakeover = {
          claimer: owner,
          claimId: proposedClaimId,
          victim,
          victimRunning,
          claimedFrom,
          expiresAt,
          deadlineKey: deadlineKey(TAKEOVER_DEADLINE_PREFIX, token, expiresAt),
        };
        await txn.put(takeoverKey(token), takeover);
        await txn.put<TokenDeadline>(takeover.deadlineKey, { token });
        await this.armAtMost(txn, expiresAt);
        return takeover;
      });
      if (intent) {
        return { claimed: true, claimId: intent.claimId, takeover: true, claimedFrom };
      }
    }
  }

  /**
   * Decide the pending takeover of `token` from the claimer's log: roll it
   * forward if its hook_created is journaled, otherwise fence the claimer and
   * drop the intent. Returns who holds the token afterwards.
   */
  async settleTakeover(
    token: string,
    match?: { claimer: HookTokenOwner; claimId?: string },
  ): Promise<{ owner: HookTokenOwner | null }> {
    const intent = await this.ctx.storage.get<HookTakeover>(takeoverKey(token));
    if (
      intent &&
      (!match ||
        (sameOwner(intent.claimer, match.claimer) &&
          (match.claimId === undefined || match.claimId === intent.claimId)))
    ) {
      const resolution = await runCell(this.env, intent.claimer.runId).resolveForcedHookClaim({
        hookId: intent.claimer.hookId,
        token,
        claimId: intent.claimId,
        victim: intent.victim,
      });
      if (resolution.journaled) {
        await this.rollForward(token, intent, resolution.hook);
      } else {
        await this.ctx.storage.transaction(async (txn) => {
          const current = await txn.get<TokenValue>([takeoverKey(token), retainedKey(token)]);
          const pending = current.get(takeoverKey(token)) as HookTakeover | undefined;
          if (pending?.claimId !== intent.claimId) return;
          await txn.delete([takeoverKey(token), intent.deadlineKey]);
          // compactRetained drops the deadline of a token under takeover, so
          // the victim keeps its retained record now: schedule it again.
          const retained = current.get(retainedKey(token)) as RetainedToken | undefined;
          if (retained) {
            await txn.put<TokenDeadline>(retained.deadlineKey, { token });
            await this.armAtMost(txn, retained.until);
          }
        });
      }
    }
    const raw = await this.ctx.storage.get<string>(recordKey(token));
    return { owner: raw === undefined ? null : ownerFromSerializedHook(raw) };
  }

  async finalize(
    token: string,
    serializedHook: string,
    owner: HookTokenOwner,
    claimId?: string,
  ): Promise<{ stored: boolean }> {
    const active = await runIsActive(this.env, owner.runId);
    const hook = parse<Hook>(serializedHook);
    const retainedUntil = tokenRetentionUntil(hook);
    if (!active && !retentionAhead(hook, this.now())) {
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
        // The authoritative hook may already have committed. A successful
        // response here would strand it without a token lookup; force replay
        // to reacquire the reservation and finish publication instead.
        throw new Error(`Hook token ${token} reservation expired`);
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
      if (!active && retainedUntil !== undefined) {
        await this.retain(txn, token, owner, retainedUntil);
      }
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
    options?: { runEnded?: boolean },
  ): Promise<{ deleted: number }> {
    if (hooks.length > MAX_RELEASE_BATCH) throw new Error('Hook token release batch is too large');
    if (hooks.length === 0) return { deleted: 0 };
    return await this.ctx.storage.transaction(async (txn) => {
      const keys = hooks.flatMap(({ token }) => [
        recordKey(token),
        claimKey(token),
        retainedKey(token),
      ]);
      const values = await txn.get<TokenValue>(keys);
      const now = this.now();
      const deletes: string[] = [];
      const deadlineDeletes: string[] = [];
      for (const { token, owner } of hooks) {
        const record = recordKey(token);
        const claim = claimKey(token);
        const indexed = values.get(record) as string | undefined;
        const existingClaim = values.get(claim) as HookClaim | undefined;
        const retained = values.get(retainedKey(token)) as RetainedToken | undefined;
        if (indexed !== undefined && sameOwner(ownerFromSerializedHook(indexed), owner)) {
          const until = tokenRetentionUntil(parse<Hook>(indexed));
          if (options?.runEnded && until !== undefined && until > now) {
            if (!retained) await this.retain(txn, token, owner, until);
          } else {
            deletes.push(record);
            if (retained) deadlineDeletes.push(retainedKey(token), retained.deadlineKey);
          }
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
      const moreClaims = await this.compactClaims();
      const moreRetained = await this.compactRetained();
      const moreTakeovers = await this.compactTakeovers();
      await this.rearm(moreClaims || moreRetained || moreTakeovers);
    } catch {
      await this.ctx.storage.setAlarm(this.now() + LIFECYCLE_COMPACTION_RETRY_MS);
    }
  }

  private async armAtMost(storage: DurableObjectTransaction, at: number): Promise<void> {
    const current = await storage.getAlarm();
    if (current === null || at < current) await storage.setAlarm(at);
  }

  private async rearm(more: boolean): Promise<void> {
    const now = this.now();
    if (more) {
      await this.ctx.storage.setAlarm(now + 1);
      return;
    }
    let next: number | undefined;
    for (const prefix of DEADLINE_PREFIXES) {
      const first = await this.ctx.storage.list({ prefix, limit: 1 });
      const key = first.keys().next().value;
      if (key !== undefined) next = Math.min(next ?? Infinity, deadlineAt(prefix, key));
    }
    if (next === undefined) await this.ctx.storage.deleteAlarm();
    else await this.ctx.storage.setAlarm(Math.max(now + 1, next));
  }

  private async retain(
    txn: DurableObjectTransaction,
    token: string,
    owner: HookTokenOwner,
    until: number,
  ): Promise<void> {
    const deadline = deadlineKey(RETAINED_DEADLINE_PREFIX, token, until);
    await txn.put<RetainedToken>(retainedKey(token), { owner, until, deadlineKey: deadline });
    await txn.put<TokenDeadline>(deadline, { token });
    await this.armAtMost(txn, until);
  }

  /** Drop a finished run's record once its retention passed; false while it still holds. */
  private async freeExpiredRetention(
    txn: DurableObjectTransaction,
    token: string,
    values: Map<string, TokenValue>,
    now: number,
  ): Promise<boolean> {
    const indexed = values.get(recordKey(token)) as string | undefined;
    const retained = values.get(retainedKey(token)) as RetainedToken | undefined;
    if (
      indexed === undefined ||
      !retained ||
      retained.until > now ||
      values.has(takeoverKey(token)) ||
      !sameOwner(ownerFromSerializedHook(indexed), retained.owner)
    ) {
      return false;
    }
    await txn.delete([recordKey(token), retainedKey(token), retained.deadlineKey]);
    return true;
  }

  /** Fence another hook's in-flight creation, publishing it instead if it already committed. */
  private async preemptClaim(token: string, observed: HookClaim): Promise<void> {
    const hook = await resolveExpiredHookClaim(this.env, observed.owner, observed.claimId, token);
    const published = await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<HookClaim>(claimKey(token));
      if (current?.claimId !== observed.claimId) return false;
      if (hook) await txn.put(recordKey(token), stringify(hook));
      await txn.delete([claimKey(token), current.deadlineKey]);
      return hook !== null;
    });
    if (published) {
      warnRepublished(observed.owner);
      await confirmRepublished(this.ctx.storage, this.env, recordKey(token), observed.owner, token);
    }
  }

  /**
   * The claimer's hook_created is journaled: dispose the victim (its
   * hook_disposed row and delivery fence commit together), then re-point the
   * record. Every step is idempotent, so any settler may repeat it.
   */
  private async rollForward(token: string, intent: HookTakeover, hook: Hook | null): Promise<void> {
    if (intent.victimRunning) {
      await runCell(this.env, intent.victim.runId).forceDisposeHook({
        hookId: intent.victim.hookId,
        token,
        claimedBy: intent.claimer,
      });
    }
    const written = await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<TokenValue>([takeoverKey(token), retainedKey(token)]);
      if (
        (current.get(takeoverKey(token)) as HookTakeover | undefined)?.claimId !== intent.claimId
      ) {
        return false;
      }
      const retained = current.get(retainedKey(token)) as RetainedToken | undefined;
      const deletes = [takeoverKey(token), intent.deadlineKey];
      if (retained) deletes.push(retainedKey(token), retained.deadlineKey);
      // The claimer disposed its hook or ended since journaling: nobody holds the token.
      if (hook) await txn.put(recordKey(token), stringify(hook));
      else deletes.push(recordKey(token));
      await txn.delete(deletes);
      return hook !== null;
    });
    if (written) {
      await confirmRepublished(this.ctx.storage, this.env, recordKey(token), intent.claimer, token);
    }
  }

  private async renewClaim(
    txn: DurableObjectTransaction,
    token: string,
    claim: HookClaim,
    now: number,
  ): Promise<void> {
    const expiresAt = now + HOOK_CLAIM_LEASE_MS;
    const deadline = deadlineKey(CLAIM_DEADLINE_PREFIX, token, expiresAt);
    await txn.delete(claim.deadlineKey);
    await txn.put(claimKey(token), { ...claim, expiresAt, deadlineKey: deadline });
    await txn.put<ClaimDeadline>(deadline, { token, claimId: claim.claimId, expiresAt });
    await this.armAtMost(txn, expiresAt);
  }

  private async dueDeadlines<T>(prefix: string, now: number): Promise<Array<[string, T]>> {
    const due = await this.ctx.storage.list<T>({
      prefix,
      end: `${prefix}${pad(now + 1)}`,
      limit: LIFECYCLE_COMPACTION_BATCH + 1,
    });
    return Array.from(due);
  }

  /** Returns true when more due claims remain than one batch holds. */
  private async compactClaims(): Promise<boolean> {
    const now = this.now();
    const due = await this.dueDeadlines<ClaimDeadline>(CLAIM_DEADLINE_PREFIX, now);
    const page = due.slice(0, LIFECYCLE_COMPACTION_BATCH);
    const observed = await this.ctx.storage.get<HookClaim>(
      page.map(([, item]) => claimKey(item.token)),
    );
    const committed = new Map<string, Hook | null>();
    await Promise.all(
      page.map(async ([, item]) => {
        const claim = observed.get(claimKey(item.token));
        if (!claim || claim.claimId !== item.claimId || claim.expiresAt > now) return;
        const hook = await resolveExpiredHookClaim(
          this.env,
          claim.owner,
          claim.claimId,
          item.token,
        );
        committed.set(claimKey(item.token), hook);
      }),
    );
    const published: Array<{ token: string; owner: HookTokenOwner }> = [];
    await this.ctx.storage.transaction(async (txn) => {
      const claims = await txn.get<HookClaim>(page.map(([, item]) => claimKey(item.token)));
      const deletes: string[] = [];
      for (const [deadline, item] of page) {
        const key = claimKey(item.token);
        const claim = claims.get(key);
        const snapshot = observed.get(key);
        if (claim?.claimId !== item.claimId || claim.expiresAt > now) {
          deletes.push(deadline);
        } else if (claim.claimId === snapshot?.claimId && claim.expiresAt === snapshot.expiresAt) {
          const hook = committed.get(key);
          if (hook) {
            // The hook committed but its client never finalized: publish its
            // record instead of renewing the claim for the life of the run.
            await txn.put(recordKey(item.token), stringify(hook));
            published.push({ token: item.token, owner: claim.owner });
          }
          deletes.push(deadline, key);
        }
      }
      for (let offset = 0; offset < deletes.length; offset += LIFECYCLE_COMPACTION_BATCH) {
        await txn.delete(deletes.slice(offset, offset + LIFECYCLE_COMPACTION_BATCH));
      }
    });
    for (const { token, owner } of published) {
      warnRepublished(owner);
      await confirmRepublished(this.ctx.storage, this.env, recordKey(token), owner, token);
    }
    return due.length > LIFECYCLE_COMPACTION_BATCH;
  }

  private async compactRetained(): Promise<boolean> {
    const now = this.now();
    const due = await this.dueDeadlines<TokenDeadline>(RETAINED_DEADLINE_PREFIX, now);
    const page = due.slice(0, LIFECYCLE_COMPACTION_BATCH);
    await this.ctx.storage.transaction(async (txn) => {
      for (const [deadline, { token }] of page) {
        const values = await txn.get<TokenValue>([
          recordKey(token),
          retainedKey(token),
          takeoverKey(token),
        ]);
        const retained = values.get(retainedKey(token)) as RetainedToken | undefined;
        if (retained?.deadlineKey === deadline) {
          await this.freeExpiredRetention(txn, token, values, now);
        }
        // A takeover pending on this token clears the marker when it settles.
        await txn.delete(deadline);
      }
    });
    return due.length > LIFECYCLE_COMPACTION_BATCH;
  }

  private async compactTakeovers(): Promise<boolean> {
    const now = this.now();
    const due = await this.dueDeadlines<TokenDeadline>(TAKEOVER_DEADLINE_PREFIX, now);
    const page = due.slice(0, LIFECYCLE_COMPACTION_BATCH);
    // Each token settles in its own claimer's and victim's cells, so one
    // unreachable cell must not hold back the rest of the page.
    const settled = await Promise.allSettled(
      page.map(async ([deadline, { token }]) => {
        const intent = await this.ctx.storage.get<HookTakeover>(takeoverKey(token));
        if (intent?.deadlineKey === deadline) await this.settleTakeover(token);
        else await this.ctx.storage.delete(deadline);
      }),
    );
    const failed = settled.find(
      (result): result is PromiseRejectedResult => result.status === 'rejected',
    );
    // The alarm's catch retries soon; the tokens that settled are done.
    if (failed) throw failed.reason;
    return due.length > LIFECYCLE_COMPACTION_BATCH;
  }
}
