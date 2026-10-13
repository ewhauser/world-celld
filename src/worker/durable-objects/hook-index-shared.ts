import type { Hook } from '@workflow/world';
import type { HookTokenOwner } from '../../config.js';
import type { CellNamespaceLike } from '../../indexes.js';
import type { RunLifecycleStatus } from '../../retention.js';
import { parse, stringify } from '../../vendor/shared/index.js';

interface RunLifecycleStub {
  getLifecycleStatus(): Promise<RunLifecycleStatus>;
  resolveExpiredHookClaim(request: {
    hookId: string;
    claimId: string;
    token?: string;
  }): Promise<Hook | null>;
  getCommittedHook(request: { hookId: string; token?: string }): Promise<Hook | null>;
}

export interface HookIndexEnv {
  WORKFLOW_DB?: CellNamespaceLike<RunLifecycleStub>;
  /** Test seam; celld deployments use Date.now(). */
  clock?: () => number;
}

export function sameOwner(left: HookTokenOwner, right: HookTokenOwner): boolean {
  return left.runId === right.runId && left.hookId === right.hookId;
}

export function ownerFromSerializedHook(raw: string): HookTokenOwner {
  const hook = parse<Hook>(raw);
  return { runId: hook.runId, hookId: hook.hookId };
}

export function indexKey(prefix: string, value: string): string {
  return `${prefix}:${encodeURIComponent(value)}`;
}

export async function runIsActive(env: HookIndexEnv, runId: string): Promise<boolean> {
  const namespace = env.WORKFLOW_DB;
  if (!namespace) throw new Error('world-celld hook index missing WORKFLOW_DB binding');
  const target = namespace.get(namespace.idFromName(runId));
  return (await target.getLifecycleStatus()) === 'active';
}

/** Resolve a stale shard reservation in its authoritative event writer. */
export async function resolveExpiredHookClaim(
  env: HookIndexEnv,
  owner: HookTokenOwner,
  claimId: string,
  token?: string,
): Promise<Hook | null> {
  const namespace = env.WORKFLOW_DB;
  if (!namespace) throw new Error('world-celld hook index missing WORKFLOW_DB binding');
  const target = namespace.get(namespace.idFromName(owner.runId));
  return await target.resolveExpiredHookClaim({ hookId: owner.hookId, claimId, token });
}

/** Read-only owner check used by index repair; unlike the resolver it never fences. */
export async function getCommittedHook(
  env: HookIndexEnv,
  owner: HookTokenOwner,
  token?: string,
): Promise<Hook | null> {
  const namespace = env.WORKFLOW_DB;
  if (!namespace) throw new Error('world-celld hook index missing WORKFLOW_DB binding');
  const target = namespace.get(namespace.idFromName(owner.runId));
  return await target.getCommittedHook({ hookId: owner.hookId, token });
}

interface IndexClaim {
  owner: HookTokenOwner;
  claimId: string;
  deadlineKey: string;
}

// Tokens can carry PII, so repair logs name only the run and hook.
function warnRepair(action: string, owner: HookTokenOwner): void {
  console.warn(
    `world-celld ${action} hook index record for run ${owner.runId} hook ${owner.hookId}`,
  );
}

export function warnRepublished(owner: HookTokenOwner): void {
  warnRepair('republished', owner);
}

/**
 * Publish the record for a claim whose hook committed in its run but whose
 * creating client stopped before finalizing. The write applies only while the
 * observed claim is unchanged, so a concurrent release or finalize wins.
 * Returns the record now stored for the key, if any.
 */
export async function publishCommittedClaim(
  storage: DurableObjectStorage,
  env: HookIndexEnv,
  keys: { record: string; claim: string },
  token?: string,
): Promise<string | null> {
  const observed = await storage.get<IndexClaim>(keys.claim);
  if (!observed) return null;
  const hook = await getCommittedHook(env, observed.owner, token);
  if (!hook) return null;
  const serialized = stringify(hook);
  const published = await storage.transaction(async (txn) => {
    const values = await txn.get<string | IndexClaim>([keys.record, keys.claim]);
    const indexed = values.get(keys.record) as string | undefined;
    if (indexed !== undefined) return { raw: indexed, written: false };
    const claim = values.get(keys.claim) as IndexClaim | undefined;
    if (claim?.claimId !== observed.claimId) return { raw: null, written: false };
    await txn.put(keys.record, serialized);
    await txn.delete([keys.claim, claim.deadlineKey]);
    return { raw: serialized, written: true };
  });
  if (!published.written) return published.raw;
  warnRepublished(observed.owner);
  return (await confirmRepublished(storage, env, keys.record, observed.owner, token))
    ? serialized
    : null;
}

/**
 * Re-ask the owner after a repair write. A hook disposed between the owner's
 * answer and the write would otherwise keep a record whose index release was
 * already spent, so withdraw it.
 */
export async function confirmRepublished(
  storage: DurableObjectStorage,
  env: HookIndexEnv,
  record: string,
  owner: HookTokenOwner,
  token?: string,
): Promise<boolean> {
  if (await getCommittedHook(env, owner, token)) return true;
  await storage.transaction(async (txn) => {
    const indexed = await txn.get<string>(record);
    if (indexed !== undefined && sameOwner(ownerFromSerializedHook(indexed), owner)) {
      await txn.delete(record);
    }
  });
  warnRepair('withdrew', owner);
  return false;
}
