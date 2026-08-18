import type { Hook } from '@workflow/world';
import type { HookTokenOwner } from '../../config.js';
import { parse } from '../../vendor/shared/index.js';
import { DurableObject } from '../do-base.js';
import type {
  ExpireRunIndexesRequest,
  ExpireRunIndexesResult,
  HookIndexReference,
  ReleaseHookIndexesRequest,
  ReleaseHookIndexesResult,
} from '../../retention.js';

interface HookClaim {
  owner: HookTokenOwner;
  reservedAt: number;
}

const STORAGE_BATCH_SIZE = 128;
const MAX_INDEX_CLEANUP_KEYS = 66;
const MAX_INDEX_CLEANUP_HOOKS = 64;
const MAX_INDEX_LIST_SIZE = 1000;

async function getMany<T>(
  storage: DurableObjectTransaction,
  keys: string[],
): Promise<Map<string, T>> {
  const result = new Map<string, T>();
  for (let offset = 0; offset < keys.length; offset += STORAGE_BATCH_SIZE) {
    const page = await storage.get<T>(keys.slice(offset, offset + STORAGE_BATCH_SIZE));
    for (const [key, value] of page) result.set(key, value);
  }
  return result;
}

async function deleteMany(storage: DurableObjectTransaction, keys: string[]): Promise<number> {
  let deleted = 0;
  const unique = Array.from(new Set(keys));
  for (let offset = 0; offset < unique.length; offset += STORAGE_BATCH_SIZE) {
    deleted += await storage.delete(unique.slice(offset, offset + STORAGE_BATCH_SIZE));
  }
  return deleted;
}

function sameOwner(left: HookTokenOwner, right: HookTokenOwner): boolean {
  return left.runId === right.runId && left.hookId === right.hookId;
}

function ownerFromHook(raw: string): HookTokenOwner {
  const hook = parse<Hook>(raw);
  return { runId: hook.runId, hookId: hook.hookId };
}

async function ownedHookIndexDeletes(
  txn: DurableObjectTransaction,
  runId: string,
  hooks: HookIndexReference[],
): Promise<string[]> {
  const hookKeys = hooks.flatMap((hook) => [
    `hook:${hook.token}`,
    `hookid:${hook.hookId}`,
    `hookclaim:${hook.token}`,
  ]);
  const values = await getMany<string | HookClaim>(txn, hookKeys);
  const deletes: string[] = [];
  for (const hook of hooks) {
    const owner = { runId, hookId: hook.hookId };
    const tokenKey = `hook:${hook.token}`;
    const idKey = `hookid:${hook.hookId}`;
    const claimKey = `hookclaim:${hook.token}`;
    const byToken = values.get(tokenKey) as string | undefined;
    const byId = values.get(idKey) as string | undefined;
    const claim = values.get(claimKey) as HookClaim | undefined;
    if (byToken !== undefined && sameOwner(ownerFromHook(byToken), owner)) {
      deletes.push(tokenKey);
    }
    if (byId !== undefined && sameOwner(ownerFromHook(byId), owner)) {
      deletes.push(idKey);
    }
    if (claim !== undefined && sameOwner(claim.owner, owner)) {
      deletes.push(claimKey);
    }
  }
  return deletes;
}

/**
 * Global index cell, replacing the Workers KV namespace world-cloudflare
 * uses for run listing and hook token/id lookup.
 *
 * A single cell (named `index`) serializes every read and write. Hook-token
 * reservation, publication, and release use storage transactions so
 * concurrent runs cannot both acquire the same token.
 *
 * The pagination contract matches KVNamespace (cursor = last returned key and
 * `list_complete` reflects whether further keys exist). Unlike KVNamespace,
 * list() also returns each stored value so callers do not need an additional
 * RPC for every listed key.
 */
export class IndexDO extends DurableObject {
  async get(key: string): Promise<string | null> {
    const value = await this.ctx.storage.get<string>(key);
    if (value && (key.startsWith('hook:') || key.startsWith('hookid:'))) {
      const owner = ownerFromHook(value);
      const fences = await this.ctx.storage.get([
        `terminal:${owner.runId}`,
        `expired:${owner.runId}`,
      ]);
      if (fences.size > 0) return null;
    }
    return value ?? null;
  }

  async put(key: string, value: string): Promise<void> {
    await this.ctx.storage.put(key, value);
  }

  /** Publish a derived index only while its owning run is not expired. */
  async putOwned(runId: string, key: string, value: string): Promise<{ stored: boolean }> {
    return await this.ctx.storage.transaction(async (txn) => {
      if ((await txn.get(`expired:${runId}`)) !== undefined) {
        return { stored: false };
      }
      await txn.put(key, value);
      return { stored: true };
    });
  }

  async delete(key: string): Promise<void> {
    await this.ctx.storage.delete(key);
  }

  async list(options?: {
    prefix?: string;
    cursor?: string;
    limit?: number;
    reverse?: boolean;
  }): Promise<{
    keys: Array<{ name: string; value: string }>;
    list_complete: boolean;
    cursor?: string;
  }> {
    const prefix = options?.prefix ?? '';
    const suppliedLimit = options?.limit;
    const requestedLimit =
      typeof suppliedLimit === 'number' && Number.isFinite(suppliedLimit)
        ? Math.floor(suppliedLimit)
        : MAX_INDEX_LIST_SIZE;
    const limit = Math.min(MAX_INDEX_LIST_SIZE, Math.max(1, requestedLimit));

    // Fetch one extra key to learn whether the listing is complete without
    // advancing past the page (the "exact limit + list_complete" contract
    // storage.ts relies on).
    const entries = await this.ctx.storage.list<string>({
      prefix,
      ...(options?.reverse
        ? { reverse: true, end: options.cursor }
        : { startAfter: options?.cursor }),
      limit: limit + 1,
    });

    const page = Array.from(entries).slice(0, limit);
    const listComplete = entries.size <= limit;

    return {
      keys: page.map(([name, value]) => ({ name, value })),
      list_complete: listComplete,
      cursor: listComplete ? undefined : page.at(-1)?.[0],
    };
  }

  async reserveHookToken(
    token: string,
    owner: HookTokenOwner,
  ): Promise<{ claimed: boolean; holder?: HookTokenOwner }> {
    return this.ctx.storage.transaction(async (txn) => {
      const claimKey = `hookclaim:${token}`;
      const values = await txn.get<string | HookClaim>([`hook:${token}`, claimKey]);
      const indexedHook = values.get(`hook:${token}`) as string | undefined;
      if (indexedHook !== undefined) {
        const holder = ownerFromHook(indexedHook);
        return { claimed: false, holder };
      }

      const claim = values.get(claimKey) as HookClaim | undefined;
      if (claim !== undefined && !sameOwner(claim.owner, owner)) {
        return { claimed: false, holder: claim.owner };
      }

      await txn.put(claimKey, { owner, reservedAt: Date.now() });
      return { claimed: true };
    });
  }

  async finalizeHookIndexes(
    token: string,
    hookId: string,
    serializedHook: string,
    owner: HookTokenOwner,
  ): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const expiredKey = `expired:${owner.runId}`;
      const terminalKey = `terminal:${owner.runId}`;
      const tokenKey = `hook:${token}`;
      const claimKey = `hookclaim:${token}`;
      const values = await txn.get<number | string | HookClaim>([
        expiredKey,
        terminalKey,
        tokenKey,
        claimKey,
      ]);
      const claim = values.get(claimKey) as HookClaim | undefined;
      if (values.get(expiredKey) !== undefined || values.get(terminalKey) !== undefined) {
        if (claim !== undefined && sameOwner(claim.owner, owner)) {
          await txn.delete(claimKey);
        }
        return;
      }
      const indexedHook = values.get(tokenKey) as string | undefined;
      if (indexedHook !== undefined && !sameOwner(ownerFromHook(indexedHook), owner)) {
        throw new Error(`Hook token ${token} is owned by another hook`);
      }
      if (claim !== undefined && !sameOwner(claim.owner, owner)) {
        throw new Error(`Hook token ${token} is reserved by another hook`);
      }

      await txn.put({ [tokenKey]: serializedHook, [`hookid:${hookId}`]: serializedHook });
      if (claim !== undefined) {
        await txn.delete(`hookclaim:${token}`);
      }
    });
  }

  async releaseHookToken(token: string, owner: HookTokenOwner): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const key = `hookclaim:${token}`;
      const claim = await txn.get<HookClaim>(key);
      if (claim !== undefined && sameOwner(claim.owner, owner)) {
        await txn.delete(key);
      }
    });
  }

  async releaseHookIndexes(request: ReleaseHookIndexesRequest): Promise<ReleaseHookIndexesResult> {
    if (request.hooks.length > MAX_INDEX_CLEANUP_HOOKS) {
      throw new Error('Hook index release page exceeds its bounded request limit');
    }
    return await this.ctx.storage.transaction(async (txn) => {
      if (request.terminal) await txn.put(`terminal:${request.runId}`, Date.now());
      const deletes = await ownedHookIndexDeletes(txn, request.runId, request.hooks);
      return { deleted: await deleteMany(txn, deletes) };
    });
  }

  /**
   * Fence a run and remove all of its known derived indexes atomically.
   * Once the fence exists, putOwned() and hook finalization cannot resurrect
   * entries from a delayed request.
   */
  async expireRun(request: ExpireRunIndexesRequest): Promise<ExpireRunIndexesResult> {
    if (
      request.keys.length > MAX_INDEX_CLEANUP_KEYS ||
      request.hooks.length > MAX_INDEX_CLEANUP_HOOKS
    ) {
      throw new Error('Index cleanup page exceeds its bounded request limit');
    }
    return await this.ctx.storage.transaction(async (txn) => {
      await txn.put(`expired:${request.runId}`, request.expiredAt);
      const deletes = [
        ...request.keys,
        `terminal:${request.runId}`,
        ...(await ownedHookIndexDeletes(txn, request.runId, request.hooks)),
      ];

      return { deleted: await deleteMany(txn, deletes) };
    });
  }
}
