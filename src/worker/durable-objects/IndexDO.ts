import type { Hook } from '@workflow/world';
import type { HookTokenOwner } from '../../config.js';
import { parse } from '../../vendor/shared/index.js';
import { DurableObject } from '../do-base.js';
import type { ExpireRunIndexesRequest, ExpireRunIndexesResult } from '../../retention.js';

interface HookClaim {
  owner: HookTokenOwner;
  reservedAt: number;
}

function sameOwner(left: HookTokenOwner, right: HookTokenOwner): boolean {
  return left.runId === right.runId && left.hookId === right.hookId;
}

function ownerFromHook(raw: string): HookTokenOwner {
  const hook = parse<Hook>(raw);
  return { runId: hook.runId, hookId: hook.hookId };
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
    const limit = options?.limit ?? 1000;

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
      const indexedHook = await txn.get<string>(`hook:${token}`);
      if (indexedHook !== undefined) {
        const holder = ownerFromHook(indexedHook);
        return { claimed: false, holder };
      }

      const claimKey = `hookclaim:${token}`;
      const claim = await txn.get<HookClaim>(claimKey);
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
      if ((await txn.get(`expired:${owner.runId}`)) !== undefined) {
        const claim = await txn.get<HookClaim>(`hookclaim:${token}`);
        if (claim !== undefined && sameOwner(claim.owner, owner)) {
          await txn.delete(`hookclaim:${token}`);
        }
        return;
      }
      const indexedHook = await txn.get<string>(`hook:${token}`);
      if (indexedHook !== undefined && !sameOwner(ownerFromHook(indexedHook), owner)) {
        throw new Error(`Hook token ${token} is owned by another hook`);
      }
      const claim = await txn.get<HookClaim>(`hookclaim:${token}`);
      if (claim !== undefined && !sameOwner(claim.owner, owner)) {
        throw new Error(`Hook token ${token} is reserved by another hook`);
      }

      await txn.put(`hook:${token}`, serializedHook);
      await txn.put(`hookid:${hookId}`, serializedHook);
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

  async deleteHookIndexes(token: string, hookId: string, owner: HookTokenOwner): Promise<void> {
    await this.ctx.storage.transaction(async (txn) => {
      const tokenKey = `hook:${token}`;
      const idKey = `hookid:${hookId}`;
      const claimKey = `hookclaim:${token}`;
      const [byToken, byId, claim] = await Promise.all([
        txn.get<string>(tokenKey),
        txn.get<string>(idKey),
        txn.get<HookClaim>(claimKey),
      ]);
      if (byToken !== undefined && sameOwner(ownerFromHook(byToken), owner)) {
        await txn.delete(tokenKey);
      }
      if (byId !== undefined && sameOwner(ownerFromHook(byId), owner)) {
        await txn.delete(idKey);
      }
      if (claim !== undefined && sameOwner(claim.owner, owner)) {
        await txn.delete(claimKey);
      }
    });
  }

  /**
   * Fence a run and remove all of its known derived indexes atomically.
   * Once the fence exists, putOwned() and hook finalization cannot resurrect
   * entries from a delayed request.
   */
  async expireRun(request: ExpireRunIndexesRequest): Promise<ExpireRunIndexesResult> {
    return await this.ctx.storage.transaction(async (txn) => {
      await txn.put(`expired:${request.runId}`, request.expiredAt);
      let deleted = 0;

      for (const key of new Set(request.keys)) {
        if (await txn.delete(key)) deleted++;
      }

      for (const hook of request.hooks) {
        const owner = { runId: request.runId, hookId: hook.hookId };
        const tokenKey = `hook:${hook.token}`;
        const idKey = `hookid:${hook.hookId}`;
        const claimKey = `hookclaim:${hook.token}`;
        const [byToken, byId, claim] = await Promise.all([
          txn.get<string>(tokenKey),
          txn.get<string>(idKey),
          txn.get<HookClaim>(claimKey),
        ]);
        if (byToken !== undefined && sameOwner(ownerFromHook(byToken), owner)) {
          if (await txn.delete(tokenKey)) deleted++;
        }
        if (byId !== undefined && sameOwner(ownerFromHook(byId), owner)) {
          if (await txn.delete(idKey)) deleted++;
        }
        if (claim !== undefined && sameOwner(claim.owner, owner)) {
          if (await txn.delete(claimKey)) deleted++;
        }
      }

      return { deleted };
    });
  }
}
