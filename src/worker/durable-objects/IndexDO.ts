import { DurableObject } from '../do-base.js';

/**
 * Global index cell, replacing the Workers KV namespace world-cloudflare
 * uses for run listing and hook token/id lookup.
 *
 * A single cell (named `index`) serializes every read and write, so unlike
 * KV the index is read-after-write consistent — the hook-token
 * check-then-write in storage.ts no longer races KV's eventual consistency
 * for sequential operations.
 *
 * The RPC surface matches the storage-layer KVNamespace interface bit-for-bit
 * (including MockKVNamespace's pagination contract: cursor = last returned
 * key, `list_complete` reflects whether further keys exist), so the vendored
 * storage.ts works against it unchanged.
 */
export class IndexDO extends DurableObject {
  async get(key: string): Promise<string | null> {
    const value = await this.ctx.storage.get<string>(key);
    return value ?? null;
  }

  async put(key: string, value: string): Promise<void> {
    await this.ctx.storage.put(key, value);
  }

  async delete(key: string): Promise<void> {
    await this.ctx.storage.delete(key);
  }

  async list(options?: { prefix?: string; cursor?: string; limit?: number }): Promise<{
    keys: Array<{ name: string }>;
    list_complete: boolean;
    cursor?: string;
  }> {
    const prefix = options?.prefix ?? '';
    const limit = options?.limit ?? 1000;

    // Fetch one extra key to learn whether the listing is complete without
    // advancing past the page (the "exact limit + list_complete" contract
    // storage.ts relies on).
    const entries = await this.ctx.storage.list<unknown>({
      prefix,
      startAfter: options?.cursor,
      limit: limit + 1,
    });

    const names = Array.from(entries.keys());
    const page = names.slice(0, limit);
    const listComplete = names.length <= limit;

    return {
      keys: page.map((name) => ({ name })),
      list_complete: listComplete,
      cursor: listComplete ? undefined : page.at(-1),
    };
  }
}
