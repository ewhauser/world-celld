import type { ExpireRunIndexesResult } from '../../retention.js';
import type { IndexListOptions, IndexListPage } from '../../indexes.js';
import { DurableObject } from '../do-base.js';

const MAX_INDEX_LIST_SIZE = 1001;

/** One of a fixed set of run-catalog shards, routed by the stable run ID hash. */
export class RunCatalogDO extends DurableObject {
  async upsertRun(
    runId: string,
    keys: string[],
    serializedMetadata: string,
  ): Promise<{ stored: boolean }> {
    if (keys.length !== 2) throw new Error('Run catalog commit requires exactly two keys');
    return await this.ctx.storage.transaction(async (txn) => {
      if ((await txn.get(`expired:${runId}`)) !== undefined) return { stored: false };
      await txn.put(Object.fromEntries(keys.map((key) => [key, serializedMetadata])));
      return { stored: true };
    });
  }

  async list(options: IndexListOptions = {}): Promise<IndexListPage> {
    const suppliedLimit = options.limit;
    const requestedLimit =
      typeof suppliedLimit === 'number' && Number.isFinite(suppliedLimit)
        ? Math.floor(suppliedLimit)
        : MAX_INDEX_LIST_SIZE;
    const limit = Math.min(MAX_INDEX_LIST_SIZE, Math.max(1, requestedLimit));
    const entries = await this.ctx.storage.list<string>({
      prefix: options.prefix ?? '',
      ...(options.reverse
        ? { reverse: true, end: options.cursor }
        : { startAfter: options.cursor }),
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

  async expireRun(
    runId: string,
    keys: string[],
    expiredAt: number,
  ): Promise<ExpireRunIndexesResult> {
    if (keys.length !== 2) throw new Error('Run catalog expiry requires exactly two keys');
    return await this.ctx.storage.transaction(async (txn) => {
      await txn.put(`expired:${runId}`, expiredAt);
      return { deleted: await txn.delete(keys) };
    });
  }
}
