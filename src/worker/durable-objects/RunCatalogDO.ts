import type { ExpireRunIndexesResult } from '../../retention.js';
import type { IndexListOptions, IndexListPage } from '../../indexes.js';
import { DurableObject } from '../do-base.js';
import {
  CATALOG_FENCE_GRACE_MS,
  LIFECYCLE_COMPACTION_BATCH,
  LIFECYCLE_COMPACTION_RETRY_MS,
  MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS,
} from '../../lifecycle.js';

const MAX_INDEX_LIST_SIZE = 1001;
const EXPIRY_GC_PREFIX = 'expiry-gc:';

interface RunCatalogEnv {
  clock?: () => number;
}

interface CatalogExpiryFence {
  expiredAt: number;
  compactAt: number;
}

interface CatalogExpiryGc {
  runId: string;
  compactAt: number;
}

function pad(ms: number): string {
  return String(Math.max(0, Math.floor(ms))).padStart(13, '0');
}

function expiredKey(runId: string): string {
  return `expired:${runId}`;
}

function expiryGcKey(runId: string, compactAt: number): string {
  return `${EXPIRY_GC_PREFIX}${pad(compactAt)}:${encodeURIComponent(runId)}`;
}

/** One of a fixed set of run-catalog shards, routed by the stable run ID hash. */
export class RunCatalogDO extends DurableObject {
  private now(): number {
    const clock = (this.env as RunCatalogEnv)?.clock;
    return typeof clock === 'function' ? clock() : Date.now();
  }

  async upsertRun(
    runId: string,
    keys: string[],
    serializedMetadata: string,
    publicationExpiresAt: number,
  ): Promise<{ stored: boolean }> {
    if (keys.length !== 2) throw new Error('Run catalog commit requires exactly two keys');
    return await this.ctx.storage.transaction(async (txn) => {
      if ((await txn.get(expiredKey(runId))) !== undefined) return { stored: false };
      const now = this.now();
      if (
        !Number.isSafeInteger(publicationExpiresAt) ||
        publicationExpiresAt <= now ||
        publicationExpiresAt > now + MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS
      ) {
        return { stored: false };
      }
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
      const marker = expiredKey(runId);
      const existing = await txn.get<CatalogExpiryFence>(marker);
      const compactAt = existing?.compactAt ?? this.now() + CATALOG_FENCE_GRACE_MS;
      await txn.put<CatalogExpiryFence>(marker, { expiredAt, compactAt });
      await txn.put<CatalogExpiryGc>(expiryGcKey(runId, compactAt), { runId, compactAt });
      await this.armAtMost(txn, compactAt);
      return { deleted: await txn.delete(keys) };
    });
  }

  async alarm(): Promise<void> {
    try {
      await this.compactExpiryFences();
    } catch {
      // Cloudflare retries only six times. Persist a fresh alarm edge so a
      // transient storage failure cannot abandon compaction permanently.
      await this.ctx.storage.setAlarm(this.now() + LIFECYCLE_COMPACTION_RETRY_MS);
    }
  }

  private async armAtMost(storage: DurableObjectStorage | DurableObjectTransaction, at: number) {
    const current = await storage.getAlarm();
    if (current === null || at < current) await storage.setAlarm(at);
  }

  private async compactExpiryFences(): Promise<void> {
    const now = this.now();
    await this.ctx.storage.transaction(async (txn) => {
      const due = await txn.list<CatalogExpiryGc>({
        prefix: EXPIRY_GC_PREFIX,
        end: `${EXPIRY_GC_PREFIX}${pad(now + 1)}`,
        limit: LIFECYCLE_COMPACTION_BATCH + 1,
      });
      const page = Array.from(due).slice(0, LIFECYCLE_COMPACTION_BATCH);
      const markerKeys = page.map(([, item]) => expiredKey(item.runId));
      const markers = await txn.get<CatalogExpiryFence>(markerKeys);
      const deletes = page.flatMap(([key, item], index) => {
        const marker = markers.get(markerKeys[index]);
        return marker?.compactAt === item.compactAt ? [key, markerKeys[index]] : [key];
      });
      for (let offset = 0; offset < deletes.length; offset += LIFECYCLE_COMPACTION_BATCH) {
        await txn.delete(deletes.slice(offset, offset + LIFECYCLE_COMPACTION_BATCH));
      }

      if (due.size > LIFECYCLE_COMPACTION_BATCH) {
        await txn.setAlarm(now + 1);
        return;
      }
      const next = await txn.list<CatalogExpiryGc>({ prefix: EXPIRY_GC_PREFIX, limit: 1 });
      const nextAt = next.values().next().value?.compactAt;
      if (nextAt === undefined) await txn.deleteAlarm();
      else await txn.setAlarm(Math.max(now + 1, nextAt));
    });
  }
}
