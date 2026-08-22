import type { WorkflowRun } from '@workflow/world';
import type { HookTokenOwner } from './config.js';
import type {
  ExpireRunIndexesRequest,
  ExpireRunIndexesResult,
  ReleaseHookIndexesRequest,
  ReleaseHookIndexesResult,
} from './retention.js';

export const RUN_CATALOG_SHARDS = 16;
export const HOOK_TOKEN_SHARDS = 32;
export const HOOK_ID_SHARDS = 32;

const INDEX_PROTOCOL_VERSION = 'v1';
const HOOK_RELEASE_BATCH = 64;

export interface IndexListOptions {
  prefix?: string;
  cursor?: string;
  /** Exclusive upper bound for creation-time retention scans. */
  end?: string;
  limit?: number;
  reverse?: boolean;
}

export interface IndexListPage {
  keys: Array<{ name: string; value: string }>;
  list_complete: boolean;
  cursor?: string;
}

export interface RunCatalogShardStub {
  upsertRun(
    runId: string,
    keys: string[],
    serializedMetadata: string,
    publicationExpiresAt: number,
  ): Promise<{ stored: boolean }>;
  list(options?: IndexListOptions): Promise<IndexListPage>;
  expireRun(runId: string, keys: string[], expiredAt: number): Promise<ExpireRunIndexesResult>;
}

export type HookClaimResult =
  | { claimed: true; claimId: string }
  | { claimed: false; holder: HookTokenOwner };

export interface HookReservation {
  claimId: string;
  tokenClaimId?: string;
  hookIdClaimId?: string;
}

export type HookReservationResult =
  | { admitted: true; reservation: HookReservation }
  | { admitted: false; holder: HookTokenOwner };

export interface HookTokenShardStub {
  get(token: string): Promise<string | null>;
  reserve(token: string, owner: HookTokenOwner, proposedClaimId: string): Promise<HookClaimResult>;
  finalize(
    token: string,
    serializedHook: string,
    owner: HookTokenOwner,
    claimId?: string,
  ): Promise<{ stored: boolean }>;
  releaseClaim(token: string, owner: HookTokenOwner, claimId: string): Promise<void>;
  releaseBatch(
    hooks: Array<{ token: string; owner: HookTokenOwner }>,
  ): Promise<{ deleted: number }>;
}

export interface HookIdShardStub {
  get(hookId: string): Promise<string | null>;
  reserve(hookId: string, owner: HookTokenOwner, proposedClaimId: string): Promise<HookClaimResult>;
  publish(
    hookId: string,
    serializedHook: string,
    owner: HookTokenOwner,
    claimId?: string,
  ): Promise<{ stored: boolean }>;
  releaseClaim(hookId: string, owner: HookTokenOwner, claimId: string): Promise<void>;
  releaseBatch(
    hooks: Array<{ hookId: string; owner: HookTokenOwner }>,
  ): Promise<{ deleted: number }>;
}

interface CellIdLike {
  toString(): string;
}

export interface CellNamespaceLike<T> {
  idFromName(name: string): CellIdLike;
  get(id: CellIdLike): T;
}

export interface WorkflowIndexBindings {
  runCatalog: CellNamespaceLike<RunCatalogShardStub>;
  hookTokens: CellNamespaceLike<HookTokenShardStub>;
  hookIds: CellNamespaceLike<HookIdShardStub>;
}

/** High-level client used by the World storage layer and retention coordinator. */
export interface WorkflowIndex {
  commitRun(
    run: WorkflowRun,
    serializedMetadata: string,
    publicationExpiresAt: number,
  ): Promise<{ stored: boolean }>;
  listRuns(options?: IndexListOptions): Promise<IndexListPage>;
  getHookByToken(token: string): Promise<string | null>;
  getHookById(hookId: string): Promise<string | null>;
  reserveHook(token: string, owner: HookTokenOwner): Promise<HookReservationResult>;
  finalizeHookIndexes(
    token: string,
    hookId: string,
    serializedHook: string,
    owner: HookTokenOwner,
    reservation?: HookReservation,
  ): Promise<void>;
  releaseHookReservation(
    token: string,
    owner: HookTokenOwner,
    reservation: HookReservation,
  ): Promise<void>;
  releaseHookIndexes(request: ReleaseHookIndexesRequest): Promise<ReleaseHookIndexesResult>;
  expireRun(request: ExpireRunIndexesRequest): Promise<ExpireRunIndexesResult>;
}

/** Stable FNV-1a routing hash. Collisions share a shard but never a storage key. */
export function stableIndexHash(value: string): number {
  let hash = 0x811c9dc5;
  for (const byte of new TextEncoder().encode(value)) {
    hash ^= byte;
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

function shardName(domain: string, value: string, cardinality: number): string {
  const width = Math.max(2, (cardinality - 1).toString(16).length);
  const shard = stableIndexHash(value) % cardinality;
  return `${domain}:${INDEX_PROTOCOL_VERSION}:${shard.toString(16).padStart(width, '0')}`;
}

export function runCatalogShardName(runId: string): string {
  return shardName('run-catalog', runId, RUN_CATALOG_SHARDS);
}

export function hookTokenShardName(token: string): string {
  return shardName('hook-token', token, HOOK_TOKEN_SHARDS);
}

export function hookIdShardName(hookId: string): string {
  return shardName('hook-id', hookId, HOOK_ID_SHARDS);
}

export function allRunCatalogShardNames(): string[] {
  return Array.from(
    { length: RUN_CATALOG_SHARDS },
    (_, shard) => `run-catalog:${INDEX_PROTOCOL_VERSION}:${shard.toString(16).padStart(2, '0')}`,
  );
}

function stub<T>(namespace: CellNamespaceLike<T>, name: string): T {
  return namespace.get(namespace.idFromName(name));
}

function runKeys(run: Pick<WorkflowRun, 'runId' | 'workflowName' | 'createdAt'>): string[] {
  const timestamp = run.createdAt.getTime().toString().padStart(13, '0');
  return [`run:${run.workflowName}:${timestamp}:${run.runId}`, `runall:${timestamp}:${run.runId}`];
}

function groupByShard<T>(values: T[], route: (value: T) => string): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  for (const value of values) {
    const name = route(value);
    const group = groups.get(name);
    if (group) group.push(value);
    else groups.set(name, [value]);
  }
  return groups;
}

function sameOwner(left: HookTokenOwner, right: HookTokenOwner): boolean {
  return left.runId === right.runId && left.hookId === right.hookId;
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const length = Math.min(left.byteLength, right.byteLength);
  for (let index = 0; index < length; index++) {
    const difference = left[index] - right[index];
    if (difference !== 0) return difference;
  }
  return left.byteLength - right.byteLength;
}

async function inBatches<T>(
  values: T[],
  operation: (batch: T[]) => Promise<{ deleted: number }>,
): Promise<number> {
  let deleted = 0;
  for (let offset = 0; offset < values.length; offset += HOOK_RELEASE_BATCH) {
    deleted += (await operation(values.slice(offset, offset + HOOK_RELEASE_BATCH))).deleted;
  }
  return deleted;
}

export function createWorkflowIndex(bindings: WorkflowIndexBindings): WorkflowIndex {
  const releaseHooks = async (request: ReleaseHookIndexesRequest): Promise<number> => {
    const tokenGroups = groupByShard(request.hooks, (hook) => hookTokenShardName(hook.token));
    const idGroups = groupByShard(request.hooks, (hook) => hookIdShardName(hook.hookId));
    const releases: Array<Promise<number>> = [];
    for (const [name, hooks] of tokenGroups) {
      const target = stub(bindings.hookTokens, name);
      releases.push(
        inBatches(
          hooks.map((hook) => ({
            token: hook.token,
            owner: { runId: request.runId, hookId: hook.hookId },
          })),
          (batch) => target.releaseBatch(batch),
        ),
      );
    }
    for (const [name, hooks] of idGroups) {
      const target = stub(bindings.hookIds, name);
      releases.push(
        inBatches(
          hooks.map((hook) => ({
            hookId: hook.hookId,
            owner: { runId: request.runId, hookId: hook.hookId },
          })),
          (batch) => target.releaseBatch(batch),
        ),
      );
    }
    return (await Promise.all(releases)).reduce((total, count) => total + count, 0);
  };

  return {
    async commitRun(run, serializedMetadata, publicationExpiresAt) {
      return await stub(bindings.runCatalog, runCatalogShardName(run.runId)).upsertRun(
        run.runId,
        runKeys(run),
        serializedMetadata,
        publicationExpiresAt,
      );
    },

    async listRuns(options) {
      const suppliedLimit = options?.limit;
      const requestedLimit =
        typeof suppliedLimit === 'number' && Number.isFinite(suppliedLimit)
          ? Math.floor(suppliedLimit)
          : 1000;
      const limit = Math.min(1000, Math.max(1, requestedLimit));
      const pages = await Promise.all(
        allRunCatalogShardNames().map((name) =>
          stub(bindings.runCatalog, name).list({ ...options, limit: limit + 1 }),
        ),
      );
      const encoder = new TextEncoder();
      const candidates = pages
        .flatMap((shardPage) =>
          shardPage.keys.map((entry) => ({ entry, encodedName: encoder.encode(entry.name) })),
        )
        .toSorted((left, right) => {
          const compared = compareBytes(left.encodedName, right.encodedName);
          return options?.reverse ? -compared : compared;
        });
      const page = candidates.slice(0, limit).map(({ entry }) => entry);
      const listComplete =
        candidates.length <= limit && pages.every((entry) => entry.list_complete);
      return {
        keys: page,
        list_complete: listComplete,
        cursor: listComplete ? undefined : page.at(-1)?.name,
      };
    },

    getHookByToken(token) {
      return stub(bindings.hookTokens, hookTokenShardName(token)).get(token);
    },

    getHookById(hookId) {
      return stub(bindings.hookIds, hookIdShardName(hookId)).get(hookId);
    },

    async reserveHook(token, owner) {
      const proposedClaimId = crypto.randomUUID();
      const tokenTarget = stub(bindings.hookTokens, hookTokenShardName(token));
      const tokenClaim = await tokenTarget.reserve(token, owner, proposedClaimId);
      if (!tokenClaim.claimed && !sameOwner(tokenClaim.holder, owner)) {
        return { admitted: false, holder: tokenClaim.holder };
      }

      const tokenClaimId = tokenClaim.claimed ? tokenClaim.claimId : undefined;
      const hookIdTarget = stub(bindings.hookIds, hookIdShardName(owner.hookId));
      const sharedClaimId = tokenClaimId ?? proposedClaimId;
      let hookIdClaim: HookClaimResult;
      try {
        hookIdClaim = await hookIdTarget.reserve(owner.hookId, owner, sharedClaimId);
      } catch (error) {
        await Promise.allSettled([
          ...(tokenClaimId ? [tokenTarget.releaseClaim(token, owner, tokenClaimId)] : []),
          hookIdTarget.releaseClaim(owner.hookId, owner, sharedClaimId),
        ]);
        throw error;
      }
      if (!hookIdClaim.claimed) {
        if (tokenClaimId) await tokenTarget.releaseClaim(token, owner, tokenClaimId);
        return { admitted: false, holder: hookIdClaim.holder };
      }

      const reservation: HookReservation = {
        claimId: `${tokenClaimId ?? '-'}:${hookIdClaim.claimId}`,
        tokenClaimId,
        hookIdClaimId: hookIdClaim.claimId,
      };
      return { admitted: true, reservation };
    },

    async finalizeHookIndexes(token, hookId, serializedHook, owner, reservation) {
      const tokenResult = await stub(bindings.hookTokens, hookTokenShardName(token)).finalize(
        token,
        serializedHook,
        owner,
        reservation?.tokenClaimId,
      );
      if (!tokenResult.stored) return;
      await stub(bindings.hookIds, hookIdShardName(hookId)).publish(
        hookId,
        serializedHook,
        owner,
        reservation?.hookIdClaimId,
      );
    },

    async releaseHookReservation(token, owner, reservation) {
      const releases: Array<Promise<void>> = [];
      if (reservation.tokenClaimId) {
        releases.push(
          stub(bindings.hookTokens, hookTokenShardName(token)).releaseClaim(
            token,
            owner,
            reservation.tokenClaimId,
          ),
        );
      }
      if (reservation.hookIdClaimId) {
        releases.push(
          stub(bindings.hookIds, hookIdShardName(owner.hookId)).releaseClaim(
            owner.hookId,
            owner,
            reservation.hookIdClaimId,
          ),
        );
      }
      await Promise.all(releases);
    },

    async releaseHookIndexes(request) {
      return { deleted: await releaseHooks(request) };
    },

    async expireRun(request) {
      const [catalog, hookDeletes] = await Promise.all([
        stub(bindings.runCatalog, runCatalogShardName(request.runId)).expireRun(
          request.runId,
          request.keys,
          request.expiredAt,
        ),
        releaseHooks({ runId: request.runId, hooks: request.hooks }),
      ]);
      return { deleted: catalog.deleted + hookDeletes };
    },
  };
}
