import type { WorkflowRunDONamespace } from './storage.js';
import type { StreamDONamespace } from './streamer.js';
import type { QueueCellNamespace } from './queue.js';
import { MAX_STREAM_LONG_POLL_MS } from './stream-protocol.js';
import type { WorkflowIndex } from './indexes.js';

/** Public alias retained for custom in-process environments. */
export type IndexNamespace = WorkflowIndex;

export interface HookTokenOwner {
  runId: string;
  hookId: string;
}

export interface CelldWorldEnv {
  WORKFLOW_DB: WorkflowRunDONamespace;
  WORKFLOW_INDEX: IndexNamespace;
  WORKFLOW_QUEUE: QueueCellNamespace;
  WORKFLOW_STREAMS: StreamDONamespace;
}

export interface CelldWorldConfig {
  /**
   * Base URL of any celld fleet node's public listener (e.g.
   * `http://fleet.internal:8080`). Requests are routed to the deployed
   * world-celld worker, which forwards them to the owning cells.
   * Default: process.env.CELLD_FLEET_URL
   */
  fleetUrl?: string;
  /**
   * Bearer secret shared with the worker (its WORLD_SECRET var).
   * Default: process.env.CELLD_WORLD_SECRET
   */
  secret?: string;
  /**
   * Pre-built namespaces (used by tests and the in-process harness). When
   * provided, fleetUrl/secret are not used.
   */
  env?: CelldWorldEnv;
  /** Default: process.env.CELLD_DEPLOYMENT_ID || 'celld-default' */
  deploymentId?: string;
  /**
   * Base URL the app's workflow endpoints are mounted on; QueueDO cells
   * deliver to `${baseUrl}/.well-known/workflow/v1/flow`.
   * Default: process.env.WORKFLOW_BASE_URL || `http://localhost:${PORT ?? 3000}`
   */
  baseUrl?: string;
  /** Number of queue cells to spread enqueues over. Default: 1 */
  queueShards?: number;
  /**
   * Keep terminal run payloads for this many milliseconds before replacing
   * them with metadata-only tombstones. Zero disables automatic cleanup.
   * Default: process.env.CELLD_RUN_RETENTION_MS || 0
   */
  runRetentionMs?: number;
  /** Duration of one bounded idle stream read. Default: 20000 */
  streamLongPollMs?: number;
  /** Runtime stream batching delay. Default: 0 */
  streamFlushIntervalMs?: number;
  /** Per-attempt fleet RPC deadline in milliseconds. Default: 30000 */
  rpcTimeoutMs?: number;
}

export interface ResolvedCelldConfig {
  fleetUrl?: string;
  secret?: string;
  env?: CelldWorldEnv;
  deploymentId: string;
  baseUrl?: string;
  queueShards: number;
  runRetentionMs: number;
  streamLongPollMs: number;
  streamFlushIntervalMs: number;
  rpcTimeoutMs: number;
}

export function resolveConfig(config?: CelldWorldConfig): ResolvedCelldConfig {
  const retentionRaw = config?.runRetentionMs ?? process.env.CELLD_RUN_RETENTION_MS ?? 0;
  const runRetentionMs =
    typeof retentionRaw === 'number' ? retentionRaw : Number.parseInt(retentionRaw, 10);
  if (!Number.isSafeInteger(runRetentionMs) || runRetentionMs < 0) {
    throw new Error('world-celld: runRetentionMs must be a non-negative safe integer');
  }

  const rpcTimeoutMs = config?.rpcTimeoutMs ?? 30_000;
  if (!Number.isSafeInteger(rpcTimeoutMs) || rpcTimeoutMs < 1) {
    throw new Error('world-celld: rpcTimeoutMs must be a positive safe integer');
  }
  const streamLongPollMs =
    config?.streamLongPollMs ??
    Math.min(MAX_STREAM_LONG_POLL_MS, Math.max(1, rpcTimeoutMs - 1_000));
  if (
    !Number.isSafeInteger(streamLongPollMs) ||
    streamLongPollMs < 1 ||
    streamLongPollMs > MAX_STREAM_LONG_POLL_MS
  ) {
    throw new Error(
      `world-celld: streamLongPollMs must be between 1 and ${MAX_STREAM_LONG_POLL_MS}`,
    );
  }
  if (streamLongPollMs >= rpcTimeoutMs) {
    throw new Error('world-celld: streamLongPollMs must be less than rpcTimeoutMs');
  }
  const streamFlushIntervalMs = config?.streamFlushIntervalMs ?? 0;
  if (!Number.isSafeInteger(streamFlushIntervalMs) || streamFlushIntervalMs < 0) {
    throw new Error('world-celld: streamFlushIntervalMs must be a non-negative safe integer');
  }

  return {
    fleetUrl: config?.fleetUrl ?? process.env.CELLD_FLEET_URL,
    secret: config?.secret ?? process.env.CELLD_WORLD_SECRET,
    env: config?.env,
    deploymentId: config?.deploymentId ?? process.env.CELLD_DEPLOYMENT_ID ?? 'celld-default',
    baseUrl: config?.baseUrl,
    queueShards: config?.queueShards ?? 1,
    runRetentionMs,
    streamLongPollMs,
    streamFlushIntervalMs,
    rpcTimeoutMs,
  };
}
