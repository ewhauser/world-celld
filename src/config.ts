import type { WorkflowRunDONamespace } from './storage.js';
import type { StreamDONamespace } from './streamer.js';
import type { QueueCellNamespace } from './queue.js';

/** Storage-layer KV interface (satisfied by IndexDO over HTTP, or a mock). */
export interface IndexNamespace {
  get(key: string): Promise<string | null>;
  put(key: string, value: string): Promise<void>;
  delete(key: string): Promise<void>;
  list(options?: {
    prefix?: string;
    cursor?: string;
    limit?: number;
    reverse?: boolean;
  }): Promise<{
    keys: Array<{ name: string }>;
    list_complete: boolean;
    cursor?: string;
  }>;
  reserveHookToken(
    token: string,
    owner: HookTokenOwner,
  ): Promise<{ claimed: boolean; holder?: HookTokenOwner }>;
  finalizeHookIndexes(
    token: string,
    hookId: string,
    serializedHook: string,
    owner: HookTokenOwner,
  ): Promise<void>;
  releaseHookToken(token: string, owner: HookTokenOwner): Promise<void>;
  deleteHookIndexes(
    token: string,
    hookId: string,
    owner: HookTokenOwner,
  ): Promise<void>;
}

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
   * deliver to `${baseUrl}/.well-known/workflow/v1/{flow|step}`.
   * Default: process.env.WORKFLOW_BASE_URL || `http://localhost:${PORT ?? 3000}`
   */
  baseUrl?: string;
  /** Number of queue cells to spread enqueues over. Default: 1 */
  queueShards?: number;
  /** Poll interval (ms) while waiting for new chunks on a live stream. Default: 250 */
  readPollMs?: number;
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
  readPollMs: number;
  rpcTimeoutMs: number;
}

export function resolveConfig(config?: CelldWorldConfig): ResolvedCelldConfig {
  return {
    fleetUrl: config?.fleetUrl ?? process.env.CELLD_FLEET_URL,
    secret: config?.secret ?? process.env.CELLD_WORLD_SECRET,
    env: config?.env,
    deploymentId:
      config?.deploymentId ?? process.env.CELLD_DEPLOYMENT_ID ?? 'celld-default',
    baseUrl: config?.baseUrl,
    queueShards: config?.queueShards ?? 1,
    readPollMs: config?.readPollMs ?? 250,
    rpcTimeoutMs: config?.rpcTimeoutMs ?? 30_000,
  };
}
