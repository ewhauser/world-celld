/**
 * @ewhauser/world-celld — celld World implementation for the Vercel Workflow
 * DevKit. Modeled on vinnymac/worlds packages/world-cloudflare/src/index.ts
 * (Apache-2.0, see NOTICE).
 */
import { SPEC_VERSION_CURRENT, type World } from '@workflow/world';
import { type CelldWorldConfig, type CelldWorldEnv, resolveConfig } from './config.js';
import { createRemoteEnv } from './remote/namespaces.js';
import { callFleetRoute } from './remote/rpc-client.js';
import { createQueue } from './queue.js';
import { createStorage } from './storage.js';
import { createStreamer, type CelldStreamer } from './streamer.js';
import type { CleanupRecord } from './retention.js';

export type { CelldWorldConfig, CelldWorldEnv, IndexNamespace } from './config.js';
export type { CelldQueueProducer } from './queue.js';
export type {
  NativeQueueEnvelope,
  NativeQueueSendOptions,
  NativeQueueSendResult,
} from './queue-protocol.js';
export type { WorkflowRunDONamespace, WorkflowRunDOStub } from './storage.js';
export type { StreamDONamespace, StreamDOStub } from './streamer.js';
export {
  MAX_STREAM_BATCH_BYTES,
  MAX_STREAM_CHUNK_BYTES,
  MAX_STREAM_ERROR_BYTES,
  MAX_STREAM_LONG_POLL_MS,
  MAX_STREAM_READ_BYTES,
  MAX_STREAM_READ_CHUNKS,
  MAX_STREAM_WRITE_CHUNKS,
} from './stream-protocol.js';
export type {
  StreamErrorData,
  StreamReadRequest,
  StreamReadResult,
  StreamTerminalState,
  StreamWriteResult,
} from './stream-protocol.js';
export type { CleanupPhase, CleanupRecord, RunTombstone } from './retention.js';

export interface CelldRetentionAdmin {
  getStatus(runId: string): Promise<CleanupRecord | null>;
  schedule(runId: string): Promise<CleanupRecord | null>;
  cleanupNow(runId: string): Promise<CleanupRecord | null>;
  rearm(runId: string): Promise<CleanupRecord | null>;
}

export type CelldWorld = World & CelldStreamer & { retention: CelldRetentionAdmin };

const REQUIRED_ENV_BINDINGS = [
  'WORKFLOW_DB',
  'WORKFLOW_INDEX',
  'WORKFLOW_QUEUE',
  'WORKFLOW_STREAMS',
] as const satisfies readonly (keyof CelldWorldEnv)[];

export function createCelldWorld(config?: CelldWorldConfig): CelldWorld {
  const resolved = resolveConfig(config);

  // Check for global test environment first (for @workflow/world-testing)
  let env = resolved.env;
  if (!env) {
    const globalEnv = (globalThis as { CELLD_ENV?: CelldWorldEnv }).CELLD_ENV;
    if (globalEnv) {
      env = globalEnv;
    }
  }

  if (!env) {
    if (resolved.fleetUrl) {
      if (!resolved.secret) {
        throw new Error(
          'world-celld: config.secret (or CELLD_WORLD_SECRET) is required with fleetUrl',
        );
      }
      env = createRemoteEnv({
        fleetUrl: resolved.fleetUrl,
        secret: resolved.secret,
        timeoutMs: resolved.rpcTimeoutMs,
      });
    }
  }

  if (!env) {
    throw new Error(
      'celld environment not configured. Provide config.fleetUrl + config.secret ' +
        '(or CELLD_FLEET_URL / CELLD_WORLD_SECRET), or pass config.env with ' +
        'WORKFLOW_DB, WORKFLOW_INDEX, WORKFLOW_QUEUE, WORKFLOW_STREAMS',
    );
  }

  for (const binding of REQUIRED_ENV_BINDINGS) {
    if (!env[binding]) {
      throw new Error(`world-celld: config.env is missing required binding ${binding}`);
    }
  }

  const storage = createStorage({
    env: {
      WORKFLOW_DB: env.WORKFLOW_DB,
      WORKFLOW_INDEX: env.WORKFLOW_INDEX,
    },
    deploymentId: resolved.deploymentId,
    runRetentionMs: resolved.runRetentionMs,
  });

  const queue = createQueue({
    env: {
      WORKFLOW_QUEUE: env.WORKFLOW_QUEUE,
    },
    deploymentId: resolved.deploymentId,
    baseUrl: resolved.baseUrl,
    deploymentUrls: resolved.deploymentUrls,
  });

  const streamer = createStreamer({
    env: {
      WORKFLOW_STREAMS: env.WORKFLOW_STREAMS,
    },
    streamLongPollMs: resolved.streamLongPollMs,
    streamFlushIntervalMs: resolved.streamFlushIntervalMs,
  });

  const runStub = (runId: string) => env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(runId));
  const retention: CelldRetentionAdmin = {
    getStatus: (runId) => runStub(runId).getCleanupStatus(),
    schedule: (runId) => {
      if (resolved.runRetentionMs === 0) {
        throw new Error(
          'world-celld: runRetentionMs must be greater than zero to schedule cleanup',
        );
      }
      return runStub(runId).scheduleCleanup({
        retentionMs: resolved.runRetentionMs,
      });
    },
    cleanupNow: (runId) =>
      runStub(runId).cleanupNow({
        retentionMs: Math.max(1, resolved.runRetentionMs),
      }),
    rearm: (runId) => runStub(runId).rearmCleanup(),
  };

  return {
    ...storage,
    ...queue,
    ...streamer,
    retention,
    // Enables resilient start: runs are created at the current spec version,
    // so the queue message carries runInput and run_started can bootstrap the
    // run when run_created has not landed yet.
    specVersion: SPEC_VERSION_CURRENT,
    capabilities: {
      atomicStartHook: { active: typeof env.WORKFLOW_INDEX.reserveStartHook === 'function' },
      // Retention lives in the index's token shards; an injected index must
      // opt in, since releasing a run's hooks would otherwise drop them.
      hookRetention: { active: env.WORKFLOW_INDEX.retainsHookTokens === true },
      // An injected index without the takeover operations cannot honor force.
      hookForceClaim:
        typeof env.WORKFLOW_INDEX.forceReserveHook === 'function' &&
        typeof env.WORKFLOW_INDEX.completeHookTakeover === 'function' &&
        typeof env.WORKFLOW_INDEX.settleHookTakeover === 'function',
    },
  };
}

/**
 * Workflow's asynchronous World factory. Probe the authenticated fleet before
 * exposing atomic start admission. Synchronous createCelldWorld() cannot do
 * this for URL-based worlds, so use this factory for remote atomic starts.
 */
export async function createWorld(config?: CelldWorldConfig): Promise<CelldWorld> {
  const resolved = resolveConfig(config);
  if (
    resolved.env ||
    (globalThis as { CELLD_ENV?: CelldWorldEnv }).CELLD_ENV ||
    !resolved.fleetUrl ||
    !resolved.secret
  ) {
    return createCelldWorld(config);
  }

  const transport = {
    fleetUrl: resolved.fleetUrl,
    secret: resolved.secret,
    timeoutMs: resolved.rpcTimeoutMs,
  };
  let active = false;
  try {
    const capability = await callFleetRoute<{ active?: boolean } | null>(
      transport,
      '/v1/index/hooks/start-capability',
      [],
      { idempotent: true },
    );
    active = capability?.active === true;
  } catch (error) {
    const status = (error as { status?: unknown }).status;
    // A rejected secret is a configuration error; every later call would fail too.
    if (status === 401 || status === 403) throw error;
    // A worker from before start Hooks (404) is still valid for ordinary runs.
    // Any other probe failure must not take the World down: leave atomic
    // starts disabled, which fails closed with START_HOOK_ADMISSION_DISABLED.
    if (status !== 404) {
      console.warn(
        'world-celld: atomic start Hook capability probe failed; atomic starts are disabled',
        error,
      );
    }
  }
  return createCelldWorld({ ...config, env: createRemoteEnv(transport, active) });
}
