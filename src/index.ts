/**
 * @ewhauser/world-celld — celld World implementation for the Vercel Workflow
 * DevKit. Modeled on vinnymac/worlds packages/world-cloudflare/src/index.ts
 * (Apache-2.0, see NOTICE).
 */
import { SPEC_VERSION_CURRENT, type World } from '@workflow/world';
import { type CelldWorldConfig, type CelldWorldEnv, resolveConfig } from './config.js';
import { createRemoteEnv } from './remote/namespaces.js';
import { createQueue } from './queue.js';
import { createStorage } from './storage.js';
import { createStreamer, type CelldStreamer } from './streamer.js';
import type { CleanupRecord } from './retention.js';

export type { CelldWorldConfig, CelldWorldEnv, IndexNamespace } from './config.js';
export type {
  EnqueueOutcome,
  EnqueueRequest,
  QueueCellConfig,
  QueueCellNamespace,
  QueueCellStub,
} from './queue.js';
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

  const storage = createStorage({
    env: {
      WORKFLOW_DB: env.WORKFLOW_DB,
      WORKFLOW_INDEX: env.WORKFLOW_INDEX,
    },
    deploymentId: resolved.deploymentId,
    runRetentionMs: resolved.runRetentionMs,
    queueShards: resolved.queueShards,
  });

  const queue = createQueue({
    env: {
      WORKFLOW_QUEUE: env.WORKFLOW_QUEUE,
    },
    deploymentId: resolved.deploymentId,
    baseUrl: resolved.baseUrl,
    queueShards: resolved.queueShards,
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
        queueShards: resolved.queueShards,
      });
    },
    cleanupNow: (runId) =>
      runStub(runId).cleanupNow({
        retentionMs: Math.max(1, resolved.runRetentionMs),
        queueShards: resolved.queueShards,
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
  };
}

// Export createWorld as an alias for compatibility with @workflow/world
export { createCelldWorld as createWorld };
