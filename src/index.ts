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
import { createStreamer } from './streamer.js';

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

export function createCelldWorld(config?: CelldWorldConfig): World {
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
    readPollMs: resolved.readPollMs,
  });

  return {
    ...storage,
    ...queue,
    ...streamer,
    // Enables resilient start: runs are created at the current spec version,
    // so the queue message carries runInput and run_started can bootstrap the
    // run when run_created has not landed yet.
    specVersion: SPEC_VERSION_CURRENT,
  };
}

// Export createWorld as an alias for compatibility with @workflow/world
export { createCelldWorld as createWorld };
