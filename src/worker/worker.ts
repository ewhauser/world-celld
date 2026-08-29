/**
 * celld worker entry: exports the Durable Object (cell) classes and the
 * default fetch router. This module (and everything it imports) must stay
 * free of Node built-ins — it runs inside celld's workerd runtime.
 */
import { createRouter, type WorkerEnv } from './router.js';
import { runRetentionSweep, type RetentionSweepEnv } from './retention-sweep.js';
import {
  createQueuePayloadStore,
  type QueuePayloadObjectStorageBinding,
} from './queue-payload-store.js';

export { HookIdDO } from './durable-objects/HookIdDO.js';
export { HookTokenDO } from './durable-objects/HookTokenDO.js';
export { RunCatalogDO } from './durable-objects/RunCatalogDO.js';
export { StreamDO } from './durable-objects/StreamDO.js';
export { WorkflowRunDO } from './durable-objects/WorkflowRunDO.js';
export { createRouter, type WorkerEnv } from './router.js';

interface ScheduledControllerLike {
  scheduledTime: number;
  cron: string;
}

type CelldWorkerEnv = Omit<WorkerEnv, 'WORKFLOW_QUEUE_PAYLOADS'> & {
  WORKFLOW_QUEUE_PAYLOADS?: QueuePayloadObjectStorageBinding;
};

async function scheduled(controller: ScheduledControllerLike, env: CelldWorkerEnv): Promise<void> {
  const result = await runRetentionSweep(
    controller.scheduledTime,
    env as unknown as RetentionSweepEnv,
  );
  if (result.scanned > 0) {
    console.info('world-celld retention sweep', {
      cron: controller.cron,
      ...result,
    });
  }
}

export default {
  async fetch(request: Request, env: CelldWorkerEnv): Promise<Response> {
    return createRouter({
      ...env,
      WORKFLOW_QUEUE_PAYLOADS: env.WORKFLOW_QUEUE_PAYLOADS
        ? createQueuePayloadStore(env.WORKFLOW_QUEUE_PAYLOADS)
        : undefined,
    })(request);
  },
  scheduled,
};
