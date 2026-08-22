/**
 * celld worker entry: exports the Durable Object (cell) classes and the
 * default fetch router. This module (and everything it imports) must stay
 * free of Node built-ins — it runs inside celld's workerd runtime.
 */
import { createRouter, type WorkerEnv } from './router.js';
import { runRetentionSweep, type RetentionSweepEnv } from './retention-sweep.js';

export { HookIdDO } from './durable-objects/HookIdDO.js';
export { HookTokenDO } from './durable-objects/HookTokenDO.js';
export { QueueDO } from './durable-objects/QueueDO.js';
export { RunCatalogDO } from './durable-objects/RunCatalogDO.js';
export { StreamDO } from './durable-objects/StreamDO.js';
export { WorkflowRunDO } from './durable-objects/WorkflowRunDO.js';
export { createRouter, type WorkerEnv } from './router.js';

interface ScheduledControllerLike {
  scheduledTime: number;
  cron: string;
}

async function scheduled(controller: ScheduledControllerLike, env: WorkerEnv): Promise<void> {
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
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    return createRouter(env)(request);
  },
  scheduled,
};
