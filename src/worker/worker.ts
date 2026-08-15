/**
 * celld worker entry: exports the Durable Object (cell) classes and the
 * default fetch router. This module (and everything it imports) must stay
 * free of Node built-ins — it runs inside celld's workerd runtime.
 */
import { createRouter, type WorkerEnv } from './router.js';

export { IndexDO } from './durable-objects/IndexDO.js';
export { QueueDO } from './durable-objects/QueueDO.js';
export { StreamDO } from './durable-objects/StreamDO.js';
export { WorkflowRunDO } from './durable-objects/WorkflowRunDO.js';
export { createRouter, type WorkerEnv } from './router.js';

export default {
  async fetch(request: Request, env: WorkerEnv): Promise<Response> {
    return createRouter(env)(request);
  },
};
