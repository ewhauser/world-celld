import { Hono } from 'hono';
import { getHookByToken, getRun, resumeHook, start } from 'workflow/api';
import { processOrder } from '../workflows/order.js';

const app = new Hono()
  .get('/', (c) =>
    c.json({
      name: 'world-celld demo',
      try: [
        'POST /orders/{orderId}',
        'GET  /runs/{runId}',
        'GET  /runs/{runId}/stream',
        'POST /approvals/{token}  {"approved": true}',
      ],
    }),
  )
  .post('/orders/:orderId', async (c) => {
    const run = await start(processOrder, [c.req.param('orderId')]);
    return c.json({ runId: run.runId });
  })
  .get('/runs/:runId', async (c) => {
    const run = getRun(c.req.param('runId'));
    return c.json({ runId: c.req.param('runId'), status: await run.status });
  })
  .get('/runs/:runId/stream', (c) => {
    const run = getRun(c.req.param('runId'));
    return new Response(run.getReadable(), {
      headers: { 'content-type': 'text/plain; charset=utf-8' },
    });
  })
  .post('/approvals/:token', async (c) => {
    const hook = await getHookByToken(c.req.param('token'));
    const { runId } = await resumeHook(hook.token, await c.req.json());
    return c.json({ runId, resumed: true });
  });

export default app;
