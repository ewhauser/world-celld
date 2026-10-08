// Test-only SDK host. The compiled handler supplies real Workflow scheduling.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { start, getRun } from 'workflow/api';
import { getWorld } from 'workflow/runtime';
import { POST } from './.well-known/workflow/v1/flow.mjs';

const manifest = JSON.parse(
  await readFile(new URL('./.well-known/workflow/v1/manifest.json', import.meta.url)),
);
const world = await getWorld();
await world.start?.();
const workflow = Object.values(manifest.workflows)
  .flatMap(Object.values)
  .find((entry) => entry.workflowId.endsWith('//fanout'));
if (!workflow) throw new Error('fanout workflow is missing from the manifest');
const runs = new Map();
const server = http.createServer(async (incoming, outgoing) => {
  try {
    const chunks = [];
    for await (const chunk of incoming) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    const path = new URL(incoming.url, process.env.WORKFLOW_BASE_URL).pathname;
    let response;
    if (path === '/health') {
      response = Response.json({ ready: true });
    } else if (path === '/metrics') {
      response = Response.json({
        ...globalThis.qualificationQueueMetrics,
        queueBatchPresent: typeof world.queueBatch === 'function',
        eventBatchPresent: typeof world.events.createBatch === 'function',
        memory: process.memoryUsage(),
        resources: process.resourceUsage(),
      });
    } else if (path === '/invoke') {
      const args = JSON.parse(body);
      const run = await start(workflow, args);
      runs.set(run.runId, run);
      response = Response.json({ runId: run.runId });
    } else if (path.startsWith('/runs/')) {
      const id = path.slice('/runs/'.length);
      const run = runs.get(id) ?? getRun(id);
      const status = await run.status;
      response = Response.json({
        status,
        ...(status === 'completed' ? { value: await run.returnValue } : {}),
        ...(status === 'failed' ? { error: await run.error } : {}),
      });
    } else {
      response = await POST(
        new Request(new URL(incoming.url, process.env.WORKFLOW_BASE_URL), {
          method: incoming.method,
          headers: incoming.headers,
          ...(body.length ? { body } : {}),
        }),
      );
    }
    outgoing.writeHead(response.status, Object.fromEntries(response.headers));
    outgoing.end(Buffer.from(await response.arrayBuffer()));
  } catch (error) {
    console.error(error);
    outgoing.writeHead(500);
    outgoing.end(String(error));
  }
});
server.listen(Number(process.env.PORT), '127.0.0.1');
