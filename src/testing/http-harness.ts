/**
 * Boots the real worker router over real cell classes (on FakeFleet state)
 * behind a node:http server — the full wire protocol without a celld binary.
 */
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { createRouter, type WorkerEnv } from '../worker/router.js';
import { IndexDO } from '../worker/durable-objects/IndexDO.js';
import { QueueDO } from '../worker/durable-objects/QueueDO.js';
import { StreamDO } from '../worker/durable-objects/StreamDO.js';
import { WorkflowRunDO } from '../worker/durable-objects/WorkflowRunDO.js';
import { FakeFleet } from './fake-cell.js';

export interface Harness {
  url: string;
  fleet: FakeFleet;
  close(): Promise<void>;
}

export interface HarnessOptions {
  secret?: string;
  /** Use FakeFleet's manually advanced clock instead of wall-clock time. */
  virtualClock?: boolean;
  /** Additional cell classes by binding key (e.g. { queue: QueueDO }). */
  extraClasses?: Record<string, new (ctx: unknown, env: unknown) => object>;
  /** env passed to cell constructors (celld `vars`). */
  cellEnv?: Record<string, unknown>;
}

export async function startHarness(options: HarnessOptions = {}): Promise<Harness> {
  const cellEnv = { ...options.cellEnv };
  const fleet = new FakeFleet(
    {
      runs: WorkflowRunDO as never,
      streams: StreamDO as never,
      index: IndexDO as never,
      queue: QueueDO as never,
      ...options.extraClasses,
    },
    cellEnv,
  );
  cellEnv.clock ??= options.virtualClock ? () => fleet.now : () => Date.now();

  // Durable Object constructors receive both vars and sibling bindings in a
  // real celld deployment. Populate the same environment for cross-cell
  // retention cleanup in the in-process fleet.
  Object.assign(cellEnv, {
    WORKFLOW_DB: fleet.namespace('runs'),
    WORKFLOW_STREAMS: fleet.namespace('streams'),
    WORKFLOW_INDEX: fleet.namespace('index'),
    WORKFLOW_QUEUE: fleet.namespace('queue'),
  });

  const env: WorkerEnv = {
    WORKFLOW_DB: fleet.namespace('runs'),
    WORKFLOW_STREAMS: fleet.namespace('streams'),
    WORKFLOW_INDEX: fleet.namespace('index'),
    WORKFLOW_QUEUE: fleet.namespace('queue'),
    WORLD_SECRET: options.secret,
  };

  const router = createRouter(env);

  const server = http.createServer(async (req, res) => {
    try {
      const chunks: Buffer[] = [];
      for await (const chunk of req) {
        chunks.push(chunk as Buffer);
      }
      const body = Buffer.concat(chunks);
      const request = new Request(`http://127.0.0.1${req.url ?? '/'}`, {
        method: req.method,
        headers: req.headers as Record<string, string>,
        body: req.method === 'GET' || req.method === 'HEAD' || body.length === 0 ? null : body,
      });
      const response = await router(request);
      res.writeHead(response.status, Object.fromEntries(response.headers.entries()));
      res.end(Buffer.from(await response.arrayBuffer()));
    } catch (error) {
      res.writeHead(500, { 'content-type': 'application/json' });
      res.end(JSON.stringify({ error: { name: 'HarnessError', message: String(error) } }));
    }
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;

  return {
    url: `http://127.0.0.1:${port}`,
    fleet,
    close: () =>
      new Promise<void>((resolve, reject) => {
        // Test clients may retain an active keep-alive connection after their
        // assertions finish. Stop accepting connections first, then destroy
        // any that remain so teardown cannot wait indefinitely.
        server.close((err) => (err ? reject(err) : resolve()));
        server.closeAllConnections();
      }),
  };
}
