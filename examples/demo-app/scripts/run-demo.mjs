/**
 * End-to-end demo driver:
 *  1. starts the in-process celld-world fleet (or uses CELLD_FLEET_URL if set)
 *  2. starts the built app (.output/server/index.mjs) pointed at it
 *  3. starts an order workflow, tails its stream, approves the hook, and
 *     prints the outcome
 *
 * Build first: pnpm build
 */
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';

const SECRET = process.env.CELLD_WORLD_SECRET ?? 'demo-secret';
const APP_PORT = Number(process.env.DEMO_APP_PORT ?? 3123);

let fleetUrl = process.env.CELLD_FLEET_URL;
let fleet = null;
if (!fleetUrl) {
  const { startDevFleet } = await import('@ewhauser/world-celld/testing');
  fleet = await startDevFleet({ secret: SECRET });
  fleetUrl = fleet.url;
  console.log(`[demo] in-process fleet at ${fleetUrl}`);
} else {
  console.log(`[demo] using real fleet at ${fleetUrl}`);
}

const app = spawn('node', ['.output/server/index.mjs'], {
  env: {
    ...process.env,
    WORKFLOW_TARGET_WORLD: '@ewhauser/world-celld',
    CELLD_FLEET_URL: fleetUrl,
    CELLD_WORLD_SECRET: SECRET,
    WORKFLOW_BASE_URL: `http://127.0.0.1:${APP_PORT}`,
    PORT: String(APP_PORT),
    NODE_ENV: 'production',
  },
  stdio: ['ignore', 'inherit', 'inherit'],
});
process.on('exit', () => app.kill());

const base = `http://127.0.0.1:${APP_PORT}`;
for (let i = 0; ; i++) {
  try {
    const res = await fetch(base);
    if (res.ok) break;
  } catch {
    if (i > 100) throw new Error('app did not start');
  }
  await delay(100);
}
console.log(`[demo] app ready at ${base}`);

const { runId } = await fetch(`${base}/orders/order-1042`, { method: 'POST' }).then((r) =>
  r.json(),
);
console.log(`[demo] started run ${runId}`);

// Tail the run's output stream and approve when the hook token appears.
// (Read until the workflow reports shipment — the run's default stream stays
// open for the run's lifetime, so don't wait for EOF.)
const streamRes = await fetch(`${base}/runs/${runId}/stream`);
const reader = streamRes.body.getReader();
const decoder = new TextDecoder();
let approved = false;
while (true) {
  const { value, done } = await reader.read();
  if (done) break;
  const text = decoder.decode(value, { stream: true });
  process.stdout.write(`[stream] ${text}`);
  const token = text.match(/POST \/approvals\/(\S+)/)?.[1];
  if (token && !approved) {
    approved = true;
    console.log(`[demo] approving via hook token ${token}`);
    await fetch(`${base}/approvals/${token}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ approved: true, comment: 'ship it' }),
    });
  }
  if (text.includes('shipped as')) break;
}
await reader.cancel().catch(() => {});

// Poll until the run reports terminal status.
let status;
for (let i = 0; i < 100; i++) {
  status = await fetch(`${base}/runs/${runId}`).then((r) => r.json());
  if (status.status === 'completed' || status.status === 'failed') break;
  await delay(200);
}
console.log(`[demo] final status: ${JSON.stringify(status)}`);

app.kill();
await fleet?.stop();
process.exit(status?.status === 'completed' ? 0 : 1);
