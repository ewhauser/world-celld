/**
 * Integration tests against a REAL celld fleet running the deployed
 * world-celld worker. Skipped unless the fleet is configured:
 *
 *   CELLD_FLEET_URL=http://127.0.0.1:8080 \
 *   CELLD_WORLD_SECRET=... \
 *   pnpm test:integration
 *
 * CELLD_CALLBACK_BASE_URL overrides the queue-delivery callback target when
 * the fleet cannot reach this machine as 127.0.0.1 (remote fleets).
 *
 * These double as the celld spike assertions from the plan:
 *  - storage.transaction + list options (startAfter/end/reverse/start) via
 *    applyEvent + paginated listEvents asc/desc
 *  - Date/Uint8Array fidelity across celld's DO RPC isolate boundary
 *  - alarm semantics via live QueueDO delivery, delay, and rearmAlarm
 *  - request body limits via a large stream chunk
 */
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createCelldWorld } from '../../src/index.js';
import { callDO } from '../../src/remote/rpc-client.js';
import type { QueueStats } from '../../src/worker/durable-objects/QueueDO.js';

const FLEET_URL = process.env.CELLD_FLEET_URL;
const SECRET = process.env.CELLD_WORLD_SECRET;

interface CapturedDelivery {
  headers: Record<string, string | string[] | undefined>;
  body: string;
  respondedWith: number;
}

async function waitFor<T>(
  poll: () => Promise<T | null | undefined | false>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await poll();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}`);
}

describe.skipIf(!FLEET_URL || !SECRET)('celld fleet integration', () => {
  const transport = { fleetUrl: FLEET_URL ?? '', secret: SECRET ?? '' };
  const deliveries: CapturedDelivery[] = [];
  /** Next responses the callback listener should give (defaults to 200). */
  const responseQueue: Array<{ status: number; body?: unknown }> = [];
  let listener: http.Server;
  let callbackBaseUrl: string;

  beforeAll(async () => {
    process.env.CELLD_QUEUE_MODE = 'cells';

    listener = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const next = responseQueue.shift() ?? { status: 200, body: { ok: true } };
      deliveries.push({
        headers: req.headers,
        body: Buffer.concat(chunks).toString('utf8'),
        respondedWith: next.status,
      });
      res.writeHead(next.status, { 'content-type': 'application/json' });
      res.end(JSON.stringify(next.body ?? { ok: next.status < 300 }));
    });
    await new Promise<void>((resolve) => listener.listen(0, '0.0.0.0', resolve));
    const { port } = listener.address() as AddressInfo;
    callbackBaseUrl = process.env.CELLD_CALLBACK_BASE_URL ?? `http://127.0.0.1:${port}`;
  });

  afterAll(async () => {
    delete process.env.CELLD_QUEUE_MODE;
    await new Promise<void>((resolve, reject) =>
      listener.close((err) => (err ? reject(err) : resolve())),
    );
  });

  function world() {
    return createCelldWorld({
      fleetUrl: transport.fleetUrl,
      secret: transport.secret,
      baseUrl: callbackBaseUrl,
      deploymentId: 'integration',
    });
  }

  it('health endpoint identifies the deployed world', async () => {
    const res = await fetch(`${transport.fleetUrl}/v1/health`);
    expect(res.status).toBe(200);
    const body = await res.json();
    expect(body.name).toBe('world-celld');
  });

  it('run lifecycle: Dates and Uint8Array survive the DO RPC boundary', async () => {
    const w = world();
    const input = new Uint8Array([0, 1, 2, 253, 254, 255]);
    const created = await w.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'integration',
        workflowName: `it-${randomUUID()}`,
        input: [input],
      },
    });

    const runId = created.run!.runId;
    const run = await w.runs.get(runId);
    expect(run.status).toBe('pending');
    expect(run.createdAt).toBeInstanceOf(Date);
    expect(run.updatedAt).toBeInstanceOf(Date);
    expect(Math.abs(run.createdAt.getTime() - Date.now())).toBeLessThan(60_000);
    expect('input' in run && run.input !== undefined).toBe(true);
    if (!('input' in run) || !run.input) throw new Error('expected run input');
    expect(run.input[0]).toBeInstanceOf(Uint8Array);
    expect(Array.from(run.input[0] as Uint8Array)).toEqual([0, 1, 2, 253, 254, 255]);
  });

  it('paginated event listing asc and desc (list options spike)', async () => {
    const w = world();
    const created = await w.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'integration',
        workflowName: `it-${randomUUID()}`,
        input: [],
      },
    });
    const runId = created.run!.runId;
    await w.events.create(runId, { eventType: 'run_started', eventData: {} });
    await w.events.create(runId, {
      eventType: 'step_created',
      eventData: { stepId: `step_${randomUUID().slice(0, 8)}#0`, stepName: 's', input: [] },
    });

    const page1 = await w.events.list({ runId, pagination: { limit: 2, sortOrder: 'asc' } });
    expect(page1.data).toHaveLength(2);
    expect(page1.hasMore).toBe(true);
    const page2 = await w.events.list({
      runId,
      pagination: { limit: 2, sortOrder: 'asc', cursor: page1.cursor ?? undefined },
    });
    expect(page2.data.length).toBeGreaterThanOrEqual(1);
    const asc = [...page1.data, ...page2.data].map((e) => e.eventId);
    expect(new Set(asc).size).toBe(asc.length); // no dropped/duplicated events

    const desc = await w.events.list({ runId, pagination: { limit: 10, sortOrder: 'desc' } });
    expect(desc.data.map((e) => e.eventId)).toEqual(asc.toReversed());
  });

  it('streams round-trip including a 1 MiB chunk (body limit spike)', async () => {
    const w = world();
    const streamName = `it-stream-${randomUUID()}`;
    const big = new Uint8Array(1024 * 1024).map((_, i) => i % 251);

    await w.writeToStream(streamName, 'wrun_integration', 'small');
    await w.writeToStream(streamName, 'wrun_integration', big);
    await w.closeStream(streamName, 'wrun_integration');

    const info = await w.getStreamInfo(streamName, 'wrun_integration');
    expect(info).toMatchObject({ tailIndex: 1, done: true });

    const chunks = await w.getStreamChunks(streamName, 'wrun_integration', {});
    expect(chunks.data[1].data.length).toBe(big.length);
    expect(chunks.data[1].data[1024 * 1024 - 1]).toBe((1024 * 1024 - 1) % 251);
  });

  it('queue delivery: live cell alarm posts x-vqs to the app', async () => {
    const w = world();
    const marker = randomUUID();
    const before = deliveries.length;

    await w.queue(`__wkf_step_it_${marker.slice(0, 8)}`, { marker });

    const delivered = await waitFor(
      async () => deliveries.slice(before).find((d) => d.body.includes(marker)),
      30_000,
      'queue delivery',
    );
    expect(delivered.headers['x-vqs-message-id']).toMatch(/^msg_/);
    expect(delivered.headers['x-vqs-message-attempt']).toBe('1');
  });

  it('delayed delivery honors delaySeconds via the cell alarm', async () => {
    const w = world();
    const marker = randomUUID();
    const before = deliveries.length;
    const enqueuedAt = Date.now();

    await w.queue(
      `__wkf_step_delay_${marker.slice(0, 8)}`,
      { marker },
      {
        delaySeconds: 3,
      },
    );

    const delivered = await waitFor(
      async () => deliveries.slice(before).find((d) => d.body.includes(marker)),
      45_000,
      'delayed delivery',
    );
    expect(delivered).toBeDefined();
    expect(Date.now() - enqueuedAt).toBeGreaterThanOrEqual(2_500);
  });

  it('503 {timeoutSeconds} redelivers the same attempt', async () => {
    const w = world();
    const marker = randomUUID();
    const before = deliveries.length;
    responseQueue.push({ status: 503, body: { timeoutSeconds: 2 } });

    await w.queue(`__wkf_step_retry_${marker.slice(0, 8)}`, { marker });

    await waitFor(
      async () => deliveries.slice(before).filter((d) => d.body.includes(marker)).length >= 2,
      45_000,
      '503 redelivery',
    );
    const mine = deliveries.slice(before).filter((d) => d.body.includes(marker));
    expect(mine[0].respondedWith).toBe(503);
    expect(mine[1].headers['x-vqs-message-attempt']).toBe('1'); // not a counted retry
  });

  it('dead-letters after max attempts and redrives (slow: ~40s of backoff)', async () => {
    const w = world();
    const marker = randomUUID();
    // 5 permanent-500 responses -> DLQ.
    for (let i = 0; i < 5; i++) responseQueue.push({ status: 500, body: { error: 'down' } });

    const { messageId } = await w.queue(`__wkf_step_dlq_${marker.slice(0, 8)}`, {
      marker,
    });

    const stats = await waitFor(
      async () => {
        const s = await callDO<QueueStats>(transport, 'queue', 'q:0', 'stats', []);
        return s.deadLetters > 0 ? s : null;
      },
      90_000,
      'dead letter',
    );
    expect(stats.deadLetters).toBeGreaterThanOrEqual(1);

    const before = deliveries.length;
    const redriven = await callDO<{ ok: boolean }>(transport, 'queue', 'q:0', 'redriveDeadLetter', [
      messageId,
    ]);
    expect(redriven.ok).toBe(true);

    await waitFor(
      async () => deliveries.slice(before).find((d) => d.body.includes(marker)),
      30_000,
      'redriven delivery',
    );

    await callDO(transport, 'queue', 'q:0', 'purgeDeadLetters', []);
  }, 150_000);

  it('rearmAlarm reports a coherent alarm state', async () => {
    const { alarmAt } = await callDO<{ alarmAt: number | null }>(
      transport,
      'queue',
      'q:0',
      'rearmAlarm',
      [],
    );
    expect(alarmAt === null || typeof alarmAt === 'number').toBe(true);
  });
});

describe.skipIf(Boolean(FLEET_URL && SECRET))('celld fleet integration (skipped)', () => {
  it('needs CELLD_FLEET_URL and CELLD_WORLD_SECRET', () => {
    expect(true).toBe(true);
  });
});
