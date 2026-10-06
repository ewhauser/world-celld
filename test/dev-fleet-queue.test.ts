import http from 'node:http';
import { afterEach, expect, test } from 'vitest';
import { createCelldWorld } from '../src/index.js';
import { startDevFleet } from '../src/testing/index.js';

const previousQueueMode = process.env.CELLD_QUEUE_MODE;

afterEach(() => {
  if (previousQueueMode === undefined) delete process.env.CELLD_QUEUE_MODE;
  else process.env.CELLD_QUEUE_MODE = previousQueueMode;
});

test('development fleet delivers native Queue publications to the app', async () => {
  process.env.CELLD_QUEUE_MODE = 'native';
  const secret = 'dev-fleet-queue-test';
  const fleet = await startDevFleet({ secret });
  let resolveDelivery!: (value: { path: string; messageId: string }) => void;
  const delivered = new Promise<{ path: string; messageId: string }>((resolve) => {
    resolveDelivery = resolve;
  });
  const app = http.createServer((request, response) => {
    resolveDelivery({
      path: request.url ?? '',
      messageId: request.headers['x-vqs-message-id'] as string,
    });
    response.writeHead(204);
    response.end();
  });

  try {
    await new Promise<void>((resolve) => app.listen(0, '127.0.0.1', resolve));
    const address = app.address();
    if (!address || typeof address === 'string') throw new Error('missing app port');
    const world = createCelldWorld({
      fleetUrl: fleet.url,
      secret,
      baseUrl: `http://127.0.0.1:${address.port}`,
    });
    const queued = await world.queue('__wkf_workflow_test', {
      __healthCheck: true,
      correlationId: 'dev-fleet-delivery',
    });

    await expect(delivered).resolves.toEqual({
      path: '/.well-known/workflow/v1/flow',
      messageId: queued.messageId,
    });
    expect(fleet.queueMessages).toHaveLength(1);
  } finally {
    await new Promise<void>((resolve) => app.close(() => resolve()));
    await fleet.stop();
  }
}, 10_000);
