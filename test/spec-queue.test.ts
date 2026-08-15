/**
 * Conformance with LIVE queue cells: same @workflow/world spec suite as
 * spec.test.ts, but CELLD_QUEUE_MODE=cells forces the world-testing server
 * child onto the production queue path — every dispatch flows
 * child --enqueue rpc--> harness QueueDO --alarm + outbound fetch--> child.
 *
 * A fresh harness per test (queue cells pin the child's callback URL from
 * the first enqueue, and each test spawns a new child on a new port), with a
 * real-time pump driving the fake fleet's alarms.
 */
import { createTestSuite } from '@workflow/world-testing';
import { afterEach, beforeEach, expect, test } from 'vitest';
import { QueueDO } from '../src/worker/durable-objects/QueueDO.js';
import { startHarness, type Harness } from '../src/testing/http-harness.js';

const SECRET = 'spec-queue-secret';

let harness: Harness | null = null;
let pump: ReturnType<typeof setInterval> | null = null;
let pumping = false;

beforeEach(async () => {
  harness = await startHarness({
    secret: SECRET,
    extraClasses: { queue: QueueDO },
  });
  process.env.CELLD_FLEET_URL = harness.url;
  process.env.CELLD_WORLD_SECRET = SECRET;
  process.env.CELLD_QUEUE_MODE = 'cells';

  const fleet = harness.fleet;
  pump = setInterval(async () => {
    if (pumping) return; // don't overlap alarm cycles
    pumping = true;
    try {
      fleet.now = Date.now();
      await fleet.fireDueAlarms();
    } finally {
      pumping = false;
    }
  }, 20);
});

afterEach(async () => {
  if (pump) clearInterval(pump);
  pump = null;
  delete process.env.CELLD_QUEUE_MODE;
  await harness?.close();
  harness = null;
});

test('starts the queue conformance harness', () => {
  expect(harness?.url).toMatch(/^http:\/\/127\.0\.0\.1:/);
});

createTestSuite('@ewhauser/world-celld');
