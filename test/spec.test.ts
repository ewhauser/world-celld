/**
 * @workflow/world spec conformance, over the REAL wire protocol.
 *
 * createTestSuite spawns @workflow/world-testing's server as a subprocess;
 * it resolves '@ewhauser/world-celld' (the built dist, via the node_modules
 * self-link) and calls createWorld() with env-var config only. We boot the
 * in-process harness — real router + real DO classes on fake cells — and
 * point CELLD_FLEET_URL at it, so storage/streamer/index traffic crosses the
 * actual HTTP RPC protocol.
 *
 * Queue dispatch runs on the in-process test pump inside the server child
 * (it inherits VITEST=true). M4 flips CELLD_QUEUE_MODE=cells to run the same
 * suite against live QueueDO cells.
 */
import { createTestSuite } from '@workflow/world-testing';
import { afterAll, beforeAll, test } from 'vitest';
import { startHarness, type Harness } from '../src/testing/http-harness.js';

const SECRET = 'spec-test-secret';
let harness: Harness;

beforeAll(async () => {
  harness = await startHarness({ secret: SECRET });
  process.env.CELLD_FLEET_URL = harness.url;
  process.env.CELLD_WORLD_SECRET = SECRET;
});

afterAll(async () => {
  await harness.close();
});

test('smoke', () => {});

createTestSuite('@ewhauser/world-celld');
