/**
 * In-process celld-world emulation for tests and local development: the real
 * worker router and the real cell classes over Map-backed fake state, behind
 * node:http — the full wire protocol without a celld fleet.
 *
 * `startDevFleet()` additionally drives cell alarms on real time, so QueueDO
 * deliveries flow — a drop-in local stand-in for a deployed fleet.
 */
export { FakeFleet, FakeStorage } from './fake-cell.js';
export { startHarness, type Harness, type HarnessOptions } from './http-harness.js';

import { startHarness, type Harness, type HarnessOptions } from './http-harness.js';

export interface DevFleet extends Harness {
  stop(): Promise<void>;
}

/** Alarm-pump interval for the dev fleet (real time). */
const PUMP_INTERVAL_MS = 25;

export async function startDevFleet(options: HarnessOptions = {}): Promise<DevFleet> {
  const harness = await startHarness(options);
  let pumping = false;
  const pump = setInterval(async () => {
    if (pumping) return;
    pumping = true;
    try {
      harness.fleet.now = Date.now();
      await harness.fleet.fireDueAlarms();
    } catch (error) {
      console.error('[world-celld dev fleet] alarm pump error:', error);
    } finally {
      pumping = false;
    }
  }, PUMP_INTERVAL_MS);
  // Don't hold the process open just for the pump.
  pump.unref?.();

  return {
    ...harness,
    async stop() {
      clearInterval(pump);
      await harness.close();
    },
  };
}
