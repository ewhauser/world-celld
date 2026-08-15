/**
 * Local stand-in for a celld fleet: the real world-celld router + cell
 * classes in-process (from @ewhauser/world-celld/testing), with alarms
 * pumped on real time so queue deliveries flow.
 *
 * Run the app against it with:
 *   WORKFLOW_TARGET_WORLD=@ewhauser/world-celld \
 *   CELLD_FLEET_URL=http://127.0.0.1:8787 \
 *   CELLD_WORLD_SECRET=dev-secret \
 *   node .output/server/index.mjs
 */
import { startDevFleet } from '@ewhauser/world-celld/testing';

const secret = process.env.CELLD_WORLD_SECRET ?? 'dev-secret';
const fleet = await startDevFleet({ secret });

console.log('local celld-world fleet (in-process emulation)');
console.log(`  CELLD_FLEET_URL=${fleet.url}`);
console.log(`  CELLD_WORLD_SECRET=${secret}`);
console.log('Ctrl-C to stop. State is in-memory only.');

// Keep the process alive.
setInterval(() => {}, 1 << 30);
