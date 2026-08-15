import { execSync } from 'node:child_process';

/**
 * The conformance suite (spec.test.ts) spawns @workflow/world-testing's
 * server, which resolves '@ewhauser/world-celld' through the self-link in
 * node_modules — i.e. the BUILT dist. Rebuild before the run so tests never
 * exercise stale output.
 */
export default function setup() {
  execSync('pnpm build', { stdio: 'ignore', cwd: new URL('..', import.meta.url).pathname });
}
