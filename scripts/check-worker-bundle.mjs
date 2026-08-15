#!/usr/bin/env node
/**
 * Verify the worker bundle is deployable to celld: bundle dist/worker.js the
 * same way `celld deploy` does (esbuild, workerd conditions) but WITHOUT
 * `--external:node:*`, so any accidental Node built-in import fails loudly
 * here instead of at fleet deploy time.
 *
 * Mirrors celld's esbuild invocation (crates/celld/deploy.rs::run_esbuild):
 *   --bundle --format=esm --platform=browser --target=es2024
 *   --conditions=workerd,worker,browser --external:cloudflare:*
 */
import { build } from 'esbuild';

const result = await build({
  entryPoints: ['dist/worker.js'],
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: 'es2024',
  conditions: ['workerd', 'worker', 'browser'],
  external: ['cloudflare:*'],
  write: false,
  logLevel: 'silent',
}).catch((error) => {
  console.error('worker bundle check FAILED — dist/worker.js is not celld-deployable:');
  for (const err of error.errors ?? [{ text: String(error) }]) {
    console.error(`  ${err.text}`);
  }
  process.exit(1);
});

const bundled = result.outputFiles[0].text;
const nodeImport = bundled.match(/from\s*["']node:[^"']+["']/);
if (nodeImport) {
  console.error(`worker bundle check FAILED — Node built-in survived bundling: ${nodeImport[0]}`);
  process.exit(1);
}

// workerd has no Buffer global (eve-ambient's celld build check treats any
// Buffer usage as a deploy blocker); catch references that esbuild can't.
const bufferUse = bundled.match(/\bBuffer\.(from|byteLength|alloc|concat)\b/);
if (bufferUse) {
  console.error(`worker bundle check FAILED — Buffer usage in worker bundle: ${bufferUse[0]}`);
  process.exit(1);
}

console.log('worker bundle check OK — dist/worker.js bundles cleanly under workerd conditions');
