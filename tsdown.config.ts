import { fileURLToPath } from 'node:url';
import { defineConfig } from 'tsdown';

const workerdBase = fileURLToPath(new URL('./src/worker/do-base-workerd.ts', import.meta.url));

export default defineConfig([
  // Node-facing entries: the DO classes resolve ./do-base.js to the Node
  // shim, so everything here imports cleanly without `cloudflare:workers`.
  {
    entry: {
      index: 'src/index.ts',
      testing: 'src/testing/index.ts',
    },
    format: 'esm',
    dts: true,
    sourcemap: true,
    clean: true,
    outDir: 'dist',
    external: [/^node:/],
    outExtensions: () => ({ js: '.js', dts: '.d.ts' }),
  },
  // Worker entry for celld: swap the DO base for the real cloudflare:workers
  // re-export (celld's JS RPC on stubs requires `extends DurableObject`).
  {
    entry: {
      worker: 'src/worker/worker.ts',
    },
    format: 'esm',
    dts: true,
    sourcemap: true,
    clean: false,
    outDir: 'dist',
    external: [/^cloudflare:/],
    alias: {
      [fileURLToPath(new URL('./src/worker/do-base.ts', import.meta.url))]: workerdBase,
    },
    outExtensions: () => ({ js: '.js', dts: '.d.ts' }),
  },
]);
