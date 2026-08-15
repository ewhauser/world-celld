import { defineConfig } from 'vitest/config';

const workloadTimeoutMs = Number.parseInt(process.env.PERF_TIMEOUT_MS ?? '180000', 10);

export default defineConfig({
  test: {
    globals: true,
    include: ['test/perf/**/*.test.ts'],
    fileParallelism: false,
    maxWorkers: 1,
    testTimeout: workloadTimeoutMs * 2 + 60_000,
  },
});
