import { defineConfig } from 'vitest/config';

const workloadTimeoutMs = Number.parseInt(process.env.PERF_TIMEOUT_MS ?? '180000', 10);
const steadySeconds = Number.parseInt(process.env.PERF_STEADY_SECONDS ?? '0', 10);

export default defineConfig({
  test: {
    globals: true,
    include: [
      process.env.PERF_FAILOVER === '1'
        ? 'test/perf/minio-failover.test.ts'
        : 'test/perf/minio.test.ts',
    ],
    fileParallelism: false,
    maxWorkers: 1,
    testTimeout: workloadTimeoutMs * 2 + steadySeconds * 1_000 + 60_000,
  },
});
