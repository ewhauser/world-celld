import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    include: ['test/perf/index-main-baseline.test.ts'],
    globalSetup: ['test/global-setup.ts'],
    testTimeout: 30_000,
  },
});
