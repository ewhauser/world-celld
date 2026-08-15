import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    include: ['test/**/*.test.ts', 'src/**/*.test.ts'],
    exclude: ['test/integration/**', 'node_modules/**'],
    passWithNoTests: true,
    globalSetup: ['test/global-setup.ts'],
    testTimeout: 30_000,
  },
});
