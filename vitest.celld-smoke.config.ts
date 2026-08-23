import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    include: ['test/integration/celld-smoke.test.ts'],
    hookTimeout: 90_000,
    testTimeout: 90_000,
  },
});
