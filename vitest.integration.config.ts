import { defineConfig } from 'vitest/config';

/**
 * Integration tests against a real celld fleet.
 * Gated on CELLD_FLEET_URL + CELLD_WORLD_SECRET (tests skip themselves when unset).
 */
export default defineConfig({
  test: {
    globals: true,
    include: ['test/integration/**/*.test.ts'],
    testTimeout: 60_000,
  },
});
