import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { resolveConfig, type CelldWorldEnv } from '../src/config.js';
import { createCelldWorld } from '../src/index.js';
import { MAX_FLEET_RPC_TIMEOUT_MS } from '../src/lifecycle.js';
import { MAX_STREAM_LONG_POLL_MS } from '../src/stream-protocol.js';
import { createMockEnv } from '../src/test-mocks.js';

describe('runtime configuration validation', () => {
  const originalGlobalEnv = (globalThis as { CELLD_ENV?: CelldWorldEnv }).CELLD_ENV;

  beforeEach(() => {
    delete process.env.CELLD_RUN_RETENTION_MS;
    delete process.env.CELLD_FLEET_URL;
    delete process.env.CELLD_WORLD_SECRET;
    delete (globalThis as { CELLD_ENV?: CelldWorldEnv }).CELLD_ENV;
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    if (originalGlobalEnv) {
      (globalThis as { CELLD_ENV?: CelldWorldEnv }).CELLD_ENV = originalGlobalEnv;
    } else {
      delete (globalThis as { CELLD_ENV?: CelldWorldEnv }).CELLD_ENV;
    }
  });

  it('resolves intentional defaults explicitly', () => {
    expect(resolveConfig()).toMatchObject({
      deploymentId: 'celld-default',
      queueShards: 1,
      runRetentionMs: 0,
      rpcTimeoutMs: 30_000,
      streamLongPollMs: MAX_STREAM_LONG_POLL_MS,
      streamFlushIntervalMs: 0,
    });
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid queueShards %s',
    (queueShards) => {
      expect(() => resolveConfig({ queueShards })).toThrow(/queueShards/);
    },
  );

  it.each([129, Number.MAX_SAFE_INTEGER])('accepts positive safe queueShards %s', (queueShards) => {
    expect(resolveConfig({ queueShards }).queueShards).toBe(queueShards);
  });

  it('does not coerce numeric strings in typed configuration options', () => {
    expect(() => resolveConfig({ queueShards: '5' as unknown as number })).toThrow(/queueShards/);
    expect(() => resolveConfig({ runRetentionMs: '5' as unknown as number })).toThrow(
      /runRetentionMs/,
    );
  });

  it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid runRetentionMs %s',
    (runRetentionMs) => {
      expect(() => resolveConfig({ runRetentionMs })).toThrow(/runRetentionMs/);
    },
  );

  it.each(['', ' ', '5junk', '1.5', '1e3', 'NaN', 'Infinity', '-1', '9007199254740992'])(
    'rejects malformed CELLD_RUN_RETENTION_MS %j instead of falling back',
    (value) => {
      vi.stubEnv('CELLD_RUN_RETENTION_MS', value);
      expect(() => resolveConfig()).toThrow(/runRetentionMs/);
    },
  );

  it('accepts a complete decimal retention environment value, including zero', () => {
    vi.stubEnv('CELLD_RUN_RETENTION_MS', '0005');
    expect(resolveConfig().runRetentionMs).toBe(5);
    vi.stubEnv('CELLD_RUN_RETENTION_MS', '0');
    expect(resolveConfig().runRetentionMs).toBe(0);
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, MAX_FLEET_RPC_TIMEOUT_MS + 1])(
    'rejects out-of-range rpcTimeoutMs %s',
    (rpcTimeoutMs) => {
      expect(() => resolveConfig({ rpcTimeoutMs })).toThrow(/rpcTimeoutMs/);
    },
  );

  it('enforces stream long-poll bounds and the RPC deadline relationship', () => {
    expect(() => resolveConfig({ streamLongPollMs: 0 })).toThrow(/streamLongPollMs/);
    expect(() => resolveConfig({ streamLongPollMs: MAX_STREAM_LONG_POLL_MS + 1 })).toThrow(
      /streamLongPollMs/,
    );
    expect(() => resolveConfig({ rpcTimeoutMs: 100, streamLongPollMs: 100 })).toThrow(
      /less than rpcTimeoutMs/,
    );
    expect(resolveConfig({ rpcTimeoutMs: 100, streamLongPollMs: 99 }).streamLongPollMs).toBe(99);
  });

  it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid streamFlushIntervalMs %s',
    (streamFlushIntervalMs) => {
      expect(() => resolveConfig({ streamFlushIntervalMs })).toThrow(/streamFlushIntervalMs/);
    },
  );

  it('fails early when a required in-process binding is missing', () => {
    const env = createMockEnv();
    const partial = { ...env, WORKFLOW_DB: undefined } as unknown as CelldWorldEnv;
    expect(() => createCelldWorld({ env: partial })).toThrow(
      /missing required binding WORKFLOW_DB/,
    );
  });

  it('requires a secret whenever fleetUrl is configured', () => {
    expect(() => createCelldWorld({ fleetUrl: 'http://fleet.test' })).toThrow(
      /secret.*required with fleetUrl/,
    );
  });
});
