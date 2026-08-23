import { afterEach, describe, expect, it, vi } from 'vitest';
import { rpcStringify } from '../src/codec.js';
import { FLEET_IDEMPOTENT_ATTEMPTS, MAX_FLEET_RPC_TIMEOUT_MS } from '../src/lifecycle.js';
import { FleetTransportError } from '../src/remote/errors.js';
import { createRemoteEnv } from '../src/remote/namespaces.js';
import { callDO } from '../src/remote/rpc-client.js';

function rpcResponse(value: unknown, init?: ResponseInit): Response {
  return new Response(rpcStringify(value), { status: 200, ...init });
}

async function abortingFetch(_input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  return await new Promise<Response>((_resolve, reject) => {
    init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true });
  });
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('remote RPC client regressions', () => {
  it('retries an idempotent read after fetch rejects and succeeds later', async () => {
    vi.useFakeTimers();
    const fetchImpl = vi
      .fn<typeof fetch>(async () => rpcResponse({ ok: true }))
      .mockRejectedValueOnce(new TypeError('connection reset'));

    const result = callDO<{ ok: boolean }>(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'runs',
      'wrun_retry',
      'getRun',
      [],
      { idempotent: true },
    );
    await vi.runAllTimersAsync();

    await expect(result).resolves.toEqual({ ok: true });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it.each([502, 503, 504])(
    'retries an idempotent read after HTTP %s and succeeds later',
    async (status) => {
      vi.useFakeTimers();
      const fetchImpl = vi
        .fn<typeof fetch>(async () => rpcResponse('recovered'))
        .mockResolvedValueOnce(
          new Response(JSON.stringify({ error: { message: `temporary ${status}` } }), { status }),
        );

      const result = callDO<string>(
        { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
        'runs',
        'wrun_status_retry',
        'getRun',
        [],
        { idempotent: true },
      );
      await vi.runAllTimersAsync();

      await expect(result).resolves.toBe('recovered');
      expect(fetchImpl).toHaveBeenCalledTimes(2);
    },
  );

  it('surfaces the final transport rejection after retry exhaustion', async () => {
    vi.useFakeTimers();
    const failures = [new Error('first'), new Error('second'), new Error('final')];
    let attempt = 0;
    const fetchImpl = vi.fn<typeof fetch>(async () => {
      throw failures[attempt++];
    });

    const outcome = callDO(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'runs',
      'wrun_exhausted',
      'getRun',
      [],
      { idempotent: true },
    ).catch((error) => error);
    await vi.runAllTimersAsync();

    await expect(outcome).resolves.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('fleet unreachable'),
      cause: failures[2],
    });
    expect(fetchImpl).toHaveBeenCalledTimes(FLEET_IDEMPOTENT_ATTEMPTS);
  });

  it('surfaces the final retryable HTTP failure after exhaustion', async () => {
    vi.useFakeTimers();
    const statuses = [502, 503, 504];
    let attempt = 0;
    const fetchImpl = vi.fn<typeof fetch>(async () => {
      const status = statuses[attempt++];
      return new Response(
        JSON.stringify({
          error: { name: `Fleet${status}Error`, message: `failure ${status}`, status },
        }),
        { status },
      );
    });

    const outcome = callDO(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'runs',
      'wrun_status_exhausted',
      'getRun',
      [],
      { idempotent: true },
    ).catch((error) => error);
    await vi.runAllTimersAsync();

    await expect(outcome).resolves.toMatchObject({
      name: 'Fleet504Error',
      message: 'failure 504',
      status: 504,
    });
    expect(fetchImpl).toHaveBeenCalledTimes(FLEET_IDEMPOTENT_ATTEMPTS);
  });

  it('reconstructs structured and non-JSON RPC failures', async () => {
    const structuredFetch = vi.fn<typeof fetch>(
      async () =>
        new Response(
          JSON.stringify({
            error: { name: 'EntityConflictError', message: 'already committed', status: 409 },
          }),
          { status: 500 },
        ),
    );
    const plainFetch = vi.fn<typeof fetch>(
      async () => new Response('upstream exploded', { status: 418 }),
    );

    const structured = await callDO(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl: structuredFetch },
      'runs',
      'wrun_structured',
      'applyEvent',
      [],
    ).catch((error) => error);
    const plain = await callDO(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl: plainFetch },
      'runs',
      'wrun_plain',
      'applyEvent',
      [],
    ).catch((error) => error);

    expect(structured).toMatchObject({
      name: 'EntityConflictError',
      message: 'already committed',
      status: 409,
    });
    expect(plain).toMatchObject({ message: 'upstream exploded', status: 418 });
  });

  it('retries malformed successful responses only for idempotent reads', async () => {
    vi.useFakeTimers();
    const fetchImpl = vi
      .fn<typeof fetch>(async () => rpcResponse({ ok: true }))
      .mockResolvedValueOnce(new Response('not tagged json', { status: 200 }));

    const result = callDO<{ ok: boolean }>(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'runs',
      'wrun_malformed',
      'getRun',
      [],
      { idempotent: true },
    );
    await vi.runAllTimersAsync();

    await expect(result).resolves.toEqual({ ok: true });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it('surfaces malformed successful responses after read retry exhaustion', async () => {
    vi.useFakeTimers();
    const fetchImpl = vi.fn<typeof fetch>(
      async () => new Response('not tagged json', { status: 200 }),
    );

    const outcome = callDO(
      { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
      'runs',
      'wrun_malformed_exhausted',
      'getRun',
      [],
      { idempotent: true },
    ).catch((error) => error);
    await vi.runAllTimersAsync();

    await expect(outcome).resolves.toMatchObject({
      name: 'FleetTransportError',
      message: expect.stringContaining('malformed successful response'),
    });
    expect(fetchImpl).toHaveBeenCalledTimes(FLEET_IDEMPOTENT_ATTEMPTS);
  });

  it('makes one namespace attempt for commit-ambiguous RunDO mutations', async () => {
    const committedMethods: string[] = [];
    const fetchImpl = vi.fn<typeof fetch>(async (input) => {
      const requestUrl =
        typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
      committedMethods.push(new URL(requestUrl).pathname.split('/').at(-1) ?? '');
      throw new TypeError('response lost after commit');
    });
    const env = createRemoteEnv({ fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl });
    const id = env.WORKFLOW_DB.idFromName('wrun_ambiguous');
    const run = env.WORKFLOW_DB.get(id);

    await expect(run.applyEvent({} as never)).rejects.toBeInstanceOf(FleetTransportError);
    await expect(
      run.resolveHookTokenClaim({ hookId: 'hook', token: 'token', claimId: 'claim' }),
    ).rejects.toBeInstanceOf(FleetTransportError);

    expect(committedMethods).toEqual(['applyEvent', 'resolveHookTokenClaim']);
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it('classifies namespace reads as retryable', async () => {
    vi.useFakeTimers();
    const fetchImpl = vi
      .fn<typeof fetch>(async () => rpcResponse({ ok: true, value: null }))
      .mockRejectedValueOnce(new TypeError('connection reset'));
    const env = createRemoteEnv({ fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl });
    const result = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName('wrun_read')).getRun();
    await vi.runAllTimersAsync();

    await expect(result).resolves.toEqual({ ok: true, value: null });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });

  it.each([0, -1, 1.5, Number.NaN, MAX_FLEET_RPC_TIMEOUT_MS + 1])(
    'rejects an invalid fleet timeout (%s) before fetch',
    async (timeoutMs) => {
      const fetchImpl = vi.fn<typeof fetch>(async () => rpcResponse(null));

      await expect(
        callDO(
          { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl, timeoutMs },
          'runs',
          'wrun_timeout_invalid',
          'getRun',
          [],
        ),
      ).rejects.toThrow(
        `world-celld: fleet RPC timeout must be between 1 and ${MAX_FLEET_RPC_TIMEOUT_MS}`,
      );
      expect(fetchImpl).not.toHaveBeenCalled();
    },
  );

  it.each([1, MAX_FLEET_RPC_TIMEOUT_MS])(
    'accepts the fleet timeout boundary %s',
    async (timeoutMs) => {
      const fetchImpl = vi.fn<typeof fetch>(async () => rpcResponse('ok'));

      await expect(
        callDO<string>(
          { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl, timeoutMs },
          'runs',
          'wrun_timeout_valid',
          'getRun',
          [],
        ),
      ).resolves.toBe('ok');
      expect(fetchImpl).toHaveBeenCalledOnce();
    },
  );

  it('aborts a fleet request at its configured deadline', async () => {
    const transport = {
      fleetUrl: 'http://fleet.test',
      secret: 'secret',
      fetchImpl: abortingFetch,
      timeoutMs: 10,
    };

    const outcome = await Promise.race([
      callDO(transport, 'runs', 'wrun_timeout', 'getRun', []).catch((error) => error),
      new Promise<'still-pending'>((resolve) => setTimeout(() => resolve('still-pending'), 100)),
    ]);

    expect(outcome).toBeInstanceOf(FleetTransportError);
  });
});
