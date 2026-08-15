import { describe, expect, it } from 'vitest';
import { FleetTransportError } from '../src/remote/errors.js';
import { callDO } from '../src/remote/rpc-client.js';

async function abortingFetch(_input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
  return await new Promise<Response>((_resolve, reject) => {
    init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true });
  });
}

async function malformedFetch(): Promise<Response> {
  return new Response('not tagged json', { status: 200 });
}

describe('remote RPC client regressions', () => {
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

  it('classifies malformed successful responses as transport failures', async () => {
    await expect(
      callDO(
        { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl: malformedFetch },
        'runs',
        'wrun_malformed',
        'getRun',
        [],
      ),
    ).rejects.toBeInstanceOf(FleetTransportError);
  });
});
