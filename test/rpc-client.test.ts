import { describe, expect, it } from 'vitest';
import { FleetTransportError } from '../src/remote/errors.js';
import { callDO } from '../src/remote/rpc-client.js';

describe('remote RPC client regressions', () => {
  it('aborts a fleet request at its configured deadline', async () => {
    const fetchImpl = async (_input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
      return await new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true });
      });
    };
    const transport = {
      fleetUrl: 'http://fleet.test',
      secret: 'secret',
      fetchImpl,
      timeoutMs: 10,
    };

    const outcome = await Promise.race([
      callDO(transport, 'runs', 'wrun_timeout', 'getRun', []).catch((error) => error),
      new Promise<'still-pending'>((resolve) =>
        setTimeout(() => resolve('still-pending'), 100),
      ),
    ]);

    expect(outcome).toBeInstanceOf(FleetTransportError);
  });

  it('classifies malformed successful responses as transport failures', async () => {
    const fetchImpl = async (): Promise<Response> => new Response('not tagged json', { status: 200 });

    await expect(
      callDO(
        { fleetUrl: 'http://fleet.test', secret: 'secret', fetchImpl },
        'runs',
        'wrun_malformed',
        'getRun',
        [],
      ),
    ).rejects.toBeInstanceOf(FleetTransportError);
  });
});
