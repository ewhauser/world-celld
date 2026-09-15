import type { QueueRunStub } from '../src/worker/router.js';
import type { QueuePayloadStore } from '../src/worker/queue-payload-store.js';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { deliverQueueMessage, type QueueDeliveryEnv } from '../src/worker/queue-delivery.js';
import type { NativeQueueEnvelope } from '../src/queue-protocol.js';

const envelope: NativeQueueEnvelope = {
  version: 1,
  messageId: 'msg_delivery',
  queueName: '__wkf_workflow_delivery',
  targetBaseUrl: 'https://app.internal/',
  runId: 'wrun_delivery',
  idempotencyKey: 'delivery-key',
  payloadKey: 'workflow-queue/wrun_delivery/msg_delivery',
};

function setup() {
  const claim = {
    claimInflight: vi.fn<QueueRunStub['claimInflight']>().mockResolvedValue({ claimed: true }),
    releaseInflight: vi.fn<QueueRunStub['releaseInflight']>().mockResolvedValue(undefined),
    completeQueueMessage: vi
      .fn<QueueRunStub['completeQueueMessage']>()
      .mockResolvedValue(undefined),
    holdInflight: vi.fn<QueueRunStub['holdInflight']>().mockResolvedValue({ held: true }),
  };
  const run = {
    unregisterQueuePayload: vi
      .fn<QueueRunStub['unregisterQueuePayload']>()
      .mockResolvedValue(undefined),
  };
  const orphan = {
    cancelQueuePayloadOrphan: vi
      .fn<QueueRunStub['cancelQueuePayloadOrphan']>()
      .mockResolvedValue(undefined),
    scheduleQueuePayloadOrphan: vi
      .fn<QueueRunStub['scheduleQueuePayloadOrphan']>()
      .mockResolvedValue(undefined),
  };
  const store = {
    read: vi.fn<QueuePayloadStore['read']>().mockResolvedValue('{"payload":true}'),
    write: vi.fn<QueuePayloadStore['write']>().mockResolvedValue(undefined),
    delete: vi.fn<QueuePayloadStore['delete']>().mockResolvedValue(undefined),
  };
  const get = vi.fn<QueueDeliveryEnv['WORKFLOW_DB']['get']>((id: { toString(): string }) => {
    const name = id.toString();
    return name.startsWith('claim:') ? claim : name.startsWith('queue-orphan:') ? orphan : run;
  });
  const env: QueueDeliveryEnv = {
    WORLD_SECRET: 'secret',
    WORKFLOW_CALLBACK_SECRET: 'callback-secret',
    WORKFLOW_DB: { idFromName: (name) => ({ toString: () => name }), get },
    WORKFLOW_QUEUE_PAYLOADS: store,
  };
  const callback = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }));
  vi.stubGlobal('fetch', callback);
  return { env, claim, run, orphan, store, get, callback };
}

describe('internal Queue delivery', () => {
  beforeEach(() => vi.useFakeTimers({ now: 1_000_000 }));
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it.each(['', 'wrong', undefined, 42])(
    'rejects invalid caller secret %s before touching storage',
    async (secret) => {
      const { env, get, callback } = setup();
      await expect(deliverQueueMessage(env, secret, envelope, 1)).rejects.toThrow(/Unauthorized/);
      expect(get).not.toHaveBeenCalled();
      expect(callback).not.toHaveBeenCalled();
    },
  );

  it('fails closed if the server secret is missing', async () => {
    const { env, get } = setup();
    delete env.WORLD_SECRET;
    await expect(deliverQueueMessage(env, 'secret', envelope, 1)).rejects.toThrow(/configured/);
    expect(get).not.toHaveBeenCalled();
  });

  it.each([0, -1, 1.5, NaN, Number.MAX_SAFE_INTEGER + 1, '1'])(
    'rejects invalid attempt %s',
    async (attempt) => {
      const { env, get } = setup();
      await expect(deliverQueueMessage(env, 'secret', envelope, attempt)).rejects.toThrow(
        /attempt/,
      );
      expect(get).not.toHaveBeenCalled();
    },
  );

  it('rejects an invalid envelope before touching storage', async () => {
    const { env, get } = setup();
    await expect(deliverQueueMessage(env, 'secret', {}, 1)).rejects.toThrow(/version/);
    expect(get).not.toHaveBeenCalled();
  });

  it.each([200, 204, 404, 409, 410, 422])(
    'completes and cleans up callback status %s',
    async (status) => {
      const { env, claim, run, orphan, store, callback } = setup();
      callback.mockResolvedValue(
        new Response(status === 204 ? null : 'x'.repeat(100_000), { status }),
      );
      expect(await deliverQueueMessage(env, 'secret', envelope, 2)).toEqual({ kind: 'complete' });
      expect(store.delete).toHaveBeenCalledWith(envelope.payloadKey);
      expect(run.unregisterQueuePayload).toHaveBeenCalledWith(envelope.messageId);
      expect(orphan.cancelQueuePayloadOrphan).toHaveBeenCalledWith(envelope.messageId);
      expect(claim.completeQueueMessage).toHaveBeenCalledWith(envelope.messageId);
      expect(claim.releaseInflight).not.toHaveBeenCalled();
      const [url, options] = callback.mock.calls[0];
      expect(url).toBe('https://app.internal/.well-known/workflow/v1/flow');
      expect(options?.body).toBe('{"payload":true}');
      expect(new Headers(options?.headers).get('x-vqs-message-attempt')).toBe('2');
      expect(new Headers(options?.headers).get('x-workflow-callback-secret')).toBe(
        'callback-secret',
      );
    },
  );

  it('does not invoke the callback for a completed claim', async () => {
    const { env, claim, store, callback } = setup();
    claim.claimInflight.mockResolvedValue({ claimed: false });
    expect(await deliverQueueMessage(env, 'secret', envelope, 1)).toEqual({ kind: 'complete' });
    expect(store.read).not.toHaveBeenCalled();
    expect(callback).not.toHaveBeenCalled();
  });

  it('suspends an active claim until its deadline', async () => {
    const { env, claim, callback } = setup();
    claim.claimInflight.mockResolvedValue({ claimed: false, retryAt: 1_030_000 });
    expect(await deliverQueueMessage(env, 'secret', envelope, 1)).toEqual({
      kind: 'suspend',
      timeoutSeconds: 30,
    });
    expect(callback).not.toHaveBeenCalled();
  });

  it('completes a missing retained payload without a callback', async () => {
    const { env, store, claim, callback } = setup();
    store.read.mockResolvedValue(null);
    expect(await deliverQueueMessage(env, 'secret', envelope, 1)).toEqual({ kind: 'complete' });
    expect(claim.completeQueueMessage).toHaveBeenCalled();
    expect(callback).not.toHaveBeenCalled();
  });

  it('retries a transient callback failure without deleting its payload', async () => {
    const { env, store, claim, callback } = setup();
    callback.mockResolvedValue(new Response('retry', { status: 500 }));
    expect(await deliverQueueMessage(env, 'secret', envelope, 1)).toEqual({ kind: 'retry' });
    expect(claim.releaseInflight).toHaveBeenCalledWith(envelope.messageId);
    expect(store.delete).not.toHaveBeenCalled();
    expect(claim.completeQueueMessage).not.toHaveBeenCalled();
  });

  it('holds the claim and extends payload protection before suspending', async () => {
    const { env, store, claim, orphan, callback } = setup();
    callback.mockResolvedValue(Response.json({ timeoutSeconds: 30 }, { status: 503 }));
    expect(await deliverQueueMessage(env, 'secret', envelope, 1)).toEqual({
      kind: 'suspend',
      timeoutSeconds: 30,
    });
    expect(claim.holdInflight).toHaveBeenCalledWith(
      expect.objectContaining({ messageId: envelope.messageId, retryAt: 1_030_000 }),
    );
    expect(orphan.scheduleQueuePayloadOrphan).toHaveBeenCalledWith(
      expect.objectContaining({ key: envelope.payloadKey }),
    );
    expect(store.delete).not.toHaveBeenCalled();
    expect(claim.releaseInflight).not.toHaveBeenCalled();
  });

  it.each(['invalid JSON', '{"timeoutSeconds":-1}', 'x'.repeat(65_537)])(
    'rejects malformed or oversized suspension response %#',
    async (body) => {
      const { env, store, claim, callback } = setup();
      callback.mockResolvedValue(new Response(body, { status: 503 }));
      await expect(deliverQueueMessage(env, 'secret', envelope, 1)).rejects.toThrow(
        /JSON|Unexpected|suspension|configured limit/,
      );
      expect(claim.releaseInflight).toHaveBeenCalledWith(envelope.messageId);
      expect(store.delete).not.toHaveBeenCalled();
      expect(claim.completeQueueMessage).not.toHaveBeenCalled();
    },
  );

  it.each(['read', 'delete'])(
    'releases the claim and rejects on payload %s failure',
    async (operation) => {
      const { env, store, claim } = setup();
      store[operation as 'read' | 'delete'].mockRejectedValue(new Error('storage unavailable'));
      await expect(deliverQueueMessage(env, 'secret', envelope, 1)).rejects.toThrow(
        'storage unavailable',
      );
      expect(claim.releaseInflight).toHaveBeenCalledWith(envelope.messageId);
      expect(claim.completeQueueMessage).not.toHaveBeenCalled();
    },
  );

  it('does not complete if final claim persistence fails', async () => {
    const { env, claim } = setup();
    claim.completeQueueMessage.mockRejectedValue(new Error('claim unavailable'));
    await expect(deliverQueueMessage(env, 'secret', envelope, 1)).rejects.toThrow(
      'claim unavailable',
    );
    expect(claim.releaseInflight).toHaveBeenCalledWith(envelope.messageId);
  });

  it('releases the claim if the callback connection fails', async () => {
    const { env, claim, callback } = setup();
    callback.mockRejectedValue(new Error('connection reset'));
    await expect(deliverQueueMessage(env, 'secret', envelope, 1)).rejects.toThrow(
      'connection reset',
    );
    expect(claim.releaseInflight).toHaveBeenCalledWith(envelope.messageId);
  });
});
