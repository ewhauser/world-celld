import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { NativeQueueEnvelope } from '../src/queue-protocol.js';
import queueConsumer, { type QueueConsumerEnv } from '../src/worker/queue-consumer.js';

const envelope: NativeQueueEnvelope = {
  version: 1,
  messageId: 'msg_consumer',
  queueName: '__wkf_workflow_consumer',
  targetBaseUrl: 'https://app.internal',
  runId: 'wrun_consumer',
  idempotencyKey: 'consumer-key',
  payloadKey: 'workflow-queue/wrun_consumer/msg_consumer',
};

function nativeMessage(body = JSON.stringify(envelope), attempts = 1) {
  return {
    body,
    attempts,
    ack: vi.fn<() => void>(),
    retry: vi.fn<(options?: { delaySeconds?: number }) => void>(),
  };
}

function consumerEnv(response: Response): QueueConsumerEnv & {
  WORKFLOW_QUEUE: { send: ReturnType<typeof vi.fn> };
  WORLD_SERVICE: { fetch: ReturnType<typeof vi.fn> };
} {
  return {
    WORKFLOW_QUEUE: {
      send: vi.fn<QueueConsumerEnv['WORKFLOW_QUEUE']['send']>().mockResolvedValue(undefined),
    },
    WORLD_SERVICE: {
      fetch: vi.fn<QueueConsumerEnv['WORLD_SERVICE']['fetch']>().mockResolvedValue(response),
    },
    WORLD_SECRET: 'queue-secret',
  };
}

describe('celld native Queue consumer', () => {
  beforeEach(() => {
    vi.useFakeTimers({ now: 1_000_000 });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('acks a successful service delivery with the authenticated RPC envelope', async () => {
    const message = nativeMessage();
    const env = consumerEnv(new Response(null, { status: 204 }));

    await queueConsumer.queue({ messages: [message] }, env);

    expect(message.ack).toHaveBeenCalledOnce();
    expect(message.retry).not.toHaveBeenCalled();
    const request = env.WORLD_SERVICE.fetch.mock.calls[0][0] as Request;
    expect(request.url).toBe('https://workflow-world.internal/v1/queue/deliver');
    expect(request.headers.get('authorization')).toBe('Bearer queue-secret');
  });

  it.each([404, 409, 410, 422])('acks permanent service status %s', async (status) => {
    const message = nativeMessage();
    const env = consumerEnv(new Response(null, { status }));
    await queueConsumer.queue({ messages: [message] }, env);
    expect(message.ack).toHaveBeenCalledOnce();
    expect(message.retry).not.toHaveBeenCalled();
  });

  it('retries transient failures with bounded exponential delay', async () => {
    const message = nativeMessage(undefined, 3);
    const env = consumerEnv(new Response('retry', { status: 500 }));
    await queueConsumer.queue({ messages: [message] }, env);
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalledWith({ delaySeconds: 8 });
  });

  it('republishes a stable envelope for workflow suspension', async () => {
    const message = nativeMessage(undefined, 2);
    const env = consumerEnv(
      Response.json({ timeoutSeconds: 30 }, { status: 503, headers: { 'retry-after': '30' } }),
    );

    await queueConsumer.queue({ messages: [message] }, env);

    expect(message.ack).toHaveBeenCalledOnce();
    expect(message.retry).not.toHaveBeenCalled();
    const [body, options] = env.WORKFLOW_QUEUE.send.mock.calls[0] as [string, unknown];
    expect(JSON.parse(body)).toMatchObject({
      messageId: envelope.messageId,
      idempotencyKey: envelope.idempotencyKey,
      deliveryFailures: 1,
      suspensionCount: 1,
      notBefore: 1_030_000,
    });
    expect(options).toEqual({ contentType: 'text', delaySeconds: 30 });
  });

  it('chains a delay beyond celld native Queue delay capacity', async () => {
    const message = nativeMessage(
      JSON.stringify({ ...envelope, notBefore: 1_000_000 + 3 * 86_400_000 }),
    );
    const env = consumerEnv(new Response(null, { status: 204 }));

    await queueConsumer.queue({ messages: [message] }, env);

    expect(env.WORLD_SERVICE.fetch).not.toHaveBeenCalled();
    expect(env.WORKFLOW_QUEUE.send).toHaveBeenCalledWith(expect.any(String), {
      contentType: 'text',
      delaySeconds: 86_400,
    });
    expect(message.ack).toHaveBeenCalledOnce();
  });

  it('retries malformed broker messages instead of acknowledging them', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const message = nativeMessage('{');
    const env = consumerEnv(new Response(null, { status: 204 }));
    await queueConsumer.queue({ messages: [message] }, env);
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalledWith({ delaySeconds: 2 });
    expect(env.WORLD_SERVICE.fetch).not.toHaveBeenCalled();
  });
});
