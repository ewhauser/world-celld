import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MAX_QUEUE_SUSPENSIONS, type NativeQueueEnvelope } from '../src/queue-protocol.js';
import type { QueueDeliveryResult } from '../src/worker/queue-delivery.js';
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
function consumerEnv(result: QueueDeliveryResult = { kind: 'complete' }) {
  return {
    WORKFLOW_QUEUE: {
      send: vi.fn<QueueConsumerEnv['WORKFLOW_QUEUE']['send']>().mockResolvedValue(undefined),
    },
    WORLD_SERVICE: {
      deliver: vi.fn<QueueConsumerEnv['WORLD_SERVICE']['deliver']>().mockResolvedValue(result),
    },
    WORLD_SECRET: 'queue-secret' as string | undefined,
  };
}

describe('celld native Queue RPC consumer', () => {
  beforeEach(() => {
    vi.useFakeTimers({ now: 1_000_000 });
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('acknowledges completion after an authenticated typed RPC call', async () => {
    const message = nativeMessage();
    const env = consumerEnv();
    await queueConsumer.queue({ messages: [message] }, env);
    expect(env.WORLD_SERVICE.deliver).toHaveBeenCalledWith('queue-secret', envelope, 1);
    expect(message.ack).toHaveBeenCalledOnce();
    expect(message.retry).not.toHaveBeenCalled();
  });

  it('retries transient outcomes with bounded exponential delay', async () => {
    const message = nativeMessage(undefined, 3);
    const env = consumerEnv({ kind: 'retry' });
    await queueConsumer.queue({ messages: [message] }, env);
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalledWith({ delaySeconds: 8 });
  });

  it('does not acknowledge when the RPC rejects', async () => {
    const message = nativeMessage(undefined, 2);
    const env = consumerEnv();
    env.WORLD_SERVICE.deliver.mockRejectedValue(new Error('RPC unavailable'));
    await queueConsumer.queue({ messages: [message] }, env);
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalledWith({ delaySeconds: 4 });
  });

  it('fails closed before calling RPC when the consumer secret is absent', async () => {
    const message = nativeMessage();
    const env = consumerEnv();
    env.WORLD_SECRET = undefined;
    await queueConsumer.queue({ messages: [message] }, env);
    expect(env.WORLD_SERVICE.deliver).not.toHaveBeenCalled();
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalledWith({ delaySeconds: 2 });
  });

  it('preserves the message and failure count when republishing a suspension', async () => {
    const message = nativeMessage(undefined, 2);
    const env = consumerEnv({ kind: 'suspend', timeoutSeconds: 30 });
    await queueConsumer.queue({ messages: [message] }, env);
    expect(message.ack).toHaveBeenCalledOnce();
    expect(message.retry).not.toHaveBeenCalled();
    const [body, options] = env.WORKFLOW_QUEUE.send.mock.calls[0];
    expect(JSON.parse(body)).toMatchObject({
      ...envelope,
      deliveryFailures: 1,
      suspensionCount: 1,
      notBefore: 1_030_000,
    });
    expect(options).toEqual({ contentType: 'text', delaySeconds: 30 });
  });

  it('does not acknowledge a suspension until its replacement is accepted', async () => {
    const message = nativeMessage();
    const env = consumerEnv({ kind: 'suspend', timeoutSeconds: 30 });
    const publication = Promise.withResolvers<unknown>();
    env.WORKFLOW_QUEUE.send.mockReturnValue(publication.promise);
    const consuming = queueConsumer.queue({ messages: [message] }, env);
    await vi.waitFor(() => expect(env.WORKFLOW_QUEUE.send).toHaveBeenCalled());
    expect(message.ack).not.toHaveBeenCalled();
    publication.reject(new Error('broker unavailable'));
    await consuming;
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalledWith({ delaySeconds: 2 });
  });

  it.each([-1, 0, NaN, 1.5])('retries an invalid suspension delay %s', async (timeoutSeconds) => {
    const message = nativeMessage();
    const env = consumerEnv({ kind: 'suspend', timeoutSeconds });
    await queueConsumer.queue({ messages: [message] }, env);
    expect(env.WORKFLOW_QUEUE.send).not.toHaveBeenCalled();
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalled();
  });

  it('does not republish after the suspension limit', async () => {
    const message = nativeMessage(
      JSON.stringify({ ...envelope, suspensionCount: MAX_QUEUE_SUSPENSIONS }),
    );
    const env = consumerEnv({ kind: 'suspend', timeoutSeconds: 30 });
    await queueConsumer.queue({ messages: [message] }, env);
    expect(env.WORKFLOW_QUEUE.send).not.toHaveBeenCalled();
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalled();
  });

  it('chains long delays without invoking delivery', async () => {
    const message = nativeMessage(
      JSON.stringify({ ...envelope, notBefore: 1_000_000 + 3 * 86_400_000 }),
    );
    const env = consumerEnv();
    await queueConsumer.queue({ messages: [message] }, env);
    expect(env.WORLD_SERVICE.deliver).not.toHaveBeenCalled();
    expect(env.WORKFLOW_QUEUE.send).toHaveBeenCalledWith(expect.any(String), {
      contentType: 'text',
      delaySeconds: 86_400,
    });
    expect(message.ack).toHaveBeenCalledOnce();
  });

  it('retries malformed broker messages', async () => {
    const message = nativeMessage('{');
    const env = consumerEnv();
    await queueConsumer.queue({ messages: [message] }, env);
    expect(message.ack).not.toHaveBeenCalled();
    expect(message.retry).toHaveBeenCalledWith({ delaySeconds: 2 });
    expect(env.WORLD_SERVICE.deliver).not.toHaveBeenCalled();
  });
});
