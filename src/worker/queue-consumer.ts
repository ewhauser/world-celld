/**
 * celld native Queue consumer.
 *
 * This companion script consumes the broker and calls the primary world
 * worker through a service binding, keeping the Queue attachment independent
 * of the public HTTP worker.
 */

import type { QueueDeliveryResult } from './queue-delivery.js';
import {
  MAX_QUEUE_SUSPENSIONS,
  QUEUE_CLAIM_STALE_MS,
  nativeQueueDelaySeconds,
  validateNativeQueueEnvelope,
  type NativeQueueEnvelope,
} from '../queue-protocol.js';
import { queueDelayDeadline } from '../validation.js';

interface NativeQueueMessage {
  readonly body: unknown;
  readonly attempts: number;
  ack(): void;
  retry(options?: { delaySeconds?: number }): void;
}

interface NativeQueueBatch {
  readonly messages: readonly NativeQueueMessage[];
}

interface QueueProducerBinding {
  send(body: string, options?: { contentType?: 'text'; delaySeconds?: number }): Promise<unknown>;
}

interface ServiceBinding {
  deliver(
    secret: string,
    envelope: NativeQueueEnvelope,
    attempt: number,
  ): Promise<QueueDeliveryResult>;
}

export interface QueueConsumerEnv {
  WORKFLOW_QUEUE: QueueProducerBinding;
  WORLD_SERVICE: ServiceBinding;
  WORLD_SECRET?: string;
}

function backoffSeconds(attempt: number): number {
  return Math.min(60, 2 ** Math.min(attempt, 30));
}

async function republish(
  env: QueueConsumerEnv,
  envelope: NativeQueueEnvelope,
  now = Date.now(),
): Promise<void> {
  await env.WORKFLOW_QUEUE.send(JSON.stringify(envelope), {
    contentType: 'text',
    delaySeconds: nativeQueueDelaySeconds(envelope.notBefore, now),
  });
}

async function consume(message: NativeQueueMessage, env: QueueConsumerEnv): Promise<void> {
  let envelope: NativeQueueEnvelope;
  try {
    if (typeof message.body !== 'string') {
      throw new TypeError('native Queue message body must be text');
    }
    envelope = validateNativeQueueEnvelope(JSON.parse(message.body));
  } catch (error) {
    console.error('world-celld native Queue rejected malformed broker message', error);
    message.retry({ delaySeconds: backoffSeconds(message.attempts) });
    return;
  }

  const now = Date.now();
  if (envelope.notBefore !== undefined && envelope.notBefore > now) {
    try {
      await republish(env, envelope, now);
      message.ack();
    } catch (error) {
      console.error('world-celld native Queue failed to continue a long delay', error);
      message.retry({ delaySeconds: backoffSeconds(message.attempts) });
    }
    return;
  }

  const attempt = (envelope.deliveryFailures ?? 0) + message.attempts;
  if (!env.WORLD_SECRET) {
    console.error('world-celld Queue consumer missing WORLD_SECRET');
    message.retry({ delaySeconds: backoffSeconds(attempt) });
    return;
  }

  let response: Awaited<ReturnType<ServiceBinding['deliver']>>;
  try {
    response = await env.WORLD_SERVICE.deliver(env.WORLD_SECRET, envelope, attempt);
  } catch (error) {
    console.error('world-celld Queue consumer could not reach the world service', error);
    message.retry({ delaySeconds: backoffSeconds(attempt) });
    return;
  }

  if (response?.kind === 'complete') {
    message.ack();
    return;
  }

  if (response?.kind === 'suspend') {
    try {
      const notBefore = queueDelayDeadline(
        Date.now(),
        response.timeoutSeconds,
        1,
        QUEUE_CLAIM_STALE_MS,
      );
      const suspensionCount = (envelope.suspensionCount ?? 0) + 1;
      if (notBefore !== null && suspensionCount <= MAX_QUEUE_SUSPENSIONS) {
        await republish(env, {
          ...envelope,
          notBefore,
          deliveryFailures: attempt - 1,
          suspensionCount,
        });
        message.ack();
        return;
      }
    } catch (error) {
      console.error('world-celld Queue consumer received an invalid suspension response', error);
    }
  }

  message.retry({ delaySeconds: backoffSeconds(attempt) });
}

export default {
  async queue(batch: NativeQueueBatch, env: QueueConsumerEnv): Promise<void> {
    await Promise.all(batch.messages.map((message) => consume(message, env)));
  },
};
