/** Internal delivery contract. No Request/Response objects cross the RPC boundary. */
import {
  QUEUE_CLAIM_STALE_MS,
  queueClaimName,
  queueOrphanName,
  validateNativeQueueEnvelope,
} from '../queue-protocol.js';
import { queueDelayDeadline } from '../validation.js';
import { timingSafeEqual } from './auth.js';
import type { QueueRunStub, WorkerEnv } from './router.js';

export type QueueDeliveryResult =
  | { kind: 'complete' }
  | { kind: 'suspend'; timeoutSeconds: number }
  | { kind: 'retry' };

export type QueueDeliveryEnv = Pick<
  WorkerEnv,
  'WORKFLOW_DB' | 'WORKFLOW_QUEUE_PAYLOADS' | 'WORLD_SECRET' | 'WORKFLOW_CALLBACK_SECRET'
>;

const MAX_QUEUE_DELIVERY_RESPONSE_BYTES = 64 * 1024;
const QUEUE_ORPHAN_GRACE_MS = 5 * 24 * 60 * 60 * 1000;
const PERMANENT_QUEUE_STATUSES = new Set([404, 409, 410, 422]);
async function readResponseText(response: Response): Promise<string> {
  if (!response.body) return '';
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let total = 0;
  let text = '';
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      total += value.byteLength;
      if (total > MAX_QUEUE_DELIVERY_RESPONSE_BYTES) {
        await reader.cancel('queue delivery response is too large').catch(() => undefined);
        throw new Error('workflow queue handler response exceeds configured limit');
      }
      text += decoder.decode(value, { stream: true });
    }
    return text + decoder.decode();
  } finally {
    reader.releaseLock();
  }
}

/** Validate at the service boundary; callers cannot bypass auth via RPC. */
export async function deliverQueueMessage(
  env: QueueDeliveryEnv,
  secret: unknown,
  input: unknown,
  attempt: unknown,
): Promise<QueueDeliveryResult> {
  if (!env.WORLD_SECRET) throw new Error('WORLD_SECRET is not configured');
  if (typeof secret !== 'string' || !secret || !(await timingSafeEqual(secret, env.WORLD_SECRET))) {
    throw new Error('Unauthorized Queue delivery');
  }
  const envelope = validateNativeQueueEnvelope(input);
  if (!Number.isSafeInteger(attempt) || (attempt as number) < 1) {
    throw new TypeError('queue delivery attempt must be a positive safe integer');
  }
  if (!env.WORKFLOW_DB) throw new Error('missing binding: WORKFLOW_DB');

  const namespace = env.WORKFLOW_DB;
  const payloadStore = env.WORKFLOW_QUEUE_PAYLOADS;
  const runStub = (name: string) => namespace.get(namespace.idFromName(name)) as QueueRunStub;
  let claim: QueueRunStub | undefined;
  let claimed = false;

  async function releaseClaim(): Promise<void> {
    if (claimed) await claim!.releaseInflight(envelope.messageId);
  }
  async function unregisterPayload(): Promise<void> {
    if (envelope.runId) {
      await runStub(envelope.runId).unregisterQueuePayload(envelope.messageId);
      await runStub(queueOrphanName(envelope.messageId)).cancelQueuePayloadOrphan(
        envelope.messageId,
      );
    }
  }

  try {
    if (envelope.idempotencyKey) {
      claim = runStub(queueClaimName(envelope.queueName, envelope.idempotencyKey));
      const result = await claim.claimInflight({
        messageId: envelope.messageId,
        staleMs: QUEUE_CLAIM_STALE_MS,
      });
      if (!result.claimed) {
        const now = Date.now();
        if (result.retryAt !== undefined && result.retryAt > now) {
          return {
            kind: 'suspend',
            timeoutSeconds: Math.max(1, Math.ceil((result.retryAt - now) / 1000)),
          };
        }
        return { kind: 'complete' };
      }
      claimed = true;
    }

    let body = envelope.body;
    if (envelope.payloadKey) {
      if (!payloadStore) throw new Error('missing binding: WORKFLOW_QUEUE_PAYLOADS');
      body = (await payloadStore.read(envelope.payloadKey)) ?? undefined;
      if (body === undefined) {
        await unregisterPayload();
        await claim?.completeQueueMessage(envelope.messageId);
        return { kind: 'complete' };
      }
    }

    const headers: Record<string, string> = {
      'content-type': 'application/json',
      'x-vqs-queue-name': envelope.queueName,
      'x-vqs-message-id': envelope.messageId,
      'x-vqs-message-attempt': String(attempt),
    };
    if (env.WORKFLOW_CALLBACK_SECRET) {
      headers['x-workflow-callback-secret'] = env.WORKFLOW_CALLBACK_SECRET;
    }
    const callback = await fetch(
      `${envelope.targetBaseUrl.replace(/\/$/, '')}/.well-known/workflow/v1/flow`,
      { method: 'POST', headers, body, signal: AbortSignal.timeout(300_000) },
    );
    if (callback.ok || PERMANENT_QUEUE_STATUSES.has(callback.status)) {
      await callback.body?.cancel().catch(() => undefined);
      if (envelope.payloadKey) {
        await payloadStore!.delete(envelope.payloadKey);
        await unregisterPayload();
      }
      // Do not acknowledge until cleanup and completion are durably accepted.
      await claim?.completeQueueMessage(envelope.messageId);
      return { kind: 'complete' };
    }

    if (callback.status === 503) {
      const responseBody = await readResponseText(callback);
      const timeoutSeconds = (JSON.parse(responseBody) as { timeoutSeconds?: unknown })
        .timeoutSeconds;
      const notBefore = queueDelayDeadline(Date.now(), timeoutSeconds, 1, QUEUE_CLAIM_STALE_MS);
      if (notBefore === null) throw new Error('workflow queue suspension deadline is invalid');
      if (envelope.payloadKey && envelope.runId) {
        await runStub(queueOrphanName(envelope.messageId)).scheduleQueuePayloadOrphan({
          messageId: envelope.messageId,
          runId: envelope.runId,
          key: envelope.payloadKey,
          expiresAt: notBefore + QUEUE_ORPHAN_GRACE_MS,
        });
      }
      if (claim) {
        await claim.holdInflight({
          messageId: envelope.messageId,
          retryAt: notBefore,
          expiresAt: notBefore + QUEUE_CLAIM_STALE_MS,
          reservationExpiresAt: notBefore + QUEUE_ORPHAN_GRACE_MS,
        });
      }
      // Validation proves a positive safe integer delay.
      return {
        kind: 'suspend',
        timeoutSeconds: timeoutSeconds as number,
      };
    }

    await callback.body?.cancel().catch(() => undefined);
    await releaseClaim();
    claimed = false;
    return { kind: 'retry' };
  } catch (error) {
    // A failed read, callback, or cleanup must not turn into an acknowledgement.
    await releaseClaim();
    throw error;
  }
}
