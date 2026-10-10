/** Internal delivery contract. No Request/Response objects cross the RPC boundary. */
import {
  QUEUE_CLAIM_STALE_MS,
  queueOrphanName,
  validateNativeQueueEnvelope,
} from '../queue-protocol.js';
import { queueDelayDeadline } from '../validation.js';
import { timingSafeEqual } from './auth.js';
import { deliveryQueueClaim } from './queue-claims.js';
import type { QueueRunStub, WorkerEnv } from './router.js';
import { configuredDeploymentUrl, parseDeploymentUrls } from '../deployment-routing.js';
import { parse } from '../vendor/shared/index.js';

export type QueueDeliveryResult =
  | { kind: 'complete' }
  | { kind: 'suspend'; timeoutSeconds: number }
  | { kind: 'retry' };

export type QueueDeliveryEnv = Pick<
  WorkerEnv,
  | 'WORKFLOW_DB'
  | 'WORKFLOW_QUEUE_PAYLOADS'
  | 'WORLD_SECRET'
  | 'WORKFLOW_CALLBACK_SECRET'
  | 'WORKFLOW_DEPLOYMENT_URLS'
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
  const claim = deliveryQueueClaim(envelope, runStub);
  let claimed = false;

  async function releaseClaim(): Promise<void> {
    if (claimed) await claim!.release(envelope.messageId);
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
    // A run-bearing body carried inline has no payload object for retention
    // to delete, so the run cell answers whether the run has expired. The
    // read overlaps the claim and adds no round trip; a run-scoped claim
    // answers it in the same transaction.
    let admission: ReturnType<QueueRunStub['getQueueAdmission']> | undefined;
    if (envelope.runId && !envelope.payloadKey && envelope.claimScope !== 'run') {
      admission = runStub(envelope.runId).getQueueAdmission();
      admission.catch(() => undefined);
    }
    if (claim) {
      const result = await claim.claim(envelope.messageId, QUEUE_CLAIM_STALE_MS);
      // A run-scoped claim of an expired run: same outcome as a deleted body.
      if (result.expired) return { kind: 'complete' };
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

    if (admission && !(await admission).ok) {
      // Same outcome as an offloaded body that retention already deleted.
      await claim?.complete(envelope.messageId);
      return { kind: 'complete' };
    }

    let body = envelope.body;
    if (envelope.payloadKey) {
      if (!payloadStore) throw new Error('missing binding: WORKFLOW_QUEUE_PAYLOADS');
      body = (await payloadStore.read(envelope.payloadKey)) ?? undefined;
      if (body === undefined) {
        await unregisterPayload();
        await claim?.complete(envelope.messageId);
        return { kind: 'complete' };
      }
    }

    const deploymentUrls = parseDeploymentUrls(env.WORKFLOW_DEPLOYMENT_URLS);
    let targetBaseUrl = envelope.targetBaseUrl;
    if (deploymentUrls && envelope.runId) {
      // The stored run is authoritative for existing deliveries. A resilient
      // initial message can precede run_created, so it carries its own pinned ID.
      const stored = await runStub(envelope.runId).getRun();
      if (!stored.ok) throw new Error('world-celld: cannot route an expired run');
      const parsedBody = parse<{ runInput?: { deploymentId?: unknown } }>(body!);
      const initialId = parsedBody.runInput?.deploymentId;
      const pinnedId = stored.value?.deploymentId ?? initialId;
      if (typeof pinnedId !== 'string' || pinnedId.length === 0) {
        throw new Error(`world-celld: no pinned deployment for run ${envelope.runId}`);
      }
      if (stored.value && initialId !== undefined && initialId !== pinnedId) {
        throw new Error(`world-celld: queue run deployment hints disagree for ${envelope.runId}`);
      }
      targetBaseUrl = configuredDeploymentUrl(deploymentUrls, pinnedId);
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
      `${targetBaseUrl.replace(/\/$/, '')}/.well-known/workflow/v1/flow`,
      { method: 'POST', headers, body, signal: AbortSignal.timeout(300_000) },
    );
    if (
      callback.ok ||
      (!deploymentUrls && PERMANENT_QUEUE_STATUSES.has(callback.status)) ||
      (!envelope.runId && PERMANENT_QUEUE_STATUSES.has(callback.status))
    ) {
      await callback.body?.cancel().catch(() => undefined);
      if (envelope.payloadKey) {
        await payloadStore!.delete(envelope.payloadKey);
        await unregisterPayload();
      }
      // Do not acknowledge until cleanup and completion are durably accepted.
      await claim?.complete(envelope.messageId);
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
        await claim.hold({
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
