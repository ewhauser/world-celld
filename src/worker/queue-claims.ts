/**
 * Idempotency claims for Queue messages. A claim lives either in its own
 * claim cell (named by queue and idempotency key) or, for run-bearing
 * messages whose envelope says so, as an entry in the run's own cell.
 */
import { queueClaimName, type NativeQueueEnvelope } from '../queue-protocol.js';
import type { QueueRunStub } from './router.js';

export interface QueueClaimHandle {
  /** Take the in-flight claim; `expired` only for run-scoped claims of an expired run. */
  claim(
    messageId: string,
    staleMs: number,
  ): Promise<{ expired?: boolean; claimed: boolean; retryAt?: number }>;
  hold(params: {
    messageId: string;
    retryAt: number;
    expiresAt: number;
    reservationExpiresAt?: number;
  }): Promise<{ held: boolean }>;
  release(messageId: string): Promise<void>;
  abandonReservation(messageId: string): Promise<void>;
  confirmPublication(messageId: string, expiresAt: number): Promise<void>;
  complete(messageId: string): Promise<void>;
}

export function cellQueueClaim(cell: QueueRunStub): QueueClaimHandle {
  return {
    claim: (messageId, staleMs) => cell.claimInflight({ messageId, staleMs }),
    hold: (params) => cell.holdInflight(params),
    release: (messageId) => cell.releaseInflight(messageId),
    abandonReservation: (messageId) => cell.abandonQueueMessageReservation(messageId),
    confirmPublication: (messageId, expiresAt) =>
      cell.confirmQueueMessagePublication({ messageId, expiresAt }),
    complete: (messageId) => cell.completeQueueMessage(messageId),
  };
}

export function runQueueClaim(run: QueueRunStub, claimName: string): QueueClaimHandle {
  return {
    claim: async (messageId, staleMs) => {
      const result = await run.claimRunQueueMessage({ claimName, messageId, staleMs });
      return result.expired ? { expired: true, claimed: false } : result;
    },
    hold: (params) => run.holdRunQueueMessage({ claimName, ...params }),
    release: (messageId) => run.releaseRunQueueMessage({ claimName, messageId }),
    abandonReservation: (messageId) =>
      run.abandonRunQueueMessageReservation({ claimName, messageId }),
    confirmPublication: (messageId, expiresAt) =>
      run.confirmRunQueueMessagePublication({ claimName, messageId, expiresAt }),
    complete: (messageId) => run.completeRunQueueMessage({ claimName, messageId }),
  };
}

/** The claim a delivered envelope was reserved under, if it has an idempotency key. */
export function deliveryQueueClaim(
  envelope: NativeQueueEnvelope,
  runStub: (name: string) => QueueRunStub,
): QueueClaimHandle | undefined {
  if (!envelope.idempotencyKey) return undefined;
  const claimName = queueClaimName(envelope.queueName, envelope.idempotencyKey);
  return envelope.claimScope === 'run' && envelope.runId
    ? runQueueClaim(runStub(envelope.runId), claimName)
    : cellQueueClaim(runStub(claimName));
}
