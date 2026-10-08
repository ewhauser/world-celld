import { isNonNegativeSafeInteger, isRecord } from './validation.js';
import { MAX_QUEUE_SCHEDULE_TIMESTAMP_MS } from './lifecycle.js';

export { QUEUE_CLAIM_STALE_MS } from './lifecycle.js';

/** celld v0.6.0 follows this native Queue producer-delay ceiling. */
export const NATIVE_QUEUE_MAX_DELAY_SECONDS = 86_400;
export const NATIVE_QUEUE_MAX_MESSAGE_BYTES = 128_000;
export const MAX_QUEUE_SUSPENSIONS = 256;
/** Unconfirmed producer reservations must become recoverable after an interrupted send. */
export const QUEUE_PUBLICATION_LEASE_MS = 5 * 60 * 1000;
export const QUEUE_RESERVATION_GRACE_MS = 5 * 24 * 60 * 60 * 1000;
export const QUEUE_PAYLOAD_REGISTRY_PREFIX = 'queue-payload:';
/** Largest run-bearing body that may travel inline in the broker envelope. */
export const MAX_INLINE_RUN_BODY_BYTES = 8 * 1024;
/** Workflow payload fields that carry user data, which run retention must be able to delete. */
const USER_DATA_FIELDS = ['runInput', 'hookInput', 'stepInput'] as const;

/**
 * A run-bearing body may stay inline when it is small and carries no user
 * data: then it holds only IDs, trace context, and metadata, and nothing in it
 * needs run retention to delete it from object storage.
 */
export function isInlineRunBody(body: string): boolean {
  if (body.length > MAX_INLINE_RUN_BODY_BYTES) return false;
  if (new TextEncoder().encode(body).byteLength > MAX_INLINE_RUN_BODY_BYTES) return false;
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return false;
  }
  return isRecord(parsed) && USER_DATA_FIELDS.every((field) => parsed[field] === undefined);
}

export interface QueuePayloadRegistration {
  messageId: string;
  key: string;
  /** Delete an orphaned resilient-start payload after this deadline. */
  orphanExpiresAt: number;
}

export interface QueuePayloadOrphan {
  messageId: string;
  runId: string;
  key: string;
  expiresAt: number;
}

/**
 * Small broker message carried by celld's native Queue.
 *
 * Run-bearing payload bytes live in object storage so the broker's 128 kB limit does not
 * narrow the Workflow World contract and run retention can delete those bytes
 * without waiting for the Queue's fixed four-day retention window.
 */
export interface NativeQueueEnvelope {
  version: 1;
  messageId: string;
  queueName: string;
  targetBaseUrl: string;
  runId?: string;
  idempotencyKey?: string;
  payloadKey?: string;
  /**
   * Inline body. Health checks always travel inline; a run-bearing body may
   * travel inline when it is small and carries no user data, in which case
   * delivery checks the run's expiry instead of reading a payload object.
   */
  body?: string;
  /** Absolute workflow redelivery deadline; long waits are chained by the consumer. */
  notBefore?: number;
  /**
   * Where the idempotency claim lives. Absent: the claim cell named by queue
   * and key. `run`: an entry in the run's own cell (requires `runId`).
   */
  claimScope?: 'run';
  /** Failed deliveries carried across suspension re-publishes. */
  deliveryFailures?: number;
  suspensionCount?: number;
}

/**
 * Enqueue-side answer from a run cell. `cell`: the run predates run-scoped
 * claims, so its idempotency claims stay in claim cells. `run`: the
 * reservation was made in the run's own cell.
 */
export type RunQueueReservation =
  | { ok: false; message: string }
  | { ok: true; scope: 'cell' }
  | { ok: true; scope: 'run'; admitted: boolean; messageId: string; publicationPending?: true };

export interface NativeQueueSendOptions {
  delaySeconds?: number;
}

export interface NativeQueueSendResult {
  messageId: string;
}

export interface NativeQueueBatchEntry {
  envelope: NativeQueueEnvelope;
  options?: NativeQueueSendOptions;
}

export type NativeQueueBatchResult =
  | { messageId: string; error?: undefined }
  | { messageId: null; error: string; retryable: boolean };

export function queuePayloadRegistryKey(messageId: string): string {
  return `${QUEUE_PAYLOAD_REGISTRY_PREFIX}${messageId}`;
}

export function queuePayloadObjectKey(runId: string, messageId: string): string {
  return `workflow-queue/${encodeURIComponent(runId)}/${encodeURIComponent(messageId)}`;
}

export function queueClaimName(queueName: string, idempotencyKey: string): string {
  return `claim:${queueName.length}:${queueName}:${idempotencyKey}`;
}

export function queueOrphanName(messageId: string): string {
  return `queue-orphan:${messageId}`;
}

export function nativeQueueDelaySeconds(notBefore: number | undefined, now: number): number {
  if (notBefore === undefined || notBefore <= now) return 0;
  return Math.min(NATIVE_QUEUE_MAX_DELAY_SECONDS, Math.ceil((notBefore - now) / 1000));
}

export function validateNativeQueueEnvelope(value: unknown): NativeQueueEnvelope {
  if (!isRecord(value)) throw new TypeError('world-celld native queue envelope must be an object');
  if (value.version !== 1)
    throw new TypeError('world-celld native queue envelope version must be 1');
  for (const field of ['messageId', 'queueName', 'targetBaseUrl'] as const) {
    if (typeof value[field] !== 'string' || value[field].length === 0) {
      throw new TypeError(`world-celld native queue envelope ${field} must be a non-empty string`);
    }
  }
  for (const field of ['runId', 'idempotencyKey', 'payloadKey', 'body'] as const) {
    if (value[field] !== undefined && typeof value[field] !== 'string') {
      throw new TypeError(`world-celld native queue envelope ${field} must be a string`);
    }
  }
  if (value.claimScope !== undefined && (value.claimScope !== 'run' || value.runId === undefined)) {
    throw new TypeError("world-celld native queue envelope claimScope must be 'run' with a runId");
  }
  if ((value.payloadKey === undefined) === (value.body === undefined)) {
    throw new TypeError(
      'world-celld native queue envelope must contain exactly one payload source',
    );
  }
  for (const field of ['notBefore', 'deliveryFailures', 'suspensionCount'] as const) {
    if (value[field] !== undefined && !isNonNegativeSafeInteger(value[field])) {
      throw new TypeError(
        `world-celld native queue envelope ${field} must be a non-negative safe integer`,
      );
    }
  }
  if (
    value.notBefore !== undefined &&
    (value.notBefore as number) > MAX_QUEUE_SCHEDULE_TIMESTAMP_MS
  ) {
    throw new TypeError(
      `world-celld native queue envelope notBefore must not exceed ${MAX_QUEUE_SCHEDULE_TIMESTAMP_MS}`,
    );
  }
  return value as unknown as NativeQueueEnvelope;
}

export function validateNativeQueueSendOptions(value: unknown): NativeQueueSendOptions {
  if (value === undefined) return {};
  if (!isRecord(value)) throw new TypeError('world-celld native queue options must be an object');
  const delaySeconds = value.delaySeconds;
  if (
    delaySeconds !== undefined &&
    (!Number.isSafeInteger(delaySeconds) ||
      (delaySeconds as number) < 0 ||
      (delaySeconds as number) > NATIVE_QUEUE_MAX_DELAY_SECONDS)
  ) {
    throw new TypeError(
      `world-celld native queue delaySeconds must be an integer between 0 and ${NATIVE_QUEUE_MAX_DELAY_SECONDS}`,
    );
  }
  return delaySeconds === undefined ? {} : { delaySeconds: delaySeconds as number };
}
