import { DurableObject } from '../do-base.js';
import type { EnqueueOutcome, EnqueueRequest } from '../../queue.js';
import type { ExpireQueueRunResult } from '../../retention.js';

/**
 * Queue cell: the celld replacement for Cloudflare Queues.
 *
 * One cell (`q:<shard>`) owns the full lifecycle of its messages: durable
 * scheduling via the cell alarm, delivery via outbound fetch (the same x-vqs
 * dialect as the in-process test pump), retries with capped exponential
 * backoff, idempotencyKey dedup, and a dead-letter table.
 *
 * Storage layout (KV-style, ordered scans via zero-padded epoch-ms keys):
 *   cfg                          pinned QueueCellConfig (first-enqueue wins)
 *   msg:<messageId>              MessageRow
 *   due:<paddedMs>:<messageId>   schedule index -> messageId
 *   inflight-deadline:<paddedMs>:<messageId>
 *                                ordered crash-recovery index -> messageId
 *   key:<idempotencyKey>         active dedup claim -> messageId
 *   dlq:<paddedMs>:<messageId>   DeadLetterRow
 *   run:<runId>:<messageId>      QueueRunReference for retention cleanup
 *   expired-run:<runId>          run fence preventing delayed enqueue
 *
 * celld gotchas designed around:
 * - celld#144 (alarm handlers can overlap): the storage-only claim phase runs
 *   inside blockConcurrencyWhile; slow delivery I/O runs outside the gate via
 *   waitUntil.
 * - six-failure alarm abandonment: the alarm handler never throws for data
 *   conditions — backoff reschedules the alarm instead. rearmAlarm() is the
 *   recovery hatch, and every enqueue also re-arms.
 * - deploys restart the fleet: in-flight deliveries die with the isolate; the
 *   inflight-deadline sweep on the next alarm redelivers (at-least-once).
 */

export interface MessageRow {
  messageId: string;
  queueName: string;
  pathname: 'flow' | 'step';
  /** Pre-encoded tagged-JSON payload — forwarded byte-identical. */
  body: string;
  runId?: string;
  /**
   * Delivery target captured at enqueue time. Carried per message (not
   * pinned on the cell) so an app redeploy at a new URL never bricks the
   * queue: old messages deliver (or dead-letter) against their original
   * target, new messages against the new one.
   */
  targetBaseUrl: string;
  idempotencyKey?: string;
  /** Completed delivery attempts. */
  attempt: number;
  enqueuedAt: number;
  lastError?: string;
}

export interface DeadLetterRow extends MessageRow {
  failedAt: number;
}

export interface QueueStats {
  pending: number;
  inflight: number;
  deadLetters: number;
  alarmAt: number | null;
}

interface QueueRunReference {
  messageId: string;
  dueKey?: string;
  inflightKey?: string;
  dlqKey?: string;
  idempotencyKey?: string;
}

interface InflightClaim {
  row: MessageRow;
  /** The exact index key is also the lease token for this delivery attempt. */
  inflightKey: string;
}

interface ExpiredRunFence {
  expiredAt: number;
  deleted: number;
}

const PERMANENT_STATUSES = new Set([404, 409, 410, 422]);

const DEFAULT_MAX_ATTEMPTS = 5;
const DEFAULT_MAX_INFLIGHT = 5;
const DEFAULT_DELIVERY_TIMEOUT_MS = 300_000;
/** Grace period past the delivery timeout before a claim counts as lost. */
const INFLIGHT_GRACE_MS = 30_000;
/** Move overdue alarms to a new timestamp so celld observes a fresh edge. */
const MIN_ALARM_DELAY_MS = 1;
/** Bound expiry recovery so one alarm cannot monopolize the cell. */
const EXPIRED_INFLIGHT_BATCH = 128;
const STORAGE_BATCH_SIZE = 128;
const EXPIRE_RUN_REFERENCE_BATCH = 64;
const PURGE_DEAD_LETTER_BATCH = 128;

const INFLIGHT_DEADLINE_PREFIX = 'inflight-deadline:';

type AlarmStorage = Pick<DurableObjectStorage, 'list' | 'getAlarm' | 'setAlarm' | 'deleteAlarm'>;

function pad(ms: number): string {
  return String(Math.max(0, Math.floor(ms))).padStart(13, '0');
}

function dueKey(atMs: number, messageId: string): string {
  return `due:${pad(atMs)}:${messageId}`;
}

function inflightKey(deadlineMs: number, messageId: string): string {
  return `${INFLIGHT_DEADLINE_PREFIX}${pad(deadlineMs)}:${messageId}`;
}

function deadlineFromInflightKey(key: string): number {
  return Number.parseInt(
    key.slice(INFLIGHT_DEADLINE_PREFIX.length, INFLIGHT_DEADLINE_PREFIX.length + 13),
    10,
  );
}

function runReferenceKey(runId: string, messageId: string): string {
  return `run:${runId}:${messageId}`;
}

function expiredRunKey(runId: string): string {
  return `expired-run:${runId}`;
}

function boundedLimit(value: number | undefined, fallback: number, maximum: number): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.max(1, Math.min(maximum, Math.floor(value)));
}

async function getMany<T>(
  storage: DurableObjectTransaction,
  keys: string[],
): Promise<Map<string, T>> {
  const result = new Map<string, T>();
  for (let offset = 0; offset < keys.length; offset += STORAGE_BATCH_SIZE) {
    const page = await storage.get<T>(keys.slice(offset, offset + STORAGE_BATCH_SIZE));
    for (const [key, value] of page) result.set(key, value);
  }
  return result;
}

async function putMany(
  storage: DurableObjectTransaction,
  entries: Iterable<readonly [string, unknown]>,
): Promise<void> {
  const all = Array.from(entries);
  for (let offset = 0; offset < all.length; offset += STORAGE_BATCH_SIZE) {
    await storage.put(Object.fromEntries(all.slice(offset, offset + STORAGE_BATCH_SIZE)));
  }
}

async function deleteMany(storage: DurableObjectTransaction, keys: string[]): Promise<number> {
  let deleted = 0;
  const unique = Array.from(new Set(keys));
  for (let offset = 0; offset < unique.length; offset += STORAGE_BATCH_SIZE) {
    deleted += await storage.delete(unique.slice(offset, offset + STORAGE_BATCH_SIZE));
  }
  return deleted;
}

function backoffSeconds(attempt: number): number {
  return Math.min(60, 2 ** attempt);
}

async function cancelUnusedResponseBody(response: Response): Promise<void> {
  try {
    await response.body?.cancel();
  } catch {
    // The delivery outcome is already known from the status. A cancellation
    // failure must not turn an acknowledged callback into a retry.
  }
}

interface QueueCellEnv {
  QUEUE_MAX_ATTEMPTS?: string;
  QUEUE_MAX_INFLIGHT?: string;
  QUEUE_DELIVERY_TIMEOUT_MS?: string;
  WORKFLOW_CALLBACK_SECRET?: string;
  /** Test seams (never set by a real deploy — celld vars are strings). */
  clock?: () => number;
  fetch?: typeof fetch;
}

const MAX_QUEUE_INFLIGHT = 128;

export class QueueDO extends DurableObject {
  #now(): number {
    const clock = (this.env as QueueCellEnv)?.clock;
    return typeof clock === 'function' ? clock() : Date.now();
  }

  #fetch(): typeof fetch {
    const impl = (this.env as QueueCellEnv)?.fetch;
    return typeof impl === 'function' ? impl : fetch;
  }

  #intVar(
    name: 'QUEUE_MAX_ATTEMPTS' | 'QUEUE_MAX_INFLIGHT' | 'QUEUE_DELIVERY_TIMEOUT_MS',
    fallback: number,
  ): number {
    const raw = (this.env as QueueCellEnv)?.[name];
    const parsed = raw ? Number.parseInt(raw, 10) : Number.NaN;
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
  }

  #maxInflight(): number {
    const configured = this.#intVar('QUEUE_MAX_INFLIGHT', DEFAULT_MAX_INFLIGHT);
    if (configured > MAX_QUEUE_INFLIGHT) {
      throw new Error(`QUEUE_MAX_INFLIGHT must be at most ${MAX_QUEUE_INFLIGHT}`);
    }
    return configured;
  }

  async enqueue(request: EnqueueRequest): Promise<EnqueueOutcome> {
    this.#maxInflight();
    const storage = this.ctx.storage;
    const now = this.#now();

    const outcome = await storage.transaction<EnqueueOutcome>(async (txn) => {
      if (request.runId && (await txn.get(expiredRunKey(request.runId))) !== undefined) {
        return {
          ok: false,
          code: 'RUN_EXPIRED',
          message: `Workflow run "${request.runId}" has expired`,
        };
      }
      // Pin the shard count from the first enqueue and reject drift loudly:
      // idempotencyKey -> cell affinity (and therefore dedup correctness)
      // depends on every producer agreeing on the shard count. The delivery
      // URL deliberately is NOT pinned — it travels per message.
      const pinned = await txn.get<{ queueShards: number; pinnedAt: number }>('cfg');
      if (!pinned) {
        await txn.put('cfg', { queueShards: request.config.queueShards, pinnedAt: now });
      } else if (pinned.queueShards !== request.config.queueShards) {
        return {
          ok: false,
          code: 'CONFIG_MISMATCH',
          message:
            `queue cell shard-count mismatch: pinned ${pinned.queueShards}, ` +
            `got ${request.config.queueShards} — changing queueShards requires draining the fleet`,
        };
      }

      // Dedup: while a message with this idempotencyKey is active, return the
      // original messageId (core re-enqueues pending steps on every replay and
      // relies on this).
      if (request.idempotencyKey) {
        const existing = await txn.get<string>(`key:${request.idempotencyKey}`);
        if (existing) {
          if (await txn.get(`msg:${existing}`)) {
            await this.#scheduleNextAlarm(txn, now);
            return { ok: true, messageId: existing, deduped: true };
          }
          // Stale claim (message gone without release) — steal it.
          await txn.delete(`key:${request.idempotencyKey}`);
        }
        await txn.put(`key:${request.idempotencyKey}`, request.messageId);
      }

      const row: MessageRow = {
        messageId: request.messageId,
        queueName: request.queueName,
        pathname: request.pathname,
        body: request.body,
        runId: request.runId,
        targetBaseUrl: request.config.targetBaseUrl,
        idempotencyKey: request.idempotencyKey,
        attempt: 0,
        enqueuedAt: now,
      };
      const dueAt = now + Math.max(0, request.delaySeconds ?? 0) * 1000;
      const scheduleKey = dueKey(dueAt, row.messageId);
      await txn.put(`msg:${row.messageId}`, row);
      await txn.put(scheduleKey, row.messageId);
      if (row.runId) {
        await txn.put<QueueRunReference>(runReferenceKey(row.runId, row.messageId), {
          messageId: row.messageId,
          dueKey: scheduleKey,
          idempotencyKey: row.idempotencyKey,
        });
      }
      await this.#armAlarmAtMost(txn, dueAt, now);

      return { ok: true, messageId: row.messageId, deduped: false };
    });

    return outcome;
  }

  async alarm(): Promise<void> {
    // celld#144: alarm handlers can overlap while awaiting. The storage-only
    // claim phase runs under the input gate; a failure is carried out and
    // rethrown outside so a failed critical section cannot reset the cell.
    let claimed: InflightClaim[] = [];
    let failure: unknown = null;
    const attempt = async () => {
      try {
        claimed = await this.#claimPhase();
      } catch (error) {
        failure = error;
      }
    };
    if (typeof this.ctx.blockConcurrencyWhile === 'function') {
      await this.ctx.blockConcurrencyWhile(attempt);
    } else {
      await attempt();
    }
    if (failure !== null) throw failure;

    for (const claim of claimed) {
      this.ctx.waitUntil(this.#deliver(claim));
    }
  }

  /**
   * Storage-only: recover lost inflight claims, claim due messages up to the
   * inflight cap, and re-arm the alarm. Never throws for data conditions.
   */
  async #claimPhase(): Promise<InflightClaim[]> {
    const storage = this.ctx.storage;
    const now = this.#now();
    const maxInflight = this.#maxInflight();
    const claimed = await storage.transaction<InflightClaim[]>(async (txn) => {
      // 1. Recover: an inflight entry past its deadline is a lost delivery
      // (node crash, deploy restart) — back to due for redelivery.
      const expiredPage = await txn.list<string>({
        prefix: INFLIGHT_DEADLINE_PREFIX,
        end: `${INFLIGHT_DEADLINE_PREFIX}${pad(now + 1)}`,
        limit: EXPIRED_INFLIGHT_BATCH + 1,
      });
      const expired = Array.from(expiredPage.entries()).slice(0, EXPIRED_INFLIGHT_BATCH);
      const expiredRows = await getMany<MessageRow>(
        txn,
        expired.map(([, messageId]) => `msg:${messageId}`),
      );
      await deleteMany(
        txn,
        expired.map(([key]) => key),
      );
      const recoveryWrites: Array<readonly [string, unknown]> = [];
      for (const [, messageId] of expired) {
        const row = expiredRows.get(`msg:${messageId}`);
        if (row) {
          const scheduleKey = dueKey(now, messageId);
          recoveryWrites.push([scheduleKey, messageId]);
          if (row.runId) {
            recoveryWrites.push([
              runReferenceKey(row.runId, row.messageId),
              {
                messageId: row.messageId,
                dueKey: scheduleKey,
                idempotencyKey: row.idempotencyKey,
              } satisfies QueueRunReference,
            ]);
          }
        }
      }
      await putMany(txn, recoveryWrites);
      if (expiredPage.size > EXPIRED_INFLIGHT_BATCH) {
        await txn.setAlarm(now + MIN_ALARM_DELAY_MS);
        return [];
      }

      // Only the configured concurrency cap matters here. Reading at most the
      // cap avoids replacing the expiry scan with an unbounded count scan.
      const activeInflight = await txn.list<string>({
        prefix: INFLIGHT_DEADLINE_PREFIX,
        limit: maxInflight,
      });
      const inflightCount = activeInflight.size;

      // 2. Claim due messages up to the cap.
      const rows: InflightClaim[] = [];
      if (inflightCount < maxInflight) {
        const due = await txn.list<string>({
          prefix: 'due:',
          end: `due:${pad(now + 1)}`,
          limit: maxInflight - inflightCount,
        });
        const timeoutMs = this.#intVar('QUEUE_DELIVERY_TIMEOUT_MS', DEFAULT_DELIVERY_TIMEOUT_MS);
        const dueRows = await getMany<MessageRow>(
          txn,
          Array.from(due.values(), (messageId) => `msg:${messageId}`),
        );
        await deleteMany(txn, Array.from(due.keys()));
        const claimWrites: Array<readonly [string, unknown]> = [];
        for (const [, messageId] of due) {
          const row = dueRows.get(`msg:${messageId}`);
          if (!row) continue; // orphaned schedule entry
          const claimKey = inflightKey(now + timeoutMs + INFLIGHT_GRACE_MS, messageId);
          claimWrites.push([claimKey, messageId]);
          if (row.runId) {
            claimWrites.push([
              runReferenceKey(row.runId, row.messageId),
              {
                messageId: row.messageId,
                inflightKey: claimKey,
                idempotencyKey: row.idempotencyKey,
              } satisfies QueueRunReference,
            ]);
          }
          rows.push({ row, inflightKey: claimKey });
        }
        await putMany(txn, claimWrites);
      }
      await this.#scheduleNextAlarm(txn, now);
      return rows;
    });

    return claimed;
  }

  async #deliver({ row, inflightKey: claimKey }: InflightClaim): Promise<void> {
    const storage = this.ctx.storage;
    const timeoutMs = this.#intVar('QUEUE_DELIVERY_TIMEOUT_MS', DEFAULT_DELIVERY_TIMEOUT_MS);
    const callbackSecret = (this.env as QueueCellEnv)?.WORKFLOW_CALLBACK_SECRET;
    const url = `${row.targetBaseUrl.replace(/\/$/, '')}/.well-known/workflow/v1/${row.pathname}`;

    let response: Response | null = null;
    let transportError: unknown = null;
    try {
      response = await this.#fetch()(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-vqs-queue-name': row.queueName,
          'x-vqs-message-id': row.messageId,
          'x-vqs-message-attempt': String(row.attempt + 1),
          ...(callbackSecret ? { 'x-workflow-callback-secret': callbackSecret } : {}),
        },
        body: row.body,
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (error) {
      transportError = error;
    }

    const now = this.#now();

    if (response?.ok) {
      // Ack: the message is done; release the dedup claim.
      await cancelUnusedResponseBody(response);
      await this.#ack(row, claimKey);
    } else if (response && response.status === 503) {
      // {timeoutSeconds} means "redeliver later, same message": the attempt
      // count does not advance and the dedup key stays claimed.
      let timeoutSeconds: number | undefined;
      try {
        const parsed = (await response.json()) as { timeoutSeconds?: number };
        timeoutSeconds = parsed?.timeoutSeconds;
      } catch {
        timeoutSeconds = undefined;
      }
      if (typeof timeoutSeconds === 'number') {
        await storage.transaction(async (txn) => {
          // Deleting the exact deadline key doubles as a lease-token check. A
          // late response from an expired claim must not mutate a newer claim.
          if (!(await txn.delete(claimKey))) return;
          if (row.runId && (await txn.get(expiredRunKey(row.runId))) !== undefined) {
            await txn.delete(`msg:${row.messageId}`);
            await txn.delete(runReferenceKey(row.runId, row.messageId));
            if (row.idempotencyKey) {
              const dedupKey = `key:${row.idempotencyKey}`;
              if ((await txn.get<string>(dedupKey)) === row.messageId) {
                await txn.delete(dedupKey);
              }
            }
            await this.#scheduleNextAlarm(txn, now);
            return;
          }
          const scheduleKey = dueKey(now + timeoutSeconds * 1000, row.messageId);
          await txn.put(scheduleKey, row.messageId);
          if (row.runId) {
            await txn.put<QueueRunReference>(runReferenceKey(row.runId, row.messageId), {
              messageId: row.messageId,
              dueKey: scheduleKey,
              idempotencyKey: row.idempotencyKey,
            });
          }
          await this.#scheduleNextAlarm(txn, now);
        });
      } else {
        await this.#retry(row, claimKey, 'HTTP 503');
      }
    } else if (response && PERMANENT_STATUSES.has(response.status)) {
      // Permanent: drop without burning retries; the handler already
      // classified this as unreplayable.
      await cancelUnusedResponseBody(response);
      await this.#ack(row, claimKey);
    } else {
      const reason = response
        ? `HTTP ${response.status}`
        : `transport error: ${String(transportError)}`;
      if (response) await cancelUnusedResponseBody(response);
      await this.#retry(row, claimKey, reason);
    }
  }

  async #ack(row: MessageRow, claimKey: string): Promise<void> {
    const storage = this.ctx.storage;
    await storage.transaction(async (txn) => {
      if (!(await txn.delete(claimKey))) return;
      await txn.delete(`msg:${row.messageId}`);
      if (row.idempotencyKey) {
        const holder = await txn.get<string>(`key:${row.idempotencyKey}`);
        if (holder === row.messageId) {
          await txn.delete(`key:${row.idempotencyKey}`);
        }
      }
      if (row.runId) {
        await txn.delete(runReferenceKey(row.runId, row.messageId));
      }
      await this.#scheduleNextAlarm(txn, this.#now());
    });
  }

  async #retry(row: MessageRow, claimKey: string, reason: string): Promise<void> {
    const storage = this.ctx.storage;
    const now = this.#now();
    const maxAttempts = this.#intVar('QUEUE_MAX_ATTEMPTS', DEFAULT_MAX_ATTEMPTS);
    const attempt = row.attempt + 1;

    await storage.transaction(async (txn) => {
      if (!(await txn.delete(claimKey))) return;
      if (row.runId && (await txn.get(expiredRunKey(row.runId))) !== undefined) {
        await txn.delete(`msg:${row.messageId}`);
        await txn.delete(runReferenceKey(row.runId, row.messageId));
        if (row.idempotencyKey) {
          const dedupKey = `key:${row.idempotencyKey}`;
          if ((await txn.get<string>(dedupKey)) === row.messageId) {
            await txn.delete(dedupKey);
          }
        }
        await this.#scheduleNextAlarm(txn, now);
        return;
      }

      if (attempt >= maxAttempts) {
        const dead: DeadLetterRow = { ...row, attempt, lastError: reason, failedAt: now };
        const deadKey = `dlq:${pad(now)}:${row.messageId}`;
        await txn.put(deadKey, dead);
        await txn.delete(`msg:${row.messageId}`);
        if (row.idempotencyKey) {
          const holder = await txn.get<string>(`key:${row.idempotencyKey}`);
          if (holder === row.messageId) {
            await txn.delete(`key:${row.idempotencyKey}`);
          }
        }
        if (row.runId) {
          await txn.put<QueueRunReference>(runReferenceKey(row.runId, row.messageId), {
            messageId: row.messageId,
            dlqKey: deadKey,
            idempotencyKey: row.idempotencyKey,
          });
        }
        await this.#scheduleNextAlarm(txn, now);
        return;
      }

      const updated: MessageRow = { ...row, attempt, lastError: reason };
      const scheduleKey = dueKey(now + backoffSeconds(attempt) * 1000, row.messageId);
      await txn.put(`msg:${row.messageId}`, updated);
      await txn.put(scheduleKey, row.messageId);
      if (row.runId) {
        await txn.put<QueueRunReference>(runReferenceKey(row.runId, row.messageId), {
          messageId: row.messageId,
          dueKey: scheduleKey,
          idempotencyKey: row.idempotencyKey,
        });
      }
      await this.#scheduleNextAlarm(txn, now);
    });
  }

  /** Arm the alarm no later than `atMs` (durable backoff never throws). */
  async #armAlarmAtMost(storage: AlarmStorage, atMs: number, now: number): Promise<void> {
    const current = await storage.getAlarm();
    if (current !== null && current <= now) {
      await storage.setAlarm(now + MIN_ALARM_DELAY_MS);
      return;
    }

    const target = atMs <= now ? now + MIN_ALARM_DELAY_MS : atMs;
    if (current === null || target < current) {
      await storage.setAlarm(target);
    }
  }

  /** Recompute the alarm from the earliest due entry / inflight deadline. */
  async #rearm(): Promise<void> {
    await this.#scheduleNextAlarm(this.ctx.storage, this.#now());
  }

  /** Keep the alarm index consistent with message state in the same transaction. */
  async #scheduleNextAlarm(storage: AlarmStorage, now: number): Promise<void> {
    let next: number | null = null;

    const due = await storage.list<string>({ prefix: 'due:', limit: 1 });
    for (const key of due.keys()) {
      next = Number.parseInt(key.slice('due:'.length, 'due:'.length + 13), 10);
    }

    const inflight = await storage.list<string>({ prefix: INFLIGHT_DEADLINE_PREFIX, limit: 1 });
    for (const key of inflight.keys()) {
      const deadline = deadlineFromInflightKey(key);
      if (next === null || deadline < next) next = deadline;
    }

    if (next !== null) {
      await storage.setAlarm(next <= now ? now + MIN_ALARM_DELAY_MS : next);
    } else {
      await storage.deleteAlarm();
    }
  }

  // ---- Admin RPC (exposed through the router's queue binding) ----

  async stats(): Promise<QueueStats> {
    const storage = this.ctx.storage;
    const [due, inflight, dlq, alarmAt] = await Promise.all([
      storage.list({ prefix: 'due:' }),
      storage.list({ prefix: INFLIGHT_DEADLINE_PREFIX }),
      storage.list({ prefix: 'dlq:' }),
      storage.getAlarm(),
    ]);
    return {
      pending: due.size,
      inflight: inflight.size,
      deadLetters: dlq.size,
      alarmAt,
    };
  }

  async listDeadLetters(params?: {
    limit?: number;
    cursor?: string;
  }): Promise<{ data: DeadLetterRow[]; cursor: string | null; hasMore: boolean }> {
    const limit = params?.limit ?? 100;
    const entries = await this.ctx.storage.list<DeadLetterRow>({
      prefix: 'dlq:',
      startAfter: params?.cursor,
      limit: limit + 1,
    });
    const keys = Array.from(entries.keys());
    const page = keys.slice(0, limit);
    const hasMore = keys.length > limit;
    return {
      data: page.map((k) => entries.get(k) as DeadLetterRow),
      cursor: hasMore ? page.at(-1)! : null,
      hasMore,
    };
  }

  /** Move a dead letter back to the live queue (attempt count reset). */
  async redriveDeadLetter(messageId: string): Promise<{ ok: boolean }> {
    const storage = this.ctx.storage;
    const now = this.#now();
    const outcome = await storage.transaction<{ ok: boolean }>(async (txn) => {
      const dlq = await txn.list<DeadLetterRow>({ prefix: 'dlq:' });
      for (const [key, dead] of dlq) {
        if (dead.messageId !== messageId) continue;
        const { failedAt: _failedAt, ...rest } = dead;
        const row: MessageRow = { ...rest, attempt: 0, lastError: undefined };

        if (row.idempotencyKey) {
          const dedupKey = `key:${row.idempotencyKey}`;
          const holder = await txn.get<string>(dedupKey);
          if (holder && holder !== row.messageId && (await txn.get(`msg:${holder}`))) {
            return { ok: false };
          }
          if (holder && holder !== row.messageId) {
            await txn.delete(dedupKey);
          }
          await txn.put(dedupKey, row.messageId);
        }

        await txn.put(`msg:${row.messageId}`, row);
        const scheduleKey = dueKey(now, row.messageId);
        await txn.put(scheduleKey, row.messageId);
        await txn.delete(key);
        if (row.runId) {
          await txn.put<QueueRunReference>(runReferenceKey(row.runId, row.messageId), {
            messageId: row.messageId,
            dueKey: scheduleKey,
            idempotencyKey: row.idempotencyKey,
          });
        }
        await this.#armAlarmAtMost(txn, now, now);
        return { ok: true };
      }
      return { ok: false };
    });

    return outcome;
  }

  async purgeDeadLetters(): Promise<{ purged: number; hasMore: boolean }> {
    const storage = this.ctx.storage;
    return await storage.transaction(async (txn) => {
      const entries = await txn.list<DeadLetterRow>({
        prefix: 'dlq:',
        limit: PURGE_DEAD_LETTER_BATCH + 1,
      });
      const page = Array.from(entries).slice(0, PURGE_DEAD_LETTER_BATCH);
      const keys = page.flatMap(([key, dead]) => [
        key,
        ...(dead.runId ? [runReferenceKey(dead.runId, dead.messageId)] : []),
      ]);
      await deleteMany(txn, keys);
      return { purged: page.length, hasMore: entries.size > PURGE_DEAD_LETTER_BATCH };
    });
  }

  /**
   * Recovery hatch for celld's six-failure alarm abandonment: derive the due
   * time again and arm it.
   */
  async rearmAlarm(): Promise<{ alarmAt: number | null }> {
    await this.#rearm();
    return { alarmAt: await this.ctx.storage.getAlarm() };
  }

  /** Fence a run and purge every live, inflight, and dead-letter message it owns. */
  async expireRun(
    runId: string,
    expiredAt: number,
    options?: { limit?: number },
  ): Promise<ExpireQueueRunResult> {
    const storage = this.ctx.storage;
    const limit = boundedLimit(
      options?.limit,
      EXPIRE_RUN_REFERENCE_BATCH,
      EXPIRE_RUN_REFERENCE_BATCH,
    );
    return await storage.transaction(async (txn) => {
      const existing = await txn.get<ExpiredRunFence>(expiredRunKey(runId));
      const references = await txn.list<QueueRunReference>({
        prefix: `run:${runId}:`,
        limit: limit + 1,
      });
      const page = Array.from(references).slice(0, limit);
      const dedupKeys = page.flatMap(([, reference]) =>
        reference.idempotencyKey ? [`key:${reference.idempotencyKey}`] : [],
      );
      const dedupHolders = await getMany<string>(txn, dedupKeys);
      const keys: string[] = [];
      let count = existing?.deleted ?? 0;
      for (const [key, reference] of page) {
        keys.push(`msg:${reference.messageId}`, key);
        if (reference.inflightKey) keys.push(reference.inflightKey);
        if (reference.dueKey) keys.push(reference.dueKey);
        if (reference.dlqKey) keys.push(reference.dlqKey);
        if (reference.idempotencyKey) {
          const dedupKey = `key:${reference.idempotencyKey}`;
          if (dedupHolders.get(dedupKey) === reference.messageId) keys.push(dedupKey);
        }
        count++;
      }
      await deleteMany(txn, keys);
      await txn.put<ExpiredRunFence>(expiredRunKey(runId), { expiredAt, deleted: count });
      await this.#scheduleNextAlarm(txn, this.#now());
      return { deleted: count, done: references.size <= limit };
    });
  }
}
