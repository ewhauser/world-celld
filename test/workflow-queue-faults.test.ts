import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { rpcParse, rpcStringify } from '../src/codec.js';
import { TOMBSTONE_KEY, type RunTombstone } from '../src/retention.js';
import { createQueue, type CelldQueueProducer } from '../src/queue.js';
import {
  QUEUE_PUBLICATION_LEASE_MS,
  queueClaimName,
  queueOrphanName,
  QUEUE_RESERVATION_GRACE_MS,
  type NativeQueueEnvelope,
} from '../src/queue-protocol.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import {
  RUN_QUEUE_CLAIM_SCOPE_KEY,
  WorkflowRunDO,
} from '../src/worker/durable-objects/WorkflowRunDO.js';
import queueConsumer from '../src/worker/queue-consumer.js';
import { deliverQueueMessage } from '../src/worker/queue-delivery.js';
import type { QueuePayloadStore } from '../src/worker/queue-payload-store.js';
import { createRouter, type WorkerEnv } from '../src/worker/router.js';

const SECRET = 'workflow-queue-fault-secret';
const PAYLOAD = { runId: 'wrun_queue_fault', stepId: 'step_queue_fault' };

function setupProducer(scope: 'cell' | 'run', suffix: string) {
  const cellEnv: Record<string, unknown> = {};
  const fleet = new FakeFleet({ runs: WorkflowRunDO }, cellEnv);
  const objects = new Map<string, string>();
  const store = {
    write: vi.fn<QueuePayloadStore['write']>(async (key, body) => {
      objects.set(key, body);
    }),
    read: vi.fn<QueuePayloadStore['read']>(async (key) => objects.get(key) ?? null),
    delete: vi.fn<QueuePayloadStore['delete']>(async (keys) => {
      for (const key of Array.isArray(keys) ? keys : [keys]) objects.delete(key);
    }),
  };
  const send = vi.fn<WorkerEnv['WORKFLOW_QUEUE']['send']>().mockResolvedValue(undefined);
  const sendBatch = vi
    .fn<NonNullable<WorkerEnv['WORKFLOW_QUEUE']['sendBatch']>>()
    .mockResolvedValue(undefined);
  const env = {
    WORKFLOW_DB: fleet.namespace('runs'),
    WORKFLOW_QUEUE: { send, sendBatch },
    WORKFLOW_QUEUE_PAYLOADS: store,
    WORLD_SECRET: SECRET,
  } as WorkerEnv;
  Object.assign(cellEnv, env, { clock: () => fleet.now });
  const runId = `wrun_queue_fault_${scope}_${suffix}`;
  const run = fleet.cell('runs', runId);
  if (scope === 'run') run.storage.data.set(RUN_QUEUE_CLAIM_SCOPE_KEY, 'run');
  const envelope: NativeQueueEnvelope = {
    version: 1,
    messageId: `msg_queue_fault_${suffix}`,
    queueName: '__wkf_workflow_faults',
    targetBaseUrl: 'https://app.internal',
    runId,
    idempotencyKey: `fault-key-${suffix}`,
    body: rpcStringify({ runId, stepId: 'step_fault' }),
  };
  const router = createRouter(env);
  const publish = (message: NativeQueueEnvelope) =>
    router(
      new Request('https://world.internal/v1/queue/send', {
        method: 'POST',
        headers: { authorization: `Bearer ${SECRET}`, 'content-type': 'application/json' },
        body: rpcStringify([message]),
      }),
    );
  const publishBatch = (messages: NativeQueueEnvelope[]) =>
    router(
      new Request('https://world.internal/v1/queue/send-batch', {
        method: 'POST',
        headers: { authorization: `Bearer ${SECRET}`, 'content-type': 'application/json' },
        body: rpcStringify([messages.map((message) => ({ envelope: message }))]),
      }),
    );
  return { env, fleet, objects, store, send, sendBatch, run, envelope, publish, publishBatch };
}

function brokerMessage(envelope: NativeQueueEnvelope, attempts = 1) {
  return {
    body: JSON.stringify(envelope),
    attempts,
    ack: vi.fn<() => void>(),
    retry: vi.fn<(options?: { delaySeconds?: number }) => void>(),
  };
}

function pump(maxAttempts = 3) {
  vi.stubEnv('CELLD_QUEUE_MODE', 'test');
  vi.stubEnv('VITEST', 'true');
  return createQueue({
    env: { WORKFLOW_QUEUE: { send: vi.fn<CelldQueueProducer['send']>() } },
    deploymentId: 'queue-fault-tests',
    maxAttempts,
    backoffDelayMs: 10,
  });
}

describe('workflow Queue fault recovery', () => {
  beforeEach(() => {
    vi.useFakeTimers({ now: 1_000_000 });
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
  });
  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  });

  describe.each(['cell', 'run'] as const)('%s-scoped batch recovery', (scope) => {
    function batch(suffix: string) {
      const producer = setupProducer(scope, suffix);
      const entries = Array.from({ length: 4 }, (_, index) => ({
        ...producer.envelope,
        messageId: `${producer.envelope.messageId}_${index}`,
        idempotencyKey: `${producer.envelope.idempotencyKey}_${index}`,
        body: rpcStringify({
          runId: producer.envelope.runId,
          stepInput: { input: new Uint8Array([index]) },
        }),
      }));
      return { ...producer, entries };
    }

    it('replays a whole batch after a lost broker response and preserves accepted payloads', async () => {
      const { sendBatch, publishBatch, entries, objects, env } = batch('batch-lost-response');
      let accepted: NativeQueueEnvelope[] = [];
      sendBatch.mockImplementationOnce(async (messages) => {
        accepted = messages.map((message) => JSON.parse(message.body) as NativeQueueEnvelope);
        throw new Error('accepted batch response lost');
      });
      const failed = rpcParse(await (await publishBatch(entries)).text()) as Array<{
        messageId: string | null;
        retryable?: boolean;
      }>;
      expect(failed.every((result) => result.messageId === null && result.retryable)).toBe(true);
      expect(accepted.map((message) => objects.get(message.payloadKey!))).toEqual(
        entries.map((entry) => entry.body),
      );
      const replay = entries.map((entry) =>
        Object.assign({}, entry, { messageId: `${entry.messageId}_replay` }),
      );
      const retry = rpcParse(await (await publishBatch(replay)).text()) as Array<{
        messageId: string;
      }>;
      expect(retry.map((result) => result.messageId)).toEqual(
        replay.map((entry) => entry.messageId),
      );
      expect(sendBatch).toHaveBeenCalledTimes(2);
      const callback = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }));
      vi.stubGlobal('fetch', callback);
      for (const message of accepted) {
        expect(await deliverQueueMessage(env, SECRET, message, 1)).toEqual({ kind: 'complete' });
      }
      expect(callback.mock.calls.map(([, init]) => init?.body)).toEqual(
        entries.map((entry) => entry.body),
      );
    });

    it('never reports success for a batch still held at the broker boundary', async () => {
      const { sendBatch, publishBatch, entries } = batch('batch-pending');
      const started = Promise.withResolvers<void>();
      const accepted = Promise.withResolvers<void>();
      sendBatch.mockImplementationOnce(() => {
        started.resolve();
        return accepted.promise;
      });
      const pending = publishBatch(entries);
      await started.promise;
      const replay = entries.map((entry) =>
        Object.assign({}, entry, { messageId: `${entry.messageId}_replay` }),
      );
      const blocked = rpcParse(await (await publishBatch(replay)).text()) as Array<{
        messageId: string | null;
        retryable?: boolean;
      }>;
      expect(blocked.every((result) => result.messageId === null && result.retryable)).toBe(true);
      expect(sendBatch).toHaveBeenCalledOnce();
      accepted.resolve();
      const original = rpcParse(await (await pending).text()) as Array<{ messageId: string }>;
      const retried = rpcParse(await (await publishBatch(replay)).text()) as Array<{
        messageId: string;
      }>;
      expect(retried).toEqual(original);
      expect(sendBatch).toHaveBeenCalledOnce();
    });

    it('rejects entries whose payload staging races with run retention', async () => {
      const { sendBatch, publishBatch, entries, objects, store, run, fleet } =
        batch('batch-retention-race');
      const started = Promise.withResolvers<void>();
      const resume = Promise.withResolvers<void>();
      store.write.mockImplementation(async (key, body) => {
        started.resolve();
        await resume.promise;
        objects.set(key, body);
      });
      const publishing = publishBatch(entries);
      await started.promise;
      run.storage.data.set(TOMBSTONE_KEY, {
        runId: entries[0].runId,
        version: 1,
        expiredAt: new Date(fleet.now),
        tombstonedAt: new Date(fleet.now),
        cleanup: {
          version: 1,
          runId: entries[0].runId!,
          workflowName: 'batch-retention-race',
          createdAt: new Date(fleet.now),
          dueAt: new Date(fleet.now),
          phase: 'tombstoned',
          generation: 1,
          attempts: 0,
          deletedPayloadKeys: 0,
          deletedStreams: 0,
          deletedQueuePayloads: 0,
        },
      } satisfies RunTombstone);
      resume.resolve();
      const result = rpcParse(await (await publishing).text()) as Array<{
        messageId: string | null;
        error?: string;
        retryable?: boolean;
      }>;
      expect(
        result.every(
          (entry) =>
            entry.messageId === null &&
            entry.retryable === false &&
            entry.error?.includes('expired'),
        ),
      ).toBe(true);
      expect(sendBatch).not.toHaveBeenCalled();
      expect(objects.size).toBe(0);
      for (const entry of entries) {
        expect(fleet.cell('runs', queueOrphanName(entry.messageId)).storage.data.size).toBe(0);
      }
    });

    it('isolates a lost confirmation reply and preserves confirmed siblings on whole-batch replay', async () => {
      const { sendBatch, publishBatch, entries, fleet, run, objects } = batch('batch-confirmation');
      const claim =
        scope === 'run'
          ? (run.instance as WorkflowRunDO)
          : (fleet.cell('runs', queueClaimName(entries[0].queueName, entries[0].idempotencyKey))
              .instance as WorkflowRunDO);
      vi.spyOn(
        claim,
        scope === 'run' ? 'confirmRunQueueMessagePublication' : 'confirmQueueMessagePublication',
      ).mockRejectedValueOnce(new Error('confirmation reply lost'));
      const first = rpcParse(await (await publishBatch(entries)).text()) as Array<{
        messageId: string | null;
        retryable?: boolean;
      }>;
      expect(first[0]).toMatchObject({ messageId: null, retryable: true });
      expect(first.slice(1).map((result) => result.messageId)).toEqual(
        entries.slice(1).map((entry) => entry.messageId),
      );
      expect(objects.size).toBe(entries.length);
      const retry = entries.map((entry) =>
        Object.assign({}, entry, { messageId: `${entry.messageId}_replay` }),
      );
      const second = rpcParse(await (await publishBatch(retry)).text()) as Array<{
        messageId: string;
      }>;
      expect(second[0].messageId).toBe(retry[0].messageId);
      expect(second.slice(1)).toEqual(first.slice(1));
      expect(sendBatch.mock.calls.map(([messages]) => messages.length)).toEqual([4, 1]);
    });

    it('cleans abandoned offloaded batch payloads after restarting orphan cells', async () => {
      const { sendBatch, publishBatch, entries, objects, fleet } = batch('batch-orphans');
      sendBatch.mockRejectedValueOnce(new Error('broker unavailable'));
      await publishBatch(entries);
      expect(objects.size).toBe(entries.length);
      for (const entry of entries) fleet.restartCell('runs', queueOrphanName(entry.messageId));
      fleet.advance(QUEUE_RESERVATION_GRACE_MS + 1);
      vi.setSystemTime(fleet.now);
      await fleet.fireDueAlarms();
      expect(objects.size).toBe(0);
    });
  });

  describe('test pump', () => {
    it('retains the same message and idempotency key through connection and body-read faults', async () => {
      const callback = vi
        .fn<typeof fetch>()
        .mockRejectedValueOnce(new Error('connection reset'))
        .mockResolvedValueOnce(
          new Response(
            new ReadableStream({
              start(controller) {
                controller.error(new Error('response connection reset'));
              },
            }),
            { status: 500 },
          ),
        )
        .mockResolvedValue(new Response(null, { status: 204 }));
      vi.stubGlobal('fetch', callback);
      const queue = pump();
      await queue.start();
      const first = await queue.queue('__wkf_workflow_faults', PAYLOAD, {
        idempotencyKey: 'connection-fault',
      });
      await vi.advanceTimersByTimeAsync(0);
      expect(callback).toHaveBeenCalledTimes(1);
      expect(
        await queue.queue('__wkf_workflow_faults', PAYLOAD, { idempotencyKey: 'connection-fault' }),
      ).toEqual(first);
      await vi.advanceTimersByTimeAsync(19);
      expect(callback).toHaveBeenCalledTimes(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(callback).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(39);
      expect(callback).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(1);
      expect(callback).toHaveBeenCalledTimes(3);
      expect(
        callback.mock.calls.map(([, options]) => [
          new Headers(options?.headers).get('x-vqs-message-id'),
          new Headers(options?.headers).get('x-vqs-message-attempt'),
        ]),
      ).toEqual([
        [first.messageId, '1'],
        [first.messageId, '2'],
        [first.messageId, '3'],
      ]);
      const next = await queue.queue('__wkf_workflow_faults', PAYLOAD, {
        idempotencyKey: 'connection-fault',
      });
      expect(next.messageId).not.toBe(first.messageId);
      await vi.advanceTimersByTimeAsync(0);
    });

    it('bounds connection-failure retries and releases the key after exhausting them', async () => {
      const callback = vi.fn<typeof fetch>().mockRejectedValue(new Error('server unavailable'));
      vi.stubGlobal('fetch', callback);
      const queue = pump(2);
      await queue.start();
      const first = await queue.queue('__wkf_workflow_faults', PAYLOAD, {
        idempotencyKey: 'bounded-fault',
      });
      await vi.advanceTimersByTimeAsync(100);
      expect(callback).toHaveBeenCalledTimes(2);
      await vi.advanceTimersByTimeAsync(1_000);
      expect(callback).toHaveBeenCalledTimes(2);
      callback.mockResolvedValue(new Response(null, { status: 204 }));
      const next = await queue.queue('__wkf_workflow_faults', PAYLOAD, {
        idempotencyKey: 'bounded-fault',
      });
      expect(next.messageId).not.toBe(first.messageId);
      await vi.advanceTimersByTimeAsync(0);
    });

    it('keeps identical keys independent across workflow queues', async () => {
      const callback = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }));
      vi.stubGlobal('fetch', callback);
      const queue = pump();
      const first = await queue.queue('__wkf_workflow_faults', PAYLOAD, { idempotencyKey: 'same' });
      const second = await queue.queue('__wkf_workflow_other', PAYLOAD, { idempotencyKey: 'same' });
      expect(second.messageId).not.toBe(first.messageId);
      await queue.start();
      await vi.advanceTimersByTimeAsync(0);
      expect(
        callback.mock.calls.map(([, options]) =>
          new Headers(options?.headers).get('x-vqs-queue-name'),
        ),
      ).toEqual(['__wkf_workflow_faults', '__wkf_workflow_other']);
    });

    it('uses a permanent status even when its unused body is broken', async () => {
      const callback = vi.fn<typeof fetch>().mockResolvedValue(
        new Response(
          new ReadableStream({
            start(controller) {
              controller.error(new Error('response body unavailable'));
            },
          }),
          { status: 410 },
        ),
      );
      vi.stubGlobal('fetch', callback);
      const queue = pump();
      await queue.start();
      const first = await queue.queue('__wkf_workflow_faults', PAYLOAD, { idempotencyKey: 'gone' });
      await vi.advanceTimersByTimeAsync(100);
      expect(callback).toHaveBeenCalledOnce();
      callback.mockResolvedValue(new Response(null, { status: 204 }));
      expect(
        (await queue.queue('__wkf_workflow_faults', PAYLOAD, { idempotencyKey: 'gone' })).messageId,
      ).not.toBe(first.messageId);
      await vi.advanceTimersByTimeAsync(0);
    });
  });

  it.each(['cell', 'run'] as const)(
    'allows a caller to retry a rejected broker publication with a %s-scoped claim',
    async (scope) => {
      const { send, envelope, publish } = setupProducer(scope, 'broker-rejection');
      send.mockRejectedValueOnce(new Error('broker unavailable'));
      expect((await publish(envelope)).status).toBe(500);
      const retried = { ...envelope, messageId: `${envelope.messageId}_retried` };
      const response = await publish(retried);
      expect(response.status).toBe(200);
      expect(rpcParse(await response.text())).toEqual({ messageId: retried.messageId });
      expect(send).toHaveBeenCalledTimes(2);
      expect(JSON.parse(send.mock.calls[1][0])).toMatchObject({ messageId: retried.messageId });
    },
  );

  it.each(['cell', 'run'] as const)(
    'recovers a %s-scoped reservation when payload registration fails',
    async (scope) => {
      const { send, envelope, publish, run } = setupProducer(scope, 'registration-fault');
      envelope.body = rpcStringify({ runId: envelope.runId, stepInput: { input: [1] } });
      vi.spyOn(run.instance as WorkflowRunDO, 'registerQueuePayload').mockRejectedValueOnce(
        new Error('run cell unavailable'),
      );
      expect((await publish(envelope)).status).toBe(500);
      const retried = { ...envelope, messageId: `${envelope.messageId}_retried` };
      const response = await publish(retried);
      expect(response.status).toBe(200);
      expect(rpcParse(await response.text())).toEqual({ messageId: retried.messageId });
      expect(send).toHaveBeenCalledOnce();
    },
  );

  it.each(['cell', 'run'] as const)(
    'retains a %s-scoped ambiguously accepted payload while enabling a real retry',
    async (scope) => {
      const { send, envelope, publish, objects, env } = setupProducer(scope, 'ambiguous-send');
      envelope.body = rpcStringify({ runId: envelope.runId, stepInput: { input: [1] } });
      let accepted!: NativeQueueEnvelope;
      send.mockImplementationOnce(async (body) => {
        accepted = JSON.parse(body) as NativeQueueEnvelope;
        throw new Error('broker accepted but reply was lost');
      });
      expect((await publish(envelope)).status).toBe(500);
      expect(objects.get(accepted.payloadKey!)).toBe(envelope.body);
      const retried = { ...envelope, messageId: `${envelope.messageId}_retried` };
      const response = await publish(retried);
      expect(response.status).toBe(200);
      expect(rpcParse(await response.text())).toEqual({ messageId: retried.messageId });
      expect(send).toHaveBeenCalledTimes(2);
      const callback = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 204 }));
      vi.stubGlobal('fetch', callback);
      expect(await deliverQueueMessage(env, SECRET, accepted, 1)).toEqual({ kind: 'complete' });
      expect(callback.mock.calls[0][1]?.body).toBe(envelope.body);
    },
  );

  describe.each(['cell', 'run'] as const)('%s-scoped publication interruption', (scope) => {
    it.each([false, true])(
      'does not acknowledge a concurrent producer before the broker, same message ID: %s',
      async (sameId) => {
        const { envelope, publish, send } = setupProducer(scope, `concurrent-${sameId}`);
        const accepted = Promise.withResolvers<unknown>();
        const started = Promise.withResolvers<void>();
        send.mockImplementationOnce(() => {
          started.resolve();
          return accepted.promise;
        });
        const publishing = publish(envelope);
        await started.promise;
        const contender = sameId
          ? envelope
          : { ...envelope, messageId: `${envelope.messageId}_contender` };
        const pending = await publish(contender);
        expect(pending.status).toBe(503);
        expect(send).toHaveBeenCalledOnce();
        accepted.reject(new Error('publication connection reset'));
        expect((await publishing).status).toBe(500);
        const retry = await publish(contender);
        expect(retry.status).toBe(200);
        expect(send).toHaveBeenCalledTimes(2);
      },
    );

    it('recovers a reservation persisted before producer death after its bounded lease', async () => {
      const { fleet, envelope, env, publish, send } = setupProducer(scope, 'producer-death');
      const claimName = queueClaimName(envelope.queueName, envelope.idempotencyKey!);
      const cellName = scope === 'run' ? envelope.runId! : claimName;
      const cell = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(cellName)) as WorkflowRunDO;
      const reservation = {
        messageId: envelope.messageId,
        expiresAt: fleet.now + QUEUE_PUBLICATION_LEASE_MS,
        publicationPending: true,
      };
      if (scope === 'run') {
        await cell.reserveRunQueueMessage({ claimName, ...reservation });
      } else {
        await cell.reserveQueueMessage(reservation);
      }
      fleet.restartCell('runs', cellName);
      const retry = { ...envelope, messageId: `${envelope.messageId}_after_restart` };
      expect((await publish(retry)).status).toBe(503);
      expect(send).not.toHaveBeenCalled();
      fleet.advance(QUEUE_PUBLICATION_LEASE_MS - 1);
      vi.setSystemTime(fleet.now);
      expect((await publish(retry)).status).toBe(503);
      fleet.advance(1);
      vi.setSystemTime(fleet.now);
      const recovered = await publish(retry);
      expect(recovered.status).toBe(200);
      expect(rpcParse(await recovered.text())).toEqual({ messageId: retry.messageId });
      expect(send).toHaveBeenCalledOnce();
      expect((await publish({ ...retry, messageId: `${retry.messageId}_duplicate` })).status).toBe(
        200,
      );
      expect(send).toHaveBeenCalledOnce();
    });

    it('cleans a reservation whose durable commit reply was lost', async () => {
      const { envelope, env, publish, send } = setupProducer(scope, 'lost-reservation-reply');
      const claimName = queueClaimName(envelope.queueName, envelope.idempotencyKey!);
      const cellName = scope === 'run' ? envelope.runId! : claimName;
      const cell = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(cellName)) as WorkflowRunDO;
      if (scope === 'run') {
        const reserve = cell.reserveRunQueueMessage.bind(cell);
        vi.spyOn(cell, 'reserveRunQueueMessage').mockImplementationOnce(async (params) => {
          await reserve(params);
          throw new Error('reservation commit reply was lost');
        });
      } else {
        const reserve = cell.reserveQueueMessage.bind(cell);
        vi.spyOn(cell, 'reserveQueueMessage').mockImplementationOnce(async (params) => {
          await reserve(params);
          throw new Error('reservation commit reply was lost');
        });
      }
      expect((await publish(envelope)).status).toBe(500);
      const retried = { ...envelope, messageId: `${envelope.messageId}_retried` };
      const response = await publish(retried);
      expect(response.status).toBe(200);
      expect(rpcParse(await response.text())).toEqual({ messageId: retried.messageId });
      expect(send).toHaveBeenCalledOnce();
    });

    it('confirms accepted publication during delivery before suspension extends its reservation', async () => {
      const { fleet, envelope, env, publish, send } = setupProducer(
        scope,
        'accepted-producer-death',
      );
      const claimName = queueClaimName(envelope.queueName, envelope.idempotencyKey!);
      const cellName = scope === 'run' ? envelope.runId! : claimName;
      const cell = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(cellName)) as WorkflowRunDO;
      const reservation = {
        messageId: envelope.messageId,
        expiresAt: fleet.now + QUEUE_PUBLICATION_LEASE_MS,
        publicationPending: true,
      };
      if (scope === 'run') {
        await cell.reserveRunQueueMessage({ claimName, ...reservation });
      } else {
        await cell.reserveQueueMessage(reservation);
      }
      fleet.restartCell('runs', cellName);
      const callback = vi
        .fn<typeof fetch>()
        .mockResolvedValue(Response.json({ timeoutSeconds: 3_600 }, { status: 503 }));
      vi.stubGlobal('fetch', callback);
      const accepted = { ...envelope, ...(scope === 'run' ? { claimScope: 'run' as const } : {}) };
      expect(await deliverQueueMessage(env, SECRET, accepted, 1)).toEqual({
        kind: 'suspend',
        timeoutSeconds: 3_600,
      });
      const duplicate = await publish({
        ...envelope,
        messageId: `${envelope.messageId}_duplicate`,
      });
      expect(duplicate.status).toBe(200);
      expect(rpcParse(await duplicate.text())).toEqual({ messageId: envelope.messageId });
      expect(send).not.toHaveBeenCalled();
    });

    it('returns the original fault if cleanup fails and never falsely acknowledges its pending reservation', async () => {
      const { fleet, envelope, env, publish, send } = setupProducer(scope, 'failed-cleanup');
      const claimName = queueClaimName(envelope.queueName, envelope.idempotencyKey!);
      const cellName = scope === 'run' ? envelope.runId! : claimName;
      const cell = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(cellName)) as WorkflowRunDO;
      if (scope === 'run') {
        vi.spyOn(cell, 'abandonRunQueueMessageReservation').mockRejectedValueOnce(
          new Error('cleanup unavailable'),
        );
      } else {
        vi.spyOn(cell, 'abandonQueueMessageReservation').mockRejectedValueOnce(
          new Error('cleanup unavailable'),
        );
      }
      send.mockRejectedValueOnce(new Error('broker unavailable'));
      const failed = await publish(envelope);
      expect(failed.status).toBe(500);
      expect(await failed.text()).toContain('broker unavailable');
      const retried = { ...envelope, messageId: `${envelope.messageId}_retried` };
      expect((await publish(retried)).status).toBe(503);
      expect(send).toHaveBeenCalledOnce();
      fleet.advance(QUEUE_PUBLICATION_LEASE_MS);
      vi.setSystemTime(fleet.now);
      expect((await publish(retried)).status).toBe(200);
      expect(send).toHaveBeenCalledTimes(2);
    });

    it('preserves delivery already running when confirmation of an accepted publication fails', async () => {
      const { envelope, env, publish, send } = setupProducer(scope, 'confirmation-fault');
      const claimName = queueClaimName(envelope.queueName, envelope.idempotencyKey!);
      const cellName = scope === 'run' ? envelope.runId! : claimName;
      const cell = env.WORKFLOW_DB.get(env.WORKFLOW_DB.idFromName(cellName)) as WorkflowRunDO;
      if (scope === 'run') {
        vi.spyOn(cell, 'confirmRunQueueMessagePublication').mockRejectedValueOnce(
          new Error('publication confirmation failed'),
        );
      } else {
        vi.spyOn(cell, 'confirmQueueMessagePublication').mockRejectedValueOnce(
          new Error('publication confirmation failed'),
        );
      }
      const callbackStarted = Promise.withResolvers<void>();
      const response = Promise.withResolvers<Response>();
      const callback = vi.fn<typeof fetch>().mockImplementation(() => {
        callbackStarted.resolve();
        return response.promise;
      });
      vi.stubGlobal('fetch', callback);
      let delivery!: ReturnType<typeof deliverQueueMessage>;
      send.mockImplementationOnce(async (body) => {
        delivery = deliverQueueMessage(env, SECRET, JSON.parse(body), 1);
        await callbackStarted.promise;
      });
      expect((await publish(envelope)).status).toBe(500);
      const retried = { ...envelope, messageId: `${envelope.messageId}_retried` };
      expect((await publish(retried)).status).toBe(200);
      const retryBody = JSON.parse(send.mock.calls[1][0]);
      expect(await deliverQueueMessage(env, SECRET, retryBody, 1)).toEqual({ kind: 'complete' });
      expect(callback).toHaveBeenCalledOnce();
      response.resolve(new Response(null, { status: 204 }));
      expect(await delivery).toEqual({ kind: 'complete' });
    });
  });

  it('preserves a held run claim across failed suspension publication and early broker redelivery', async () => {
    const { env, fleet, envelope, publish, send, run } = setupProducer(
      'run',
      'suspension-recovery',
    );
    expect((await publish(envelope)).status).toBe(200);
    const initial = JSON.parse(send.mock.calls[0][0]) as NativeQueueEnvelope;
    const callback = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({ timeoutSeconds: 30 }, { status: 503 }))
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal('fetch', callback);
    const consumerEnv = {
      WORKFLOW_QUEUE: env.WORKFLOW_QUEUE,
      WORLD_SECRET: SECRET,
      WORLD_SERVICE: {
        deliver: (secret: string, message: NativeQueueEnvelope, attempt: number) =>
          deliverQueueMessage(env, secret, message, attempt),
      },
    };
    send.mockRejectedValueOnce(new Error('suspension publish failed'));
    const first = brokerMessage(initial);
    await queueConsumer.queue({ messages: [first] }, consumerEnv);
    expect(first.ack).not.toHaveBeenCalled();
    expect(first.retry).toHaveBeenCalledWith({ delaySeconds: 2 });
    expect(callback).toHaveBeenCalledOnce();

    fleet.advance(2_000);
    vi.setSystemTime(fleet.now);
    const retry = brokerMessage(initial, 2);
    await queueConsumer.queue({ messages: [retry] }, consumerEnv);
    expect(retry.ack).toHaveBeenCalledOnce();
    const resumed = JSON.parse(send.mock.calls[2][0]) as NativeQueueEnvelope;
    expect(resumed).toMatchObject({
      messageId: initial.messageId,
      notBefore: 1_030_000,
      deliveryFailures: 1,
    });
    expect(callback).toHaveBeenCalledOnce();

    fleet.advance(10_000);
    vi.setSystemTime(fleet.now);
    const early = brokerMessage(resumed);
    await queueConsumer.queue({ messages: [early] }, consumerEnv);
    expect(early.ack).toHaveBeenCalledOnce();
    expect(callback).toHaveBeenCalledOnce();

    fleet.advance(18_000);
    vi.setSystemTime(fleet.now);
    const due = brokerMessage(resumed);
    await queueConsumer.queue({ messages: [due] }, consumerEnv);
    expect(due.ack).toHaveBeenCalledOnce();
    expect(due.retry).not.toHaveBeenCalled();
    expect(callback).toHaveBeenCalledTimes(2);
    expect(new Headers(callback.mock.calls[1][1]?.headers).get('x-vqs-message-attempt')).toBe('2');
    expect(
      Array.from(run.storage.data.keys()).filter((key) => key.startsWith('queue-claim:')),
    ).toEqual([]);
    expect(fleet.hasCell('runs', queueClaimName(initial.queueName, initial.idempotencyKey!))).toBe(
      false,
    );
  });
});
