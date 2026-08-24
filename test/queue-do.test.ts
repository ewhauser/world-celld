/**
 * QueueDO state-machine tests on the fake-cell harness: real class, Map
 * storage, virtual clock, manual alarm dispatch.
 */
import { beforeEach, describe, expect, it, vi, type Mock } from 'vitest';
import type { EnqueueRequest } from '../src/queue.js';
import { stringify } from '../src/vendor/shared/index.js';
import { QueueDO, type MessageRow } from '../src/worker/durable-objects/QueueDO.js';
import { FakeFleet } from '../src/testing/fake-cell.js';
import {
  LIFECYCLE_COMPACTION_RETRY_MS,
  MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
  MAX_QUEUE_SCHEDULE_TIMESTAMP_MS,
  QUEUE_FENCE_GRACE_MS,
} from '../src/lifecycle.js';
import type { RunLifecycleStatus } from '../src/retention.js';
import {
  checkedQueueTimestampAdd,
  MAX_QUEUE_DELAY_SECONDS,
  MAX_QUEUE_TIMESTAMP_MS,
} from '../src/validation.js';

class TestRunLifecycleDO {
  constructor(private ctx: { storage: DurableObjectStorage }) {}

  async getLifecycleStatus(): Promise<RunLifecycleStatus> {
    return (await this.ctx.storage.get<RunLifecycleStatus>('status')) ?? 'active';
  }
}

type FetchStub = Mock<typeof fetch>;

function jsonResponse(status: number, body?: unknown, headers?: Record<string, string>) {
  return new Response(body === undefined ? null : JSON.stringify(body), { status, headers });
}

const enqueueReq = (over: Partial<EnqueueRequest> = {}): EnqueueRequest => {
  const payload = over.runId
    ? { runId: over.runId }
    : { __healthCheck: true as const, correlationId: 'queue-do-test' };
  return {
    messageId: over.messageId ?? `msg_${Math.random().toString(36).slice(2)}`,
    queueName: '__wkf_workflow_test',
    pathname: 'flow',
    body: JSON.stringify(payload),
    config: { targetBaseUrl: 'http://app.test:3000', queueShards: 1 },
    ...over,
  };
};

const MIN_TEST_ALARM_DELAY_MS = 1;
const INFLIGHT_DEADLINE_PREFIX = 'inflight-deadline:';

function padded(ms: number): string {
  return String(Math.max(0, Math.floor(ms))).padStart(13, '0');
}

function storedMessage(messageId: string, now: number): MessageRow {
  return {
    messageId,
    queueName: '__wkf_workflow_test',
    pathname: 'flow',
    body: '{"__healthCheck":true,"correlationId":"queue-do-test"}',
    targetBaseUrl: 'http://app.test:3000',
    attempt: 0,
    enqueuedAt: now,
  };
}

function expectQueueTimestampContract(data: Map<string, unknown>, alarmAt: number | null): void {
  const timestamped = /^(?:due|inflight-deadline|dlq|expired-run-gc):(\d+):/;
  const timestamps = Array.from(data.keys()).flatMap((key) => {
    const match = timestamped.exec(key);
    return match ? [match[1]] : [];
  });
  expect(timestamps.every((timestamp) => /^\d{13}$/.test(timestamp))).toBe(true);
  expect(alarmAt === null || alarmAt <= MAX_QUEUE_TIMESTAMP_MS).toBe(true);
}

describe('QueueDO', () => {
  let fleet: FakeFleet;
  let queue: QueueDO;
  let fetchStub: FetchStub;
  let storage: () => Map<string, unknown>;

  beforeEach(() => {
    fetchStub = vi.fn<typeof fetch>(async () => jsonResponse(200, { ok: true }));
    const cellEnv: Record<string, unknown> = {
      clock: () => fleet.now,
      fetch: fetchStub,
    };
    fleet = new FakeFleet({ queue: QueueDO, runs: TestRunLifecycleDO as never }, cellEnv);
    cellEnv.WORKFLOW_DB = fleet.namespace('runs');
    queue = fleet.namespace('queue').get({ toString: () => 'q:0' }) as QueueDO;
    storage = () => fleet.cell('queue', 'q:0').storage.data;
  });

  async function tick(advanceMs = 0) {
    fleet.advance(Math.max(MIN_TEST_ALARM_DELAY_MS, advanceMs));
    await fleet.fireDueAlarms();
    await fleet.settle();
  }

  function setRunStatus(runId: string, status: RunLifecycleStatus) {
    fleet.cell('runs', runId).storage.data.set('status', status);
  }

  async function expireAndAcknowledge(runId: string, expiredAt = fleet.now) {
    const result = await queue.expireRun(runId, expiredAt);
    if (!result.done) throw new Error(`expected final expiry receipt for ${runId}`);
    expect(await queue.acknowledgeExpireRun(runId, result.receipt)).toEqual({
      acknowledged: true,
    });
    return result;
  }

  it('delivers an enqueued message with x-vqs headers and acks on 2xx', async () => {
    const req = enqueueReq({ messageId: 'msg_1', idempotencyKey: 'step-1' });
    const outcome = await queue.enqueue(req);
    expect(outcome).toEqual({ ok: true, messageId: 'msg_1', deduped: false });

    await tick();

    expect(fetchStub).toHaveBeenCalledOnce();
    const [url, init] = fetchStub.mock.calls[0];
    expect(url).toBe('http://app.test:3000/.well-known/workflow/v1/flow');
    expect(init.headers['x-vqs-queue-name']).toBe('__wkf_workflow_test');
    expect(init.headers['x-vqs-message-id']).toBe('msg_1');
    expect(init.headers['x-vqs-message-attempt']).toBe('1');
    expect(init.body).toBe('{"__healthCheck":true,"correlationId":"queue-do-test"}');

    // Fully settled: no message, schedule, claim, or alarm left behind.
    const keys = Array.from(storage().keys());
    expect(keys.filter((k) => !k.startsWith('cfg'))).toEqual([]);
    expect(await queue.stats()).toMatchObject({ pending: 0, inflight: 0, deadLetters: 0 });
  });

  it('rejects an inflight configuration above the bounded transaction limit', async () => {
    const invalidFleet = new FakeFleet(
      { queue: QueueDO },
      {
        QUEUE_MAX_INFLIGHT: '129',
        clock: () => invalidFleet.now,
        fetch: fetchStub,
      },
    );
    const invalidQueue = invalidFleet.namespace('queue').get({ toString: () => 'q:0' }) as QueueDO;

    await expect(invalidQueue.enqueue(enqueueReq())).rejects.toThrow(
      'QUEUE_MAX_INFLIGHT must be at most 128',
    );
  });

  it.each([
    ['QUEUE_MAX_ATTEMPTS', '5junk'],
    ['QUEUE_MAX_ATTEMPTS', '0'],
    ['QUEUE_MAX_ATTEMPTS', '1.5'],
    ['QUEUE_MAX_ATTEMPTS', ''],
    ['QUEUE_MAX_INFLIGHT', '5junk'],
    ['QUEUE_MAX_INFLIGHT', '0'],
    ['QUEUE_DELIVERY_TIMEOUT_MS', '5junk'],
    ['QUEUE_DELIVERY_TIMEOUT_MS', '0'],
    ['QUEUE_DELIVERY_TIMEOUT_MS', '300001'],
  ] as const)(
    'rejects malformed runtime setting %s=%j before queue mutation',
    async (name, value) => {
      const invalidFleet = new FakeFleet(
        { queue: QueueDO },
        { [name]: value, clock: () => invalidFleet.now, fetch: fetchStub },
      );
      const invalidQueue = invalidFleet
        .namespace('queue')
        .get({ toString: () => 'q:0' }) as QueueDO;

      await expect(invalidQueue.enqueue(enqueueReq())).rejects.toThrow(name);
      const invalidStorage = invalidFleet.cell('queue', 'q:0').storage;
      expect(invalidStorage.data.size).toBe(0);
      expect(invalidStorage.alarmAt).toBeNull();
      expect(fetchStub).not.toHaveBeenCalled();
    },
  );

  it.each([
    { config: { targetBaseUrl: 'http://app.test:3000', queueShards: 0 } },
    { config: { targetBaseUrl: 'http://app.test:3000', queueShards: 1.5 } },
    {
      config: {
        targetBaseUrl: 'http://app.test:3000',
        queueShards: Number.MAX_SAFE_INTEGER + 1,
      },
    },
    { delaySeconds: -1 },
    { delaySeconds: MAX_QUEUE_DELAY_SECONDS },
    { delaySeconds: Number.MAX_SAFE_INTEGER },
    { body: 'null' },
    { body: '{}' },
    { body: '{"data":"not-a-queue-payload"}' },
    { body: '{"payload":{"__type":"Uint8Array"}}' },
  ])('rejects malformed enqueue envelope before queue mutation: %j', async (over) => {
    await expect(queue.enqueue(enqueueReq(over as Partial<EnqueueRequest>))).rejects.toThrow(
      /world-celld queue enqueue/,
    );
    expect(storage().size).toBe(0);
    expect(fleet.cell('queue', 'q:0').storage.alarmAt).toBeNull();
    expect(fetchStub).not.toHaveBeenCalled();
  });

  it('classifies an invalid payload as permanent 422 without storage or delivery', async () => {
    await expect(queue.enqueue(enqueueReq({ body: '{}' }))).rejects.toMatchObject({ status: 422 });
    expect(storage().size).toBe(0);
    expect(fleet.cell('queue', 'q:0').storage.alarmAt).toBeNull();
    expect(fetchStub).not.toHaveBeenCalled();
  });

  it('accepts and stores a valid tagged-JSON queue payload byte-identically', async () => {
    const runId = 'wrun_tagged_queue_payload';
    setRunStatus(runId, 'active');
    const body = stringify({
      runId,
      runInput: {
        input: new Uint8Array([0, 1, 254, 255]),
        deploymentId: 'queue-do-tests',
        workflowName: 'tagged-payload',
        specVersion: 6,
      },
    });

    await expect(
      queue.enqueue(enqueueReq({ messageId: 'msg_tagged_payload', runId, body, delaySeconds: 60 })),
    ).resolves.toMatchObject({ ok: true });
    expect(storage().get('msg:msg_tagged_payload')).toMatchObject({ body });
    expect(fetchStub).not.toHaveBeenCalled();
  });

  it.each([129, Number.MAX_SAFE_INTEGER])(
    'accepts a positive safe queue shard count %s',
    async (queueShards) => {
      const isolatedFleet = new FakeFleet(
        { queue: QueueDO, runs: TestRunLifecycleDO as never },
        { clock: () => isolatedFleet.now, fetch: fetchStub },
      );
      const isolatedQueue = isolatedFleet.namespace('queue').get({
        toString: () => 'q:0',
      }) as QueueDO;
      await expect(
        isolatedQueue.enqueue(
          enqueueReq({
            messageId: `msg_shards_${queueShards}`,
            config: { targetBaseUrl: 'http://app.test:3000', queueShards },
          }),
        ),
      ).resolves.toMatchObject({ ok: true });
    },
  );

  it('rejects a run-owned enqueue when WORKFLOW_DB is missing without persisting it', async () => {
    const missingBindingFleet = new FakeFleet(
      { queue: QueueDO },
      { clock: () => missingBindingFleet.now, fetch: fetchStub },
    );
    const missingBindingQueue = missingBindingFleet.namespace('queue').get({
      toString: () => 'q:0',
    }) as QueueDO;

    await expect(
      missingBindingQueue.enqueue(enqueueReq({ runId: 'wrun_missing_binding' })),
    ).rejects.toThrow(/missing WORKFLOW_DB/);
    expect(missingBindingFleet.cell('queue', 'q:0').storage.data.size).toBe(0);
    expect(fetchStub).not.toHaveBeenCalled();
  });

  it('dedups on idempotencyKey while active and releases after ack', async () => {
    await queue.enqueue(enqueueReq({ messageId: 'msg_a', idempotencyKey: 'k1' }));
    const dup = await queue.enqueue(enqueueReq({ messageId: 'msg_b', idempotencyKey: 'k1' }));
    expect(dup).toEqual({ ok: true, messageId: 'msg_a', deduped: true });

    await tick();
    expect(fetchStub).toHaveBeenCalledTimes(1);

    // Key released after ack: a new message with the same key delivers.
    const fresh = await queue.enqueue(enqueueReq({ messageId: 'msg_c', idempotencyKey: 'k1' }));
    expect(fresh.deduped).toBe(false);
    await tick();
    expect(fetchStub).toHaveBeenCalledTimes(2);
  });

  it('orders inflight claims by deadline and keeps same-deadline message keys distinct', async () => {
    const releases: Array<(response: Response) => void> = [];
    fetchStub.mockImplementation(() => new Promise<Response>((resolve) => releases.push(resolve)));
    await queue.enqueue(enqueueReq({ messageId: 'msg_same_a' }));
    await queue.enqueue(enqueueReq({ messageId: 'msg_same_b' }));

    const delivery = tick();
    await vi.waitFor(() => expect(fetchStub).toHaveBeenCalledTimes(2));

    const claimKeys = Array.from(storage().keys()).filter((key) =>
      key.startsWith(INFLIGHT_DEADLINE_PREFIX),
    );
    expect(claimKeys).toHaveLength(2);
    expect(claimKeys.map((key) => key.slice(INFLIGHT_DEADLINE_PREFIX.length, -11))).toEqual([
      padded(fleet.now + 330_000),
      padded(fleet.now + 330_000),
    ]);
    expect(claimKeys).toEqual([
      `${INFLIGHT_DEADLINE_PREFIX}${padded(fleet.now + 330_000)}:msg_same_a`,
      `${INFLIGHT_DEADLINE_PREFIX}${padded(fleet.now + 330_000)}:msg_same_b`,
    ]);
    expect(fleet.cell('queue', 'q:0').storage.alarmAt).toBe(fleet.now + 330_000);

    for (const release of releases) release(new Response(null, { status: 204 }));
    await delivery;
  });

  it('reschedules the alarm to the next due message after ack', async () => {
    let finishDelivery!: (response: Response) => void;
    fetchStub.mockImplementation(
      () => new Promise<Response>((resolve) => (finishDelivery = resolve)),
    );
    await queue.enqueue(enqueueReq({ messageId: 'msg_ack_first' }));

    const delivery = tick();
    await vi.waitFor(() => expect(fetchStub).toHaveBeenCalledOnce());
    const nextDueAt = fleet.now + 600_000;
    await queue.enqueue(enqueueReq({ messageId: 'msg_ack_next', delaySeconds: 600 }));
    expect(fleet.cell('queue', 'q:0').storage.alarmAt).toBe(fleet.now + 330_000);

    finishDelivery(new Response(null, { status: 204 }));
    await delivery;
    expect(fleet.cell('queue', 'q:0').storage.alarmAt).toBe(nextDueAt);
  });

  it('reschedules the alarm to retry backoff after a transient callback', async () => {
    fetchStub.mockResolvedValue(new Response('retry', { status: 500 }));
    await queue.enqueue(enqueueReq({ messageId: 'msg_retry_alarm' }));

    await tick();

    expect(fleet.cell('queue', 'q:0').storage.alarmAt).toBe(fleet.now + 2_000);
    expect(
      Array.from(storage().keys()).some(
        (key) => key === `due:${padded(fleet.now + 2_000)}:msg_retry_alarm`,
      ),
    ).toBe(true);
  });

  it.each([
    ['successful', 200],
    ['permanent-error', 410],
    ['transient-error', 500],
  ])('cancels an unused %s callback response body', async (_label, status) => {
    const cancel = vi.fn<(reason?: unknown) => void>();
    fetchStub.mockResolvedValue(
      new Response(
        new ReadableStream({
          cancel,
        }),
        { status },
      ),
    );
    await queue.enqueue(enqueueReq({ messageId: `msg_cancel_${status}` }));

    await tick();

    expect(cancel).toHaveBeenCalledOnce();
  });

  it('recovers when enqueue persistence is interrupted before the due index is written', async () => {
    const request = enqueueReq({
      messageId: 'msg_atomic_enqueue',
      idempotencyKey: 'atomic-enqueue',
    });
    const cellStorage = fleet.cell('queue', 'q:0').storage;
    cellStorage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key.startsWith('due:'),
      new Error('injected crash during enqueue'),
    );

    await expect(queue.enqueue(request)).rejects.toThrow('injected crash during enqueue');

    // The caller did not receive an ack, so replaying the same enqueue must
    // repair or recreate all state needed to eventually deliver the message.
    await queue.enqueue(request);
    await tick();
    expect(fetchStub).toHaveBeenCalledOnce();
  });

  it('does not leave stale lifecycle state when acknowledgement is interrupted', async () => {
    await queue.enqueue(enqueueReq({ messageId: 'msg_atomic_ack', idempotencyKey: 'atomic-ack' }));
    const cellStorage = fleet.cell('queue', 'q:0').storage;
    cellStorage.failNextMutation(
      (mutation) =>
        mutation.operation === 'delete' &&
        mutation.key.startsWith('inflight-deadline:') &&
        mutation.key.endsWith(':msg_atomic_ack'),
      new Error('injected crash during ack'),
    );

    await tick();

    const data = storage();
    const messageIsRecoverable = data.has('msg:msg_atomic_ack');
    const acknowledgementFullyCommitted =
      !Array.from(data.keys()).some(
        (key) => key.startsWith('inflight-deadline:') && key.endsWith(':msg_atomic_ack'),
      ) && !data.has('key:atomic-ack');
    expect(messageIsRecoverable || acknowledgementFullyCommitted).toBe(true);
  });

  it('honors delaySeconds via the alarm', async () => {
    await queue.enqueue(enqueueReq({ messageId: 'msg_d', delaySeconds: 42 }));

    await tick();
    expect(fetchStub).not.toHaveBeenCalled();

    await tick(41_000);
    expect(fetchStub).not.toHaveBeenCalled();

    await tick(1_500);
    expect(fetchStub).toHaveBeenCalledOnce();
  });

  it('orders ordinary and far-future deadlines through the exact admissible boundary', async () => {
    const startTime = 998;
    const boundaryFleet = new FakeFleet(
      { queue: QueueDO },
      { clock: () => boundaryFleet.now, fetch: fetchStub },
      startTime,
    );
    const boundaryQueue = boundaryFleet.namespace('queue').get({
      toString: () => 'q:0',
    }) as QueueDO;
    const exactBoundaryDelay = (MAX_QUEUE_SCHEDULE_TIMESTAMP_MS - startTime) / 1000;

    await boundaryQueue.enqueue(
      enqueueReq({ messageId: 'msg_boundary', delaySeconds: exactBoundaryDelay }),
    );
    await boundaryQueue.enqueue(enqueueReq({ messageId: 'msg_ordinary', delaySeconds: 1 }));
    const boundaryStorage = boundaryFleet.cell('queue', 'q:0').storage;
    expect(Array.from((await boundaryStorage.list<string>({ prefix: 'due:' })).keys())).toEqual([
      `due:${padded(startTime + 1_000)}:msg_ordinary`,
      `due:${padded(MAX_QUEUE_SCHEDULE_TIMESTAMP_MS)}:msg_boundary`,
    ]);
    expect(boundaryStorage.alarmAt).toBe(startTime + 1_000);
    await expect(
      boundaryQueue.enqueue(
        enqueueReq({
          messageId: 'msg_over_boundary',
          delaySeconds: exactBoundaryDelay + 1,
        }),
      ),
    ).rejects.toThrow(/delaySeconds/);
    expect(
      checkedQueueTimestampAdd(
        MAX_QUEUE_SCHEDULE_TIMESTAMP_MS,
        MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
      ),
    ).toBe(MAX_QUEUE_TIMESTAMP_MS);
    expect(
      checkedQueueTimestampAdd(
        MAX_QUEUE_SCHEDULE_TIMESTAMP_MS + 1,
        0,
        MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS,
      ),
    ).toBeNull();
    expect(checkedQueueTimestampAdd(MAX_QUEUE_TIMESTAMP_MS, 0)).toBe(MAX_QUEUE_TIMESTAMP_MS);
    expect(checkedQueueTimestampAdd(MAX_QUEUE_TIMESTAMP_MS, 1)).toBeNull();
    const oneMillisecondOverFleet = new FakeFleet(
      { queue: QueueDO },
      { clock: () => oneMillisecondOverFleet.now, fetch: fetchStub },
      MAX_QUEUE_SCHEDULE_TIMESTAMP_MS + 1,
    );
    const oneMillisecondOverQueue = oneMillisecondOverFleet.namespace('queue').get({
      toString: () => 'q:0',
    }) as QueueDO;
    await expect(
      oneMillisecondOverQueue.enqueue(enqueueReq({ messageId: 'msg_one_ms_over' })),
    ).rejects.toThrow(/delivery headroom/);
    const oneMillisecondOverStorage = oneMillisecondOverFleet.cell('queue', 'q:0').storage;
    expect(oneMillisecondOverStorage.data.size).toBe(0);
    expect(oneMillisecondOverStorage.alarmAt).toBeNull();

    boundaryFleet.advance(1_000);
    await boundaryFleet.fireDueAlarms();
    expect(fetchStub).toHaveBeenCalledOnce();
    expect(boundaryStorage.data.has('msg:msg_boundary')).toBe(true);
    expect(boundaryStorage.alarmAt).toBe(MAX_QUEUE_SCHEDULE_TIMESTAMP_MS);

    boundaryFleet.advance(MAX_QUEUE_SCHEDULE_TIMESTAMP_MS - boundaryFleet.now);
    await boundaryFleet.fireDueAlarms();
    expect(fetchStub).toHaveBeenCalledTimes(2);
    expect(boundaryStorage.data.has('msg:msg_boundary')).toBe(false);
    expectQueueTimestampContract(boundaryStorage.data, boundaryStorage.alarmAt);
  });

  it.each([
    ['HTTP 500', jsonResponse(500, { error: 'boom' })],
    ['valid 503 hint', jsonResponse(503, { timeoutSeconds: 1 })],
    ['missing 503 hint', jsonResponse(503, {})],
    ['malformed 503 hint', jsonResponse(503, { timeoutSeconds: '5junk' })],
  ])('dead-letters %s at the retry horizon without an overflow cycle', async (_, response) => {
    const boundaryFleet = new FakeFleet(
      { queue: QueueDO },
      { clock: () => boundaryFleet.now, fetch: fetchStub },
      MAX_QUEUE_SCHEDULE_TIMESTAMP_MS,
    );
    const boundaryQueue = boundaryFleet.namespace('queue').get({
      toString: () => 'q:0',
    }) as QueueDO;
    const boundaryStorage = boundaryFleet.cell('queue', 'q:0').storage;
    let sawExactMaxClaim = false;
    fetchStub.mockImplementation(async () => {
      sawExactMaxClaim = boundaryStorage.data.has(
        `${INFLIGHT_DEADLINE_PREFIX}${padded(MAX_QUEUE_TIMESTAMP_MS)}:msg_horizon_retry`,
      );
      return response;
    });

    await boundaryQueue.enqueue(enqueueReq({ messageId: 'msg_horizon_retry' }));
    boundaryFleet.advance(1);
    await boundaryFleet.fireDueAlarms();
    await boundaryFleet.settle();

    expect(fetchStub).toHaveBeenCalledOnce();
    await expect(boundaryQueue.listDeadLetters()).resolves.toMatchObject({
      data: [
        expect.objectContaining({
          messageId: 'msg_horizon_retry',
          attempt: 1,
          lastError: expect.stringContaining('timestamp horizon'),
        }),
      ],
    });
    expect(sawExactMaxClaim).toBe(true);
    expect(boundaryStorage.alarmAt).toBeNull();
    expectQueueTimestampContract(boundaryStorage.data, boundaryStorage.alarmAt);
  });

  it('dead-letters an unrecoverable max-deadline inflight claim after restart', async () => {
    const boundaryFleet = new FakeFleet(
      { queue: QueueDO },
      { clock: () => boundaryFleet.now, fetch: fetchStub },
      MAX_QUEUE_TIMESTAMP_MS,
    );
    const messageId = 'msg_horizon_restart';
    const boundaryStorage = boundaryFleet.cell('queue', 'q:0').storage;
    boundaryStorage.data.set(`msg:${messageId}`, {
      ...storedMessage(messageId, boundaryFleet.now),
      attempt: 2,
    });
    boundaryStorage.data.set(
      `${INFLIGHT_DEADLINE_PREFIX}${padded(MAX_QUEUE_TIMESTAMP_MS)}:${messageId}`,
      messageId,
    );
    boundaryStorage.alarmAt = MAX_QUEUE_TIMESTAMP_MS;
    boundaryFleet.restartCell('queue', 'q:0');

    await boundaryFleet.fireDueAlarms();

    const boundaryQueue = boundaryFleet.namespace('queue').get({
      toString: () => 'q:0',
    }) as QueueDO;
    await expect(boundaryQueue.listDeadLetters()).resolves.toMatchObject({
      data: [
        expect.objectContaining({
          messageId,
          attempt: 2,
          lastError: expect.stringContaining('inflight recovery'),
        }),
      ],
    });
    expect(fetchStub).not.toHaveBeenCalled();
    expect(boundaryStorage.alarmAt).toBeNull();
    expectQueueTimestampContract(boundaryStorage.data, boundaryStorage.alarmAt);
  });

  it('redelivers after 503 {timeoutSeconds} without advancing the attempt', async () => {
    fetchStub
      .mockResolvedValueOnce(jsonResponse(503, { timeoutSeconds: 30 }))
      .mockResolvedValueOnce(jsonResponse(200, { ok: true }));

    await queue.enqueue(enqueueReq({ messageId: 'msg_e', idempotencyKey: 'k-e' }));
    await tick();
    expect(fetchStub).toHaveBeenCalledTimes(1);
    // Key stays claimed during the wait.
    const dup = await queue.enqueue(enqueueReq({ messageId: 'msg_f', idempotencyKey: 'k-e' }));
    expect(dup.deduped).toBe(true);

    await tick(31_000);
    expect(fetchStub).toHaveBeenCalledTimes(2);
    // Same message, attempt header unchanged (503-redeliver is not a retry).
    expect(fetchStub.mock.calls[1][1].headers['x-vqs-message-attempt']).toBe('1');
  });

  it.each([0, -1, 1.5, Number.MAX_SAFE_INTEGER])(
    'treats invalid 503 timeoutSeconds %s as a normal retry rather than ack or immediate redelivery',
    async (timeoutSeconds) => {
      fetchStub.mockResolvedValue(jsonResponse(503, { timeoutSeconds }));
      await queue.enqueue(enqueueReq({ messageId: 'msg_invalid_timeout' }));

      await tick();

      expect(fetchStub).toHaveBeenCalledOnce();
      expect(storage().get('msg:msg_invalid_timeout')).toMatchObject({ attempt: 1 });
      const dueKeys = Array.from(storage().keys()).filter((key) => key.startsWith('due:'));
      expect(dueKeys).toEqual([`due:${padded(fleet.now + 2_000)}:msg_invalid_timeout`]);
      expect(Array.from(storage().keys()).some((key) => key.startsWith('dlq:'))).toBe(false);
    },
  );

  it('consumes attempts and reaches the DLQ for malformed 503 retry hints', async () => {
    fetchStub.mockResolvedValue(jsonResponse(503, { timeoutSeconds: '5junk' }));
    await queue.enqueue(enqueueReq({ messageId: 'msg_malformed_retry_hint' }));

    for (const advance of [0, 2_100, 4_100, 8_100, 16_100]) await tick(advance);

    expect(fetchStub.mock.calls.map((call) => call[1].headers['x-vqs-message-attempt'])).toEqual([
      '1',
      '2',
      '3',
      '4',
      '5',
    ]);
    await expect(queue.listDeadLetters()).resolves.toMatchObject({
      data: [expect.objectContaining({ messageId: 'msg_malformed_retry_hint', attempt: 5 })],
    });
  });

  it('drops permanently on 404/409/410/422 without retrying', async () => {
    fetchStub.mockResolvedValue(jsonResponse(410, { error: 'gone', permanent: true }));

    await queue.enqueue(enqueueReq({ messageId: 'msg_g', idempotencyKey: 'k-g' }));
    await tick();
    expect(fetchStub).toHaveBeenCalledTimes(1);

    await tick(120_000);
    expect(fetchStub).toHaveBeenCalledTimes(1);
    expect(await queue.stats()).toMatchObject({ pending: 0, inflight: 0, deadLetters: 0 });
  });

  it('retries transient failures with capped backoff, then dead-letters', async () => {
    fetchStub.mockResolvedValue(jsonResponse(500, { error: 'boom' }));

    await queue.enqueue(enqueueReq({ messageId: 'msg_h', idempotencyKey: 'k-h' }));

    // Attempt 1 immediately; retries at +2s, +4s, +8s, +16s (cap 60) → 5 total.
    await tick();
    expect(fetchStub).toHaveBeenCalledTimes(1);
    await tick(2_100);
    expect(fetchStub).toHaveBeenCalledTimes(2);
    await tick(4_100);
    expect(fetchStub).toHaveBeenCalledTimes(3);
    await tick(8_100);
    expect(fetchStub).toHaveBeenCalledTimes(4);
    await tick(16_100);
    expect(fetchStub).toHaveBeenCalledTimes(5);

    // Fifth failure hits maxAttempts → dead letter, key released, no alarm.
    const stats = await queue.stats();
    expect(stats).toMatchObject({ pending: 0, inflight: 0, deadLetters: 1 });

    const dlq = await queue.listDeadLetters();
    expect(dlq.data).toHaveLength(1);
    expect(dlq.data[0]).toMatchObject({ messageId: 'msg_h', attempt: 5 });
    expect(dlq.data[0].lastError).toContain('500');

    // Attempt numbers advanced on the wire.
    expect(fetchStub.mock.calls.map((c) => c[1].headers['x-vqs-message-attempt'])).toEqual([
      '1',
      '2',
      '3',
      '4',
      '5',
    ]);

    // Key released → new enqueue with the same key is fresh.
    const fresh = await queue.enqueue(enqueueReq({ messageId: 'msg_i', idempotencyKey: 'k-h' }));
    expect(fresh.deduped).toBe(false);
  });

  it('treats network errors as transient', async () => {
    fetchStub
      .mockRejectedValueOnce(new Error('ECONNREFUSED'))
      .mockResolvedValueOnce(jsonResponse(200, { ok: true }));

    await queue.enqueue(enqueueReq({ messageId: 'msg_j' }));
    await tick();
    expect(fetchStub).toHaveBeenCalledTimes(1);
    await tick(2_100);
    expect(fetchStub).toHaveBeenCalledTimes(2);
    expect(await queue.stats()).toMatchObject({ pending: 0, inflight: 0, deadLetters: 0 });
  });

  it('keeps a transiently failed message recoverable if retry persistence is interrupted', async () => {
    fetchStub.mockResolvedValue(jsonResponse(500, { error: 'retry me' }));
    await queue.enqueue(
      enqueueReq({ messageId: 'msg_atomic_retry', idempotencyKey: 'atomic-retry' }),
    );

    const cellStorage = fleet.cell('queue', 'q:0').storage;
    cellStorage.failNextMutation((mutation) => {
      const attempt =
        mutation.value !== null && typeof mutation.value === 'object' && 'attempt' in mutation.value
          ? (mutation.value as { attempt?: unknown }).attempt
          : undefined;
      return (
        mutation.operation === 'put' && mutation.key === 'msg:msg_atomic_retry' && attempt === 1
      );
    }, new Error('injected crash between retry writes'));

    await tick();

    expect(storage().has('msg:msg_atomic_retry')).toBe(true);

    // A crash must leave either a due schedule or an inflight recovery marker.
    const stats = await queue.stats();
    expect(stats.pending + stats.inflight).toBe(1);
    expect(stats.alarmAt).not.toBeNull();
  });

  it('keeps a message recoverable if the dead-letter transition is interrupted', async () => {
    fetchStub.mockResolvedValue(jsonResponse(500, { error: 'dead letter me' }));
    await queue.enqueue(enqueueReq({ messageId: 'msg_atomic_dlq', idempotencyKey: 'atomic-dlq' }));
    const data = storage();
    const row = data.get('msg:msg_atomic_dlq') as MessageRow;
    data.set('msg:msg_atomic_dlq', { ...row, attempt: 4 });

    const cellStorage = fleet.cell('queue', 'q:0').storage;
    cellStorage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key.startsWith('dlq:'),
      new Error('injected crash during dead-letter transition'),
    );

    await tick();

    expect(data.has('msg:msg_atomic_dlq')).toBe(true);
    const stats = await queue.stats();
    expect(stats.pending + stats.inflight + stats.deadLetters).toBe(1);
    expect(stats.alarmAt).not.toBeNull();
  });

  it('recovers a lost inflight claim after its deadline (crash simulation)', async () => {
    // Simulate a claim whose delivery died with the node: message row +
    // expired inflight marker, no due entry.
    await queue.enqueue(enqueueReq({ messageId: 'msg_k' }));
    const data = storage();
    // Remove the due entry and plant an expired inflight claim.
    for (const key of Array.from(data.keys())) {
      if (key.startsWith('due:')) data.delete(key);
    }
    data.set(`${INFLIGHT_DEADLINE_PREFIX}${padded(fleet.now - 1)}:msg_k`, 'msg_k');
    fleet.cell('queue', 'q:0').storage.alarmAt = fleet.now;

    await tick(); // sweep moves it back to due
    await tick(); // next alarm delivers it
    expect(fetchStub).toHaveBeenCalledOnce();
  });

  it('uses the exact inflight key as a lease token against late callback completion', async () => {
    const finishes: Array<(response: Response) => void> = [];
    fetchStub.mockImplementation(() => new Promise<Response>((resolve) => finishes.push(resolve)));
    fleet = new FakeFleet(
      { queue: QueueDO },
      {
        clock: () => fleet.now,
        fetch: fetchStub,
        QUEUE_DELIVERY_TIMEOUT_MS: '10',
      },
    );
    queue = fleet.namespace('queue').get({ toString: () => 'q:0' }) as QueueDO;
    storage = () => fleet.cell('queue', 'q:0').storage.data;
    await queue.enqueue(enqueueReq({ messageId: 'msg_lease' }));

    await queue.alarm();
    await vi.waitFor(() => expect(fetchStub).toHaveBeenCalledOnce());
    const firstClaim = Array.from(storage().keys()).find((key) =>
      key.startsWith(INFLIGHT_DEADLINE_PREFIX),
    )!;

    fleet.advance(30_011);
    await queue.alarm();
    await vi.waitFor(() => expect(fetchStub).toHaveBeenCalledTimes(2));
    const pendingDeliveries = fleet.cell('queue', 'q:0').pendingWaits;
    const secondClaim = Array.from(storage().keys()).find((key) =>
      key.startsWith(INFLIGHT_DEADLINE_PREFIX),
    )!;
    expect(secondClaim).not.toBe(firstClaim);

    finishes[0](new Response(null, { status: 204 }));
    await pendingDeliveries[0];
    expect(storage().has('msg:msg_lease')).toBe(true);
    expect(storage().has(secondClaim)).toBe(true);

    finishes[1](new Response(null, { status: 204 }));
    await pendingDeliveries[1];
    expect(storage().has('msg:msg_lease')).toBe(false);
    expect(storage().has(secondClaim)).toBe(false);
  });

  it('processes expired inflight records in bounded alarm batches', async () => {
    const data = storage();
    const expiredDeadline = fleet.now - 1;
    for (let index = 0; index < 130; index++) {
      const messageId = `msg_expired_${String(index).padStart(3, '0')}`;
      data.set(`msg:${messageId}`, storedMessage(messageId, fleet.now));
      data.set(`${INFLIGHT_DEADLINE_PREFIX}${padded(expiredDeadline)}:${messageId}`, messageId);
    }
    fleet.cell('queue', 'q:0').storage.alarmAt = fleet.now;

    await tick();
    expect(fetchStub).not.toHaveBeenCalled();
    expect(Array.from(data.keys()).filter((key) => key.startsWith('due:'))).toHaveLength(128);
    expect(
      Array.from(data.keys()).filter((key) => key.startsWith(INFLIGHT_DEADLINE_PREFIX)),
    ).toHaveLength(2);
    expect(fleet.cell('queue', 'q:0').storage.alarmAt).toBe(fleet.now + 1);

    await tick();
    expect(fetchStub).toHaveBeenCalledTimes(5);
    expect(
      Array.from(data.keys()).filter(
        (key) => key.startsWith(INFLIGHT_DEADLINE_PREFIX) && key.includes(padded(expiredDeadline)),
      ),
    ).toHaveLength(0);
  });

  it('bounds inflight alarm work by expired records, concurrency, and the earliest key', async () => {
    const cellStorage = fleet.cell('queue', 'q:0').storage;
    const data = cellStorage.data;
    const expiredDeadline = fleet.now - 1;
    for (const messageId of ['msg_expired_a', 'msg_expired_b']) {
      data.set(`msg:${messageId}`, storedMessage(messageId, fleet.now));
      data.set(`${INFLIGHT_DEADLINE_PREFIX}${padded(expiredDeadline)}:${messageId}`, messageId);
    }
    for (let index = 0; index < 1_000; index++) {
      const messageId = `msg_active_${String(index).padStart(4, '0')}`;
      data.set(
        `${INFLIGHT_DEADLINE_PREFIX}${padded(fleet.now + 60_000 + index)}:${messageId}`,
        messageId,
      );
    }
    cellStorage.alarmAt = fleet.now;
    cellStorage.listCalls.length = 0;

    await tick();

    const inflightCalls = cellStorage.listCalls.filter(
      (call) => call.options.prefix === INFLIGHT_DEADLINE_PREFIX,
    );
    expect(inflightCalls.map((call) => call.options.limit)).toEqual([129, 5, 1]);
    expect(inflightCalls.map((call) => call.resultSize)).toEqual([2, 5, 1]);
    expect(inflightCalls.every((call) => call.transactional)).toBe(true);
    expect(fetchStub).not.toHaveBeenCalled();
  });

  it('rejects shard-count drift with CONFIG_MISMATCH', async () => {
    await queue.enqueue(enqueueReq({ messageId: 'msg_l' }));
    const drift = await queue.enqueue(
      enqueueReq({
        messageId: 'msg_m',
        config: { targetBaseUrl: 'http://app.test:3000', queueShards: 4 },
      }),
    );
    expect(drift).toMatchObject({ ok: false, code: 'CONFIG_MISMATCH' });
  });

  it('allows the delivery URL to change between enqueues (per-message target)', async () => {
    await queue.enqueue(enqueueReq({ messageId: 'msg_url_1' }));
    const moved = await queue.enqueue(
      enqueueReq({
        messageId: 'msg_url_2',
        config: { targetBaseUrl: 'http://moved.test:4000', queueShards: 1 },
      }),
    );
    expect(moved.ok).toBe(true);

    await tick();
    const urls = fetchStub.mock.calls.map((c) => c[0]);
    expect(urls).toContain('http://app.test:3000/.well-known/workflow/v1/flow');
    expect(urls).toContain('http://moved.test:4000/.well-known/workflow/v1/flow');
  });

  it('redrives a dead letter with attempts reset', async () => {
    fetchStub.mockResolvedValue(jsonResponse(500, { error: 'down' }));
    await queue.enqueue(enqueueReq({ messageId: 'msg_n', idempotencyKey: 'k-n' }));
    for (const ms of [0, 2_100, 4_100, 8_100, 16_100]) {
      await tick(ms);
    }
    expect((await queue.stats()).deadLetters).toBe(1);

    fetchStub.mockResolvedValue(jsonResponse(200, { ok: true }));
    const redriven = await queue.redriveDeadLetter('msg_n');
    expect(redriven.ok).toBe(true);

    await tick();
    expect(await queue.stats()).toMatchObject({ pending: 0, inflight: 0, deadLetters: 0 });
    const lastCall = fetchStub.mock.calls.at(-1)!;
    expect(lastCall[1].headers['x-vqs-message-attempt']).toBe('1');
  });

  it('does not redrive a dead letter when a newer message owns its idempotency key', async () => {
    fetchStub.mockResolvedValue(jsonResponse(500, { error: 'down' }));
    await queue.enqueue(
      enqueueReq({ messageId: 'msg_old_dead', idempotencyKey: 'shared-redrive-key' }),
    );
    for (const ms of [0, 2_100, 4_100, 8_100, 16_100]) {
      await tick(ms);
    }
    expect((await queue.stats()).deadLetters).toBe(1);

    const fresh = await queue.enqueue(
      enqueueReq({
        messageId: 'msg_new_live',
        idempotencyKey: 'shared-redrive-key',
        delaySeconds: 60,
      }),
    );
    expect(fresh.deduped).toBe(false);

    const redriven = await queue.redriveDeadLetter('msg_old_dead');
    expect(redriven.ok).toBe(false);
    expect(await queue.stats()).toMatchObject({ pending: 1, inflight: 0, deadLetters: 1 });
    expect(storage().get('key:shared-redrive-key')).toBe('msg_new_live');
    expect(storage().has('msg:msg_old_dead')).toBe(false);
  });

  it('never exposes both dead and live copies when redrive persistence is interrupted', async () => {
    fetchStub.mockResolvedValue(jsonResponse(500, { error: 'down' }));
    await queue.enqueue(
      enqueueReq({ messageId: 'msg_atomic_redrive', idempotencyKey: 'atomic-redrive' }),
    );
    for (const ms of [0, 2_100, 4_100, 8_100, 16_100]) {
      await tick(ms);
    }
    expect((await queue.stats()).deadLetters).toBe(1);

    const cellStorage = fleet.cell('queue', 'q:0').storage;
    cellStorage.failNextMutation(
      (mutation) => mutation.operation === 'delete' && mutation.key.startsWith('dlq:'),
      new Error('injected crash during redrive'),
    );

    await expect(queue.redriveDeadLetter('msg_atomic_redrive')).rejects.toThrow(
      'injected crash during redrive',
    );

    const data = storage();
    const hasDeadCopy = Array.from(data.keys()).some(
      (key) => key.startsWith('dlq:') && key.endsWith(':msg_atomic_redrive'),
    );
    const hasLiveCopy = data.has('msg:msg_atomic_redrive');
    expect(Number(hasDeadCopy) + Number(hasLiveCopy)).toBe(1);
  });

  it('purges dead letters', async () => {
    fetchStub.mockResolvedValue(jsonResponse(500, { error: 'down' }));
    await queue.enqueue(enqueueReq({ messageId: 'msg_o' }));
    for (const ms of [0, 2_100, 4_100, 8_100, 16_100]) {
      await tick(ms);
    }
    expect((await queue.purgeDeadLetters()).purged).toBe(1);
    expect((await queue.stats()).deadLetters).toBe(0);
  });

  it('rejects malformed queue admin input before storage mutation', async () => {
    const queueStorage = fleet.cell('queue', 'q:0').storage;
    queueStorage.resetOperationCounts();
    await expect(queue.listDeadLetters({ limit: 0 })).rejects.toThrow(/dead-letter limit/);
    await expect(queue.listDeadLetters({ limit: '5' as unknown as number })).rejects.toThrow(
      /dead-letter limit/,
    );
    await expect(queue.redriveDeadLetter('')).rejects.toThrow(/messageId/);
    await expect(queue.expireRun('', fleet.now)).rejects.toThrow(/runId/);
    await expect(queue.expireRun('wrun_admin', -1)).rejects.toThrow(/expiredAt/);
    await expect(queue.expireRun('wrun_admin', fleet.now, { limit: 65 })).rejects.toThrow(/limit/);
    await expect(
      queue.expireRun('wrun_admin', fleet.now, { limit: '5' as unknown as number }),
    ).rejects.toThrow(/limit/);
    await expect(
      queue.acknowledgeExpireRun('wrun_admin', { expiredAt: -1, deleted: 0 }),
    ).rejects.toThrow(/receipt/);
    expect(queueStorage.operationCounts.transaction).toBe(0);
    expect(queueStorage.data.size).toBe(0);
  });

  it('purges every message state for an expired run and fences late enqueue', async () => {
    const runId = 'wrun_queue_cleanup';
    await queue.enqueue(
      enqueueReq({
        messageId: 'msg_pending',
        runId,
        idempotencyKey: 'pending-key',
        delaySeconds: 60,
      }),
    );
    await queue.enqueue(
      enqueueReq({
        messageId: 'msg_inflight',
        runId,
        idempotencyKey: 'inflight-key',
        delaySeconds: 60,
      }),
    );
    await queue.enqueue(
      enqueueReq({
        messageId: 'msg_dead',
        runId,
        idempotencyKey: 'dead-key',
        delaySeconds: 60,
      }),
    );

    const data = storage();
    for (const key of Array.from(data.keys())) {
      if (key.startsWith('due:') && key.endsWith(':msg_inflight')) data.delete(key);
      if (key.startsWith('due:') && key.endsWith(':msg_dead')) data.delete(key);
    }
    const inflightKey = `${INFLIGHT_DEADLINE_PREFIX}${padded(fleet.now + 30_000)}:msg_inflight`;
    data.set(inflightKey, 'msg_inflight');
    data.set(`run:${runId}:msg_inflight`, {
      messageId: 'msg_inflight',
      inflightKey,
      idempotencyKey: 'inflight-key',
    });
    const dead = data.get('msg:msg_dead') as MessageRow;
    data.delete('msg:msg_dead');
    const deadKey = `dlq:${String(fleet.now).padStart(13, '0')}:msg_dead`;
    data.set(deadKey, { ...dead, failedAt: fleet.now });
    data.set(`run:${runId}:msg_dead`, {
      messageId: 'msg_dead',
      dlqKey: deadKey,
      idempotencyKey: 'dead-key',
    });

    expect(await queue.expireRun(runId, fleet.now)).toEqual({
      deleted: 3,
      done: true,
      receipt: { expiredAt: fleet.now, deleted: 3 },
    });
    expect(await queue.stats()).toMatchObject({ pending: 0, inflight: 0, deadLetters: 0 });
    expect(Array.from(data.keys()).filter((key) => key.startsWith(`run:${runId}:`))).toEqual([]);
    expect(data.has(`expired-run:${runId}`)).toBe(true);
    expect(data.has('key:pending-key')).toBe(false);
    expect(data.has('key:inflight-key')).toBe(false);
    expect(data.has('key:dead-key')).toBe(false);

    const late = await queue.enqueue(enqueueReq({ messageId: 'msg_late', runId }));
    expect(late).toMatchObject({ ok: false, code: 'RUN_EXPIRED' });
  });

  it('rejects an authoritatively expired run before queue cleanup installs a fence', async () => {
    const runId = 'wrun_expired_before_queue_cleanup';
    setRunStatus(runId, 'expired');

    await expect(queue.enqueue(enqueueReq({ messageId: 'msg_too_late', runId }))).resolves.toEqual({
      ok: false,
      code: 'RUN_EXPIRED',
      message: `Workflow run "${runId}" has expired`,
    });
    expect(storage().has(`expired-run:${runId}`)).toBe(false);
    expect(fetchStub).not.toHaveBeenCalled();
  });

  it('drops a scheduled message which becomes expired before delivery', async () => {
    const runId = 'wrun_expired_before_delivery';
    await queue.enqueue(
      enqueueReq({ messageId: 'msg_delayed_expiry', runId, idempotencyKey: 'delayed-expiry' }),
    );
    setRunStatus(runId, 'expired');

    await tick();

    expect(fetchStub).not.toHaveBeenCalled();
    expect(storage().has('msg:msg_delayed_expiry')).toBe(false);
    expect(storage().has('key:delayed-expiry')).toBe(false);
    expect(storage().has(`run:${runId}:msg_delayed_expiry`)).toBe(false);
  });

  it('pages large run expiration with batch reads/deletes and retries atomically', async () => {
    const runId = 'wrun_large_queue_cleanup';
    const cellStorage = fleet.cell('queue', 'q:0').storage;
    const data = cellStorage.data;
    for (let index = 0; index < 150; index++) {
      const messageId = `msg_large_${String(index).padStart(3, '0')}`;
      const idempotencyKey = `large-key-${index}`;
      const scheduleKey = `due:${padded(fleet.now + 60_000)}:${messageId}`;
      data.set(`msg:${messageId}`, {
        ...storedMessage(messageId, fleet.now),
        runId,
        idempotencyKey,
      });
      data.set(scheduleKey, messageId);
      data.set(`key:${idempotencyKey}`, messageId);
      data.set(`run:${runId}:${messageId}`, { messageId, dueKey: scheduleKey, idempotencyKey });
    }

    cellStorage.failNextMutation(
      (mutation) => mutation.operation === 'put' && mutation.key === `expired-run:${runId}`,
      new Error('injected queue cleanup crash'),
    );
    await expect(queue.expireRun(runId, fleet.now)).rejects.toThrow('injected queue cleanup crash');
    expect(Array.from(data.keys()).filter((key) => key.startsWith(`run:${runId}:`))).toHaveLength(
      150,
    );

    cellStorage.operationCalls.length = 0;
    cellStorage.listCalls.length = 0;
    expect(await queue.expireRun(runId, fleet.now)).toEqual({ deleted: 64, done: false });
    expect(cellStorage.listCalls[0]).toMatchObject({ options: { limit: 65 }, resultSize: 65 });
    expect(
      cellStorage.operationCalls.filter(
        (call) => call.operation === 'get' && call.keys[0]?.startsWith('key:large-key-'),
      ),
    ).toEqual([expect.objectContaining({ keys: expect.any(Array) })]);
    expect(
      cellStorage.operationCalls.find(
        (call) => call.operation === 'get' && call.keys[0]?.startsWith('key:large-key-'),
      )?.keys,
    ).toHaveLength(64);

    expect(await queue.expireRun(runId, fleet.now)).toEqual({ deleted: 128, done: false });
    expect(await queue.expireRun(runId, fleet.now)).toEqual({
      deleted: 150,
      done: true,
      receipt: { expiredAt: fleet.now, deleted: 150 },
    });
    expect(Array.from(data.keys()).filter((key) => key.startsWith(`run:${runId}:`))).toEqual([]);
  });

  it('does not let an in-flight retry resurrect an expired run', async () => {
    let finishDelivery!: (response: Response) => void;
    fetchStub.mockImplementation(
      () => new Promise<Response>((resolve) => (finishDelivery = resolve)),
    );
    const runId = 'wrun_expire_during_delivery';
    await queue.enqueue(
      enqueueReq({
        messageId: 'msg_expiring',
        runId,
        idempotencyKey: 'expiring-key',
      }),
    );

    const delivery = tick();
    await vi.waitFor(() => expect(fetchStub).toHaveBeenCalledOnce());
    expect(await queue.expireRun(runId, fleet.now)).toEqual({
      deleted: 1,
      done: true,
      receipt: { expiredAt: fleet.now, deleted: 1 },
    });
    finishDelivery(jsonResponse(500, { error: 'late failure' }));
    await delivery;

    expect(await queue.stats()).toMatchObject({ pending: 0, inflight: 0, deadLetters: 0 });
    expect(storage().has('msg:msg_expiring')).toBe(false);
    expect(storage().has('key:expiring-key')).toBe(false);
    expect(Array.from(storage().keys()).some((key) => key.endsWith(':msg_expiring'))).toBe(false);
  });

  it('falls back to the authoritative tombstone after fence compaction', async () => {
    let finishDelivery!: (response: Response) => void;
    fetchStub.mockImplementation(
      () => new Promise<Response>((resolve) => (finishDelivery = resolve)),
    );
    const runId = 'wrun_compacted_queue_fence';
    await queue.enqueue(
      enqueueReq({ messageId: 'msg_compacted_retry', runId, idempotencyKey: 'compacted-key' }),
    );
    await queue.alarm();
    await vi.waitFor(() => expect(fetchStub).toHaveBeenCalledOnce());

    await expireAndAcknowledge(runId);
    setRunStatus(runId, 'expired');
    fleet.advance(QUEUE_FENCE_GRACE_MS);
    fleet.restartCell('queue', 'q:0');
    queue = fleet.namespace('queue').get({ toString: () => 'q:0' }) as QueueDO;
    await queue.alarm();

    expect(storage().has(`expired-run:${runId}`)).toBe(false);
    expect(storage().has('expired-run-filter:v1')).toBe(false);
    await expect(
      queue.enqueue(enqueueReq({ messageId: 'msg_after_compaction', runId })),
    ).resolves.toMatchObject({ ok: false, code: 'RUN_EXPIRED' });

    const deadKey = `dlq:${padded(fleet.now)}:msg_stale_dead`;
    storage().set(deadKey, {
      ...storedMessage('msg_stale_dead', fleet.now),
      runId,
      failedAt: fleet.now,
    });
    storage().set(`run:${runId}:msg_stale_dead`, {
      messageId: 'msg_stale_dead',
      dlqKey: deadKey,
    });
    await expect(queue.redriveDeadLetter('msg_stale_dead')).resolves.toEqual({ ok: false });
    expect(storage().has(deadKey)).toBe(false);

    finishDelivery(jsonResponse(500, { error: 'late after compaction' }));
    await fleet.settle();
    expect(storage().has('msg:msg_compacted_retry')).toBe(false);
    expect(Array.from(storage().keys()).some((key) => key.endsWith(':msg_compacted_retry'))).toBe(
      false,
    );
  });

  it('retains the cumulative cleanup receipt until the coordinator acknowledges it', async () => {
    const runId = 'wrun_unacknowledged_queue_receipt';
    await queue.enqueue(enqueueReq({ messageId: 'msg_receipt', runId, delaySeconds: 60 }));
    const result = await queue.expireRun(runId, fleet.now);
    if (!result.done) throw new Error('expected final expiry receipt');

    fleet.advance(QUEUE_FENCE_GRACE_MS * 2);
    await fleet.fireDueAlarms();
    expect(storage().get(`expired-run:${runId}`)).toMatchObject({ deleted: 1, compactAt: null });
    expect(await queue.acknowledgeExpireRun(runId, { ...result.receipt, deleted: 0 })).toEqual({
      acknowledged: false,
    });

    await queue.acknowledgeExpireRun(runId, result.receipt);
    fleet.advance(QUEUE_FENCE_GRACE_MS);
    await fleet.fireDueAlarms();
    expect(storage().has(`expired-run:${runId}`)).toBe(false);
  });

  it('schedules expiry-fence GC at the exact timestamp boundary and retains it past the horizon', async () => {
    const exactFleet = new FakeFleet(
      { queue: QueueDO },
      { clock: () => exactFleet.now, fetch: fetchStub },
      MAX_QUEUE_TIMESTAMP_MS - QUEUE_FENCE_GRACE_MS,
    );
    const exactQueue = exactFleet.namespace('queue').get({ toString: () => 'q:0' }) as QueueDO;
    const exactRunId = 'wrun_exact_gc_horizon';
    const exactReceipt = await exactQueue.expireRun(exactRunId, exactFleet.now);
    if (!exactReceipt.done) throw new Error('expected exact-boundary expiry receipt');
    await expect(
      exactQueue.acknowledgeExpireRun(exactRunId, exactReceipt.receipt),
    ).resolves.toEqual({ acknowledged: true });
    const exactStorage = exactFleet.cell('queue', 'q:0').storage;
    expect(exactStorage.alarmAt).toBe(MAX_QUEUE_TIMESTAMP_MS);
    expect(
      Array.from(exactStorage.data.keys()).find((key) => key.startsWith('expired-run-gc:')),
    ).toMatch(`expired-run-gc:${padded(MAX_QUEUE_TIMESTAMP_MS)}:`);
    expectQueueTimestampContract(exactStorage.data, exactStorage.alarmAt);
    exactFleet.advance(QUEUE_FENCE_GRACE_MS);
    await exactFleet.fireDueAlarms();
    expect(exactStorage.data.has(`expired-run:${exactRunId}`)).toBe(false);
    expect(exactStorage.alarmAt).toBeNull();

    const overflowFleet = new FakeFleet(
      { queue: QueueDO },
      { clock: () => overflowFleet.now, fetch: fetchStub },
      MAX_QUEUE_TIMESTAMP_MS - QUEUE_FENCE_GRACE_MS + 1,
    );
    const overflowQueue = overflowFleet.namespace('queue').get({
      toString: () => 'q:0',
    }) as QueueDO;
    const overflowRunId = 'wrun_overflow_gc_horizon';
    const overflowReceipt = await overflowQueue.expireRun(overflowRunId, overflowFleet.now);
    if (!overflowReceipt.done) throw new Error('expected overflow-boundary expiry receipt');
    await expect(
      overflowQueue.acknowledgeExpireRun(overflowRunId, overflowReceipt.receipt),
    ).resolves.toEqual({ acknowledged: true });
    const overflowStorage = overflowFleet.cell('queue', 'q:0').storage;
    expect(overflowStorage.data.get(`expired-run:${overflowRunId}`)).toMatchObject({
      compactAt: null,
    });
    expect(
      Array.from(overflowStorage.data.keys()).filter((key) => key.startsWith('expired-run-gc:')),
    ).toEqual([]);
    expect(overflowStorage.alarmAt).toBeNull();
    expectQueueTimestampContract(overflowStorage.data, overflowStorage.alarmAt);
  });

  it('compacts many acknowledged exact fences to no derivative state in paged alarm work', async () => {
    const runs = Array.from({ length: 300 }, (_, index) => `wrun_compact_${index}`);
    for (const runId of runs) {
      await expireAndAcknowledge(runId);
    }
    expect(
      Array.from(storage().keys()).filter((key) => key.startsWith('expired-run:')),
    ).toHaveLength(300);

    fleet.advance(QUEUE_FENCE_GRACE_MS);
    for (let page = 0; page < 3; page++) {
      await fleet.fireDueAlarms();
      fleet.advance(1);
    }
    expect(Array.from(storage().keys()).filter((key) => key.startsWith('expired-run:'))).toEqual(
      [],
    );
    expect(Array.from(storage().keys()).filter((key) => key.startsWith('expired-run-gc:'))).toEqual(
      [],
    );
    expect(storage().has('expired-run-filter:v1')).toBe(false);
  });

  it('re-arms compaction after an injected alarm failure', async () => {
    const runId = 'wrun_compaction_alarm_retry';
    await expireAndAcknowledge(runId);
    fleet.advance(QUEUE_FENCE_GRACE_MS);
    const cellStorage = fleet.cell('queue', 'q:0').storage;
    cellStorage.failNextMutation(
      (mutation) => mutation.operation === 'delete' && mutation.key === `expired-run:${runId}`,
      new Error('injected compaction failure'),
    );

    await fleet.fireDueAlarms();
    expect(storage().has(`expired-run:${runId}`)).toBe(true);
    expect(cellStorage.alarmAt).toBe(fleet.now + LIFECYCLE_COMPACTION_RETRY_MS);
    fleet.advance(LIFECYCLE_COMPACTION_RETRY_MS);
    await fleet.fireDueAlarms();
    expect(storage().has(`expired-run:${runId}`)).toBe(false);
  });

  it('keeps delayed cleanup replay and concurrent compaction fail closed', async () => {
    const runId = 'wrun_cleanup_replay_after_compaction';
    setRunStatus(runId, 'expired');
    await expireAndAcknowledge(runId);
    fleet.advance(QUEUE_FENCE_GRACE_MS);

    const [, replay] = await Promise.all([queue.alarm(), queue.expireRun(runId, fleet.now)]);
    if (!replay.done) throw new Error('expected replay receipt');
    await queue.acknowledgeExpireRun(runId, replay.receipt);
    await expect(
      queue.enqueue(enqueueReq({ messageId: 'msg_after_cleanup_replay', runId })),
    ).resolves.toMatchObject({ ok: false, code: 'RUN_EXPIRED' });

    fleet.advance(QUEUE_FENCE_GRACE_MS);
    await queue.alarm();
    expect(
      Array.from(storage().keys()).filter(
        (key) => key === `expired-run:${runId}` || key.startsWith('expired-run-gc:'),
      ),
    ).toEqual([]);
  });

  it('uses authority to admit active runs without a derivative fence', async () => {
    const runId = 'wrun_authority_active';
    setRunStatus(runId, 'active');

    await expect(
      queue.enqueue(enqueueReq({ messageId: 'msg_authority_active', runId, delaySeconds: 60 })),
    ).resolves.toMatchObject({ ok: true });
  });

  it('rearmAlarm derives the alarm from pending work (abandonment recovery)', async () => {
    await queue.enqueue(enqueueReq({ messageId: 'msg_p', delaySeconds: 60 }));
    // Simulate celld abandoning the alarm.
    fleet.cell('queue', 'q:0').storage.alarmAt = null;

    const { alarmAt } = await queue.rearmAlarm();
    expect(alarmAt).toBe(fleet.now + 60_000);
  });

  it('rearmAlarm selects the earliest lexicographic inflight deadline', async () => {
    const data = storage();
    const early = fleet.now + 10_000;
    const late = fleet.now + 90_000;
    data.set(`${INFLIGHT_DEADLINE_PREFIX}${padded(late)}:msg_late`, 'msg_late');
    data.set(`${INFLIGHT_DEADLINE_PREFIX}${padded(early)}:msg_early`, 'msg_early');

    expect(await queue.rearmAlarm()).toEqual({ alarmAt: early });
  });

  it('moves an overdue alarm to a fresh timestamp so celld observes a new edge', async () => {
    await queue.enqueue(enqueueReq({ messageId: 'msg_stale_alarm' }));
    fleet.cell('queue', 'q:0').storage.alarmAt = fleet.now - 1_000;

    const { alarmAt } = await queue.rearmAlarm();
    expect(alarmAt).toBe(fleet.now + MIN_TEST_ALARM_DELAY_MS);

    await tick();
    expect(fetchStub).toHaveBeenCalledOnce();
    expect(await queue.stats()).toMatchObject({ pending: 0, inflight: 0, alarmAt: null });
  });

  it('caps concurrent deliveries at the inflight limit', async () => {
    const pending: Array<() => void> = [];
    let peakConcurrency = 0;
    let active = 0;
    fetchStub.mockImplementation(
      () =>
        new Promise((resolve) => {
          active++;
          peakConcurrency = Math.max(peakConcurrency, active);
          pending.push(() => {
            active--;
            resolve(jsonResponse(200, { ok: true }));
          });
        }),
    );
    // Unblock hanging deliveries from outside the alarm cycle so the claim
    // phase's inflight cap is what limits concurrency.
    const unblock = setInterval(() => {
      for (const release of pending.splice(0)) release();
    }, 5);

    try {
      for (let i = 0; i < 8; i++) {
        await queue.enqueue(enqueueReq({ messageId: `msg_cap_${i}` }));
      }
      await tick(1);
      expect(peakConcurrency).toBe(5); // DEFAULT_MAX_INFLIGHT
      expect(fetchStub).toHaveBeenCalledTimes(5);
      expect((await queue.stats()).pending).toBe(3);

      await tick(1); // next alarm claims the remainder
      expect(fetchStub).toHaveBeenCalledTimes(8);
    } finally {
      clearInterval(unblock);
    }
  });
});
