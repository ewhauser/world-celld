/**
 * Queue tests, derived from world-cloudflare's queue.test.ts (Apache-2.0,
 * see NOTICE) and adapted for the celld design: the producer enqueues into
 * QueueDO cells instead of Cloudflare Queues, and the handler speaks the
 * single x-vqs dialect with permanent-error statuses.
 */
import { SPEC_VERSION_CURRENT, type ValidQueueName } from '@workflow/world';
import { WorkflowWorldError } from '@workflow/errors';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createQueue, shardFor } from '../src/queue.js';
import { parse, stringify } from '../src/vendor/shared/index.js';
import { clearMockData, createMockEnv, recordedEnqueues } from '../src/test-mocks.js';
import { MAX_QUEUE_DELAY_SECONDS } from '../src/validation.js';

const WORKFLOW_PAYLOAD = { runId: 'wrun_queue_test' };

function vqsRequest(
  message: unknown,
  headers: Record<string, string> = {
    'x-vqs-queue-name': 'workflow:test-queue',
    'x-vqs-message-id': 'msg_test',
    'x-vqs-message-attempt': '1',
  },
): Request {
  return new Request('http://localhost', {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: stringify(message),
  });
}

type QueueMessageHandler = Parameters<ReturnType<typeof createQueue>['createQueueHandler']>[1];

describe('Queue (celld QueueDO integration)', () => {
  let mockEnv: ReturnType<typeof createMockEnv>;
  let queue: ReturnType<typeof createQueue>;

  const originalVitest = process.env.VITEST;
  const originalNodeEnv = process.env.NODE_ENV;

  beforeEach(() => {
    clearMockData();
    mockEnv = createMockEnv();
  });

  afterEach(() => {
    vi.useRealTimers();
    process.env.VITEST = originalVitest;
    process.env.NODE_ENV = originalNodeEnv;
    vi.unstubAllGlobals();
    vi.clearAllMocks();
  });

  it.each([
    ['queueShards', 0],
    ['queueShards', Number.MAX_SAFE_INTEGER + 1],
    ['httpTimeoutMs', 0],
    ['httpTimeoutMs', 300_001],
    ['maxAttempts', 0],
    ['backoffDelayMs', -1],
    ['backoffDelayMs', 1.5],
    ['backoffDelayMs', 60_001],
  ] as const)('rejects invalid queue option %s=%s', (name, value) => {
    expect(() =>
      createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
        [name]: value,
      }),
    ).toThrow(name);
  });

  it.each([129, Number.MAX_SAFE_INTEGER])('accepts queueShards=%s', (queueShards) => {
    expect(() =>
      createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
        queueShards,
      }),
    ).not.toThrow();
  });

  it('requires the queue binding before constructing the queue', () => {
    expect(() =>
      createQueue({
        env: {} as { WORKFLOW_QUEUE: typeof mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
      }),
    ).toThrow(/missing WORKFLOW_QUEUE/);
  });

  describe('queue() - Production Mode', () => {
    beforeEach(() => {
      delete process.env.VITEST;
      process.env.NODE_ENV = 'production';

      queue = createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
        baseUrl: 'http://app.internal:3000',
      });
    });

    it('should enqueue into a queue cell with a tagged-JSON body', async () => {
      const queueName = '__wkf_workflow_test' as ValidQueueName;
      const message = { runId: 'wrun_test_message', stepId: 'step_test' };

      const result = await queue.queue(queueName, message);

      expect(recordedEnqueues).toHaveLength(1);
      const enq = recordedEnqueues[0];
      expect(enq.cellName).toBe('q:0');
      expect(enq.queueName).toBe(queueName);
      expect(enq.pathname).toBe('flow');
      expect(enq.messageId).toMatch(/^msg_/);
      expect(parse(enq.body)).toEqual(message);
      expect(enq.config).toEqual({
        targetBaseUrl: 'http://app.internal:3000',
        queueShards: 1,
      });
      expect(result.messageId).toBe(enq.messageId);
    });

    it('should route workflow queues to the flow pathname', async () => {
      await queue.queue('__wkf_workflow_test', WORKFLOW_PAYLOAD);
      expect(recordedEnqueues[0].pathname).toBe('flow');
    });

    it('should include the idempotency key in the enqueue request', async () => {
      const idempotencyKey = 'unique-key-123';
      await queue.queue('__wkf_workflow_test', WORKFLOW_PAYLOAD, { idempotencyKey });
      expect(recordedEnqueues[0].idempotencyKey).toBe(idempotencyKey);
    });

    it('should pass delaySeconds through to the queue cell', async () => {
      await queue.queue('__wkf_workflow_test', WORKFLOW_PAYLOAD, { delaySeconds: 42 });
      expect(recordedEnqueues[0].delaySeconds).toBe(42);
    });

    it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, MAX_QUEUE_DELAY_SECONDS + 1])(
      'rejects invalid delaySeconds %s before enqueue',
      async (delaySeconds) => {
        await expect(
          queue.queue('__wkf_workflow_test', WORKFLOW_PAYLOAD, { delaySeconds }),
        ).rejects.toThrow(/delaySeconds/);
        expect(recordedEnqueues).toHaveLength(0);
      },
    );

    it('rejects a malformed queue payload before resolving a cell', async () => {
      await expect(queue.queue('__wkf_workflow_test', {})).rejects.toMatchObject({ status: 422 });
      expect(recordedEnqueues).toHaveLength(0);
    });

    it('should generate unique monotonic message IDs', async () => {
      const first = await queue.queue('__wkf_workflow_test', WORKFLOW_PAYLOAD);
      const second = await queue.queue('__wkf_workflow_test', WORKFLOW_PAYLOAD);

      expect(first.messageId).toMatch(/^msg_/);
      expect(second.messageId).toMatch(/^msg_/);
      expect(first.messageId).not.toBe(second.messageId);
    });

    it('should return the original messageId when the cell dedups on idempotencyKey', async () => {
      const first = await queue.queue(
        '__wkf_workflow_a',
        { ...WORKFLOW_PAYLOAD, stepId: 'step_1' },
        { idempotencyKey: 'step-abc' },
      );
      const second = await queue.queue(
        '__wkf_workflow_a',
        { ...WORKFLOW_PAYLOAD, stepId: 'step_1' },
        { idempotencyKey: 'step-abc' },
      );

      expect(second.messageId).toBe(first.messageId);
      expect(recordedEnqueues).toHaveLength(1);
    });

    it('should round-trip Uint8Array payloads (binary-safe transport)', async () => {
      const input = new Uint8Array([0, 1, 2, 250, 251, 252]);
      await queue.queue('__wkf_workflow_test', {
        runId: 'wrun_1',
        runInput: {
          input,
          deploymentId: 'd',
          workflowName: 'w',
          specVersion: SPEC_VERSION_CURRENT,
        },
      });

      const body = parse<{ runInput: { input: Uint8Array } }>(recordedEnqueues[0].body);
      expect(body.runInput.input).toBeInstanceOf(Uint8Array);
      expect(Array.from(body.runInput.input)).toEqual([0, 1, 2, 250, 251, 252]);
    });

    it('should shard on idempotencyKey so equal keys land on the same cell', async () => {
      queue = createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
        baseUrl: 'http://app.internal:3000',
        queueShards: 4,
      });

      await queue.queue('__wkf_workflow_a', WORKFLOW_PAYLOAD, { idempotencyKey: 'k-1' });
      await queue.queue('__wkf_workflow_b', WORKFLOW_PAYLOAD, { idempotencyKey: 'k-1' });

      // Cell-level dedup on the same key means only the first enqueue lands.
      expect(recordedEnqueues).toHaveLength(1);
      expect(recordedEnqueues[0].cellName).toBe(`q:${shardFor('k-1', 4)}`);
    });
  });

  describe('queue() - Test Mode', () => {
    beforeEach(() => {
      process.env.VITEST = 'true';
      queue = createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
      });
    });

    it('should not enqueue into queue cells in test mode', async () => {
      await queue.queue('__wkf_workflow_q', WORKFLOW_PAYLOAD);
      expect(recordedEnqueues).toHaveLength(0);
    });

    it('should detect test mode from NODE_ENV', async () => {
      delete process.env.VITEST;
      process.env.NODE_ENV = 'test';

      queue = createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
      });

      await queue.queue('__wkf_workflow_q', WORKFLOW_PAYLOAD);
      expect(recordedEnqueues).toHaveLength(0);
    });

    it('should dedup messages on idempotencyKey while inflight', async () => {
      const first = await queue.queue(
        '__wkf_workflow_a',
        { ...WORKFLOW_PAYLOAD, stepId: 'step_1' },
        { idempotencyKey: 'step-abc' },
      );
      const second = await queue.queue(
        '__wkf_workflow_a',
        { ...WORKFLOW_PAYLOAD, stepId: 'step_1' },
        { idempotencyKey: 'step-abc' },
      );

      expect(second.messageId).toBe(first.messageId);

      const third = await queue.queue(
        '__wkf_workflow_a',
        { ...WORKFLOW_PAYLOAD, stepId: 'step_2' },
        { idempotencyKey: 'step-other' },
      );
      expect(third.messageId).not.toBe(first.messageId);
    });

    it('should cancel an unused successful callback response body', async () => {
      const cancel = vi.fn<(reason?: unknown) => void>();
      const fetchStub = vi.fn<typeof fetch>().mockResolvedValue(
        new Response(
          new ReadableStream({
            cancel,
          }),
          { status: 200 },
        ),
      );
      vi.stubGlobal('fetch', fetchStub);

      await queue.start();
      await queue.queue('__wkf_workflow_q', WORKFLOW_PAYLOAD);

      await vi.waitFor(() => expect(cancel).toHaveBeenCalledOnce());
    });

    it('chunks waits beyond the host timer ceiling without early delivery', async () => {
      vi.useFakeTimers({ now: Date.now() });
      const fetchStub = vi
        .fn<typeof fetch>()
        .mockResolvedValue(new Response(null, { status: 204 }));
      vi.stubGlobal('fetch', fetchStub);
      const delaySeconds = Math.floor(0x7fffffff / 1000) + 2;

      await queue.start();
      await queue.queue('__wkf_workflow_q', WORKFLOW_PAYLOAD, { delaySeconds });
      await vi.advanceTimersByTimeAsync(0x7fffffff);
      expect(fetchStub).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(delaySeconds * 1000 - 0x7fffffff - 1);
      expect(fetchStub).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      await vi.waitFor(() => expect(fetchStub).toHaveBeenCalledOnce());
    });

    it('delivers at the exact queue timestamp boundary and rejects one second beyond it', async () => {
      const startTime = 9_999_999_998_999;
      vi.useFakeTimers({ now: startTime });
      const fetchStub = vi
        .fn<typeof fetch>()
        .mockResolvedValue(new Response(null, { status: 204 }));
      vi.stubGlobal('fetch', fetchStub);

      await queue.start();
      await queue.queue('__wkf_workflow_q', WORKFLOW_PAYLOAD, { delaySeconds: 1 });
      await expect(
        queue.queue('__wkf_workflow_q', WORKFLOW_PAYLOAD, { delaySeconds: 2 }),
      ).rejects.toThrow(/delaySeconds/);
      await vi.advanceTimersByTimeAsync(999);
      expect(fetchStub).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(1);
      await vi.waitFor(() => expect(fetchStub).toHaveBeenCalledOnce());
    });
  });

  describe('createQueueHandler() (single x-vqs dialect)', () => {
    beforeEach(() => {
      queue = createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
      });
    });

    it('should invoke handler with the parsed message and metadata', async () => {
      const handler = vi.fn<QueueMessageHandler>().mockResolvedValue(undefined);
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(
        vqsRequest({ runId: 'wrun_handler', stepId: 'step_handler' }),
      );

      expect(response.status).toBe(204);
      expect(response.body).toBeNull();
      await expect(response.text()).resolves.toBe('');
      expect(handler).toHaveBeenCalledOnce();
      expect(handler).toHaveBeenCalledWith(
        { runId: 'wrun_handler', stepId: 'step_handler' },
        expect.objectContaining({
          queueName: 'workflow:test-queue',
          attempt: 1,
          messageId: 'msg_test',
        }),
      );
    });

    it('should revive Uint8Array payloads before invoking the handler', async () => {
      const handler = vi.fn<QueueMessageHandler>().mockResolvedValue(undefined);
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const input = new Uint8Array([9, 8, 7]);
      const response = await queueHandler(
        vqsRequest({
          runId: 'wrun_binary',
          runInput: {
            input,
            deploymentId: 'deployment',
            workflowName: 'workflow',
            specVersion: SPEC_VERSION_CURRENT,
          },
        }),
      );

      expect(response.status).toBe(204);
      const [message] = handler.mock.calls[0];
      expect(message.runInput.input).toBeInstanceOf(Uint8Array);
      expect(Array.from(message.runInput.input)).toEqual([9, 8, 7]);
    });

    it('should pass the attempt from the x-vqs-message-attempt header', async () => {
      const handler = vi.fn<QueueMessageHandler>().mockResolvedValue(undefined);
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      await queueHandler(
        vqsRequest(WORKFLOW_PAYLOAD, {
          'x-vqs-queue-name': 'workflow:test-queue',
          'x-vqs-message-id': 'msg_test',
          'x-vqs-message-attempt': '3',
        }),
      );

      expect(handler).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({ attempt: 3 }),
      );
    });

    it.each(['0', '-1', '1.5', '5junk', 'Infinity', '9007199254740992'])(
      'rejects malformed attempt header %j without invoking the handler',
      async (attempt) => {
        const handler = vi.fn<QueueMessageHandler>();
        const queueHandler = queue.createQueueHandler('workflow:', handler);
        const response = await queueHandler(
          vqsRequest(WORKFLOW_PAYLOAD, {
            'x-vqs-queue-name': 'workflow:test-queue',
            'x-vqs-message-id': 'msg_test',
            'x-vqs-message-attempt': attempt,
          }),
        );
        expect(response.status).toBe(400);
        expect(handler).not.toHaveBeenCalled();
      },
    );

    it.each([
      '{',
      'null',
      '[]',
      '{}',
      '{"data":"not-a-queue-payload"}',
      '{"data":{"__type":"Uint8Array"}}',
      '{"data":{"__type":"Uint8Array","data":"not base64!"}}',
      '{"data":{"__uint8array":true,"data":[0,256]}}',
    ])('rejects malformed tagged JSON without invoking the handler', async (body) => {
      const handler = vi.fn<QueueMessageHandler>();
      const queueHandler = queue.createQueueHandler('workflow:', handler);
      const response = await queueHandler(
        new Request('http://localhost', {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-vqs-queue-name': 'workflow:test-queue',
            'x-vqs-message-id': 'msg_test',
            'x-vqs-message-attempt': '1',
          },
          body,
        }),
      );
      expect(response.status).toBe(422);
      expect(handler).not.toHaveBeenCalled();
    });

    it('should signal redelivery when the handler returns timeoutSeconds', async () => {
      const handler = vi.fn<QueueMessageHandler>().mockResolvedValue({ timeoutSeconds: 30 });
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(vqsRequest(WORKFLOW_PAYLOAD));

      expect(response.status).toBe(503);
      expect(response.headers.get('Retry-After')).toBe('30');
      const body = await response.json();
      expect(body.timeoutSeconds).toBe(30);
    });

    it.each([-1, 0, 1.5, Number.NaN, Number.POSITIVE_INFINITY, MAX_QUEUE_DELAY_SECONDS + 1])(
      'rejects invalid handler timeoutSeconds %s as a transient failure',
      async (timeoutSeconds) => {
        const handler = vi.fn<QueueMessageHandler>().mockResolvedValue({ timeoutSeconds });
        const queueHandler = queue.createQueueHandler('workflow:', handler);
        const response = await queueHandler(vqsRequest(WORKFLOW_PAYLOAD));
        expect(response.status).toBe(500);
        expect(response.headers.get('Retry-After')).toBe('2');
      },
    );

    it('rejects the wrong method and content type before invoking the handler', async () => {
      const handler = vi.fn<QueueMessageHandler>();
      const queueHandler = queue.createQueueHandler('workflow:', handler);
      const getResponse = await queueHandler(
        new Request('http://localhost', {
          method: 'GET',
          headers: { 'content-type': 'application/json' },
        }),
      );
      expect(getResponse.status).toBe(405);
      const contentTypeResponse = await queueHandler(
        new Request('http://localhost', {
          method: 'POST',
          headers: { 'content-type': 'text/plain' },
          body: '{}',
        }),
      );
      expect(contentTypeResponse.status).toBe(415);
      expect(handler).not.toHaveBeenCalled();
    });

    it('should surface permanent errors with their own status', async () => {
      const handler = vi
        .fn<QueueMessageHandler>()
        .mockRejectedValue(new WorkflowWorldError('run already terminal', { status: 410 }));
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(vqsRequest(WORKFLOW_PAYLOAD));

      expect(response.status).toBe(410);
      const body = await response.json();
      expect(body.permanent).toBe(true);
    });

    it('should return 500 with Retry-After on transient errors', async () => {
      const handler = vi.fn<QueueMessageHandler>().mockRejectedValue(new Error('Handler error'));
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(vqsRequest(WORKFLOW_PAYLOAD));

      expect(response.status).toBe(500);
      expect(response.headers.get('Retry-After')).toBeDefined();
      const errorBody = await response.json();
      expect(errorBody.error).toContain('Handler error');
    });

    it('should reject messages with invalid queue name prefix', async () => {
      const handler = vi.fn<QueueMessageHandler>();
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(
        vqsRequest(WORKFLOW_PAYLOAD, {
          'x-vqs-queue-name': 'invalid:test-queue',
          'x-vqs-message-id': 'msg_test',
          'x-vqs-message-attempt': '1',
        }),
      );

      expect(response.status).toBe(400);
      expect(handler).not.toHaveBeenCalled();
    });

    it('should reject requests missing the x-vqs headers', async () => {
      const handler = vi.fn<QueueMessageHandler>();
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(
        new Request('http://localhost', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: stringify(WORKFLOW_PAYLOAD),
        }),
      );

      expect(response.status).toBe(400);
      expect(handler).not.toHaveBeenCalled();
    });
  });

  describe('getDeploymentId()', () => {
    it('should return configured deployment ID', async () => {
      queue = createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'custom-deployment-123',
      });
      await expect(queue.getDeploymentId()).resolves.toBe('custom-deployment-123');
    });
  });

  describe('start()', () => {
    it('should exist and be callable repeatedly', async () => {
      queue = createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
      });
      await expect(queue.start()).resolves.toBeUndefined();
      await queue.start();
    });
  });

  describe('shardFor()', () => {
    it('is stable and within range', () => {
      for (const key of ['a', 'b', 'step-123', 'msg_x']) {
        const shard = shardFor(key, 8);
        expect(shard).toBe(shardFor(key, 8));
        expect(shard).toBeGreaterThanOrEqual(0);
        expect(shard).toBeLessThan(8);
      }
      expect(shardFor('anything', 1)).toBe(0);
    });

    it.each([0, 1.5, Number.MAX_SAFE_INTEGER + 1])(
      'rejects an invalid shard count %s',
      (shards) => {
        expect(() => shardFor('anything', shards)).toThrow(/queueShards/);
      },
    );

    it.each([129, Number.MAX_SAFE_INTEGER])('accepts a positive safe shard count %s', (shards) => {
      expect(shardFor('anything', shards)).toBeGreaterThanOrEqual(0);
      expect(shardFor('anything', shards)).toBeLessThan(shards);
    });
  });
});
