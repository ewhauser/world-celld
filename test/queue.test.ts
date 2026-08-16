/**
 * Queue tests, derived from world-cloudflare's queue.test.ts (Apache-2.0,
 * see NOTICE) and adapted for the celld design: the producer enqueues into
 * QueueDO cells instead of Cloudflare Queues, and the handler speaks the
 * single x-vqs dialect with permanent-error statuses.
 */
import type { ValidQueueName } from '@workflow/world';
import { WorkflowWorldError } from '@workflow/errors';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createQueue, shardFor } from '../src/queue.js';
import { parse, stringify } from '../src/vendor/shared/index.js';
import { clearMockData, createMockEnv, recordedEnqueues } from '../src/test-mocks.js';

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
    headers,
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
    process.env.VITEST = originalVitest;
    process.env.NODE_ENV = originalNodeEnv;
    vi.clearAllMocks();
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
      const message = { data: 'test-message' };

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
      await queue.queue('__wkf_workflow_test', {});
      expect(recordedEnqueues[0].pathname).toBe('flow');
    });

    it('should include the idempotency key in the enqueue request', async () => {
      const idempotencyKey = 'unique-key-123';
      await queue.queue('__wkf_workflow_test', { data: 'test' }, { idempotencyKey });
      expect(recordedEnqueues[0].idempotencyKey).toBe(idempotencyKey);
    });

    it('should pass delaySeconds through to the queue cell', async () => {
      await queue.queue('__wkf_workflow_test', {}, { delaySeconds: 42 });
      expect(recordedEnqueues[0].delaySeconds).toBe(42);
    });

    it('should generate unique monotonic message IDs', async () => {
      const first = await queue.queue('__wkf_workflow_test', {});
      const second = await queue.queue('__wkf_workflow_test', {});

      expect(first.messageId).toMatch(/^msg_/);
      expect(second.messageId).toMatch(/^msg_/);
      expect(first.messageId).not.toBe(second.messageId);
    });

    it('should return the original messageId when the cell dedups on idempotencyKey', async () => {
      const first = await queue.queue(
        '__wkf_workflow_a',
        { data: 1 },
        { idempotencyKey: 'step-abc' },
      );
      const second = await queue.queue(
        '__wkf_workflow_a',
        { data: 1 },
        { idempotencyKey: 'step-abc' },
      );

      expect(second.messageId).toBe(first.messageId);
      expect(recordedEnqueues).toHaveLength(1);
    });

    it('should round-trip Uint8Array payloads (binary-safe transport)', async () => {
      const input = new Uint8Array([0, 1, 2, 250, 251, 252]);
      await queue.queue('__wkf_workflow_test', {
        runId: 'wrun_1',
        runInput: { input, deploymentId: 'd', workflowName: 'w', specVersion: 3 },
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

      await queue.queue('__wkf_workflow_a', { n: 1 }, { idempotencyKey: 'k-1' });
      await queue.queue('__wkf_workflow_b', { n: 2 }, { idempotencyKey: 'k-1' });

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
      await queue.queue('__wkf_workflow_q', { data: 'test' });
      expect(recordedEnqueues).toHaveLength(0);
    });

    it('should detect test mode from NODE_ENV', async () => {
      delete process.env.VITEST;
      process.env.NODE_ENV = 'test';

      queue = createQueue({
        env: { WORKFLOW_QUEUE: mockEnv.WORKFLOW_QUEUE },
        deploymentId: 'test-deployment',
      });

      await queue.queue('__wkf_workflow_q', { data: 'test' });
      expect(recordedEnqueues).toHaveLength(0);
    });

    it('should dedup messages on idempotencyKey while inflight', async () => {
      const first = await queue.queue(
        '__wkf_workflow_a',
        { data: 1 },
        { idempotencyKey: 'step-abc' },
      );
      const second = await queue.queue(
        '__wkf_workflow_a',
        { data: 1 },
        { idempotencyKey: 'step-abc' },
      );

      expect(second.messageId).toBe(first.messageId);

      const third = await queue.queue(
        '__wkf_workflow_a',
        { data: 2 },
        { idempotencyKey: 'step-other' },
      );
      expect(third.messageId).not.toBe(first.messageId);
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

      const response = await queueHandler(vqsRequest({ data: 'test-data' }));

      expect(response.status).toBe(200);
      expect(handler).toHaveBeenCalledOnce();
      expect(handler).toHaveBeenCalledWith(
        { data: 'test-data' },
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
      const response = await queueHandler(vqsRequest({ runInput: { input } }));

      expect(response.status).toBe(200);
      const [message] = handler.mock.calls[0];
      expect(message.runInput.input).toBeInstanceOf(Uint8Array);
      expect(Array.from(message.runInput.input)).toEqual([9, 8, 7]);
    });

    it('should pass the attempt from the x-vqs-message-attempt header', async () => {
      const handler = vi.fn<QueueMessageHandler>().mockResolvedValue(undefined);
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      await queueHandler(
        vqsRequest(
          { data: 'test' },
          {
            'x-vqs-queue-name': 'workflow:test-queue',
            'x-vqs-message-id': 'msg_test',
            'x-vqs-message-attempt': '3',
          },
        ),
      );

      expect(handler).toHaveBeenCalledWith(
        expect.anything(),
        expect.objectContaining({ attempt: 3 }),
      );
    });

    it('should signal redelivery when the handler returns timeoutSeconds', async () => {
      const handler = vi.fn<QueueMessageHandler>().mockResolvedValue({ timeoutSeconds: 30 });
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(vqsRequest({ data: 'test' }));

      expect(response.status).toBe(503);
      expect(response.headers.get('Retry-After')).toBe('30');
      const body = await response.json();
      expect(body.timeoutSeconds).toBe(30);
    });

    it('should surface permanent errors with their own status', async () => {
      const handler = vi
        .fn<QueueMessageHandler>()
        .mockRejectedValue(new WorkflowWorldError('run already terminal', { status: 410 }));
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(vqsRequest({ data: 'test' }));

      expect(response.status).toBe(410);
      const body = await response.json();
      expect(body.permanent).toBe(true);
    });

    it('should return 500 with Retry-After on transient errors', async () => {
      const handler = vi.fn<QueueMessageHandler>().mockRejectedValue(new Error('Handler error'));
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(vqsRequest({ data: 'test' }));

      expect(response.status).toBe(500);
      expect(response.headers.get('Retry-After')).toBeDefined();
      const errorBody = await response.json();
      expect(errorBody.error).toContain('Handler error');
    });

    it('should reject messages with invalid queue name prefix', async () => {
      const handler = vi.fn<QueueMessageHandler>();
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(
        vqsRequest(
          { data: 'test' },
          {
            'x-vqs-queue-name': 'invalid:test-queue',
            'x-vqs-message-id': 'msg_test',
            'x-vqs-message-attempt': '1',
          },
        ),
      );

      expect(response.status).toBe(400);
      expect(handler).not.toHaveBeenCalled();
    });

    it('should reject requests missing the x-vqs headers', async () => {
      const handler = vi.fn<QueueMessageHandler>();
      const queueHandler = queue.createQueueHandler('workflow:', handler);

      const response = await queueHandler(
        new Request('http://localhost', { method: 'POST', body: stringify({ data: 'x' }) }),
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
  });
});
