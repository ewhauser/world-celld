import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createCelldWorld } from '../../src/index.js';
import { callDO } from '../../src/remote/rpc-client.js';
import { parse } from '../../src/vendor/shared/index.js';
import type { QueueStats } from '../../src/worker/durable-objects/QueueDO.js';
import { RunExpiredError } from '@workflow/errors';
import type { CleanupRecord } from '../../src/retention.js';

function positiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  const value = raw === undefined ? fallback : Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer; received ${raw ?? value}`);
  }
  return value;
}

function nonNegativeNumber(name: string, fallback = 0): number {
  const raw = process.env[name];
  const value = raw === undefined ? fallback : Number(raw);
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative number; received ${raw ?? value}`);
  }
  return value;
}

function nonNegativeInteger(name: string, fallback = 0): number {
  const raw = process.env[name];
  const value = raw === undefined ? fallback : Number.parseInt(raw, 10);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer; received ${raw ?? value}`);
  }
  return value;
}

function percentile(values: number[], quantile: number): number {
  if (values.length === 0) return 0;
  const sorted = values.toSorted((a, b) => a - b);
  const index = Math.max(0, Math.ceil(quantile * sorted.length) - 1);
  return Number(sorted[index].toFixed(2));
}

function rate(count: number, durationMs: number): number {
  return Number((count / Math.max(durationMs / 1000, 0.001)).toFixed(2));
}

interface LatencySummary {
  count: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

function summarizeLatency(values: number[]): LatencySummary {
  return {
    count: values.length,
    p50: percentile(values, 0.5),
    p95: percentile(values, 0.95),
    p99: percentile(values, 0.99),
    max: percentile(values, 1),
  };
}

async function runPool(
  count: number,
  concurrency: number,
  task: (index: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const workers = Array.from({ length: Math.min(count, concurrency) }, async () => {
    while (true) {
      const index = next++;
      if (index >= count) return;
      await task(index);
    }
  });
  await Promise.all(workers);
}

async function waitUntil(predicate: () => Promise<boolean> | boolean, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return false;
}

interface PerfPayload {
  perfRunId: string;
  sequence: number;
  padding: string;
}

interface Delivery {
  messageId: string;
  latencyMs: number;
  callbackAttempts: number;
}

interface PerfResult {
  schemaVersion: 2;
  recordedAt: string;
  backend: { name: 'minio'; version: string; celldVersion: string };
  workload: Record<string, number>;
  correctness: Record<string, number | boolean>;
  performance: {
    enqueueDurationMs: number;
    totalDurationMs: number;
    enqueuePerSecond: number;
    deliveryPerSecond: number;
    enqueueLatencyMs: LatencySummary;
    deliveryLatencyMs: LatencySummary;
    firstAttemptDeliveryLatencyMs: LatencySummary;
    retriedDeliveryLatencyMs: LatencySummary;
  };
  queueStats: QueueStats[];
  budgets: Record<string, number>;
}

interface RetentionPerfResult {
  schemaVersion: 1;
  recordedAt: string;
  backend: { name: 'minio'; version: string; celldVersion: string };
  workload: { runs: number; concurrency: number; queueShards: number; retentionMs: number };
  correctness: {
    created: number;
    tombstoned: number;
    expiredReads: number;
    deletedPayloadKeys: number;
    deletedStreams: number;
    deletedQueueMessages: number;
    pending: number;
    inflight: number;
    deadLetters: number;
  };
  performance: {
    setupDurationMs: number;
    cleanupDurationMs: number;
    setupRunsPerSecond: number;
    cleanupRunsPerSecond: number;
    terminalWriteLatencyMs: LatencySummary;
    cleanupLagMs: LatencySummary;
  };
}

describe('MinIO single-node queue performance and loss', () => {
  const fleetUrl = process.env.CELLD_FLEET_URL ?? '';
  const secret = process.env.CELLD_WORLD_SECRET ?? '';
  const callbackPort = positiveInteger('PERF_CALLBACK_PORT', 3000);
  const messageCount = positiveInteger('PERF_MESSAGES', 1000);
  const concurrency = positiveInteger('PERF_CONCURRENCY', 32);
  const queueShards = positiveInteger('PERF_QUEUE_SHARDS', 2);
  const payloadBytes = positiveInteger('PERF_PAYLOAD_BYTES', 256);
  const retryEvery = nonNegativeInteger('PERF_RETRY_EVERY', 20);
  const timeoutMs = positiveInteger('PERF_TIMEOUT_MS', 180_000);
  const minEnqueuePerSecond = nonNegativeNumber('PERF_MIN_ENQUEUE_PER_SECOND');
  const minDeliveryPerSecond = nonNegativeNumber('PERF_MIN_DELIVERY_PER_SECOND');
  const maxDeliveryP99Ms = nonNegativeNumber('PERF_MAX_DELIVERY_P99_MS');
  const resultPath = process.env.PERF_RESULT_PATH ?? '.perf-results/minio-latest.json';
  const retentionRuns = positiveInteger('PERF_RETENTION_RUNS', 100);
  const retentionConcurrency = positiveInteger('PERF_RETENTION_CONCURRENCY', 16);
  const retentionMs = positiveInteger('PERF_RUN_RETENTION_MS', 1_000);
  const retentionResultPath =
    process.env.PERF_RETENTION_RESULT_PATH ?? '.perf-results/minio-retention-latest.json';
  const runId = randomUUID();
  const startedAt = new Map<number, number>();
  const accepted = new Map<number, string>();
  const successful = new Map<number, Delivery>();
  const callbackAttempts = new Map<number, number>();
  const enqueueLatencies: number[] = [];
  const deliveryLatencies: number[] = [];
  const enqueueErrors: Array<{ sequence: number; error: string }> = [];
  const invalidCallbacks: string[] = [];
  let successfulDuplicates = 0;
  let listener: http.Server;

  beforeAll(async () => {
    process.env.CELLD_QUEUE_MODE = 'cells';
    if (!fleetUrl || !secret) {
      throw new Error('CELLD_FLEET_URL and CELLD_WORLD_SECRET are required');
    }

    listener = http.createServer(async (request, response) => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(chunk as Buffer);

      let payload: PerfPayload;
      try {
        payload = parse<PerfPayload>(Buffer.concat(chunks).toString('utf8'));
      } catch (error) {
        invalidCallbacks.push(`malformed body: ${String(error)}`);
        response.writeHead(400).end();
        return;
      }

      if (
        payload.perfRunId !== runId ||
        !Number.isSafeInteger(payload.sequence) ||
        payload.sequence < 0 ||
        payload.sequence >= messageCount
      ) {
        invalidCallbacks.push(`unexpected payload: ${JSON.stringify(payload)}`);
        response.writeHead(400).end();
        return;
      }

      const sequence = payload.sequence;
      const attemptCount = (callbackAttempts.get(sequence) ?? 0) + 1;
      callbackAttempts.set(sequence, attemptCount);

      if (retryEvery > 0 && sequence % retryEvery === 0 && attemptCount === 1) {
        response.writeHead(503, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ timeoutSeconds: 0 }));
        return;
      }

      const messageId = request.headers['x-vqs-message-id'];
      const start = startedAt.get(sequence);
      if (typeof messageId !== 'string' || start === undefined) {
        invalidCallbacks.push(`missing metadata for sequence ${sequence}`);
        response.writeHead(400).end();
        return;
      }

      if (successful.has(sequence)) {
        successfulDuplicates++;
      } else {
        const latencyMs = performance.now() - start;
        successful.set(sequence, { messageId, latencyMs, callbackAttempts: attemptCount });
        deliveryLatencies.push(latencyMs);
      }

      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ ok: true }));
    });

    await new Promise<void>((resolve, reject) => {
      listener.once('error', reject);
      listener.listen(callbackPort, '0.0.0.0', resolve);
    });
  });

  afterAll(async () => {
    delete process.env.CELLD_QUEUE_MODE;
    if (!listener) return;
    await new Promise<void>((resolve, reject) =>
      listener.close((error) => (error ? reject(error) : resolve())),
    );
  });

  it('delivers every accepted message and records latency and throughput', async () => {
    const world = createCelldWorld({
      fleetUrl,
      secret,
      baseUrl: process.env.CELLD_CALLBACK_BASE_URL,
      deploymentId: `perf-${runId}`,
      queueShards,
      rpcTimeoutMs: timeoutMs,
    });
    const queueName = `__wkf_step_perf_${runId.replaceAll('-', '')}`;
    const padding = 'x'.repeat(Math.max(0, payloadBytes - 96));
    const workloadStart = performance.now();

    await runPool(messageCount, concurrency, async (sequence) => {
      const enqueueStart = performance.now();
      startedAt.set(sequence, enqueueStart);
      try {
        const outcome = await world.queue(queueName, { perfRunId: runId, sequence, padding });
        accepted.set(sequence, String(outcome.messageId));
        enqueueLatencies.push(performance.now() - enqueueStart);
      } catch (error) {
        enqueueErrors.push({ sequence, error: String(error) });
      }
    });

    const enqueueEnd = performance.now();
    const allDelivered = await waitUntil(
      () => Array.from(accepted.keys()).every((sequence) => successful.has(sequence)),
      timeoutMs,
    );
    const drained = await waitUntil(async () => {
      const stats = await Promise.all(
        Array.from({ length: queueShards }, (_, shard) =>
          callDO<QueueStats>({ fleetUrl, secret }, 'queue', `q:${shard}`, 'stats', []),
        ),
      );
      return stats.every((entry) => entry.pending === 0 && entry.inflight === 0);
    }, timeoutMs);
    const workloadEnd = performance.now();

    const queueStats = await Promise.all(
      Array.from({ length: queueShards }, (_, shard) =>
        callDO<QueueStats>({ fleetUrl, secret }, 'queue', `q:${shard}`, 'stats', []),
      ),
    );
    const missing = Array.from(accepted.keys()).filter((sequence) => !successful.has(sequence));
    const mismatchedMessageIds = Array.from(successful, ([sequence, delivery]) => ({
      sequence,
      accepted: accepted.get(sequence),
      delivered: delivery.messageId,
    })).filter((entry) => entry.accepted !== entry.delivered);
    const deadLetters = queueStats.reduce((sum, entry) => sum + entry.deadLetters, 0);
    const pending = queueStats.reduce((sum, entry) => sum + entry.pending, 0);
    const inflight = queueStats.reduce((sum, entry) => sum + entry.inflight, 0);
    const enqueueDurationMs = enqueueEnd - workloadStart;
    const totalDurationMs = workloadEnd - workloadStart;
    const enqueuePerSecond = rate(accepted.size, enqueueDurationMs);
    const deliveryPerSecond = rate(successful.size, totalDurationMs);
    const deliveryP99Ms = percentile(deliveryLatencies, 0.99);
    const firstAttemptDeliveryLatencies = Array.from(successful.values())
      .filter((delivery) => delivery.callbackAttempts === 1)
      .map((delivery) => delivery.latencyMs);
    const retriedDeliveryLatencies = Array.from(successful.values())
      .filter((delivery) => delivery.callbackAttempts > 1)
      .map((delivery) => delivery.latencyMs);

    const result: PerfResult = {
      schemaVersion: 2,
      recordedAt: new Date().toISOString(),
      backend: {
        name: 'minio',
        version: process.env.PERF_MINIO_VERSION ?? 'unknown',
        celldVersion: process.env.PERF_CELLD_VERSION ?? 'unknown',
      },
      workload: {
        messages: messageCount,
        concurrency,
        queueShards,
        payloadBytes,
        retryEvery,
      },
      correctness: {
        allDelivered,
        drained,
        accepted: accepted.size,
        delivered: successful.size,
        missing: missing.length,
        enqueueErrors: enqueueErrors.length,
        invalidCallbacks: invalidCallbacks.length,
        mismatchedMessageIds: mismatchedMessageIds.length,
        callbackAttempts: Array.from(callbackAttempts.values()).reduce(
          (sum, count) => sum + count,
          0,
        ),
        successfulDuplicates,
        pending,
        inflight,
        deadLetters,
      },
      performance: {
        enqueueDurationMs: Number(enqueueDurationMs.toFixed(2)),
        totalDurationMs: Number(totalDurationMs.toFixed(2)),
        enqueuePerSecond,
        deliveryPerSecond,
        enqueueLatencyMs: summarizeLatency(enqueueLatencies),
        deliveryLatencyMs: summarizeLatency(deliveryLatencies),
        firstAttemptDeliveryLatencyMs: summarizeLatency(firstAttemptDeliveryLatencies),
        retriedDeliveryLatencyMs: summarizeLatency(retriedDeliveryLatencies),
      },
      queueStats,
      budgets: {
        minEnqueuePerSecond,
        minDeliveryPerSecond,
        maxDeliveryP99Ms,
      },
    };

    await mkdir(path.dirname(resultPath), { recursive: true });
    await writeFile(resultPath, `${JSON.stringify(result, null, 2)}\n`);
    console.log(`\nworld-celld MinIO performance result\n${JSON.stringify(result, null, 2)}`);

    expect(enqueueErrors, 'enqueue errors').toEqual([]);
    expect(accepted.size, 'accepted message count').toBe(messageCount);
    expect(allDelivered, `missing accepted sequences: ${missing.slice(0, 20).join(', ')}`).toBe(
      true,
    );
    expect(missing, 'accepted messages without a successful callback').toEqual([]);
    expect(invalidCallbacks, 'invalid callbacks').toEqual([]);
    expect(mismatchedMessageIds, 'accepted and delivered message IDs').toEqual([]);
    expect(successfulDuplicates, 'duplicate successful deliveries').toBe(0);
    expect(drained, 'queues did not drain before the timeout').toBe(true);
    expect(pending, 'pending messages after drain').toBe(0);
    expect(inflight, 'inflight messages after drain').toBe(0);
    expect(deadLetters, 'dead letters').toBe(0);

    expect(
      minEnqueuePerSecond === 0 || enqueuePerSecond >= minEnqueuePerSecond,
      `enqueue throughput ${enqueuePerSecond}/s is below ${minEnqueuePerSecond}/s`,
    ).toBe(true);
    expect(
      minDeliveryPerSecond === 0 || deliveryPerSecond >= minDeliveryPerSecond,
      `delivery throughput ${deliveryPerSecond}/s is below ${minDeliveryPerSecond}/s`,
    ).toBe(true);
    expect(
      maxDeliveryP99Ms === 0 || deliveryP99Ms <= maxDeliveryP99Ms,
      `delivery p99 ${deliveryP99Ms}ms exceeds ${maxDeliveryP99Ms}ms`,
    ).toBe(true);
  });

  it('reclaims terminal run payloads without loss or resurrection', async () => {
    const world = createCelldWorld({
      fleetUrl,
      secret,
      baseUrl: process.env.CELLD_CALLBACK_BASE_URL,
      deploymentId: `retention-perf-${runId}`,
      queueShards,
      runRetentionMs: retentionMs,
      rpcTimeoutMs: timeoutMs,
    });
    const runIds: string[] = [];
    const terminalWriteLatencies: number[] = [];
    const setupStart = performance.now();

    await runPool(retentionRuns, retentionConcurrency, async (sequence) => {
      const created = await world.events.create(null, {
        eventType: 'run_created',
        eventData: {
          deploymentId: `retention-perf-${runId}`,
          workflowName: `retention-perf-${sequence}`,
          input: [`payload-${sequence}`, 'x'.repeat(payloadBytes)],
        },
      });
      const workflowRunId = created.run!.runId;
      runIds.push(workflowRunId);
      const streamName = `retention-${workflowRunId}`;
      await world.writeToStream(streamName, workflowRunId, `stream-${sequence}`);
      await world.closeStream(streamName, workflowRunId);
      await world.queue(
        `__wkf_workflow_retention_${runId.replaceAll('-', '')}`,
        { runId: workflowRunId },
        { delaySeconds: 3_600, idempotencyKey: `retention:${workflowRunId}` },
      );
      const terminalStart = performance.now();
      await world.events.create(workflowRunId, {
        eventType: 'run_completed',
        eventData: { output: [`done-${sequence}`] },
      });
      terminalWriteLatencies.push(performance.now() - terminalStart);
    });

    const setupEnd = performance.now();
    let completedStatuses: CleanupRecord[] = [];
    const cleaned = await waitUntil(async () => {
      const statuses = await Promise.all(runIds.map((id) => world.retention.getStatus(id)));
      completedStatuses = statuses.filter(
        (status): status is CleanupRecord => status?.phase === 'tombstoned',
      );
      return completedStatuses.length === runIds.length;
    }, timeoutMs);
    const finishedAt = performance.now();
    if (!cleaned) {
      throw new Error(`retention cleanup timed out after ${finishedAt - setupEnd}ms`);
    }

    const expiredReads = (
      await Promise.all(
        runIds.map(async (id) => {
          try {
            await world.runs.get(id);
            return false;
          } catch (error) {
            return RunExpiredError.is(error);
          }
        }),
      )
    ).filter(Boolean).length;
    const queueStats = await Promise.all(
      Array.from({ length: queueShards }, (_, shard) =>
        callDO<QueueStats>({ fleetUrl, secret }, 'queue', `q:${shard}`, 'stats', []),
      ),
    );
    const cleanupLag = completedStatuses.map(
      (status) => status.tombstonedAt!.getTime() - status.dueAt.getTime(),
    );
    const cleanupStart = Math.min(...completedStatuses.map((status) => status.dueAt.getTime()));
    const cleanupEnd = Math.max(
      ...completedStatuses.map((status) => status.tombstonedAt!.getTime()),
    );
    const setupDurationMs = setupEnd - setupStart;
    const cleanupDurationMs = Math.max(0, cleanupEnd - cleanupStart);
    const pending = queueStats.reduce((sum, entry) => sum + entry.pending, 0);
    const inflight = queueStats.reduce((sum, entry) => sum + entry.inflight, 0);
    const deadLetters = queueStats.reduce((sum, entry) => sum + entry.deadLetters, 0);

    const result: RetentionPerfResult = {
      schemaVersion: 1,
      recordedAt: new Date().toISOString(),
      backend: {
        name: 'minio',
        version: process.env.PERF_MINIO_VERSION ?? 'unknown',
        celldVersion: process.env.PERF_CELLD_VERSION ?? 'unknown',
      },
      workload: {
        runs: retentionRuns,
        concurrency: retentionConcurrency,
        queueShards,
        retentionMs,
      },
      correctness: {
        created: runIds.length,
        tombstoned: completedStatuses.length,
        expiredReads,
        deletedPayloadKeys: completedStatuses.reduce(
          (sum, status) => sum + status.deletedPayloadKeys,
          0,
        ),
        deletedStreams: completedStatuses.reduce((sum, status) => sum + status.deletedStreams, 0),
        deletedQueueMessages: completedStatuses.reduce(
          (sum, status) => sum + status.deletedQueueMessages,
          0,
        ),
        pending,
        inflight,
        deadLetters,
      },
      performance: {
        setupDurationMs: Number(setupDurationMs.toFixed(2)),
        cleanupDurationMs,
        setupRunsPerSecond: rate(runIds.length, setupDurationMs),
        cleanupRunsPerSecond: rate(completedStatuses.length, cleanupDurationMs),
        terminalWriteLatencyMs: summarizeLatency(terminalWriteLatencies),
        cleanupLagMs: summarizeLatency(cleanupLag),
      },
    };

    await mkdir(path.dirname(retentionResultPath), { recursive: true });
    await writeFile(retentionResultPath, `${JSON.stringify(result, null, 2)}\n`);
    console.log(`\nworld-celld MinIO retention result\n${JSON.stringify(result, null, 2)}`);

    expect(runIds).toHaveLength(retentionRuns);
    expect(completedStatuses).toHaveLength(retentionRuns);
    expect(expiredReads).toBe(retentionRuns);
    expect(result.correctness.deletedPayloadKeys).toBeGreaterThan(0);
    expect(result.correctness.deletedStreams).toBe(retentionRuns);
    expect(result.correctness.deletedQueueMessages).toBe(retentionRuns);
    expect(pending).toBe(0);
    expect(inflight).toBe(0);
    expect(deadLetters).toBe(0);
  });
});
