import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { mkdir, writeFile } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createCelldWorld } from '../../src/index.js';
import { parse } from '../../src/vendor/shared/index.js';
import { RunExpiredError } from '@workflow/errors';
import type { CleanupRecord } from '../../src/retention.js';
import { lazyHookResumeSetup } from './lazy-hook-resume.js';

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
  __healthCheck: boolean;
  correlationId: string;
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
  budgets: Record<string, number>;
}

interface RetentionPerfResult {
  schemaVersion: 1;
  recordedAt: string;
  backend: { name: 'minio'; version: string; celldVersion: string };
  workload: { runs: number; concurrency: number; retentionMs: number };
  correctness: {
    created: number;
    tombstoned: number;
    expiredReads: number;
    deletedPayloadKeys: number;
    deletedStreams: number;
    deletedQueuePayloads: number;
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
  const payloadBytes = positiveInteger('PERF_PAYLOAD_BYTES', 256);
  const retryEvery = nonNegativeInteger('PERF_RETRY_EVERY', 20);
  // Workflow messages carry a runId, which moves their body into the fleet
  // bucket and adds payload bookkeeping and an idempotency claim per message.
  const runBearingQueue = process.env.PERF_QUEUE_RUN_BEARING === '1';
  const timeoutMs = positiveInteger('PERF_TIMEOUT_MS', 180_000);
  // Emulates the client-to-fleet network: every World client request waits
  // this long before it is sent, so round-trip savings show on loopback.
  const clientRttMs = nonNegativeInteger('PERF_CLIENT_RTT_MS');
  const originalFetch = globalThis.fetch;
  const minEnqueuePerSecond = nonNegativeNumber('PERF_MIN_ENQUEUE_PER_SECOND');
  const minDeliveryPerSecond = nonNegativeNumber('PERF_MIN_DELIVERY_PER_SECOND');
  const maxDeliveryP99Ms = nonNegativeNumber('PERF_MAX_DELIVERY_P99_MS');
  const resultPath = process.env.PERF_RESULT_PATH ?? '.perf-results/minio-latest.json';
  const retentionRuns = positiveInteger('PERF_RETENTION_RUNS', 100);
  const retentionConcurrency = positiveInteger('PERF_RETENTION_CONCURRENCY', 16);
  const retentionMs = positiveInteger('PERF_RUN_RETENTION_MS', 1_000);
  const retentionResultPath =
    process.env.PERF_RETENTION_RESULT_PATH ?? '.perf-results/minio-retention-latest.json';
  const streamChunkCount = positiveInteger('PERF_STREAM_CHUNKS', 1000);
  const streamChunkBytes = positiveInteger('PERF_STREAM_CHUNK_BYTES', 32);
  const streamResultPath =
    process.env.PERF_STREAM_RESULT_PATH ?? '.perf-results/minio-stream-latest.json';
  const workflowRuns = positiveInteger('PERF_WORKFLOW_RUNS', 25);
  const workflowConcurrency = positiveInteger('PERF_WORKFLOW_CONCURRENCY', 8);
  const workflowResultPath =
    process.env.PERF_WORKFLOW_RESULT_PATH ?? '.perf-results/minio-workflow-latest.json';
  const steadySeconds = nonNegativeInteger('PERF_STEADY_SECONDS');
  const steadyRate = positiveInteger('PERF_STEADY_RATE', 25);
  const steadyCount = steadySeconds * steadyRate;
  if (!Number.isSafeInteger(steadyCount) || steadyCount > 100_000) {
    throw new Error('PERF_STEADY_SECONDS * PERF_STEADY_RATE must be at most 100000');
  }
  const steadyResultPath =
    process.env.PERF_STEADY_RESULT_PATH ?? '.perf-results/minio-steady-latest.json';
  const runId = randomUUID();
  const workflowMarker = `workflow-${runId}`;
  const steadyMarker = `steady-${runId}`;
  const workflowStartedAt = new Map<number, number>();
  const workflowAccepted = new Map<number, string>();
  const workflowDelivered = new Map<number, Delivery>();
  let workflowDuplicates = 0;
  const steadyStartedAt = new Map<number, number>();
  const steadyAccepted = new Map<number, string>();
  const steadyDelivered = new Map<number, Delivery>();
  let steadyDuplicates = 0;
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
    process.env.CELLD_QUEUE_MODE = 'native';
    if (clientRttMs > 0) {
      globalThis.fetch = async (input, init) => {
        await delay(clientRttMs);
        return await originalFetch(input, init);
      };
    }
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

      const [payloadRunId, sequenceText] = payload.correlationId.split('|', 2);
      const sequence = /^\d+$/.test(sequenceText ?? '') ? Number(sequenceText) : Number.NaN;
      const workflowCallback = payloadRunId === workflowMarker;
      const steadyCallback = payloadRunId === steadyMarker;
      if (
        !payload['__healthCheck'] ||
        (payloadRunId !== runId && !workflowCallback && !steadyCallback) ||
        !Number.isSafeInteger(sequence) ||
        sequence < 0 ||
        sequence >= (workflowCallback ? workflowRuns : steadyCallback ? steadyCount : messageCount)
      ) {
        invalidCallbacks.push(`unexpected payload: ${JSON.stringify(payload)}`);
        response.writeHead(400).end();
        return;
      }

      const attemptCount =
        workflowCallback || steadyCallback ? 1 : (callbackAttempts.get(sequence) ?? 0) + 1;
      if (!workflowCallback && !steadyCallback) callbackAttempts.set(sequence, attemptCount);

      if (
        !workflowCallback &&
        !steadyCallback &&
        retryEvery > 0 &&
        sequence % retryEvery === 0 &&
        attemptCount === 1
      ) {
        response.writeHead(503, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ timeoutSeconds: 0 }));
        return;
      }

      const messageId = request.headers['x-vqs-message-id'];
      const start = (
        workflowCallback ? workflowStartedAt : steadyCallback ? steadyStartedAt : startedAt
      ).get(sequence);
      if (typeof messageId !== 'string' || start === undefined) {
        invalidCallbacks.push(`missing metadata for sequence ${sequence}`);
        response.writeHead(400).end();
        return;
      }

      const delivered = workflowCallback
        ? workflowDelivered
        : steadyCallback
          ? steadyDelivered
          : successful;
      if (delivered.has(sequence)) {
        if (workflowCallback) workflowDuplicates++;
        else if (steadyCallback) steadyDuplicates++;
        else successfulDuplicates++;
      } else {
        const latencyMs = performance.now() - start;
        delivered.set(sequence, { messageId, latencyMs, callbackAttempts: attemptCount });
        if (!workflowCallback && !steadyCallback) deliveryLatencies.push(latencyMs);
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
    globalThis.fetch = originalFetch;
    if (!listener) return;
    listener.closeAllConnections();
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
      rpcTimeoutMs: timeoutMs,
    });
    const queueName = `__wkf_workflow_perf_${runId.replaceAll('-', '')}`;
    const padding = 'x'.repeat(Math.max(0, payloadBytes - 96));
    const queueRunId = runBearingQueue
      ? (
          await world.events.create(null, {
            eventType: 'run_created',
            eventData: { deploymentId: `perf-${runId}`, workflowName: 'perf-queue', input: [] },
          })
        ).run.runId
      : undefined;
    const workloadStart = performance.now();

    await runPool(messageCount, concurrency, async (sequence) => {
      const enqueueStart = performance.now();
      startedAt.set(sequence, enqueueStart);
      try {
        const outcome = await world.queue(
          queueName,
          {
            __healthCheck: true,
            correlationId: `${runId}|${sequence}|${padding}`,
            ...(queueRunId ? { runId: queueRunId } : {}),
          },
          queueRunId ? { idempotencyKey: `perf-${sequence}` } : undefined,
        );
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
    const workloadEnd = performance.now();

    const missing = Array.from(accepted.keys()).filter((sequence) => !successful.has(sequence));
    const mismatchedMessageIds = Array.from(successful, ([sequence, delivery]) => ({
      sequence,
      accepted: accepted.get(sequence),
      delivered: delivery.messageId,
    })).filter((entry) => entry.accepted !== entry.delivered);
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
        payloadBytes,
        retryEvery,
        runBearing: runBearingQueue,
      },
      correctness: {
        allDelivered,
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

  it('measures a mixed workflow lifecycle through the real fleet', async () => {
    const deploymentId = `workflow-perf-${runId}`;
    const workflowName = `workflow-perf-${runId}`;
    const world = createCelldWorld({
      fleetUrl,
      secret,
      baseUrl: process.env.CELLD_CALLBACK_BASE_URL,
      deploymentId,
      rpcTimeoutMs: timeoutMs,
    });
    const stageMs: Record<string, number[]> = {
      create: [],
      replay: [],
      step: [],
      hook: [],
      resume: [],
      stream: [],
      queue: [],
      read: [],
      complete: [],
    };
    const payload = 'x'.repeat(payloadBytes);
    const failures: string[] = [];
    let lazyResumePreloads = 0;
    const started = performance.now();
    const measure = async (stage: string, action: () => Promise<void>) => {
      const began = performance.now();
      await action();
      stageMs[stage].push(performance.now() - began);
    };

    await runPool(workflowRuns, workflowConcurrency, async (sequence) => {
      try {
        let workflowRunId = '';
        await measure('create', async () => {
          const created = await world.events.create(null, {
            eventType: 'run_created',
            eventData: { deploymentId, workflowName, input: [payload, sequence] },
          });
          workflowRunId = created.run.runId;
          await world.events.create(workflowRunId, { eventType: 'run_started' });
        });
        // Every workflow invocation after the first starts with a run_started
        // replay against the already-running run.
        await measure('replay', async () => {
          await world.events.create(workflowRunId, { eventType: 'run_started' });
        });
        const stepId = `step-${sequence}`;
        await measure('step', async () => {
          await world.events.create(workflowRunId, {
            eventType: 'step_created',
            correlationId: stepId,
            eventData: { stepName: 'perf-step', input: [payload] },
          });
          await world.events.create(workflowRunId, {
            eventType: 'step_completed',
            correlationId: stepId,
            eventData: { result: [sequence] },
          });
        });
        const token = `perf-hook-${runId}-${sequence}`;
        await measure('hook', async () => {
          await world.events.create(workflowRunId, {
            eventType: 'hook_created',
            correlationId: `hook-${sequence}`,
            eventData: { token },
          });
          const hook = await world.hooks.getByToken(token);
          if (hook.runId !== workflowRunId) throw new Error('hook owner mismatch');
        });
        await measure('resume', async () => {
          const resumeId = `01K${String(sequence).padStart(23, '0')}`;
          const { usablePreload } = await lazyHookResumeSetup(world, {
            runId: workflowRunId,
            hookId: `hook-${sequence}`,
            token,
            resumeId,
            payload,
          });
          lazyResumePreloads += usablePreload ? 1 : 0;
        });
        const streamName = `perf-stream-${workflowRunId}`;
        await measure('stream', async () => {
          await world.writeToStream(streamName, workflowRunId, payload);
          await world.closeStream(streamName, workflowRunId);
          const chunks = await world.getStreamChunks(streamName, workflowRunId, {});
          if (new TextDecoder().decode(chunks.data[0]?.data) !== payload) {
            throw new Error('stream payload mismatch');
          }
        });
        await measure('read', async () => {
          const [run, step] = await Promise.all([
            world.runs.get(workflowRunId),
            world.steps.get(workflowRunId, stepId),
          ]);
          if (run.runId !== workflowRunId || step.stepId !== stepId) {
            throw new Error('run or step read mismatch');
          }
        });
        await measure('queue', async () => {
          workflowStartedAt.set(sequence, performance.now());
          const queued = await world.queue(`__wkf_workflow_perf_${runId.replaceAll('-', '')}`, {
            __healthCheck: true,
            correlationId: `${workflowMarker}|${sequence}|${payload}`,
          });
          workflowAccepted.set(sequence, String(queued.messageId));
        });
        await measure('complete', async () => {
          await world.events.create(workflowRunId, {
            eventType: 'run_completed',
            eventData: { output: [sequence] },
          });
        });
      } catch (error) {
        failures.push(`${sequence}: ${String(error)}`);
      }
    });

    const listMs: number[] = [];
    const listNoDataMs: number[] = [];
    let listed = await world.runs.list({ workflowName, pagination: { limit: 20 } });
    for (let iteration = 0; iteration < 5; iteration++) {
      let began = performance.now();
      listed = await world.runs.list({ workflowName, pagination: { limit: 20 } });
      listMs.push(performance.now() - began);
      began = performance.now();
      await world.runs.list({ workflowName, resolveData: 'none', pagination: { limit: 20 } });
      listNoDataMs.push(performance.now() - began);
    }
    const delivered = await waitUntil(
      () => workflowAccepted.size === workflowRuns && workflowDelivered.size === workflowRuns,
      timeoutMs,
    );
    const elapsedMs = performance.now() - started;
    const mismatched = Array.from(workflowAccepted, ([sequence, messageId]) => ({
      sequence,
      expected: messageId,
      actual: workflowDelivered.get(sequence)?.messageId,
    })).filter(({ expected, actual }) => expected !== actual);
    const result = {
      schemaVersion: 1,
      recordedAt: new Date().toISOString(),
      backend: { name: 'minio', celldVersion: process.env.PERF_CELLD_VERSION ?? 'unknown' },
      workload: { runs: workflowRuns, concurrency: workflowConcurrency, payloadBytes, clientRttMs },
      correctness: {
        completed: stageMs.complete.length,
        accepted: workflowAccepted.size,
        delivered: workflowDelivered.size,
        duplicateCallbacks: workflowDuplicates,
        mismatchedMessageIds: mismatched.length,
        listReturned: listed.data.length,
        lazyResumePreloads,
        failures,
      },
      performance: {
        elapsedMs: Number(elapsedMs.toFixed(2)),
        runsPerSecond: rate(stageMs.complete.length, elapsedMs),
        listMs: summarizeLatency(listMs),
        listNoDataMs: summarizeLatency(listNoDataMs),
        queueDeliveryMs: summarizeLatency(
          Array.from(workflowDelivered.values(), (delivery) => delivery.latencyMs),
        ),
        stages: Object.fromEntries(
          Object.entries(stageMs).map(([name, samples]) => [name, summarizeLatency(samples)]),
        ),
      },
    };
    await mkdir(path.dirname(workflowResultPath), { recursive: true });
    await writeFile(workflowResultPath, `${JSON.stringify(result, null, 2)}\n`);
    console.log(`\nworld-celld MinIO workflow result\n${JSON.stringify(result, null, 2)}`);
    expect(failures).toEqual([]);
    expect(stageMs.complete).toHaveLength(workflowRuns);
    expect(workflowAccepted.size).toBe(workflowRuns);
    expect(delivered).toBe(true);
    expect(workflowDuplicates).toBe(0);
    expect(mismatched).toEqual([]);
    expect(listed.data.length).toBeGreaterThan(0);
  });

  it.skipIf(steadySeconds === 0)('measures sustained queue latency and backlog', async () => {
    const world = createCelldWorld({
      fleetUrl,
      secret,
      baseUrl: process.env.CELLD_CALLBACK_BASE_URL,
      deploymentId: `steady-perf-${runId}`,
      rpcTimeoutMs: timeoutMs,
    });
    const queueName = `__wkf_workflow_steady_${runId.replaceAll('-', '')}`;
    const padding = 'x'.repeat(Math.max(0, payloadBytes - 96));
    const errors: string[] = [];
    const scheduleLagMs: number[] = [];
    const backlog: Array<{
      elapsedMs: number;
      accepted: number;
      delivered: number;
      pending: number;
    }> = [];
    const began = performance.now();
    const sampler = setInterval(() => {
      backlog.push({
        elapsedMs: Math.round(performance.now() - began),
        accepted: steadyAccepted.size,
        delivered: steadyDelivered.size,
        pending: Math.max(0, steadyAccepted.size - steadyDelivered.size),
      });
    }, 1_000);
    try {
      await runPool(steadyCount, concurrency, async (sequence) => {
        const scheduledAt = began + (sequence * 1_000) / steadyRate;
        const remainingMs = scheduledAt - performance.now();
        if (remainingMs > 0) await new Promise((resolve) => setTimeout(resolve, remainingMs));
        scheduleLagMs.push(Math.max(0, performance.now() - scheduledAt));
        steadyStartedAt.set(sequence, performance.now());
        try {
          const queued = await world.queue(queueName, {
            __healthCheck: true,
            correlationId: `${steadyMarker}|${sequence}|${padding}`,
          });
          steadyAccepted.set(sequence, String(queued.messageId));
        } catch (error) {
          errors.push(`${sequence}: ${String(error)}`);
        }
      });
      const producerFinishedMs = performance.now() - began;
      const allDelivered = await waitUntil(
        () => steadyAccepted.size === steadyCount && steadyDelivered.size === steadyCount,
        timeoutMs,
      );
      const totalMs = performance.now() - began;
      const mismatched = Array.from(steadyAccepted, ([sequence, messageId]) => ({
        expected: messageId,
        actual: steadyDelivered.get(sequence)?.messageId,
      })).filter(({ expected, actual }) => expected !== actual);
      const result = {
        schemaVersion: 1,
        recordedAt: new Date().toISOString(),
        workload: { seconds: steadySeconds, ratePerSecond: steadyRate, concurrency, payloadBytes },
        correctness: {
          accepted: steadyAccepted.size,
          delivered: steadyDelivered.size,
          allDelivered,
          duplicateCallbacks: steadyDuplicates,
          mismatchedMessageIds: mismatched.length,
          errors,
        },
        performance: {
          producerFinishedMs: Number(producerFinishedMs.toFixed(2)),
          totalMs: Number(totalMs.toFixed(2)),
          achievedEnqueuePerSecond: rate(steadyAccepted.size, producerFinishedMs),
          achievedDeliveryPerSecond: rate(steadyDelivered.size, totalMs),
          scheduleLagMs: summarizeLatency(scheduleLagMs),
          deliveryLatencyMs: summarizeLatency(
            Array.from(steadyDelivered.values(), (delivery) => delivery.latencyMs),
          ),
          backlog,
        },
      };
      await mkdir(path.dirname(steadyResultPath), { recursive: true });
      await writeFile(steadyResultPath, `${JSON.stringify(result, null, 2)}\n`);
      console.log(`\nworld-celld MinIO steady result\n${JSON.stringify(result, null, 2)}`);
      expect(errors).toEqual([]);
      expect(steadyAccepted.size).toBe(steadyCount);
      expect(allDelivered).toBe(true);
      expect(steadyDuplicates).toBe(0);
      expect(mismatched).toEqual([]);
    } finally {
      clearInterval(sampler);
    }
  });

  it('measures bulk stream writes and replay through the real fleet', async () => {
    const world = createCelldWorld({
      fleetUrl,
      secret,
      baseUrl: process.env.CELLD_CALLBACK_BASE_URL,
      deploymentId: `perf-stream-${runId}`,
      rpcTimeoutMs: timeoutMs,
    });
    const created = await world.events.create(null, {
      eventType: 'run_created',
      eventData: { deploymentId: `perf-stream-${runId}`, workflowName: 'perf-stream', input: [] },
    });
    const streamRunId = created.run.runId;
    const streamName = `perf-bulk-${streamRunId}`;
    const chunks = Array.from({ length: streamChunkCount }, (_, index) =>
      new Uint8Array(streamChunkBytes).fill(index % 251),
    );

    let streamRequests = 0;
    const fetchBefore = globalThis.fetch;
    globalThis.fetch = async (input, init) => {
      const url = input instanceof Request ? input.url : String(input);
      if (new URL(url).pathname.startsWith('/v1/streams/')) streamRequests++;
      return await fetchBefore(input, init);
    };
    const phase = async (action: () => Promise<void>) => {
      streamRequests = 0;
      const began = performance.now();
      await action();
      return { ms: Number((performance.now() - began).toFixed(2)), requests: streamRequests };
    };
    try {
      // Registers the stream so the timed write measures only data requests.
      await world.streams.write(streamRunId, streamName, new Uint8Array([0]));
      const write = await phase(() => world.streams.writeMulti(streamRunId, streamName, chunks));
      await world.streams.close(streamRunId, streamName);

      let paged = 0;
      const page = await phase(async () => {
        let cursor: string | undefined;
        for (;;) {
          const result = await world.streams.getChunks(streamRunId, streamName, {
            limit: 1000,
            cursor,
          });
          paged += result.data.length;
          if (!result.hasMore) break;
          cursor = result.cursor ?? undefined;
        }
      });

      let replayed = 0;
      const replay = await phase(async () => {
        const reader = (await world.streams.get(streamRunId, streamName)).getReader();
        for (;;) {
          const { done } = await reader.read();
          if (done) break;
          replayed++;
        }
      });

      const result = {
        schemaVersion: 1,
        recordedAt: new Date().toISOString(),
        backend: { name: 'minio', celldVersion: process.env.PERF_CELLD_VERSION ?? 'unknown' },
        workload: { chunks: streamChunkCount, chunkBytes: streamChunkBytes },
        correctness: { paged, replayed },
        performance: {
          write: { ...write, chunksPerSecond: rate(streamChunkCount, write.ms) },
          page: { ...page, chunksPerSecond: rate(paged, page.ms) },
          replay: { ...replay, chunksPerSecond: rate(replayed, replay.ms) },
        },
      };
      await mkdir(path.dirname(streamResultPath), { recursive: true });
      await writeFile(streamResultPath, `${JSON.stringify(result, null, 2)}\n`);
      console.log(`\nworld-celld MinIO stream result\n${JSON.stringify(result, null, 2)}`);
      expect(paged).toBe(streamChunkCount + 1);
      expect(replayed).toBe(streamChunkCount + 1);
    } finally {
      globalThis.fetch = fetchBefore;
    }
  });

  it('reclaims terminal run payloads without loss or resurrection', async () => {
    const world = createCelldWorld({
      fleetUrl,
      secret,
      baseUrl: process.env.CELLD_CALLBACK_BASE_URL,
      deploymentId: `retention-perf-${runId}`,
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
      const workflowRunId = created.run.runId;
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
    const cleanupLag = completedStatuses.map(
      (status) => status.tombstonedAt!.getTime() - status.dueAt.getTime(),
    );
    const cleanupStart = Math.min(...completedStatuses.map((status) => status.dueAt.getTime()));
    const cleanupEnd = Math.max(
      ...completedStatuses.map((status) => status.tombstonedAt!.getTime()),
    );
    const setupDurationMs = setupEnd - setupStart;
    const cleanupDurationMs = Math.max(0, cleanupEnd - cleanupStart);

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
        deletedQueuePayloads: completedStatuses.reduce(
          (sum, status) => sum + status.deletedQueuePayloads,
          0,
        ),
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
    expect(result.correctness.deletedQueuePayloads).toBe(retentionRuns);
  });
});
