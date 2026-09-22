import { randomUUID } from 'node:crypto';
import { access, mkdir, writeFile } from 'node:fs/promises';
import http from 'node:http';
import path from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createCelldWorld } from '../../src/index.js';
import { parse } from '../../src/vendor/shared/index.js';

const primaryUrl = process.env.CELLD_FLEET_URL ?? '';
const peerUrl = process.env.CELLD_PEER_URL ?? '';
const secret = process.env.CELLD_WORLD_SECRET ?? '';
const callbackBaseUrl = process.env.CELLD_CALLBACK_BASE_URL ?? '';
const readyPath = process.env.PERF_FAILOVER_READY_PATH ?? '';
const donePath = process.env.PERF_FAILOVER_DONE_PATH ?? '';
const resultPath =
  process.env.PERF_FAILOVER_RESULT_PATH ?? '.perf-results/minio-failover-latest.json';
const timeoutMs = Number(process.env.PERF_TIMEOUT_MS ?? 180_000);
const messageCount = Number(process.env.PERF_FAILOVER_MESSAGES ?? 32);
const marker = randomUUID();

function percentile(values: number[], fraction: number): number {
  if (values.length === 0) return 0;
  const sorted = values.toSorted((a, b) => a - b);
  return Number(sorted[Math.ceil(fraction * sorted.length) - 1].toFixed(2));
}

async function waitFor<T>(read: () => Promise<T | false | null>, label: string): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await read();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  const detail = lastError instanceof Error ? lastError.message : 'no result';
  throw new Error(`timed out waiting for ${label}: ${detail}`);
}

describe('MinIO two-node ownership recovery', () => {
  const started = new Map<number, number>();
  const accepted = new Map<number, string>();
  const delivered = new Map<number, { messageId: string; latencyMs: number }>();
  const invalidCallbacks: string[] = [];
  let duplicateCallbacks = 0;
  let retryAttemptsBeforeFault = 0;
  let listener: http.Server;

  beforeAll(async () => {
    if (
      !primaryUrl ||
      !peerUrl ||
      !secret ||
      !callbackBaseUrl ||
      !readyPath ||
      !donePath ||
      !Number.isSafeInteger(timeoutMs) ||
      timeoutMs <= 0 ||
      !Number.isSafeInteger(messageCount) ||
      messageCount <= 0
    ) {
      throw new Error('invalid two-node perf configuration');
    }
    process.env.CELLD_QUEUE_MODE = 'native';
    listener = http.createServer(async (request, response) => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(chunk as Buffer);
      try {
        const payload = parse<{ __healthCheck: boolean; correlationId: string }>(
          Buffer.concat(chunks).toString('utf8'),
        );
        const [receivedMarker, sequenceText] = payload.correlationId.split('|', 2);
        const sequence = Number(sequenceText);
        const messageId = request.headers['x-vqs-message-id'];
        const begin = started.get(sequence);
        if (
          !payload['__healthCheck'] ||
          receivedMarker !== marker ||
          !Number.isSafeInteger(sequence) ||
          sequence < 0 ||
          sequence >= messageCount * 3 ||
          typeof messageId !== 'string' ||
          begin === undefined
        ) {
          throw new Error('unexpected callback payload or metadata');
        }
        if (sequence >= messageCount * 2) {
          try {
            await access(donePath);
          } catch {
            retryAttemptsBeforeFault++;
            response.writeHead(503, { 'content-type': 'application/json' });
            response.end('{"timeoutSeconds":1}');
            return;
          }
        }
        if (delivered.has(sequence)) duplicateCallbacks++;
        else delivered.set(sequence, { messageId, latencyMs: performance.now() - begin });
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end('{"ok":true}');
      } catch (error) {
        invalidCallbacks.push(String(error));
        response.writeHead(400).end();
      }
    });
    const port = Number(new URL(callbackBaseUrl).port);
    await new Promise<void>((resolve, reject) => {
      listener.once('error', reject);
      listener.listen(port, '0.0.0.0', resolve);
    });
  });

  afterAll(async () => {
    delete process.env.CELLD_QUEUE_MODE;
    if (!listener) return;
    listener.closeAllConnections();
    await new Promise<void>((resolve, reject) => {
      listener.close((error) => (error ? reject(error) : resolve()));
    });
  });

  it('preserves state and delivery after the owner is killed', async () => {
    const deploymentId = `failover-${marker}`;
    const world = (fleetUrl: string) =>
      createCelldWorld({
        fleetUrl,
        secret,
        baseUrl: callbackBaseUrl,
        deploymentId,
        rpcTimeoutMs: 10_000,
      });
    const primary = world(primaryUrl);
    const created = await primary.events.create(null, {
      eventType: 'run_created',
      eventData: { deploymentId, workflowName: 'failover-perf', input: [marker] },
    });
    const runId = created.run.runId;
    const streamName = `failover-stream-${marker}`;
    await primary.writeToStream(streamName, runId, 'before');

    const send = async (client: ReturnType<typeof world>, first: number) => {
      const began = performance.now();
      await Promise.all(
        Array.from({ length: messageCount }, async (_, offset) => {
          const sequence = first + offset;
          started.set(sequence, performance.now());
          const queued = await client.queue(
            `__wkf_workflow_failover_${marker.replaceAll('-', '')}`,
            {
              __healthCheck: true,
              correlationId: `${marker}|${sequence}`,
            },
          );
          accepted.set(sequence, String(queued.messageId));
        }),
      );
      return performance.now() - began;
    };
    const beforeMs = await send(primary, 0);
    await waitFor(async () => delivered.size >= messageCount, 'pre-failure deliveries');
    await send(primary, messageCount * 2);
    await waitFor(async () => retryAttemptsBeforeFault > 0, 'accepted retrying work');
    await mkdir(path.dirname(readyPath), { recursive: true });
    await writeFile(readyPath, JSON.stringify({ runId, accepted: accepted.size }));
    await waitFor(async () => {
      try {
        await access(donePath);
        return true;
      } catch {
        return false;
      }
    }, 'primary termination signal');

    const recoveryStarted = performance.now();
    const peer = world(peerUrl);
    await waitFor(async () => {
      const run = await peer.runs.get(runId);
      return run.runId === runId;
    }, 'peer ownership and run read');
    const recoveryMs = performance.now() - recoveryStarted;
    const chunks = await peer.getStreamChunks(streamName, runId, {});
    expect(new TextDecoder().decode(chunks.data[0]?.data)).toBe('before');
    await peer.writeToStream(streamName, runId, 'after');
    const postWrite = await peer.getStreamChunks(streamName, runId, {});
    expect(new TextDecoder().decode(postWrite.data[1]?.data)).toBe('after');
    const afterMs = await send(peer, messageCount);
    await waitFor(async () => delivered.size === messageCount * 3, 'post-failure deliveries');

    const mismatched = Array.from(accepted, ([sequence, messageId]) => ({
      expected: messageId,
      actual: delivered.get(sequence)?.messageId,
    })).filter(({ expected, actual }) => expected !== actual);
    const preLatencies = Array.from(delivered.entries())
      .filter(([sequence]) => sequence < messageCount)
      .map(([, delivery]) => delivery.latencyMs);
    const postLatencies = Array.from(delivered.entries())
      .filter(([sequence]) => sequence >= messageCount && sequence < messageCount * 2)
      .map(([, delivery]) => delivery.latencyMs);
    const pendingLatencies = Array.from(delivered.entries())
      .filter(([sequence]) => sequence >= messageCount * 2)
      .map(([, delivery]) => delivery.latencyMs);
    const result = {
      schemaVersion: 1,
      recordedAt: new Date().toISOString(),
      workload: { messagesPerPhase: messageCount, phases: 3, nodes: 2, fault: 'primary SIGKILL' },
      correctness: {
        runRestored: true,
        streamRestored: true,
        accepted: accepted.size,
        delivered: delivered.size,
        retryAttemptsBeforeFault,
        duplicateCallbacks,
        mismatchedMessageIds: mismatched.length,
        invalidCallbacks,
      },
      performance: {
        recoveryMs: Number(recoveryMs.toFixed(2)),
        preFailureEnqueuePerSecond: Number(((messageCount * 1_000) / beforeMs).toFixed(2)),
        postFailureEnqueuePerSecond: Number(((messageCount * 1_000) / afterMs).toFixed(2)),
        preFailureDeliveryP99Ms: percentile(preLatencies, 0.99),
        postFailureDeliveryP99Ms: percentile(postLatencies, 0.99),
        pendingRecoveryDeliveryP99Ms: percentile(pendingLatencies, 0.99),
      },
    };
    await mkdir(path.dirname(resultPath), { recursive: true });
    await writeFile(resultPath, `${JSON.stringify(result, null, 2)}\n`);
    console.log(`\nworld-celld two-node failover result\n${JSON.stringify(result, null, 2)}`);
    expect(invalidCallbacks).toEqual([]);
    expect(accepted.size).toBe(messageCount * 3);
    expect(delivered.size).toBe(messageCount * 3);
    expect(retryAttemptsBeforeFault).toBeGreaterThan(0);
    expect(duplicateCallbacks).toBe(0);
    expect(mismatched).toEqual([]);
  });
});
