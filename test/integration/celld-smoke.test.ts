import { randomUUID } from 'node:crypto';
import { execFile, spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';
import { RunExpiredError } from '@workflow/errors';
import { build } from 'esbuild';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createCelldWorld } from '../../src/index.js';

const execFileAsync = promisify(execFile);
const CELLD_BIN = process.env.CELLD_SMOKE_CELLD_BIN;
const MINIO_BIN = process.env.CELLD_SMOKE_MINIO_BIN;
const MC_BIN = process.env.CELLD_SMOKE_MC_BIN;
const CONFIGURED = Boolean(CELLD_BIN && MINIO_BIN && MC_BIN);
const SECRET = 'world-celld-smoke-secret';
const BUCKET = 'world-celld-smoke';
const ACCESS_KEY = 'world-celld-smoke-access';
const SECRET_KEY = 'world-celld-smoke-secret-key';
const MAX_PROCESS_LOG_BYTES = 128 * 1024;

interface CapturedDelivery {
  body: string;
  headers: http.IncomingHttpHeaders;
  receivedAt: number;
}

interface ManagedProcess {
  child: ChildProcessWithoutNullStreams;
  logs: () => string;
  name: string;
}

interface RestartEvidence {
  oldPid: number;
  newPid: number;
  startedAt: number;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor<T>(
  poll: () => Promise<T | null | undefined | false>,
  timeoutMs: number,
  label: string,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await poll();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await delay(100);
  }
  const suffix = lastError instanceof Error ? `: ${lastError.message}` : '';
  throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}${suffix}`);
}

async function freePort(): Promise<number> {
  const server = http.createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}

function startManaged(
  name: string,
  command: string,
  args: string[],
  env: NodeJS.ProcessEnv,
): ManagedProcess {
  const child = spawn(command, args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  const capture = (chunk: Buffer) => {
    output += chunk.toString('utf8');
    if (output.length > MAX_PROCESS_LOG_BYTES) output = output.slice(-MAX_PROCESS_LOG_BYTES);
  };
  child.stdout.on('data', capture);
  child.stderr.on('data', capture);
  child.on('error', (error) => capture(Buffer.from(`\nprocess error: ${String(error)}\n`)));
  return { child, logs: () => output, name };
}

async function waitForExit(
  process: ManagedProcess,
  timeoutMs: number,
): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
  if (process.child.exitCode !== null || process.child.signalCode !== null) {
    return { code: process.child.exitCode, signal: process.child.signalCode };
  }
  return await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`${process.name} did not exit within ${timeoutMs}ms`)),
      timeoutMs,
    );
    process.child.once('exit', (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal });
    });
  });
}

async function stopManaged(
  process: ManagedProcess | undefined,
  signal: NodeJS.Signals = 'SIGTERM',
): Promise<void> {
  if (!process || process.child.exitCode !== null || process.child.signalCode !== null) return;
  process.child.kill(signal);
  try {
    await waitForExit(process, 5_000);
  } catch {
    process.child.kill('SIGKILL');
    await waitForExit(process, 5_000);
  }
}

async function prepareWorker(
  destination: string,
  entryPoint: string,
  configPath: string,
): Promise<void> {
  await mkdir(destination, { recursive: true });
  await build({
    entryPoints: [entryPoint],
    bundle: true,
    format: 'esm',
    platform: 'browser',
    target: 'es2024',
    conditions: ['workerd', 'worker', 'browser'],
    external: ['cloudflare:*'],
    outfile: join(destination, 'index.js'),
    logLevel: 'silent',
  });

  const source = await readFile(configPath, 'utf8');
  const main = '"main": "worker.ts",';
  if (!source.includes(main)) throw new Error(`${configPath} main entry changed`);
  await writeFile(
    join(destination, 'wrangler.jsonc'),
    source.replace(main, '"main": "index.js",\n  "no_bundle": true,'),
  );
}

class NativeCelldRuntime {
  private celld: ManagedProcess | undefined;
  private readonly baseEnv: NodeJS.ProcessEnv;

  constructor(
    private readonly binary: string,
    private readonly endpoint: string,
    private readonly publicPort: number,
    private readonly internalPort: number,
    private readonly watchDirectory: string,
  ) {
    this.baseEnv = {
      ...process.env,
      AWS_ACCESS_KEY_ID: ACCESS_KEY,
      AWS_SECRET_ACCESS_KEY: SECRET_KEY,
      AWS_REGION: 'us-east-1',
      AWS_EC2_METADATA_DISABLED: 'true',
      CELLD_DURABILITY: 'bucket',
      CELLD_MAX_RSS_MB: '0',
      CELLD_NODE: 'world-celld-smoke-node',
      CELLD_OPERATION_DEADLINE_MS: '10000',
      CELLD_STORAGE_PROBE: '0',
      CELLD_TTL_MS: '2000',
      CELLD_VAR_QUEUE_DELIVERY_TIMEOUT_MS: '5000',
      CELLD_VAR_QUEUE_MAX_INFLIGHT: '1',
      CELLD_VAR_WORLD_SECRET: SECRET,
      CELLD_WAKER_TICK_MS: '50',
      CELLD_WATCH: watchDirectory,
      RUST_LOG: 'info',
    };
  }

  get url(): string {
    return `http://127.0.0.1:${this.publicPort}`;
  }

  private spawn(): ManagedProcess {
    return startManaged(
      'celld',
      this.binary,
      [
        '--bucket',
        `s3://${BUCKET}`,
        '--endpoint',
        this.endpoint,
        '--region',
        'us-east-1',
        '--listen',
        `127.0.0.1:${this.publicPort}`,
        '--internal-listen',
        `127.0.0.1:${this.internalPort}`,
      ],
      this.baseEnv,
    );
  }

  async start(): Promise<void> {
    if (this.celld && this.celld.child.exitCode === null && this.celld.child.signalCode === null) {
      throw new Error('celld is already running');
    }
    this.celld = this.spawn();
    await waitFor(
      async () => {
        if (this.celld?.child.exitCode !== null || this.celld?.child.signalCode !== null) {
          throw new Error(`celld exited during startup\n${this.celld?.logs() ?? ''}`);
        }
        const response = await fetch(`${this.url}/v1/health`, {
          signal: AbortSignal.timeout(1_000),
        });
        return response.ok ? true : null;
      },
      45_000,
      'celld readiness',
    );
  }

  async restart(downtimeMs: number, dropLocalState = false): Promise<RestartEvidence> {
    const current = this.celld;
    const oldPid = current?.child.pid;
    if (!current || oldPid === undefined) throw new Error('celld is not running');
    if (!current.child.kill('SIGKILL')) throw new Error('failed to SIGKILL celld');
    const exit = await waitForExit(current, 5_000);
    if (exit.signal !== 'SIGKILL') {
      throw new Error(
        `celld restart expected SIGKILL, got code=${exit.code} signal=${exit.signal}`,
      );
    }
    if (dropLocalState) {
      await rm(this.watchDirectory, { recursive: true, force: true });
      await mkdir(this.watchDirectory, { recursive: true });
    }
    await delay(downtimeMs);
    const startedAt = Date.now();
    await this.start();
    const newPid = this.celld?.child.pid;
    if (newPid === undefined) throw new Error('restarted celld has no pid');
    return { oldPid, newPid, startedAt };
  }

  async stop(): Promise<void> {
    await stopManaged(this.celld);
  }

  killNow(): void {
    if (this.celld?.child.exitCode === null && this.celld.child.signalCode === null) {
      this.celld.child.kill('SIGKILL');
    }
  }

  logs(): string {
    return this.celld?.logs() ?? '';
  }
}

describe.skipIf(!CONFIGURED)('real celld v0.4.0 native-services restart smoke', () => {
  const deliveries: CapturedDelivery[] = [];
  let temporaryRoot: string;
  let minio: ManagedProcess | undefined;
  let runtime: NativeCelldRuntime | undefined;
  let callbackServer: http.Server | undefined;
  let callbackBaseUrl: string;
  let firstRunId: string;

  const emergencyStop = () => {
    runtime?.killNow();
    if (minio?.child.exitCode === null && minio.child.signalCode === null) {
      minio.child.kill('SIGKILL');
    }
    callbackServer?.closeAllConnections();
  };

  beforeAll(async () => {
    const requestedRoot = process.env.CELLD_SMOKE_TEMP_ROOT;
    temporaryRoot = requestedRoot ?? (await mkdtemp(join(tmpdir(), 'world-celld-smoke-')));
    if (requestedRoot) await mkdir(temporaryRoot, { recursive: true });
    process.env.CELLD_QUEUE_MODE = 'native';
    process.once('exit', emergencyStop);

    try {
      const minioPort = await freePort();
      const minioConsolePort = await freePort();
      const celldPort = await freePort();
      const celldInternalPort = await freePort();
      const minioUrl = `http://127.0.0.1:${minioPort}`;
      const storageEnv = {
        ...process.env,
        MINIO_ROOT_USER: ACCESS_KEY,
        MINIO_ROOT_PASSWORD: SECRET_KEY,
      };

      minio = startManaged(
        'minio',
        MINIO_BIN!,
        [
          'server',
          join(temporaryRoot, 'minio-data'),
          '--address',
          `127.0.0.1:${minioPort}`,
          '--console-address',
          `127.0.0.1:${minioConsolePort}`,
        ],
        storageEnv,
      );
      await waitFor(
        async () => {
          if (minio?.child.exitCode !== null || minio?.child.signalCode !== null) {
            throw new Error(`minio exited during startup\n${minio?.logs() ?? ''}`);
          }
          const response = await fetch(`${minioUrl}/minio/health/live`, {
            signal: AbortSignal.timeout(1_000),
          });
          return response.ok ? true : null;
        },
        30_000,
        'MinIO readiness',
      );

      const mcConfig = join(temporaryRoot, 'mc-config');
      const mcEnv = { ...process.env, MC_CONFIG_DIR: mcConfig };
      await waitFor(
        async () => {
          if (minio?.child.exitCode !== null || minio?.child.signalCode !== null) {
            throw new Error(`minio exited during client initialization\n${minio?.logs() ?? ''}`);
          }
          await execFileAsync(
            MC_BIN!,
            ['alias', 'set', 'smoke', minioUrl, ACCESS_KEY, SECRET_KEY],
            { env: mcEnv },
          );
          return true;
        },
        30_000,
        'MinIO client readiness',
      );
      await execFileAsync(MC_BIN!, ['mb', '--ignore-existing', `smoke/${BUCKET}`], { env: mcEnv });

      const workerDirectory = join(temporaryRoot, 'worker');
      const queueWorkerDirectory = join(temporaryRoot, 'queue-worker');
      await prepareWorker(workerDirectory, 'dist/worker.js', 'celld-worker/wrangler.jsonc');
      await prepareWorker(
        queueWorkerDirectory,
        'dist/queue-consumer.js',
        'celld-queue-worker/wrangler.jsonc',
      );
      const storageClientEnv = {
        ...process.env,
        AWS_ACCESS_KEY_ID: ACCESS_KEY,
        AWS_SECRET_ACCESS_KEY: SECRET_KEY,
        AWS_REGION: 'us-east-1',
        AWS_EC2_METADATA_DISABLED: 'true',
      };
      const version = await execFileAsync(CELLD_BIN!, ['--version'], { env: storageClientEnv });
      if (!/\b0\.4\.0\b/.test(version.stdout)) {
        throw new Error(`expected celld v0.4.0, got ${version.stdout.trim()}`);
      }
      // The consumer deploy establishes the Queue attachment and its named
      // script pointer. The primary deploy goes last so it remains the public
      // application selected by the fleet-wide pointer.
      await execFileAsync(
        CELLD_BIN!,
        [
          'deploy',
          queueWorkerDirectory,
          '--bucket',
          `s3://${BUCKET}`,
          '--endpoint',
          minioUrl,
          '--region',
          'us-east-1',
        ],
        { env: storageClientEnv, maxBuffer: 4 * 1024 * 1024 },
      );
      await execFileAsync(
        CELLD_BIN!,
        [
          'deploy',
          workerDirectory,
          '--bucket',
          `s3://${BUCKET}`,
          '--endpoint',
          minioUrl,
          '--region',
          'us-east-1',
        ],
        { env: storageClientEnv, maxBuffer: 4 * 1024 * 1024 },
      );

      callbackServer = http.createServer(async (request, response) => {
        const chunks: Buffer[] = [];
        for await (const chunk of request) chunks.push(chunk as Buffer);
        deliveries.push({
          body: Buffer.concat(chunks).toString('utf8'),
          headers: request.headers,
          receivedAt: Date.now(),
        });
        response.writeHead(200, { 'content-type': 'application/json' });
        response.end('{"ok":true}');
      });
      await new Promise<void>((resolve) => callbackServer!.listen(0, '127.0.0.1', resolve));
      const callbackPort = (callbackServer.address() as AddressInfo).port;
      callbackBaseUrl = `http://127.0.0.1:${callbackPort}`;

      runtime = new NativeCelldRuntime(
        CELLD_BIN!,
        minioUrl,
        celldPort,
        celldInternalPort,
        join(temporaryRoot, 'celld-state'),
      );
      await runtime.start();
    } catch (error) {
      await runtime?.stop();
      await stopManaged(minio);
      await rm(temporaryRoot, { recursive: true, force: true });
      process.off('exit', emergencyStop);
      throw error;
    }
  });

  afterAll(async () => {
    delete process.env.CELLD_QUEUE_MODE;
    await runtime?.stop();
    if (callbackServer) {
      await new Promise<void>((resolve, reject) => {
        callbackServer!.close((error) => (error ? reject(error) : resolve()));
        callbackServer!.closeAllConnections();
      });
    }
    await stopManaged(minio);
    if (temporaryRoot) await rm(temporaryRoot, { recursive: true, force: true });
    process.off('exit', emergencyStop);
  });

  function world(options: {
    deploymentId: string;
    runRetentionMs?: number;
    streamLongPollMs?: number;
  }) {
    return createCelldWorld({
      fleetUrl: runtime!.url,
      secret: SECRET,
      baseUrl: callbackBaseUrl,
      ...options,
    });
  }

  it('recovers durable state and one accepted native Queue delivery after a process restart', async () => {
    const deploymentId = `restart-${randomUUID()}`;
    const w = world({ deploymentId });
    const workflowName = `restart-${randomUUID()}`;
    const created = await w.events.create(null, {
      eventType: 'run_created',
      eventData: { deploymentId, workflowName, input: ['durable-before-restart'] },
    });
    firstRunId = created.run.runId;
    const streamName = `restart-stream-${randomUUID()}`;
    await w.writeToStream(streamName, firstRunId, 'durable-stream-chunk');

    const marker = randomUUID();
    const before = deliveries.length;
    const { messageId } = await w.queue(
      `__wkf_workflow_restart_${marker.slice(0, 8)}`,
      { __healthCheck: true, correlationId: marker },
      { delaySeconds: 2, idempotencyKey: `restart:${marker}` },
    );
    expect(deliveries.slice(before).filter((delivery) => delivery.body.includes(marker))).toEqual(
      [],
    );

    // Keep celld down past the queue deadline. Delivery therefore requires the
    // restarted runtime to restore the accepted broker message.
    const restart = await runtime!.restart(2_500, true);
    expect(restart.newPid).not.toBe(restart.oldPid);

    const restored = await waitFor(
      async () => {
        const run = await w.runs.get(firstRunId);
        return run.workflowName === workflowName ? run : null;
      },
      45_000,
      'run state after celld restart',
    );
    expect(restored.status).toBe('pending');
    const stream = await w.getStreamChunks(streamName, firstRunId, {});
    expect(new TextDecoder().decode(stream.data[0].data)).toBe('durable-stream-chunk');

    const delivered = await waitFor(
      async () => deliveries.slice(before).find((delivery) => delivery.body.includes(marker)),
      45_000,
      'overdue native Queue delivery after celld restart',
    );
    expect(delivered.receivedAt).toBeGreaterThanOrEqual(restart.startedAt);
    expect(delivered.headers['x-vqs-message-id']).toBe(messageId);
    expect(delivered.headers['x-vqs-message-attempt']).toBe('1');
    await delay(1_500);
    expect(
      deliveries.slice(before).filter((delivery) => delivery.body.includes(marker)),
    ).toHaveLength(1);
  });

  it('keeps a stream usable after cancelling an in-flight HTTP long poll', async () => {
    const w = world({
      deploymentId: `long-poll-${randomUUID()}`,
      streamLongPollMs: 20_000,
    });
    const streamName = `abort-stream-${randomUUID()}`;
    const readable = await w.readFromStream(streamName, firstRunId);
    const reader = readable.getReader();
    let settled = false;
    const pending = reader.read();
    void pending.then(
      () => {
        settled = true;
        return undefined;
      },
      () => {
        settled = true;
        return undefined;
      },
    );

    await delay(300);
    expect(settled).toBe(false);
    await reader.cancel('integration reader disconnected');
    await expect(pending).resolves.toMatchObject({ done: true });

    await w.writeToStream(streamName, firstRunId, 'usable-after-abort');
    await w.closeStream(streamName, firstRunId);
    const chunks = await w.getStreamChunks(streamName, firstRunId, {});
    expect(chunks.done).toBe(true);
    expect(chunks.data).toHaveLength(1);
    expect(new TextDecoder().decode(chunks.data[0].data)).toBe('usable-after-abort');
  });

  it('resumes multi-page retention cleanup from durable progress after SIGKILL', async () => {
    const deploymentId = `retention-restart-${randomUUID()}`;
    const w = world({ deploymentId, runRetentionMs: 3_600_000 });
    const created = await w.events.create(null, {
      eventType: 'run_created',
      eventData: { deploymentId, workflowName: deploymentId, input: ['retention-payload'] },
    });
    const runId = created.run.runId;
    const streamName = `retention-restart-stream-${randomUUID()}`;
    const chunks = Array.from({ length: 4_097 }, (_, index) => Uint8Array.of(index % 251));
    await w.writeChunksToStream(streamName, runId, chunks);
    await w.closeStream(streamName, runId);
    await w.queue(
      `__wkf_workflow_retention_restart_${randomUUID().slice(0, 8)}`,
      { runId },
      { delaySeconds: 3_600, idempotencyKey: `retention-restart:${runId}` },
    );
    await w.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output: ['done'] },
    });

    const interrupted = await w.retention.cleanupNow(runId);
    expect(interrupted).not.toBeNull();
    expect(interrupted?.phase).not.toBe('tombstoned');
    expect(interrupted?.generation).toBeGreaterThan(0);

    // The next action is an ungraceful process kill. The 4,097 chunks require
    // 33 bounded stream-cleanup pages, so the returned nonterminal progress
    // cannot be collapsed into the initiating RPC.
    const restart = await runtime!.restart(250, true);
    expect(restart.newPid).not.toBe(restart.oldPid);

    const tombstone = await waitFor(
      async () => {
        const status = await w.retention.getStatus(runId);
        return status?.phase === 'tombstoned' ? status : null;
      },
      60_000,
      `retention cleanup after restart\n${runtime!.logs()}`,
    );
    expect(tombstone.generation).toBeGreaterThan(interrupted!.generation);
    expect(tombstone.deletedStreams).toBe(1);
    expect(tombstone.deletedQueuePayloads).toBe(1);
    expect(tombstone.deletedPayloadKeys).toBeGreaterThan(0);
    await expect(w.runs.get(runId)).rejects.toSatisfy((error) => RunExpiredError.is(error));
    await expect(w.getStreamInfo(streamName, runId)).rejects.toThrow(/expired/);
  });
});
