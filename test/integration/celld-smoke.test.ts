import { randomUUID, randomBytes, createHash } from 'node:crypto';
import { execFile, spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, symlink, copyFile, writeFile } from 'node:fs/promises';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve as resolvePath } from 'node:path';
import { promisify } from 'node:util';
import { RunExpiredError } from '@workflow/errors';
import { SPEC_VERSION_CURRENT } from '@workflow/world';
import { build } from 'esbuild';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createCelldWorld } from '../../src/index.js';
import { rpcParse, rpcStringify } from '../../src/codec.js';
import type { NativeQueueEnvelope, NativeQueueBatchResult } from '../../src/queue-protocol.js';

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

function percentile(values: number[], p: number): number {
  return values.toSorted((a, b) => a - b)[Math.ceil(values.length * p) - 1];
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

  let source = await readFile(configPath, 'utf8');
  if (configPath === 'celld-worker/wrangler.jsonc') {
    // Test-only HTTP driver for the named RPC boundary. Never packaged or deployed
    // outside this temporary fixture; normal delivery uses the companion script.
    await writeFile(
      join(destination, 'implementation.js'),
      await readFile(join(destination, 'index.js')),
    );
    await writeFile(
      join(destination, 'index.js'),
      `
      export * from './implementation.js';
      import worker from './implementation.js';
      export default {
        ...worker,
        async fetch(request, env) {
          const gate = request.headers.get('x-test-broker-gate');
          if (gate && new URL(request.url).pathname === '/v1/queue/send-batch') {
            const binding = env.WORKFLOW_QUEUE;
            return worker.fetch(request, {...env, WORKFLOW_QUEUE: {
              send: binding.send.bind(binding),
              async sendBatch(messages) {
                await binding.sendBatch(messages);
                await fetch(gate, {method: 'POST', body: JSON.stringify(messages)});
              },
            }});
          }
          if (new URL(request.url).pathname !== '/__test/queue-rpc') return worker.fetch(request, env);
          const [secret, envelope, attempt] = await request.json();
          try {
            return Response.json(await env.TEST_QUEUE_RPC.deliver(secret, envelope, attempt));
          } catch (error) {
            return Response.json({error: String(error)}, {status: 400});
          }
        },
      };
    `,
    );
    const wrapped = await build({
      entryPoints: [join(destination, 'index.js')],
      bundle: true,
      format: 'esm',
      platform: 'browser',
      target: 'es2024',
      conditions: ['workerd', 'worker', 'browser'],
      external: ['cloudflare:*'],
      write: false,
      logLevel: 'silent',
    });
    await writeFile(join(destination, 'index.js'), wrapped.outputFiles[0].contents);
    source = source.replace(
      '"vars": {',
      '"services": [{"binding":"TEST_QUEUE_RPC","service":"workflow-world","entrypoint":"QueueDeliveryRpc"}],\n  "vars": {',
    );
  }
  const main = '"main": "worker.ts",';
  if (!source.includes(main)) throw new Error(`${configPath} main entry changed`);
  await writeFile(
    join(destination, 'wrangler.jsonc'),
    source
      .replace(main, '"main": "index.js",\n  "no_bundle": true,')
      .replace('"WORLD_SECRET": ""', `"WORLD_SECRET": ${JSON.stringify(SECRET)}`),
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
      CELLD_OTEL: '1',
      CELLD_OTEL_FLUSH_MS: '1000',
      CELLD_OPERATION_DEADLINE_MS: '10000',
      // Restart checks use a short lease; load measurements need headroom for
      // object-store renewal under concurrent payload publication.
      CELLD_TTL_MS: process.env.CELLD_QUEUE_BENCHMARK ? '30000' : '2000',
      CELLD_WAKER_TICK_MS: '50',
      CELLD_WATCH: watchDirectory,
      RUST_LOG: 'info',
    };
  }

  get pid(): number {
    if (!this.celld?.child.pid) throw new Error('celld is not running');
    return this.celld.child.pid;
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

describe.skipIf(!CONFIGURED)('real celld v0.6.0 native-services restart smoke', () => {
  const deliveries: CapturedDelivery[] = [];
  const suspendedQueues = new Set<string>();
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
      if (!/\b0\.6\.0\b/.test(version.stdout)) {
        throw new Error(`expected celld v0.6.0, got ${version.stdout.trim()}`);
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
        const queueName = String(request.headers['x-vqs-queue-name'] ?? '');
        const matching = deliveries.filter((d) => d.headers['x-vqs-queue-name'] === queueName);
        if (suspendedQueues.has(queueName)) {
          response.writeHead(503, { 'content-type': 'application/json' });
          response.end('{"timeoutSeconds":5}');
          return;
        }
        if (matching.length === 1 && queueName.includes('rpc_suspend')) {
          response.writeHead(503, { 'content-type': 'application/json' });
          response.end('{"timeoutSeconds":1}');
          return;
        }
        if (matching.length === 1 && queueName.includes('rpc_retry')) {
          response.writeHead(500);
          response.end('transient');
          return;
        }
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

  async function sdkHost(
    version: 7 | 8,
    path: 'single' | 'batch',
    execution: 'queued' | 'inline' = 'queued',
  ) {
    const directory = join(temporaryRoot, `sdk-v${version}-${path}-${execution}`);
    await mkdir(directory, { recursive: true });
    const fixture = resolvePath(`test/fixtures/workflow-v${version}`);
    await symlink(join(fixture, 'node_modules'), join(directory, 'node_modules'), 'dir');
    await copyFile(join(fixture, 'package.json'), join(directory, 'package.json'));
    await copyFile('test/fixtures/queue-fanout/fanout.ts', join(directory, 'fanout.ts'));
    await copyFile('test/fixtures/queue-fanout/server.mjs', join(directory, 'server.mjs'));
    await execFileAsync(
      process.execPath,
      [join(fixture, 'node_modules/workflow/bin/run.js'), 'build'],
      {
        cwd: directory,
        env: process.env,
        maxBuffer: 4 * 1024 * 1024,
      },
    );
    const port = await freePort();
    const url = `http://127.0.0.1:${port}`;
    const app = startManaged(
      `sdk-v${version}-${path}-${execution}`,
      process.execPath,
      [join(directory, 'server.mjs')],
      {
        ...process.env,
        PORT: String(port),
        WORKFLOW_BASE_URL: url,
        WORKFLOW_TARGET_WORLD: resolvePath('test/fixtures/queue-fanout/world.mjs'),
        CELLD_FLEET_URL: runtime!.url,
        CELLD_WORLD_SECRET: SECRET,
        CELLD_DEPLOYMENT_ID: `sdk-v${version}-${path}-${randomUUID()}`,
        QUALIFICATION_QUEUE_PATH: path,
        WORKFLOW_TURBO: execution === 'queued' ? '0' : '1',
      },
    );
    try {
      await waitFor(
        async () => {
          if (app.child.exitCode !== null) throw new Error(app.logs());
          return (await fetch(`${url}/health`)).ok;
        },
        30_000,
        `SDK host readiness\n${app.logs()}`,
      );
    } catch (error) {
      await stopManaged(app);
      throw new Error(`${String(error)}\n${app.logs()}`, { cause: error });
    }
    return { app, url };
  }

  interface FanoutValue {
    results: Array<{
      index: number;
      length: number;
      attempt: number;
      startedAt: number;
      digest: string;
    }>;
    sum: number;
  }

  async function invokeFanout(url: string, count: number, input: string, retry: boolean) {
    const startedAt = Date.now();
    const response = await fetch(`${url}/invoke`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify([count, input, retry]),
    });
    if (!response.ok) throw new Error(await response.text());
    expect(response.status).toBe(200);
    const { runId } = (await response.json()) as { runId: string };
    const value = await waitFor(
      async () => {
        const status = (await fetch(`${url}/runs/${runId}`).then((r) => r.json())) as {
          status: string;
          value?: FanoutValue;
          error?: unknown;
        };
        if (status.status === 'failed') throw new Error(JSON.stringify(status.error));
        return status.status === 'completed' ? status.value : null;
      },
      120_000,
      `SDK fanout ${count}, run ${runId}`,
    );
    expect(value.results.map((result) => result.index)).toEqual(
      Array.from({ length: count }, (_, i) => i),
    );
    expect(value.results.every((result) => result.length === input.length)).toBe(true);
    const digest = createHash('sha256').update(input).digest('hex');
    expect(value.results.every((result) => result.digest === digest)).toBe(true);
    expect(value.sum).toBe((count * (count - 1)) / 2);
    expect(value.results[0].attempt).toBe(retry ? 2 : 1);
    return {
      runId,
      count,
      bytes: input.length,
      firstStepMs: Math.min(...value.results.map((result) => result.startedAt)) - startedAt,
      completionMs: Date.now() - startedAt,
    };
  }

  it('completes SDK v8 queued Promise.all fan-out, retries, and offloaded inputs', async () => {
    const { app, url } = await sdkHost(8, 'batch');
    try {
      for (const count of [1, 8, 32, 128]) {
        const result = await invokeFanout(url, count, 'qualification', false);
        console.log(`CELLD_SDK_FANOUT ${JSON.stringify({ version: 8, ...result })}`);
      }
      await invokeFanout(url, 8, randomBytes(150_000).toString('base64'), true);
      const metrics = (await fetch(`${url}/metrics`).then((r) => r.json())) as {
        batches: number;
        singles: number;
        queueBatchPresent: boolean;
        eventBatchPresent: boolean;
      };
      // beta.58 gates queueBatch on events.createBatch, absent in this adapter.
      expect(metrics.batches).toBe(0);
      expect(metrics.singles).toBeGreaterThan(128);
      expect(metrics.queueBatchPresent).toBe(true);
      expect(metrics.eventBatchPresent).toBe(false);
    } finally {
      await stopManaged(app);
    }
  }, 300_000);

  it('completes default inline fan-out without relying on queue batching', async () => {
    const { app, url } = await sdkHost(8, 'batch', 'inline');
    try {
      await invokeFanout(url, 128, 'inline-qualification', false);
      const metrics = (await fetch(`${url}/metrics`).then((r) => r.json())) as { batches: number };
      expect(metrics.batches).toBe(0);
    } finally {
      await stopManaged(app);
    }
  });

  it('preserves the documented v7 SDK rejection of a World declaring v8', async () => {
    await expect(
      execFileAsync(
        process.execPath,
        [
          '--input-type=module',
          '-e',
          "import {getWorld} from '@workflow/core/runtime'; await getWorld();",
        ],
        {
          cwd: resolvePath('test/fixtures/workflow-v7'),
          env: {
            ...process.env,
            WORKFLOW_TARGET_WORLD: resolvePath('test/fixtures/queue-fanout/world.mjs'),
            CELLD_FLEET_URL: runtime!.url,
            CELLD_WORLD_SECRET: SECRET,
          },
        },
      ),
    ).rejects.toThrow(
      /supports Worlds with spec version 6 through 7.*World declares spec version 8/,
    );
  });

  it.skipIf(!process.env.CELLD_QUEUE_BENCHMARK)(
    'measures alternating SDK fan-out batches against singles',
    async () => {
      const hosts = { single: await sdkHost(8, 'single'), batch: await sdkHost(8, 'batch') };
      type Path = keyof typeof hosts;
      const samples: Array<{
        path: Path;
        bytes: number;
        concurrency: number;
        round: number;
        firstStepMs: number;
        completionMs: number;
        runId: string;
      }> = [];
      const groups: Array<Record<string, unknown>> = [];
      async function processResources(pid: number) {
        const { stdout } = await execFileAsync('ps', ['-p', String(pid), '-o', 'time=,rss=']);
        const [time, rss] = stdout.trim().split(/\s+/);
        const parts = time.split(':').map(Number);
        const cpuMs = parts.reduce((total, part) => total * 60 + part, 0) * 1000;
        return { cpuMs, rssKiB: Number(rss) };
      }
      const metrics = (path: Path) =>
        fetch(`${hosts[path].url}/metrics`).then((r) => r.json()) as Promise<{
          singles: number;
          batches: number;
          entries: number;
          resources: { userCPUTime: number; systemCPUTime: number; maxRSS: number };
          memory: { rss: number };
        }>;
      const largeInput = randomBytes(150_000).toString('base64');
      const nativeGroups: Array<{
        path: Path;
        bytes: number;
        concurrency: number;
        round: number;
        publicationMs: number;
        firstDeliveryMs: number;
        completionMs: number;
        celldCpuMs: number;
        celldRssKiB: number;
        minioCpuMs: number;
        minioRssKiB: number;
      }> = [];
      try {
        for (const bytes of [0, 200_000]) {
          for (const concurrency of [1, 4]) {
            for (let round = -1; round < 10; round++) {
              const paths: Path[] = round % 2 === 0 ? ['single', 'batch'] : ['batch', 'single'];
              for (const path of paths) {
                const deploymentId = `native-ab-${randomUUID()}`;
                const w = world({ deploymentId });
                const queueName = `__wkf_workflow_native_ab_${randomUUID().slice(0, 8)}`;
                const runs = await Promise.all(
                  Array.from({ length: concurrency }, () =>
                    w.events.create(null, {
                      eventType: 'run_created',
                      eventData: { deploymentId, workflowName: deploymentId, input: [] },
                    }),
                  ),
                );
                const messages = runs.map(({ run }) =>
                  Array.from({ length: 32 }, (_, index) => ({
                    message:
                      bytes === 0
                        ? { runId: run.runId, stepId: `step_${index}` }
                        : {
                            runId: run.runId,
                            runInput: {
                              input: largeInput,
                              deploymentId,
                              workflowName: deploymentId,
                              specVersion: SPEC_VERSION_CURRENT,
                            },
                          },
                    opts: { idempotencyKey: `${run.runId}-${index}` },
                  })),
                );
                const celldBefore = await processResources(runtime!.pid);
                const minioBefore = await processResources(minio!.child.pid!);
                const startedAt = Date.now();
                const outcomes =
                  path === 'batch'
                    ? (
                        await Promise.all(
                          messages.map((entries) => w.queueBatch!(queueName, entries)),
                        )
                      ).flat()
                    : await Promise.all(
                        messages
                          .flat()
                          .map((entry) => w.queue(queueName, entry.message, entry.opts)),
                      );
                expect(
                  outcomes.every((outcome) => !('error' in outcome) || outcome.error === undefined),
                ).toBe(true);
                expect(new Set(outcomes.map((outcome) => outcome.messageId)).size).toBe(
                  32 * concurrency,
                );
                const publicationMs = Date.now() - startedAt;
                await waitFor(
                  async () =>
                    new Set(
                      deliveries
                        .filter((d) => d.headers['x-vqs-queue-name'] === queueName)
                        .map((d) => d.headers['x-vqs-message-id']),
                    ).size ===
                    32 * concurrency,
                  30_000,
                  'native A/B callbacks',
                );
                const delivered = deliveries.filter(
                  (d) => d.headers['x-vqs-queue-name'] === queueName,
                );
                const celldAfter = await processResources(runtime!.pid);
                const minioAfter = await processResources(minio!.child.pid!);
                if (round >= 0)
                  nativeGroups.push({
                    path,
                    bytes,
                    concurrency,
                    round,
                    publicationMs,
                    firstDeliveryMs: Math.min(...delivered.map((d) => d.receivedAt)) - startedAt,
                    completionMs: Math.max(...delivered.map((d) => d.receivedAt)) - startedAt,
                    celldCpuMs: celldAfter.cpuMs - celldBefore.cpuMs,
                    celldRssKiB: celldAfter.rssKiB,
                    minioCpuMs: minioAfter.cpuMs - minioBefore.cpuMs,
                    minioRssKiB: minioAfter.rssKiB,
                  });
                for (let i = deliveries.length - 1; i >= 0; i--)
                  if (deliveries[i].headers['x-vqs-queue-name'] === queueName)
                    deliveries.splice(i, 1);
              }
              if (round >= 0)
                console.log(
                  `CELLD_NATIVE_AB_PROGRESS ${JSON.stringify({ bytes, concurrency, round: round + 1, rounds: 10 })}`,
                );
            }
          }
        }
        for (const path of ['single', 'batch'] as const) {
          await invokeFanout(hosts[path].url, 32, 'warmup', false);
          await invokeFanout(hosts[path].url, 32, largeInput, false);
        }
        for (const bytes of [32, 200_000]) {
          for (const concurrency of [1, 4]) {
            for (let round = 0; round < 10; round++) {
              const paths: Path[] = round % 2 === 0 ? ['single', 'batch'] : ['batch', 'single'];
              for (const path of paths) {
                const appBefore = await metrics(path);
                const celldBefore = await processResources(runtime!.pid);
                const minioBefore = await processResources(minio!.child.pid!);
                const started = Date.now();
                const results = await Promise.all(
                  Array.from({ length: concurrency }, () =>
                    invokeFanout(
                      hosts[path].url,
                      32,
                      bytes === 200_000 ? largeInput : 'x'.repeat(bytes),
                      false,
                    ),
                  ),
                );
                const elapsedMs = Date.now() - started;
                const appAfter = await metrics(path);
                const celldAfter = await processResources(runtime!.pid);
                const minioAfter = await processResources(minio!.child.pid!);
                samples.push(
                  ...results.map((result) =>
                    Object.assign({}, result, { path, bytes, concurrency, round }),
                  ),
                );
                groups.push({
                  path,
                  bytes,
                  concurrency,
                  round,
                  elapsedMs,
                  stepsPerSecond: (32 * concurrency * 1000) / elapsedMs,
                  singles: appAfter.singles - appBefore.singles,
                  batches: appAfter.batches - appBefore.batches,
                  batchEntries: appAfter.entries - appBefore.entries,
                  appCpuMs:
                    (appAfter.resources.userCPUTime +
                      appAfter.resources.systemCPUTime -
                      appBefore.resources.userCPUTime -
                      appBefore.resources.systemCPUTime) /
                    1000,
                  appRssBytes: appAfter.memory.rss,
                  appMaxRssKiB: appAfter.resources.maxRSS,
                  celldCpuMs: celldAfter.cpuMs - celldBefore.cpuMs,
                  celldRssKiB: celldAfter.rssKiB,
                  minioCpuMs: minioAfter.cpuMs - minioBefore.cpuMs,
                  minioRssKiB: minioAfter.rssKiB,
                });
              }
              console.log(
                `CELLD_SDK_AB_PROGRESS ${JSON.stringify({ bytes, concurrency, round: round + 1, rounds: 10 })}`,
              );
            }
          }
        }
        const summaries = [32, 200_000].flatMap((bytes) =>
          [1, 4].flatMap((concurrency) =>
            (['single', 'batch'] as const).map((path) => {
              const rows = samples.filter(
                (row) =>
                  row.path === path && row.bytes === bytes && row.concurrency === concurrency,
              );
              const workloads = groups.filter(
                (group) =>
                  group.path === path && group.bytes === bytes && group.concurrency === concurrency,
              );
              return {
                path,
                bytes,
                concurrency,
                runs: rows.length,
                queueBatchCalls: workloads.reduce(
                  (total, group) => total + Number(group.batches),
                  0,
                ),
                singleQueueCalls: workloads.reduce(
                  (total, group) => total + Number(group.singles),
                  0,
                ),
                firstStepP50Ms: percentile(
                  rows.map((row) => row.firstStepMs),
                  0.5,
                ),
                firstStepP95Ms: percentile(
                  rows.map((row) => row.firstStepMs),
                  0.95,
                ),
                completionP50Ms: percentile(
                  rows.map((row) => row.completionMs),
                  0.5,
                ),
                completionP95Ms: percentile(
                  rows.map((row) => row.completionMs),
                  0.95,
                ),
                stepsPerSecond:
                  (32 * rows.length * 1000) /
                  workloads.reduce((total, group) => total + Number(group.elapsedMs), 0),
              };
            }),
          ),
        );
        const nativeSummaries = [0, 200_000].flatMap((bytes) =>
          [1, 4].flatMap((concurrency) =>
            (['single', 'batch'] as const).map((path) => {
              const rows = nativeGroups.filter(
                (row) =>
                  row.bytes === bytes && row.concurrency === concurrency && row.path === path,
              );
              return {
                path,
                bytes,
                concurrency,
                groups: rows.length,
                publicationP50Ms: percentile(
                  rows.map((row) => row.publicationMs),
                  0.5,
                ),
                publicationP95Ms: percentile(
                  rows.map((row) => row.publicationMs),
                  0.95,
                ),
                firstDeliveryP50Ms: percentile(
                  rows.map((row) => row.firstDeliveryMs),
                  0.5,
                ),
                firstDeliveryP95Ms: percentile(
                  rows.map((row) => row.firstDeliveryMs),
                  0.95,
                ),
                completionP50Ms: percentile(
                  rows.map((row) => row.completionMs),
                  0.5,
                ),
                completionP95Ms: percentile(
                  rows.map((row) => row.completionMs),
                  0.95,
                ),
                messagesPerSecond:
                  (32 * concurrency * rows.length * 1000) /
                  rows.reduce((total, row) => total + row.completionMs, 0),
              };
            }),
          ),
        );
        const report = {
          schemaVersion: 1,
          createdAt: new Date().toISOString(),
          node: process.version,
          platform: process.platform,
          arch: process.arch,
          sdk: '5.0.0-beta.58',
          celld: '0.6.0',
          nodeLeaseTtlMs: 30_000,
          operationDeadlineMs: 10_000,
          sdkExecution: 'WORKFLOW_TURBO=0; events.createBatch absent',
          nativeSummaries,
          nativeGroups,
          summaries,
          samples,
          groups,
        };
        const reportPath = resolvePath(
          process.env.CELLD_QUEUE_BENCHMARK_OUTPUT ??
            '.perf-results/queue-batch-qualification.json',
        );
        await mkdir(resolvePath(reportPath, '..'), { recursive: true });
        await writeFile(reportPath, JSON.stringify(report, null, 2) + '\n');
        console.log(`CELLD_NATIVE_AB_SUMMARY ${JSON.stringify(nativeSummaries)}`);
        console.log(`CELLD_SDK_AB_SUMMARY ${JSON.stringify(summaries)}`);
        expect(nativeGroups).toHaveLength(80);
        expect(samples).toHaveLength(200);
        expect(groups.every((group) => group.batches === 0)).toBe(true);
        expect(
          summaries
            .filter((summary) => summary.path === 'batch')
            .every((summary) => summary.runs > 0),
        ).toBe(true);
      } catch (error) {
        const failurePath = resolvePath('.perf-results/queue-batch-failure.log');
        await mkdir(resolvePath(failurePath, '..'), { recursive: true });
        await writeFile(
          failurePath,
          `${String(error)}\nCELLD\n${runtime!.logs()}\nMINIO\n${minio!.logs()}\nHOSTS\n${Object.values(
            hosts,
          )
            .map((host) => host.app.logs())
            .join('\n')}`,
        );
        await writeFile(
          resolvePath('.perf-results/queue-batch-partial.json'),
          JSON.stringify({ complete: false, nativeGroups, groups, samples }, null, 2),
        );
        throw new Error(`Queue benchmark failed; diagnostics: ${failurePath}`, { cause: error });
      } finally {
        await Promise.all(Object.values(hosts).map(({ app }) => stopManaged(app)));
      }
    },
    900_000,
  );

  it('replays a whole offloaded batch after losing the successful HTTP publication response', async () => {
    const deploymentId = `lost-batch-reply-${randomUUID()}`;
    const real = world({ deploymentId });
    const created = await real.events.create(null, {
      eventType: 'run_created',
      eventData: { deploymentId, workflowName: deploymentId, input: [] },
    });
    const queueName = `__wkf_workflow_lost_batch_${randomUUID().slice(0, 8)}`;
    let acceptedIds: Array<string | null> = [];
    let acceptedStatus: number | undefined;
    let publicationCalls = 0;
    const proxy = http.createServer(async (incoming, outgoing) => {
      try {
        const chunks: Buffer[] = [];
        for await (const chunk of incoming) chunks.push(chunk as Buffer);
        const upstream = await fetch(`${runtime!.url}${incoming.url}`, {
          method: incoming.method,
          headers: incoming.headers as Record<string, string>,
          body: Buffer.concat(chunks),
        });
        const body = await upstream.text();
        if (incoming.url === '/v1/queue/send-batch' && ++publicationCalls === 1) {
          acceptedStatus = upstream.status;
          acceptedIds = (rpcParse(body) as NativeQueueBatchResult[]).map(
            (result) => result.messageId,
          );
          outgoing.writeHead(503);
          outgoing.end('accepted publication response lost');
        } else {
          outgoing.writeHead(upstream.status, Object.fromEntries(upstream.headers));
          outgoing.end(body);
        }
      } catch (error) {
        outgoing.writeHead(500);
        outgoing.end(String(error));
      }
    });
    await new Promise<void>((resolve) => proxy.listen(0, '127.0.0.1', resolve));
    const client = createCelldWorld({
      fleetUrl: `http://127.0.0.1:${(proxy.address() as AddressInfo).port}`,
      secret: SECRET,
      baseUrl: callbackBaseUrl,
      deploymentId,
    });
    const entries = Array.from({ length: 8 }, (_, index) => ({
      message: {
        runId: created.run.runId,
        runInput: {
          input: `${index}${'x'.repeat(200_000)}`,
          deploymentId,
          workflowName: deploymentId,
          specVersion: SPEC_VERSION_CURRENT,
        },
      },
      opts: { idempotencyKey: `${queueName}-${index}` },
    }));
    suspendedQueues.add(queueName);
    try {
      await expect(client.queueBatch!(queueName, entries)).rejects.toThrow(
        /accepted publication response lost/,
      );
      expect(acceptedStatus).toBe(200);
      expect(publicationCalls).toBe(1);
      const replay = await client.queueBatch!(queueName, entries);
      expect(replay.map((result) => result.messageId)).toEqual(acceptedIds);
      expect(replay.every((result) => result.error === undefined)).toBe(true);
      expect(publicationCalls).toBe(2);
      await waitFor(
        async () =>
          new Set(
            deliveries
              .filter((d) => d.headers['x-vqs-queue-name'] === queueName)
              .map((d) => d.headers['x-vqs-message-id']),
          ).size === entries.length,
        30_000,
        'accepted offloaded batch delivery',
      );
      suspendedQueues.delete(queueName);
      const before = deliveries.length;
      await waitFor(
        async () =>
          new Set(
            deliveries
              .slice(before)
              .filter((d) => d.headers['x-vqs-queue-name'] === queueName)
              .map((d) => d.headers['x-vqs-message-id']),
          ).size === entries.length,
        30_000,
        'completion after lost batch HTTP response',
      );
    } finally {
      suspendedQueues.delete(queueName);
      proxy.closeAllConnections();
      await new Promise<void>((resolve) => proxy.close(() => resolve()));
    }
  });

  it('recovers an offloaded batch after SIGKILL between broker acceptance and claim confirmation', async () => {
    const w = world({ deploymentId: `batch-crash-${randomUUID()}` });
    const created = await w.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'batch-crash',
        workflowName: 'batch-crash',
        input: [],
      },
    });
    const queueName = `__wkf_workflow_batch_crash_${randomUUID().slice(0, 8)}`;
    const accepted = Promise.withResolvers<NativeQueueEnvelope[]>();
    const gate = http.createServer(async (request, _response) => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(chunk as Buffer);
      const messages = JSON.parse(Buffer.concat(chunks).toString()) as Array<{ body: string }>;
      accepted.resolve(messages.map((message) => JSON.parse(message.body) as NativeQueueEnvelope));
      // Hold the broker reply until the producer process is killed.
    });
    await new Promise<void>((resolve) => gate.listen(0, '127.0.0.1', resolve));
    const gateUrl = `http://127.0.0.1:${(gate.address() as AddressInfo).port}`;
    const entries = Array.from({ length: 8 }, (_, index) => ({
      envelope: {
        version: 1 as const,
        messageId: `msg_crash_${randomUUID()}`,
        queueName,
        targetBaseUrl: callbackBaseUrl,
        runId: created.run.runId,
        idempotencyKey: `${queueName}-${index}`,
        notBefore: Date.now() + 3_000,
        body: rpcStringify({
          runId: created.run.runId,
          stepInput: { input: new TextEncoder().encode(`${index}${'x'.repeat(200_000)}`) },
        }),
      },
      options: { delaySeconds: 3 },
    }));
    const publish = async (pause: boolean) => {
      const response = await fetch(`${runtime!.url}/v1/queue/send-batch`, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${SECRET}`,
          'content-type': 'application/json',
          ...(pause ? { 'x-test-broker-gate': gateUrl } : {}),
        },
        body: rpcStringify([entries]),
        signal: AbortSignal.timeout(30_000),
      });
      return {
        status: response.status,
        results: rpcParse(await response.text()) as NativeQueueBatchResult[],
      };
    };
    suspendedQueues.add(queueName);
    try {
      const pending = publish(true).then(
        (result) => result,
        () => null,
      );
      const brokerMessages = await Promise.race([
        accepted.promise,
        delay(30_000).then(() => {
          throw new Error('broker acceptance gate timed out');
        }),
      ]);
      expect(brokerMessages.every((message) => message.payloadKey && !message.body)).toBe(true);
      const contender = await publish(false);
      expect(
        contender.results.every((result) => result.messageId === null && result.retryable),
      ).toBe(true);
      await runtime!.restart(200, true);
      expect(await pending).toBeNull();
      await waitFor(
        async () =>
          new Set(
            deliveries
              .filter((d) => d.headers['x-vqs-queue-name'] === queueName)
              .map((d) => d.headers['x-vqs-message-id']),
          ).size === entries.length,
        30_000,
        'all accepted batch callbacks after restart',
      );
      const replay = await publish(false);
      expect(replay.status).toBe(200);
      expect(replay.results.map((result) => result.messageId)).toEqual(
        entries.map((entry) => entry.envelope.messageId),
      );
      for (const entry of entries) {
        expect(
          deliveries.some(
            (d) =>
              d.headers['x-vqs-message-id'] === entry.envelope.messageId &&
              d.body === entry.envelope.body,
          ),
        ).toBe(true);
      }
      suspendedQueues.delete(queueName);
      const before = deliveries.length;
      await waitFor(
        async () =>
          new Set(
            deliveries
              .slice(before)
              .filter((d) => d.headers['x-vqs-queue-name'] === queueName)
              .map((d) => d.headers['x-vqs-message-id']),
          ).size === entries.length,
        30_000,
        'eventual completion of the recovered batch',
      );
    } finally {
      suspendedQueues.delete(queueName);
      gate.closeAllConnections();
      await new Promise<void>((resolve) => gate.close(() => resolve()));
    }
  });

  it('delivers a native Queue batch and measures wide fan-out publication', async () => {
    const w = world({ deploymentId: `batch-${randomUUID()}` });
    const marker = randomUUID().slice(0, 8);
    const count = 32;
    const singleQueue = `__wkf_workflow_single_${marker}`;
    const batchQueue = `__wkf_workflow_batch_${marker}`;
    const messages = Array.from({ length: count }, (_, index) => ({
      message: { __healthCheck: true as const, correlationId: `${marker}-${index}` },
      opts: { idempotencyKey: `${marker}-${index}` },
    }));
    const singlesStarted = performance.now();
    const singles = await Promise.all(
      messages.map(({ message, opts }) => w.queue(singleQueue, message, opts)),
    );
    const singlesMs = performance.now() - singlesStarted;
    const batchStarted = performance.now();
    const batched = await w.queueBatch!(batchQueue, messages);
    const batchMs = performance.now() - batchStarted;
    expect(singles).toHaveLength(count);
    expect(batched).toHaveLength(count);
    expect(batched.every((result) => result.error === undefined)).toBe(true);
    await waitFor(
      async () =>
        deliveries.filter((delivery) => delivery.headers['x-vqs-queue-name'] === singleQueue)
          .length >= count &&
        deliveries.filter((delivery) => delivery.headers['x-vqs-queue-name'] === batchQueue)
          .length >= count,
      30_000,
      'single and batch Queue deliveries',
    );
    console.log(`CELLD_QUEUE_FANOUT ${JSON.stringify({ count, singlesMs, batchMs })}`);
  });

  it('delivers offloaded run input from a native Queue batch', async () => {
    const deploymentId = `batch-payload-${randomUUID()}`;
    const w = world({ deploymentId });
    const created = await w.events.create(null, {
      eventType: 'run_created',
      eventData: { deploymentId, workflowName: 'batch-payload', input: [] },
    });
    const marker = `batch-payload-${randomUUID()}`;
    const queueName = `__wkf_workflow_${randomUUID().slice(0, 8)}`;
    const results = await w.queueBatch!(queueName, [
      {
        message: {
          runId: created.run.runId,
          runInput: {
            input: `${marker}${'x'.repeat(200_000)}`,
            deploymentId,
            workflowName: 'batch-payload',
            specVersion: SPEC_VERSION_CURRENT,
          },
        },
        opts: { idempotencyKey: marker },
      },
    ]);
    expect(results).toHaveLength(1);
    expect(results[0].error).toBeUndefined();
    const delivered = await waitFor(
      async () => deliveries.find((delivery) => delivery.body.includes(marker)),
      30_000,
      'offloaded batch payload delivery',
    );
    expect(delivered.headers['x-vqs-queue-name']).toBe(queueName);
    expect(delivered.body).toContain('x'.repeat(1000));
  });

  it.each([
    ['wrong-secret', { version: 1 }, 1, 'Unauthorized'],
    [SECRET, {}, 1, 'version'],
    [
      SECRET,
      {
        version: 1,
        messageId: 'msg_invalid_rpc',
        queueName: '__wkf_workflow_invalid_rpc',
        targetBaseUrl: 'https://app.invalid',
        body: '{}',
      },
      0,
      'attempt',
    ],
  ])(
    'rejects invalid requests across the actual RPC boundary %#',
    async (secret, envelope, attempt, expected) => {
      const before = deliveries.length;
      const response = await fetch(`${runtime!.url}/__test/queue-rpc`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify([secret, envelope, attempt]),
      });
      expect(response.status).toBe(400);
      expect(await response.text()).toContain(expected);
      expect(deliveries.length).toBe(before);
    },
  );

  it('does not expose the removed HTTP delivery route', async () => {
    const response = await fetch(`${runtime!.url}/v1/queue/deliver`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${SECRET}` },
      body: '[]',
    });
    expect(response.status).toBe(404);
    await response.arrayBuffer();
  });

  it.each(['suspend', 'retry'])('preserves RPC %s delivery semantics', async (mode) => {
    const w = world({ deploymentId: `rpc-${randomUUID()}` });
    const queueName = `__wkf_workflow_rpc_${mode}_${randomUUID().slice(0, 8)}`;
    await w.queue(
      queueName,
      { __healthCheck: true, correlationId: queueName },
      { idempotencyKey: queueName },
    );
    const observed = await waitFor(
      async () => {
        const found = deliveries.filter((d) => d.headers['x-vqs-queue-name'] === queueName);
        return found.length >= 2 ? found : null;
      },
      20_000,
      `RPC ${mode} second callback`,
    );
    expect(observed[0].headers['x-vqs-message-attempt']).toBe('1');
    expect(observed[1].headers['x-vqs-message-attempt']).toBe(mode === 'suspend' ? '1' : '2');
    expect(observed[0].headers['x-vqs-message-id']).toBe(observed[1].headers['x-vqs-message-id']);
  });

  it('persists opt-in runtime telemetry in the fleet bucket', async () => {
    const response = await fetch(`${runtime!.url}/v1/health`);
    expect(response.ok).toBe(true);
    await response.arrayBuffer();
    await waitFor(
      async () => {
        const { stdout } = await execFileAsync(
          MC_BIN!,
          ['ls', '--recursive', '--json', `smoke/${BUCKET}/telemetry/`],
          { env: { ...process.env, MC_CONFIG_DIR: join(temporaryRoot, 'mc-config') } },
        );
        return stdout
          .split('\n')
          .filter(Boolean)
          .map((line) => JSON.parse(line) as { key?: string; size?: number })
          .some((object) => object.key?.endsWith('.parquet') && (object.size ?? 0) > 0)
          ? true
          : null;
      },
      15_000,
      'persisted runtime telemetry',
    );
  });

  it.each(['step_failed', 'step_retrying'] as const)(
    'preserves opaque %s errors through RPC and an actual celld process restart',
    async (eventType) => {
      const deploymentId = `step-error-${randomUUID()}`;
      const w = world({ deploymentId });
      const created = await w.events.create(null, {
        eventType: 'run_created',
        eventData: { deploymentId, workflowName: deploymentId, input: [] },
      });
      const runId = created.run.runId;
      const errors = [new Uint8Array([0, 1, 127, 128, 254, 255]), null];
      for (const [index, error] of errors.entries()) {
        const stepId = `error-step-${index}`;
        await w.events.create(runId, {
          eventType: 'step_started',
          correlationId: stepId,
          eventData: { stepName: stepId, input: [] },
        });
        const outcome = await w.events.create(runId, {
          eventType,
          correlationId: stepId,
          eventData: { error },
        });
        expect(outcome.step?.error).toEqual(error);
        expect(outcome.event?.eventData).toEqual({ error });
      }

      const restart = await runtime!.restart(0, true);
      expect(restart.newPid).not.toBe(restart.oldPid);
      const steps = await w.steps.list({ runId, resolveData: 'all' });
      expect(steps.data.map((step) => step.error)).toEqual(errors);
      for (const [index, error] of errors.entries()) {
        const step = await w.steps.get(runId, `error-step-${index}`, { resolveData: 'all' });
        expect(step.error).toEqual(error);
        expect(step.status).toBe(eventType === 'step_failed' ? 'failed' : 'pending');
      }
    },
  );

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
      // User data keeps the body in object storage, where retention must delete it.
      {
        runId,
        runInput: {
          input: ['retention-restart'],
          deploymentId: 'smoke',
          workflowName: 'retention-restart',
          specVersion: SPEC_VERSION_CURRENT,
        },
      },
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
