import { spawnSync } from 'node:child_process';
import { mkdir, writeFile } from 'node:fs/promises';
import { cpus, platform, release, arch, totalmem } from 'node:os';
import { resolve, join } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const output = resolve(
  process.env.PERF_MATRIX_DIR ?? join(root, '.perf-results', `matrix-${Date.now()}`),
);
const head = spawnSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' });
const status = spawnSync('git', ['status', '--short'], { cwd: root, encoding: 'utf8' });
const soakSeconds = Number(process.env.PERF_MATRIX_SOAK_SECONDS ?? 60);
if (!Number.isSafeInteger(soakSeconds) || soakSeconds < 1) {
  throw new Error('PERF_MATRIX_SOAK_SECONDS must be a positive integer');
}

const profiles = [
  { name: 'low-256', concurrency: 8, payloadBytes: 256, messages: 1000, steadySeconds: 0 },
  { name: 'medium-256', concurrency: 32, payloadBytes: 256, messages: 1000, steadySeconds: 0 },
  { name: 'high-4096', concurrency: 64, payloadBytes: 4096, messages: 2000, steadySeconds: 0 },
  {
    name: 'steady-256',
    concurrency: 32,
    payloadBytes: 256,
    messages: 1000,
    steadySeconds: soakSeconds,
  },
];
const manifest = {
  schemaVersion: 1,
  recordedAt: new Date().toISOString(),
  gitCommit: head.status === 0 ? head.stdout.trim() : 'unknown',
  gitStatus: status.status === 0 ? status.stdout.trim().split('\n').filter(Boolean) : ['unknown'],
  runtime: {
    node: process.version,
    platform: platform(),
    release: release(),
    arch: arch(),
    cpuModel: cpus()[0]?.model ?? 'unknown',
    cpuCount: cpus().length,
    totalMemoryBytes: totalmem(),
    celldVersion: 'v0.5.0',
    minioVersion: 'RELEASE.2025-09-07T16-13-09Z',
  },
  profiles: [],
};
await mkdir(output, { recursive: true });
const manifestPath = join(output, 'manifest.json');
for (const profile of profiles) {
  const directory = join(output, profile.name);
  await mkdir(directory, { recursive: true });
  const env = {
    ...process.env,
    PERF_RESULTS_DIR: directory,
    PERF_CONCURRENCY: String(profile.concurrency),
    PERF_PAYLOAD_BYTES: String(profile.payloadBytes),
    PERF_MESSAGES: String(profile.messages),
    PERF_WORKFLOW_CONCURRENCY: String(Math.min(profile.concurrency, 16)),
    PERF_WORKFLOW_RUNS: '25',
    PERF_RETENTION_RUNS: '100',
    PERF_STEADY_SECONDS: String(profile.steadySeconds),
    PERF_STEADY_RATE: '25',
  };
  const startedAt = new Date().toISOString();
  const result = spawnSync('bash', ['test/perf/minio/run.sh'], {
    cwd: root,
    env,
    stdio: 'inherit',
  });
  manifest.profiles.push({
    ...profile,
    startedAt,
    finishedAt: new Date().toISOString(),
    resultDirectory: directory,
    exitCode: result.status,
    signal: result.signal,
  });
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${profile.name} failed; partial results are in ${directory}`);
  }
}
console.log(`Performance matrix results: ${manifestPath}`);
