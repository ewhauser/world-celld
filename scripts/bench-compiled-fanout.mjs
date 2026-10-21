/**
 * Compiled Workflow 5.0.1 fan-out benchmark on the in-process fleet.
 * Run after pnpm build && pnpm --dir examples/demo-app build.
 */
import http from 'node:http';
import { spawn } from 'node:child_process';
import { setTimeout as delay } from 'node:timers/promises';
import { createCelldWorld } from '../dist/index.js';
import { startDevFleet } from '../dist/testing.js';

const secret = 'fanout-benchmark-secret';
const appPort = 32145;
const base = `http://127.0.0.1:${appPort}`;
const fleet = await startDevFleet({ secret });
const world = createCelldWorld({ fleetUrl: fleet.url, secret });
const widths = [8, 64, 128];
const samples = Number(process.env.BENCH_SAMPLES ?? 3);
const concurrentRuns = Number(process.env.BENCH_CONCURRENT_RUNS ?? 4);
const throughputSamples = Number(process.env.BENCH_THROUGHPUT_SAMPLES ?? 3);
const output = [];
let app;
let proxy;
let currentRpc = [];

try {
  proxy = http.createServer(async (request, response) => {
    const started = performance.now();
    const body = await Array.fromAsync(request).then((chunks) => Buffer.concat(chunks));
    try {
      const upstream = await fetch(`${fleet.url}${request.url}`, {
        method: request.method,
        headers: {
          authorization: request.headers.authorization ?? '',
          'content-type': request.headers['content-type'] ?? 'application/json',
        },
        body: request.method === 'GET' ? undefined : body,
      });
      response.writeHead(upstream.status, Object.fromEntries(upstream.headers));
      response.end(Buffer.from(await upstream.arrayBuffer()));
      if (request.url?.includes('/applyEvent')) {
        currentRpc.push({ method: request.url.split('/').at(-1), ms: performance.now() - started });
      }
    } catch (error) {
      response.writeHead(502).end(String(error));
    }
  });
  await new Promise((resolve) => proxy.listen(0, '127.0.0.1', resolve));
  const proxyUrl = `http://127.0.0.1:${proxy.address().port}`;

  for (const enabled of [false, true]) {
    app = spawn('node', ['.output/server/index.mjs'], {
      cwd: new URL('../examples/demo-app/', import.meta.url),
      env: {
        ...process.env,
        WORKFLOW_TARGET_WORLD: '@ewhauser/world-celld',
        CELLD_FLEET_URL: proxyUrl,
        CELLD_WORLD_SECRET: secret,
        CELLD_EVENT_BATCHING: enabled ? '1' : '0',
        WORKFLOW_BASE_URL: base,
        PORT: String(appPort),
        NODE_ENV: 'production',
      },
      stdio: ['ignore', 'ignore', 'inherit'],
    });
    for (let attempt = 0; attempt < 200; attempt++) {
      try {
        if ((await fetch(base)).ok) break;
      } catch {
        /* starting */
      }
      if (attempt === 199) throw new Error('compiled app failed to start');
      await delay(50);
    }

    for (const width of widths) {
      for (let sample = 0; sample < samples; sample++) {
        currentRpc = [];
        const publicationsBefore = fleet.queuePublications.length;
        const started = performance.now();
        const startResponse = await fetch(`${base}/bench/fanout/${width}`, { method: 'POST' });
        if (!startResponse.ok) throw new Error(`start failed: ${await startResponse.text()}`);
        const { runId } = await startResponse.json();
        let run;
        for (let attempt = 0; attempt < 2000; attempt++) {
          run = await world.runs.get(runId);
          if (run.status === 'completed' || run.status === 'failed') break;
          await delay(20);
        }
        if (run?.status !== 'completed')
          throw new Error(`run ${runId} did not complete: ${run?.status}`);
        const completed = performance.now();
        const resultResponse = await fetch(`${base}/bench/result/${runId}`);
        if (!resultResponse.ok)
          throw new Error(`result read failed: ${await resultResponse.text()}`);
        const result = await resultResponse.json();
        if (result.branches?.length !== width || typeof result.joinedAt !== 'number') {
          throw new Error('compiled workflow returned an incomplete fan-out result');
        }
        const starts = result.branches.map((branch) => branch.startedAt);
        if (starts.some((value) => !Number.isFinite(value))) {
          throw new Error('compiled workflow omitted a branch start timestamp');
        }
        const rpcs = currentRpc;
        output.push({
          enabled,
          width,
          sample,
          cold: sample === 0,
          totalMs: completed - started,
          firstToLastStartMs: Math.max(...starts) - Math.min(...starts),
          firstStartToJoinMs: result.joinedAt - Math.min(...starts),
          eventRpcs: rpcs.length,
          batchRpcs: rpcs.filter((rpc) => rpc.method === 'applyEventBatch').length,
          eventRpcMs: rpcs.reduce((sum, rpc) => sum + rpc.ms, 0),
          queuePublications: fleet.queuePublications.length - publicationsBefore,
          stepCount: result.branches.length,
        });
        console.log(JSON.stringify(output.at(-1)));
      }
    }
    for (let sample = 0; sample < throughputSamples; sample++) {
      const started = performance.now();
      const starts = await Promise.all(
        Array.from({ length: concurrentRuns }, () =>
          fetch(`${base}/bench/fanout/64`, { method: 'POST' }).then(async (response) => {
            if (!response.ok) throw new Error(`concurrent start failed: ${await response.text()}`);
            return response.json();
          }),
        ),
      );
      const pending = new Set(starts.map(({ runId }) => runId));
      for (let attempt = 0; pending.size > 0 && attempt < 2000; attempt++) {
        await Promise.all(
          [...pending].map(async (runId) => {
            const run = await world.runs.get(runId);
            if (run.status === 'failed') throw new Error(`concurrent run ${runId} failed`);
            if (run.status === 'completed') pending.delete(runId);
          }),
        );
        if (pending.size > 0) await delay(20);
      }
      if (pending.size > 0) throw new Error(`${pending.size} concurrent runs did not complete`);
      const elapsedMs = performance.now() - started;
      const record = {
        kind: 'throughput',
        enabled,
        width: 64,
        sample,
        cold: sample === 0 && samples === 0,
        concurrentRuns,
        elapsedMs,
        runsPerSecond: (concurrentRuns * 1000) / elapsedMs,
      };
      console.log(JSON.stringify(record));
    }
    app.kill();
    await new Promise((resolve) => app.once('exit', resolve));
    app = undefined;
  }
  console.log(JSON.stringify({ benchmark: 'compiled-fanout', workflow: '5.0.1', samples: output }));
} finally {
  app?.kill();
  proxy?.close();
  await fleet.stop();
}
