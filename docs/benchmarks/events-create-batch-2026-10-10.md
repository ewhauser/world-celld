# Workflow 5.0.1 compiled fan-out: scoped `events.createBatch`

Run on 2026-10-10, macOS arm64, Node 26.5.0, Workflow 5.0.1. Both modes used the same built app, adapter revision, in-process celld emulator, Map-backed storage, 100 ms step body, 1 KiB input per branch, single-message queue transport, and default inline-step settings. The only switch was `CELLD_EVENT_BATCHING=0` versus `1`. The app was restarted between modes; the fleet remained running. Each width's first sample is labeled cold in the raw data, though only width 8 is the first run in a new app process. Raw [single-workflow](events-create-batch-2026-10-10.jsonl) and [concurrent-throughput](events-create-batch-throughput-2026-10-10.jsonl) samples are retained.

Reproduce:

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm --dir examples/demo-app build
BENCH_SAMPLES=3 BENCH_THROUGHPUT_SAMPLES=0 node scripts/bench-compiled-fanout.mjs
BENCH_SAMPLES=0 BENCH_THROUGHPUT_SAMPLES=3 node scripts/bench-compiled-fanout.mjs
```

The script starts a compiled Nitro Workflow app and the in-process fleet, runs widths 8, 64, and 128 three times in each mode, and reads the compiled workflow's hydrated return value. It uses a fleet HTTP proxy to count event RPCs and their response durations. One event RPC corresponds to one RunDO transaction on these paths. Queue publications come from the fleet's native Queue stand-in. `firstToLastStartMs` uses the step bodies' returned start timestamps; `firstStartToJoinMs` uses the workflow's returned join timestamp. Total time spans the start request through observing terminal status. A separate invocation with `BENCH_SAMPLES=0 BENCH_THROUGHPUT_SAMPLES=3` measured four simultaneous width-64 workflows per sample.

| Width | Mode     | First total ms | Later total ms | Later start spread ms | Later start-to-join ms | Later event RPCs | Batch RPCs/run | Queue publications |
| ----: | :------- | -------------: | :------------- | :-------------------- | :--------------------- | :--------------- | -------------: | -----------------: |
|     8 | disabled |            286 | 189, 193       | 19, 20                | 125, 124               | 28, 28           |              0 |                  6 |
|     8 | enabled  |            273 | 188, 186       | 20, 22                | 125, 127               | 22, 22           |              2 |                  6 |
|    64 | disabled |            598 | 513, 544       | 46, 34                | 161, 151               | 201, 311         |              0 |                 62 |
|    64 | enabled  |            567 | 282, 556       | 32, 45                | 146, 160               | 132, 238         |              3 |                 62 |
|   128 | disabled |           1969 | 1915, 1947     | 61, 62                | 193, 192               | 497, 549         |              0 |                126 |
|   128 | enabled  |           1988 | 1795, 1966     | 76, 74                | 208, 218               | 388, 390         |              5 |                126 |

Batching reduced event RPC and transaction counts in these samples. Total latency differences are inconclusive at all widths; the 64-wide enabled warm samples span 282–556 ms. The two warm samples per cell are too few for a confidence interval. This is **not** evidence of an end-to-end speedup.

Four concurrent width-64 runs completed at 2.14 and 2.25 runs/s without batching and 2.27 and 2.32 runs/s with batching in the two warm samples. Cold throughput samples were 2.16 and 2.21 runs/s, respectively. This small, noisy set does not establish a throughput improvement.

The in-process fleet is not a real celld deployment or object store. Its Map transaction commit time is not separately exposed, so the proxy's summed event RPC durations include serialization, routing, transaction work, and HTTP response time, and concurrent calls overlap. Queue delivery timestamps and retry counts are not exposed by this harness; only publication counts are recorded. The concurrent measurement covers one bounded width and four runs, rather than a throughput saturation curve. The real-celld restart smoke covers durability separately.
