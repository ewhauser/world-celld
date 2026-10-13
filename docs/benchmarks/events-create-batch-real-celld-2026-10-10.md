# Compiled fan-out on real celld v0.6.0 and MinIO

This follows the [in-process prototype benchmark](events-create-batch-2026-10-10.md) with an actual celld durability backend. **Single-workflow latency is inconclusive**: batching consistently lowers event RPC counts, while latency varies enough that neither a speedup nor a regression is established. A separate bounded four-workflow throughput test observed a 15% higher warm median with batching on this local single-node setup; that is a measured result, not a general capacity claim.

## Setup and reproduction

- Workflow SDK and compiled Nitro app: 5.0.1 in both modes. Adapter commit and workload identical; only `CELLD_EVENT_BATCHING=0` versus `1` changes.
- Celld v0.6.0, one local celld node with `CELLD_DURABILITY=bucket`, and one local MinIO process on disk. These are the repository's checksum-pinned smoke binaries and temporary deployment fixtures. This is a **single-node** benchmark, not a multi-node fleet or remote object store.
- macOS arm64, Apple M3 Max (16 CPUs, 64 GiB), Node 26.5.0. Two compiled app processes stayed up, one per mode. Trials ran sequentially in ABBA-interleaved order against the same celld/MinIO processes.
- Each run used 8, 64, or 128 independent steps with 100 ms work and a 1 KiB input per step. The existing single-message Queue path and inline-step defaults stayed fixed. The harness asserted unique branches, complete steps, attempt 1, 1 KiB payloads, and body duration of at least 90 ms.

```sh
pnpm install --frozen-lockfile
pnpm build
pnpm --dir examples/demo-app build
CELLD_FANOUT_BENCH=1 \
CELLD_FANOUT_BENCH_RESULT=/tmp/real-fanout.jsonl \
bash test/integration/celld-smoke/run.sh -t 'benchmarks compiled fan-out'

# Separately, four concurrent width-64 workflows per trial:
CELLD_FANOUT_BENCH=1 \
CELLD_FANOUT_BENCH_ONLY_THROUGHPUT=1 \
CELLD_FANOUT_BENCH_THROUGHPUT=1 \
CELLD_FANOUT_BENCH_RESULT=/tmp/real-throughput.jsonl \
bash test/integration/celld-smoke/run.sh -t 'benchmarks compiled fan-out'
```

The five-trial-per-mode replication is [raw JSONL](events-create-batch-real-celld-2026-10-10.jsonl). An earlier three-trial-per-mode pass on the same setup, before increasing the harness's repetition count, is [also retained](events-create-batch-real-celld-pilot-2026-10-10.jsonl). The first width-8 run for each app in each session is process-cold; the remaining width-8 runs and all width-64/128 runs are warm.

## Five-trial replication

Values are medians, with min–max for total completion. Start times are milliseconds after the start request. Join is measured from the first branch body's start. Each reported mode has five runs at each width.

| Width | Mode   | Total ms         | First start ms | Last start ms | First start → join ms | Event RPCs | Batch RPCs | Queue publishes / deliveries | Retries      |
| ----: | :----- | :--------------- | -------------: | ------------: | --------------------: | ---------: | ---------: | :--------------------------- | :----------- |
|     8 | single | 401 (384–808)    |            116 |           208 |                   183 |         28 |          0 | 6 / 6                        | 0            |
|     8 | batch  | 491 (380–593)    |            103 |           262 |                   196 |         21 |          2 | 6 / 6                        | 0            |
|    64 | single | 809 (719–1143)   |            149 |           366 |                   398 |        215 |          0 | 62 / 62                      | 0            |
|    64 | batch  | 792 (615–1055)   |            130 |           391 |                   401 |        162 |          3 | 62 / 62                      | 0            |
|   128 | single | 2227 (1921–2486) |            209 |           568 |                   664 |        471 |          0 | 126 / 126                    | 0            |
|   128 | batch  | 1794 (1736–3389) |            117 |           740 |                   778 |        313 |          5 | 126 / 126*                   | 1 in one run |

\* One batched width-128 run had a retried Queue delivery, so that trial recorded 127 deliveries for 126 publications. All other trials had one delivery per publication. The first Queue publish request began at median 5 ms after the run request in both modes at all warm widths. First callback delivery medians were 75/70 ms (single/batch) at width 8, 61/96 ms at width 64, and 63/84 ms at width 128. These observations include local proxy overhead in both modes.

## Uncertainty across both sessions

The three-trial pass went the other way at width 128: total medians were 1894 ms single versus 2188 ms batch. Pooling warm samples from both sessions gives the following exploratory medians. The intervals are 95% percentile bootstrap intervals for the **batch minus single median**, with independent resampling inside each mode (20,000 draws). Samples are few and temporally correlated, so the intervals describe observed variability rather than a production guarantee.

| Width | Warm samples per mode | Single median ms | Batch median ms | Difference ms, bootstrap interval |
| ----: | --------------------: | ---------------: | --------------: | :-------------------------------- |
|     8 |                     6 |              404 |             451 | +47 (−40 to +96)                  |
|    64 |                     8 |              795 |             824 | +30 (−113 to +137)                |
|   128 |                     8 |             2171 |            2024 | −147 (−471 to +312)               |

Batching cut median event RPC counts in the five-trial pass from 28 to 21 at width 8, 215 to 162 at width 64, and 471 to 313 at width 128. Each event RPC invokes one RunDO transaction on these code paths, but the proxy does not independently observe transaction commits. The raw data includes the sum of event RPC response durations; those calls overlap, and their duration contains HTTP, routing, storage reads and writes, and commit, so the sum is **not** commit time or wall time. Celld/MinIO do not expose a per-transaction durable-commit timestamp to this harness. Callback arrival and retry counts are observable; broker internal scheduling and acknowledgment timing are not.

## Bounded concurrent throughput

Two fresh celld/MinIO sessions each ran three interleaved trials per mode with four concurrent compiled width-64 workflows. The first trial per mode in each session was cold. The remaining four warm samples per mode were:

| Mode   | Warm runs/s                | Warm median runs/s | Event RPCs per four-run trial | Queue publishes / deliveries | Retries |
| :----- | :------------------------- | -----------------: | :---------------------------- | :--------------------------- | :------ |
| Single | 0.756, 0.740, 0.737, 0.755 |              0.748 | 775–797                       | 248 / 248                    | 0       |
| Batch  | 0.860, 0.865, 0.773, 0.856 |              0.858 | 535–539                       | 248 / 248                    | 0       |

The observed warm median is 14.7% higher with batching; the four values in each mode are too few to extrapolate a saturation curve or fleet-wide throughput. Cold runs/s were 0.651 and 0.516 single versus 0.787 and 0.815 batch. Raw data from [session one](events-create-batch-real-celld-throughput-2026-10-10.jsonl) and [session two](events-create-batch-real-celld-throughput-replication-2026-10-10.jsonl) is retained. This throughput result is separate from the one-workflow latency tables above. No Queue batching or SDK upgrade was included.
