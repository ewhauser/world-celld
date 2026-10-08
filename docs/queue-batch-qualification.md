# Queue batch promotion qualification

## Decision

The native `queueBatch` prototype has recovery coverage, but it is **not ready
for promotion as an SDK workflow performance improvement**. There is also a
native first-delivery latency regression under load. The pinned
Workflow `5.0.0-beta.58` runtime calls `queueBatch` only from the event batch
fan-out path. That path requires `world.events.createBatch`, which this adapter
does not implement. Actual SDK workflows therefore use single queue sends even
when `queueBatch` is exposed.

The queue capability remains unconditional. No production configuration switch
was added. The A/B control is implemented only by the SDK test fixture, which
can hide `queueBatch` from the runtime.

Before adopting this as a workflow optimization, pipeline payload preparation
and publication to remove the first-delivery barrier, then implement and qualify atomic
`events.createBatch` on the authoritative RunDO, or use an SDK that batches
queue dispatch independently of event batching. A client shim issuing multiple
single event writes would not satisfy the event batch atomicity contract.

## Correctness evidence

The real celld v0.6.0 and pinned MinIO suite exercises:

- Real compiled SDK `Promise.all` fan-out of 1, 8, 32, and 128 branches, exact
  ordered fan-in and expected sum, and terminal completion for every run.
- A retried step with a 200 KB random input. Every branch returns the expected
  byte length and SHA-256 digest; the retried branch completes on attempt two.
- Both default inline execution and queued execution (`WORKFLOW_TURBO=0`).
  The queued test verifies that the World exposes `queueBatch`, lacks
  `events.createBatch`, and receives zero SDK batch calls.
- Loss of a successful batch HTTP response after the worker confirms its
  publication. The client reports failure, does not blindly retry the write,
  and a whole-batch retry returns the original accepted message IDs. All eight
  offloaded messages eventually finish delivery.
- SIGKILL after the native broker accepts eight offloaded messages, while a
  test-only gate prevents the producer from confirming the claims. A concurrent
  retry reports pending entries rather than success. Restart discards local
  state and recovers from object storage; delivery confirms each publication,
  a whole-batch replay returns the original IDs, and every message completes.

The regular suite covers both historical claim cells and current run-scoped
claims for ambiguous acceptance, pending publication, individual confirmation
failure, retention during payload staging, and orphan cleanup after cell
restart. A 101-entry partial publication test replays the entire batch and
verifies that its 100 confirmed entries are not republished.

The v7 compatibility boundary is intentional: the beta.53 SDK rejects a World
advertising v8, and writes to v7 runs remain unsupported. A pinned v7 runtime
startup test preserves this failure. Drain v7 runs with their matching adapter
and SDK before upgrading; this qualification does not establish mixed-version
operation.

## Reproduce

```sh
pnpm install --frozen-lockfile
pnpm check
pnpm test:integration:celld-smoke
pnpm test:perf:queue-batch
```

The smoke suite is already included in CI. The longer benchmark is an explicit
local command and writes `.perf-results/queue-batch-qualification.json`.
`CELLD_QUEUE_BENCHMARK_OUTPUT` can select another artifact path. It runs ten
alternating A/B rounds per payload/concurrency combination, after warm-up. A failed run saves logs and partial samples with
`complete: false` instead of producing a successful report.
Run it without another benchmark or test suite competing for local resources.

## Measurement method

The report records two different workloads:

1. **Native publication and callback delivery:** 32 messages per run, with
   concurrency 1 or 4, using small inline envelopes or offloaded 200 KB random
   inputs. Singles and batches alternate within each round. Publication time
   ends when the producer returns; first/last delivery times use callback
   receipt timestamps. These callbacks acknowledge delivery without executing
   SDK steps. They measure the adapter and broker path.
2. **SDK workflows:** 32 real steps per workflow, using 32-byte or 200 KB random
   inputs at concurrency 1 or 4. One host exposes `queueBatch`; the A/B host
   hides it. Both use identical single event storage and `WORKFLOW_TURBO=0`. The SDK still
   executes three steps lazily inline and queues the other 29; each workflow
   makes 30 single sends including its initial delivery.
   Time to first step is measured inside the step body. Completion is measured
   by polling the authoritative run through the SDK, at 100 ms intervals.
   Every output is checked, and every group records actual single/batch calls.

Native groups record celld/MinIO CPU deltas and RSS snapshots. SDK groups also
record application CPU, RSS, and process maximum RSS. RSS snapshots are not
fleet-wide peak-memory measurements; process maximum RSS is cumulative across
profiles. Local CPU timings and small p95 samples are descriptive evidence,
not a production SLO or a cloud/fleet capacity qualification.

## Lease limit observed during qualification

Two initial load attempts used the restart smoke's two-second node lease. Both
lost producer connections during the 128-message offloaded single-send workload.
The diagnostic attempt recorded an ambiguous renewal followed by
`node_lease_watchdog_fence` and process self-fencing. It completed 67 of the 80
native groups before failing; it did not reach the SDK benchmark.

Those samples are retained locally in
`.perf-results/queue-batch-ttl-2s-partial.json`, marked `complete: false`, with
`.perf-results/queue-batch-ttl-2s-failure.log`. They do not constitute a successful
load qualification. The load benchmark uses a 30-second node lease and a
10-second operation deadline, while correctness/restart tests keep the
original two-second lease. The longer lease is a test harness setting, not a
production queue feature switch. Production lease headroom under storage
latency remains an operating requirement to qualify for the actual fleet.

## Results on 2026-10-08

Successful run: macOS arm64, Node v26.5.0, celld v0.6.0, pinned SDK beta.58,
local MinIO, 30-second node lease, 10-second operation deadline. All 80 native
workload groups and all 200 SDK workflows completed. Every SDK branch returned
the expected input digest and ordered result. Hosted Node 22 CI and production
fleet qualification are separate from this local result.

### Native publication and callback delivery

Times are milliseconds, reported as p50 / p95. Each cell shows **singles →
batch**; each method has ten samples per profile. The 0-byte input profile uses
small inline envelopes with no user data.

| Input            | Concurrency | Publication p50 / p95         | First callback p50 / p95      | Last callback p50 / p95       |
| ---------------- | ----------: | ----------------------------- | ----------------------------- | ----------------------------- |
| Inline           |           1 | 142 / 301 → 133 / 188         | 65 / 114 → 79 / 121           | 189 / 397 → 105 / 164         |
| Inline           |           4 | 1,068 / 1,250 → 990 / 2,125   | 187 / 405 → 364 / 979         | 1,079 / 1,249 → 958 / 2,076   |
| 200 KB offloaded |           1 | 813 / 1,240 → 949 / 1,401     | 421 / 662 → 581 / 804         | 857 / 1,269 → 627 / 837       |
| 200 KB offloaded |           4 | 4,145 / 4,806 → 3,906 / 7,465 | 1,066 / 1,466 → 2,404 / 5,758 | 4,194 / 4,785 → 2,966 / 6,249 |

At four concurrent offloaded runs, batching improved callback delivery rate
from 30.5 to 37.9 messages/s, but delayed the first callback from 1,066 to
2,404 ms at p50 and from 1,466 to 5,758 ms at p95. Publication p95 also grew
from 4,806 to 7,465 ms. Preparation in groups of 16 followed by publication of
the entire prepared set explains the first-delivery barrier in the current
implementation. Individual claim confirmations after publication also allow
callbacks to finish before the producer returns.

CPU deltas per native group, milliseconds, shown as median singles → batch:

| Input            | Concurrency | celld CPU     | MinIO CPU      |
| ---------------- | ----------: | ------------- | -------------- |
| Inline           |           1 | 235 → 80      | 315 → 120      |
| Inline           |           4 | 885 → 510     | 1,675 → 825    |
| 200 KB offloaded |           1 | 1,255 → 1,175 | 2,585 → 2,500  |
| 200 KB offloaded |           4 | 5,565 → 4,430 | 10,450 → 8,800 |

RSS snapshots were similar between methods within each native profile. They
reflect a shared process with accumulated run state, not isolated per-method
memory footprints. During the later SDK workload the highest observed celld
RSS snapshot was 3.00 GiB; no bounded-memory or fleet-capacity claim follows from this run.

### SDK workflows

Times are milliseconds, p50 / p95, shown as **batch capability hidden →
exposed**. Both hosts made 3,000 single sends during the timed groups and zero
batch sends. There are ten workflows per method at concurrency 1 and forty at
concurrency 4. Since the batch path never ran, timing differences cannot be
attributed to queue batching.

| Input  | Concurrency | First step p50 / p95          | Completion p50 / p95          |
| ------ | ----------: | ----------------------------- | ----------------------------- |
| 32 B   |           1 | 159 / 282 → 139 / 231         | 451 / 1,118 → 453 / 646       |
| 200 KB |           1 | 545 / 1,053 → 483 / 674       | 2,186 / 2,891 → 2,274 / 4,096 |
| 32 B   |           4 | 666 / 1,173 → 772 / 1,056     | 2,565 / 3,549 → 2,906 / 5,530 |
| 200 KB |           4 | 1,848 / 2,486 → 1,830 / 2,306 | 6,169 / 7,711 → 6,028 / 7,612 |

Raw successful report: `.perf-results/queue-batch-qualification.json` (kept
locally, excluded from the package and source control). It includes every
workflow sample, native/SDK group, throughput summary, and resource snapshot.
SHA-256: `1b493c7f751d1e62a629801357d2e622f581e7dfe0fafd94437bb30a904546c1`.

Local validation: `pnpm check` passed 621 tests plus worker bundle and package
checks; the native smoke passed 19 tests; the explicit benchmark passed its
output and dispatch-count checks. Performance promotion remains gated on the
native latency repair and actual SDK use of batching.
