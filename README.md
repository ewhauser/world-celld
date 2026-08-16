# world-celld

A [Workflow DevKit](https://useworkflow.dev) `World` backed by
[celld](https://github.com/denoland/celld).

`world-celld` stores workflow runs, hooks, and streams in celld cells. Scheduled
work is delivered with durable cell alarms. This gives Node applications a
self-hosted alternative to platform-specific Workflow backends.

> [!WARNING]
> `world-celld` is experimental and has not been proven in production. Its API
> and storage layout may change before 1.0.

## What it provides

- Workflow run, step, event, and hook persistence
- Durable streams
- Delayed work, retries, deduplication, and dead-letter storage
- Configurable cleanup of terminal workflow payloads
- An authenticated HTTP connection between a Node application and a celld fleet
- An in-process fleet for local development and conformance testing

## Try the example

You need Node.js 22 or later and pnpm 11.

```sh
pnpm install
pnpm build
pnpm --dir examples/demo-app build
pnpm --dir examples/demo-app demo
```

The example starts an in-memory fleet and runs an order workflow through steps,
a sleep, an approval hook, and an output stream. It uses the same worker, cell
classes, and HTTP protocol as a celld deployment; only persistence and cell
routing are emulated.

See [`examples/demo-app`](./examples/demo-app) for the application and workflow
source.

## Use it in an application

Install the package from npm:

```sh
pnpm add @ewhauser/world-celld
```

Workflow DevKit can load the package from environment variables:

```sh
WORKFLOW_TARGET_WORLD=@ewhauser/world-celld
CELLD_FLEET_URL=http://fleet.internal:8080
CELLD_WORLD_SECRET=replace-with-a-secret
WORKFLOW_BASE_URL=https://workflow.example.com
```

You can also construct the World directly:

```ts
import { createCelldWorld } from '@ewhauser/world-celld';

const world = createCelldWorld({
  fleetUrl: 'http://fleet.internal:8080',
  secret: process.env.CELLD_WORLD_SECRET!,
  baseUrl: 'https://workflow.example.com',
  // Keep completed, failed, and cancelled run payloads for 30 days.
  runRetentionMs: 30 * 24 * 60 * 60 * 1000,
});
```

`WORKFLOW_BASE_URL` (or `baseUrl`) is where queue cells deliver flow and step
requests. It must be reachable from every celld node.

## Deploy the worker

Before deploying, you need a celld fleet, `esbuild` on `PATH`, and an object
store that meets celld's conditional-write requirements. Refer to the
[celld documentation](https://github.com/denoland/celld) for fleet and storage
setup.

From a source checkout:

```sh
celld deploy ./celld-worker --bucket s3://my-cells-bucket
```

From an installed package, first copy the deployable worker into your project:

```sh
cp -R node_modules/@ewhauser/world-celld/celld-worker ./workflow-world
celld deploy ./workflow-world --bucket s3://my-cells-bucket
```

The worker rejects stateful requests unless `WORLD_SECRET` is configured. Pass
the same secret to the fleet and the application:

```sh
CELLD_VAR_WORLD_SECRET="$CELLD_WORLD_SECRET" \
celld --bucket s3://my-cells-bucket \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise 10.0.0.12:8081
```

Use a secret manager rather than putting the value in `wrangler.jsonc`. Keep
celld's internal listener on a trusted network; the World bearer token protects
the worker RPC routes, not celld's administrative endpoints.

More deployment detail is in [`celld-worker/README.md`](./celld-worker/README.md).

## How it works

```text
Workflow application
  |
  | authenticated HTTP RPC
  v
celld worker router
  |-- WorkflowRunDO  one cell per workflow run
  |-- StreamDO       stream chunks and run/stream indexes
  |-- IndexDO        run and hook lookup indexes
  `-- QueueDO        delayed delivery, retries, and dead letters
          |
          | HTTP callbacks
          v
Workflow application /.well-known/workflow/v1/{flow|step}
```

The application-side package implements the Workflow `World` interface and
translates its storage, stream, and queue operations into RPC calls. The celld
worker accepts only a fixed set of methods and routes each request to the named
cell that owns the data.

## Configuration

Application options can be passed to `createCelldWorld()` unless an environment
variable is shown below.

| Option           | Environment variable     | Default                  |
| ---------------- | ------------------------ | ------------------------ |
| `fleetUrl`       | `CELLD_FLEET_URL`        | required                 |
| `secret`         | `CELLD_WORLD_SECRET`     | required with `fleetUrl` |
| `baseUrl`        | `WORKFLOW_BASE_URL`      | `http://localhost:$PORT` |
| `deploymentId`   | `CELLD_DEPLOYMENT_ID`    | `celld-default`          |
| `queueShards`    | —                        | `1`                      |
| `runRetentionMs` | `CELLD_RUN_RETENTION_MS` | `0` (disabled)           |
| `readPollMs`     | —                        | `250`                    |
| `rpcTimeoutMs`   | —                        | `30000`                  |

The deployed worker also accepts these celld variables:

| Variable                    | Default  | Purpose                                              |
| --------------------------- | -------- | ---------------------------------------------------- |
| `WORLD_SECRET`              | none     | Required bearer secret for RPC routes                |
| `WORKFLOW_CALLBACK_SECRET`  | none     | Sent with deliveries as `x-workflow-callback-secret` |
| `QUEUE_MAX_ATTEMPTS`        | `5`      | Attempts before a message is dead-lettered           |
| `QUEUE_MAX_INFLIGHT`        | `5`      | Concurrent deliveries per queue cell                 |
| `QUEUE_DELIVERY_TIMEOUT_MS` | `300000` | Timeout for an application callback                  |

`queueShards` is part of queue placement and is pinned when a queue cell is
first used. Drain pending work before changing it.

## Terminal run retention

When `runRetentionMs` is greater than zero, a terminal transition atomically
records the run's `expiredAt` and arms its cell alarm. The complete run, event,
step, hook, and stream data remains readable until that deadline. Active and
pending runs are never eligible for automatic cleanup.

At expiration, the run cell fences new writes and removes the run's derived
indexes, stream chunks, pending and dead-letter queue messages, and durable run
payloads. Cleanup is a persisted, idempotent state machine: an interrupted
phase records its error and re-arms itself with capped backoff.

The final state is a metadata-only tombstone, not an empty cell. It prevents a
delayed queue delivery or stale RPC from recreating an expired run. Reads and
writes against that run return `RunExpiredError`, and the run no longer appears
in listings. Tombstones contain no workflow input, output, event, step, hook,
stream, or queue payload.

The returned World exposes authenticated operational methods:

```ts
const status = await world.retention.getStatus(runId);
await world.retention.schedule(runId); // requires runRetentionMs > 0
await world.retention.cleanupNow(runId);
await world.retention.rearm(runId); // recover a missed or abandoned alarm
```

The retention deadline is pinned when the run becomes terminal. Changing the
configuration affects newly terminal runs; call `schedule()` explicitly for
an existing terminal run that has no cleanup record.

## Operational notes

- Delivery is at least once. Workflow steps and other external side effects
  must be idempotent.
- Queue cells expose `stats`, `listDeadLetters`, `redriveDeadLetter`,
  `purgeDeadLetters`, and `rearmAlarm` through the authenticated RPC endpoint.
- Run cells expose retention status, scheduling, immediate cleanup, and alarm
  recovery through `world.retention`.
- A new application URL applies to newly enqueued messages. Existing messages
  retain the callback URL with which they were created.
- Fleet restarts can interrupt in-flight callbacks; expired claims are
  delivered again.

## Development

```sh
pnpm format
pnpm check
```

`pnpm check` runs formatting, linting, type checking, the build, tests, and the
worker and npm-package checks. The package check inspects the exact tarball and
installs it in a clean temporary consumer with lifecycle scripts disabled.
Oxlint runs its correctness, suspicious, and performance categories with
type-aware checks and warnings denied. The default test suite includes the
upstream `@workflow/world-testing` conformance suite and does not require celld.
To run the live fleet tests:

```sh
CELLD_FLEET_URL=http://fleet.internal:8080 \
CELLD_WORLD_SECRET=replace-with-a-secret \
pnpm test:integration
```

### Local MinIO performance and loss test

The opt-in performance harness starts a fresh MinIO bucket and a single celld
node with Docker Compose. Its queue workload verifies that every accepted
message reaches a successful callback, including forced `503` redeliveries. A
second workload creates terminal runs with streams and delayed queue messages,
then verifies complete payload cleanup without resurrection. Results include
queue and cleanup throughput plus p50, p95, p99, and maximum latency and are
saved under `.perf-results/`.

> MinIO Community is **not a supported celld production store**. It does not
> implement the conditional writes celld needs for ownership fencing. This
> deliberately single-node harness disables celld's storage probe and tests
> the queue's persistence, redelivery, and performance paths only; it does not
> validate multi-node ownership or failover correctness.

With Docker and the Compose plugin installed, run:

```sh
pnpm test:perf:minio
```

The defaults send 1,000 messages with concurrency 32 across two queue cells,
and force every twentieth message through one `503` redelivery.

#### Reference result

The following directional baseline was recorded on August 15, 2026. MinIO and
celld ran as native arm64 processes because Docker was unavailable on the test
machine; container results will differ.

| Environment | Value                                                                                                    |
| ----------- | -------------------------------------------------------------------------------------------------------- |
| Machine     | MacBook Pro (Mac15,8), Apple M3 Max, 16 cores (12 performance, 4 efficiency), 64 GB RAM                  |
| OS          | macOS 26.5.2 (25F84), arm64                                                                              |
| Services    | celld v0.2.1; MinIO RELEASE.2025-09-07T16-13-09Z                                                         |
| Workload    | 1,000 messages; concurrency 32; 2 queue shards; 256-byte target payload; every 20th message retried once |

| Metric                  |                                                                            Result |
| ----------------------- | --------------------------------------------------------------------------------: |
| Delivery integrity      | 1,000 accepted, 1,000 delivered, 0 missing, 0 duplicate successes, 0 dead letters |
| Callback attempts       |                                           1,050, including 50 forced redeliveries |
| Enqueue throughput      |                                                                 882.55 messages/s |
| Delivery throughput     |                                                                 808.81 messages/s |
| Enqueue latency         |                          p50 24.37 ms; p95 75.60 ms; p99 176.67 ms; max 200.36 ms |
| All delivery latency    |                         p50 53.95 ms; p95 167.86 ms; p99 210.84 ms; max 312.03 ms |
| First-attempt latency   |                         p50 52.55 ms; p95 166.35 ms; p99 209.80 ms; max 212.01 ms |
| Retried-message latency |                         p50 82.52 ms; p95 262.63 ms; p99 312.03 ms; max 312.03 ms |
| Final queue state       |                         0 pending, 0 in flight, 0 dead letters across both shards |

Use these numbers as a smoke-test reference, not a portable performance
guarantee. For regression tracking, compare repeated runs on the same machine
and runtime configuration. Two shards gave the best latency balance for this
workload; benchmark your own traffic before changing the shard count. A fleet
pins its shard count on first use, so changing it requires draining the queues.

Override the workload or set machine-specific regression budgets with
environment variables:

```sh
PERF_MESSAGES=10000 \
PERF_CONCURRENCY=64 \
PERF_QUEUE_SHARDS=16 \
PERF_MIN_DELIVERY_PER_SECOND=100 \
PERF_MAX_DELIVERY_P99_MS=5000 \
pnpm test:perf:minio
```

Useful controls are `PERF_PAYLOAD_BYTES`, `PERF_RETRY_EVERY`,
`PERF_TIMEOUT_MS`, `PERF_QUEUE_MAX_INFLIGHT`,
`PERF_MIN_ENQUEUE_PER_SECOND`, `PERF_MIN_DELIVERY_PER_SECOND`, and
`PERF_MAX_DELIVERY_P99_MS`. Throughput and latency budgets default to disabled
because local machines vary. Message-loss, dead-letter, message-ID, queue-drain,
and duplicate-success checks are always enforced.

Bug reports and focused pull requests are welcome. Please include a regression
test for behavior changes and run the checks above before submitting a PR.

## Supply-chain security

CI and release workflows start with no permissions and grant only the access a
job needs. Third-party actions are pinned to verified release commits. Release
builds do not use caches, and the exact npm tarball is checksummed, clean-room
installed, and verified again before publication.

Release Please maintains version and changelog pull requests. Releases publish
from GitHub-hosted runners through npm trusted publishing (OIDC), without a
long-lived npm token. npm records provenance for public releases. A draft
GitHub release holds the release notes while the package is verified; the Git
tag and public release are created only after npm accepts the matching tarball.
Published GitHub releases are immutable. Dependency updates use cooldowns, and
pnpm permits lifecycle scripts only for an explicit allowlist.

See [`SECURITY.md`](./SECURITY.md) to report a vulnerability and
[`RELEASING.md`](./RELEASING.md) for the release procedure.

## License

Apache-2.0. Parts of the implementation are adapted from
[`vinnymac/worlds`](https://github.com/vinnymac/worlds), also under Apache-2.0.
See [`NOTICE`](./NOTICE) for details.
