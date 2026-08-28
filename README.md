# world-celld

A [Workflow DevKit](https://useworkflow.dev) `World` backed by
[celld](https://github.com/denoland/celld).

`world-celld` stores workflow runs, hooks, and streams in celld cells. Scheduled
work is delivered through celld's native Queues, with run-bearing message bodies
stored in the fleet's object store. This gives Node applications a
self-hosted alternative to platform-specific Workflow backends.

> [!WARNING]
> `world-celld` is experimental and has not been proven in production. Its API
> and storage layout may change before 1.0.

## What it provides

- Workflow run, step, event, and hook persistence
- Durable streams
- Delayed work, retries, consumer-side deduplication, and dead-letter routing
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

`WORKFLOW_BASE_URL` (or `baseUrl`) is where the native Queue bridge delivers flow
requests. It must be reachable from every celld node.

## Deploy the worker

Before deploying, you need a celld v0.4.0 fleet, `esbuild` on `PATH`, and an
object store that meets celld's conditional-write requirements. Refer to the
[celld documentation](https://github.com/denoland/celld) for fleet and storage
setup.

No second storage service is required for queue payloads. celld v0.4.0 serves
the `WORKFLOW_QUEUE_PAYLOADS` binding from the existing fleet bucket under
`r2/workflow-world-queue-payloads/`. `r2_buckets` is the Wrangler-compatible
configuration key for that binding; it does not require Cloudflare R2.

Deploy the Queue consumer first, then the primary HTTP worker. The order matters:
the consumer deploy creates the Queue attachment; the primary deploy must go last
so it remains the fleet's public application.

From a source checkout:

```sh
celld deploy ./celld-queue-worker --bucket s3://my-cells-bucket
celld deploy ./celld-worker --bucket s3://my-cells-bucket
```

From an installed package, first copy the deployable worker into your project:

```sh
cp -R node_modules/@ewhauser/world-celld/celld-worker ./workflow-world
cp -R node_modules/@ewhauser/world-celld/celld-queue-worker ./workflow-world-queue
celld deploy ./workflow-world-queue --bucket s3://my-cells-bucket
celld deploy ./workflow-world --bucket s3://my-cells-bucket
```

This is a hard queue cutover. Existing `QueueDO` messages and dead letters are
not migrated into celld's native Queue; drain or otherwise account for them
before upgrading an existing fleet. The old QueueDO cell data becomes
unreachable after the binding is removed.

The worker rejects stateful requests unless `WORLD_SECRET` is configured. Pass
the same secret to the fleet and the application:

```sh
CELLD_VAR_WORLD_SECRET="$CELLD_WORLD_SECRET" \
CELLD_VAR_WORKFLOW_RETENTION_MS=7776000000 \
celld --bucket s3://my-cells-bucket \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise 10.0.0.12:8081
```

Use a secret manager rather than putting the value in `wrangler.jsonc`. Keep
celld's internal listener on a trusted network; the World bearer token protects
the worker RPC routes, not celld's administrative endpoints.

The example sets a fleet-wide maximum workflow age of 90 days. Leave
`WORKFLOW_RETENTION_MS` at `0` to disable that policy.

More deployment detail is in [`celld-worker/README.md`](./celld-worker/README.md)
and [`celld-queue-worker/README.md`](./celld-queue-worker/README.md).

## How it works

```text
Workflow application
  |
  | authenticated HTTP RPC + binary stream batches
  v
celld worker router
  |-- scheduled       hourly maximum-age retention discovery
  |-- WorkflowRunDO  one cell per workflow run
  |-- RunCatalogDO   16 stable run-id shards; merged ordered listing
  |-- HookTokenDO    32 stable token-ownership shards
  |-- HookIdDO       32 stable hook-id lookup shards
  |-- StreamDO       stream chunks and run/stream indexes
  |-- object storage run-bearing queue payload bodies in the fleet bucket
  `-- native Queue producer
          |
          v
native Queue --> companion consumer --> service binding --> worker router
                                                        |
                                                        v
                         Workflow application /.well-known/workflow/v1/flow
```

The application-side package implements the Workflow `World` interface and
translates its storage and queue operations into JSON RPC calls. Stream chunk
writes and bounded long-poll reads use a binary batch route; stream control and
retention operations remain fixed, authenticated RPC methods. The worker routes
each request to the named cell that owns the data.

Index routing is a versioned hard-cutover protocol. Run catalog writes hash the
`runId` across 16 shards, while hook ownership and hook-ID lookup hash their
natural keys across 32 shards each. A single stateless worker request queries
all 16 catalog shards in parallel and merges their lexicographic pages before
authoritative RunDO reads. The RunDO is also the only permanent lifecycle
authority: hook shards and queue idempotency claims consult it directly, while
catalog expiry fences compact after bounded protocol leases. Token and hook-ID admission are
each transactional inside their natural-key shards. Exact claim IDs plus a
serialized cancellation fence resolve ambiguous delivery without pretending
the two ownership shards form a cross-Durable-Object transaction. Stateless cohesive index routes keep
catalog listing, terminal commits, hook finalization, and hook release to one
public request each while exposing the internal shard work in measurements.

The routing rationale and before/after RPC, storage, latency, and contention
evidence are recorded in [`docs/index-sharding.md`](./docs/index-sharding.md).
The expiry authority, producer/consumer map, bounded horizons, and
post-compaction costs are recorded in
[`docs/lifecycle-compaction.md`](./docs/lifecycle-compaction.md).

## Configuration

Application options can be passed to `createCelldWorld()` unless an environment
variable is shown below.

| Option                  | Environment variable     | Default                  |
| ----------------------- | ------------------------ | ------------------------ |
| `fleetUrl`              | `CELLD_FLEET_URL`        | required                 |
| `secret`                | `CELLD_WORLD_SECRET`     | required with `fleetUrl` |
| `baseUrl`               | `WORKFLOW_BASE_URL`      | `http://localhost:$PORT` |
| `deploymentId`          | `CELLD_DEPLOYMENT_ID`    | `celld-default`          |
| `runRetentionMs`        | `CELLD_RUN_RETENTION_MS` | `0` (disabled)           |
| `streamLongPollMs`      | —                        | `20000`                  |
| `streamFlushIntervalMs` | —                        | `0`                      |
| `rpcTimeoutMs`          | —                        | `30000` (max `300000`)   |

The deployed worker also accepts these celld variables:

| Variable                        | Default | Purpose                                              |
| ------------------------------- | ------- | ---------------------------------------------------- |
| `WORLD_SECRET`                  | none    | Required bearer secret for RPC routes                |
| `WORKFLOW_CALLBACK_SECRET`      | none    | Sent with deliveries as `x-workflow-callback-secret` |
| `WORKFLOW_RETENTION_MS`         | `0`     | Maximum run age from creation; includes active runs  |
| `WORKFLOW_RETENTION_BATCH_SIZE` | `128`   | Runs admitted by each cron sweep (maximum `1000`)    |

Queue deadlines use fixed-width 13-digit epoch-millisecond keys. A requested
`delaySeconds` or handler redelivery timeout is accepted only when its deadline
leaves room for the 15-minute idempotency-claim window below the absolute
`9999999999999` limit. A suspension that cannot preserve that headroom becomes
a normal broker retry instead of being clamped. celld's native
producer delay is capped at 86,400 seconds, so the consumer chains longer waits
while preserving the absolute deadline and stable message identity. Long
test-mode waits are chunked at the host timer limit without changing this
deadline contract.

## Workflow retention

### Fleet-wide maximum age

Set the deployed worker's `WORKFLOW_RETENTION_MS` variable to expire every
workflow after a maximum age measured from `createdAt`. For example,
`7776000000` is 90 days. This policy applies to pending, running, completed,
failed, and cancelled runs.

The packaged worker declares an hourly UTC celld cron trigger. Each occurrence
scans one bounded creation-time catalog page and asks the authoritative run
cells to enforce the cutoff. A run cell immediately fences reads, writes, stream
activity, and queue payload work, removes its catalog entry, then finishes the
existing bounded stream, object-store queue-payload, and run-payload cleanup phases through
its durable alarm.
Repeated cron invocations and alarm retries are idempotent.

One occurrence admits at most `WORKFLOW_RETENTION_BATCH_SIZE` runs (default
`128`, maximum `1000`). If the fleet was down or a shorter policy creates a
backlog, later occurrences continue with the next oldest entries. Edit
`triggers.crons` in the copied `celld-worker/wrangler.jsonc` if hourly discovery
does not provide the desired expiration resolution or catch-up rate.

### Terminal payload retention

When `runRetentionMs` is greater than zero, a terminal transition atomically
records the run's `expiredAt` and arms its cell alarm. The complete run, event,
step, hook, and stream data remains readable until that deadline. Active and
pending runs are never eligible for automatic cleanup.

At expiration, the run cell fences new writes and removes the run's derived
indexes, stream chunks, object-store queue payloads, and durable run payloads.
Native Queue pointer messages can remain until celld's fixed four-day retention
expires; a pointer whose object-store body was removed is acknowledged as permanently gone
and cannot resurrect the run. Cleanup is a persisted, idempotent state machine. Each alarm processes
one bounded page, persists its progress, and re-arms the next alarm; an
interrupted phase records its error and retries with capped backoff.

Terminal hook and wait disposal uses the same alarm mechanism even when run
retention is disabled, so a terminal event never performs an unbounded storage
transaction.

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
an existing terminal run that has no cleanup record. If terminal retention and
fleet-wide maximum age are both enabled, the earlier deadline wins.

## Operational notes

- Delivery is at least once. Workflow steps and other external side effects
  must be idempotent.
- Native Queue operations are administered with celld's `queue info`, `peek`,
  `pause`, `resume`, `purge`, and `redrive` commands.
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

### Real celld restart smoke

The required CI smoke owns native celld v0.4.0 and MinIO processes on loopback,
uses fresh temporary bucket and runtime state, and kills celld with `SIGKILL`
before deleting its local working state and starting a new process against the
bucket-backed state. It checks that:

- acknowledged run and stream state survives the process restart;
- an accepted delayed queue message that becomes due while celld is down is
  delivered once after the native broker is restored;
- the companion Queue consumer can call the primary worker through its service
  binding and recover run-bearing payloads from the fleet object store;
- multi-page retention cleanup continues from a persisted nonterminal phase;
- cancelling an in-flight HTTP long poll leaves the stream writable and
  readable.

Run the same bounded smoke locally on Linux x86-64 or macOS arm64:

```sh
pnpm test:integration:celld-smoke
```

The runner downloads celld and MinIO artifacts at pinned versions and verifies
their SHA-256 digests before use. MinIO does not implement the conditional-write
contract celld requires for production ownership fencing, so the smoke disables
the storage probe and runs exactly one celld process at a time. It proves the
single-process restart boundaries above, not multi-node ownership, handoff, or
failover correctness.

### Local MinIO performance and loss test

The opt-in performance harness starts a fresh MinIO bucket and a single celld
node with Docker Compose. Its queue workload verifies that every accepted
message reaches a successful callback, including forced `503` redeliveries. A
second workload creates terminal runs with streams and delayed queue messages,
then verifies complete payload cleanup without resurrection. Results include
queue and cleanup throughput plus p50, p95, p99, and maximum latency and are
saved under `.perf-results/`. The harness pins celld v0.4.0 and deploys the same
two-script native Queue topology as the restart smoke.

> MinIO Community is **not a supported celld production store**. It does not
> implement the conditional writes celld needs for ownership fencing. This
> deliberately single-node harness disables celld's storage probe and tests
> the queue's persistence, redelivery, and performance paths only; it does not
> validate multi-node ownership or failover correctness.

With Docker and the Compose plugin installed, run:

```sh
pnpm test:perf:minio
```

The defaults send 1,000 messages with concurrency 32 and force every twentieth
message through one `503` redelivery. The old QueueDO/shard baseline is not
comparable to the native celld Queue path and was intentionally removed; record
a fresh machine-specific baseline before enabling budgets.

Override the workload or set machine-specific regression budgets with
environment variables:

```sh
PERF_MESSAGES=10000 \
PERF_CONCURRENCY=64 \
PERF_MIN_DELIVERY_PER_SECOND=100 \
PERF_MAX_DELIVERY_P99_MS=5000 \
pnpm test:perf:minio
```

Useful controls are `PERF_PAYLOAD_BYTES`, `PERF_RETRY_EVERY`,
`PERF_TIMEOUT_MS`,
`PERF_MIN_ENQUEUE_PER_SECOND`, `PERF_MIN_DELIVERY_PER_SECOND`, and
`PERF_MAX_DELIVERY_P99_MS`. Throughput and latency budgets default to disabled
because local machines vary. Message-loss, message-ID, callback validity, and
duplicate-success checks are always enforced.

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
