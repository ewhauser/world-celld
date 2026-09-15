# celld v0.5.0 upgrade and feature review

Reviewed September 15, 2026. The previous runtime baseline was celld v0.4.0;
v0.5.0 is the latest published runtime release. world-celld 0.5.0 adopts it
as a breaking release: celld v0.4.x and node-level worker-variable overrides
are no longer supported. The npm and runtime versions are independent.

Sources: [v0.4.1 release](https://github.com/denoland/celld/releases/tag/v0.4.1),
[v0.5.0 release](https://github.com/denoland/celld/releases/tag/v0.5.0), and
[versioned runtime documentation](https://github.com/denoland/celld/blob/v0.5.0/docs/README.md).

## Required deployment changes

1. Schedule downtime and stop **every old celld node** before starting v0.5.0.
   The v0.5.0 release requires a full fleet stop even from v0.4.1. Do not mix
   v0.4.x and v0.5.0 nodes or assume an in-place downgrade is safe.
2. Move `CELLD_VAR_*` worker overrides into `vars` in private copies of the
   Wrangler configs. Both scripts need the same `WORLD_SECRET`. The primary
   worker also accepts `WORKFLOW_CALLBACK_SECRET`, `WORKFLOW_RETENTION_MS`, and
   `WORKFLOW_RETENTION_BATCH_SIZE`. Preserve existing values when migrating.
   Remove the old node variables: v0.5.0 rejects them at startup.
3. Keep these config copies outside source control and populate them from your
   secret manager. Variables become part of the deployment in the fleet bucket;
   restrict access accordingly. `.dev.vars` is only for `celld dev` and is not
   uploaded by `celld deploy`.
4. Remove `CELLD_STORAGE_PROBE` and `CELLD_OUTPUT_GATE` if configured, including
   values of `0`, `1`, or an empty string. Storage-contract checks and durability
   gating are mandatory. Consult the versioned runtime documentation for other
   removed tuning variables, including shutdown and Queue batching settings.
5. If using OTLP, remove `CELLD_OTEL_SINK` and set
   `CELLD_OTEL=https://your-collector:4318` to the collector base URL. The old
   `OTEL_EXPORTER_OTLP_ENDPOINT` variables no longer select the destination.
   `CELLD_OTEL=1` still writes telemetry into the fleet bucket.
6. Using the v0.5.0 CLI, deploy the configured Queue consumer first, then the
   primary worker. Start v0.5.0 nodes with the existing bucket, storage
   credentials, and listener configuration. Check an authenticated World call
   and a queued callback before reopening traffic.

There is no World data-model or binding-name migration in this change. Existing
native Queue attachments and payload keys retain their identities. The older
QueueDO-to-native-Queue cutover instructions apply only to installations that
still use QueueDO.

## Adopt telemetry

Enable bucket telemetry on every node to record runtime traces and logs without
adding a collector. Add these variables to the existing node service definition:

```sh
CELLD_OTEL=1
CELLD_OTEL_RETENTION=30d
OTEL_SERVICE_NAME=world-celld
```

celld writes Parquet files under `telemetry/` in the fleet bucket and expires
them after 30 days. The default flush interval is five minutes. Telemetry
retention is independent of World run retention. This is an opt-in operational
setting; installing the package does not change node environments.

For an existing OTLP collector, use these settings instead:

```sh
CELLD_OTEL=https://collector.example.com:4318
OTEL_SERVICE_NAME=world-celld
```

celld appends `/v1/traces` and `/v1/logs` to the collector base URL. Supply any
collector credentials through `OTEL_EXPORTER_OTLP_HEADERS` from your secret
manager. There is no fallback to the removed sink or endpoint configuration.

## Development variables

For `celld dev`, put local values in `.dev.vars` beside the Wrangler config:

```dotenv
WORLD_SECRET=local-development-secret
WORKFLOW_RETENTION_MS=0
```

Use the same local secret in the consumer and primary worker configurations.
Changes reload automatically. Both `.dev.vars` and `.celld/` are ignored by
this repository; add those ignore rules when copying the workers elsewhere.
These variables do not reach a production deployment. Keep using the native
smoke for verification of the complete two-script topology.

## Feature decisions

| Addition                                                                                                     | Relevance to world-celld                                                                                                                                          | Decision                                                                                                                                                                                                         |
| ------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| v0.4.1 native Queue producer grouping and stronger durability gates                                          | Existing `WORKFLOW_QUEUE.send()` calls gain runtime batching and durability improvements.                                                                         | Adopt through the runtime upgrade. Keep pointer messages and application idempotency; runtime improvements do not imply exactly-once callbacks. Do not infer a project throughput gain from upstream benchmarks. |
| v0.4.1 fleet balancing, paged restore, restart recovery, and fewer false durability timeouts                 | Run, catalog, hook, and stream cells benefit without changing their addressing or schema.                                                                         | Adopt defaults. The local restart smoke covers one process and bucket recovery, not multi-node balancing or handoff.                                                                                             |
| v0.4.1 alarm scheduling and v0.5.0 alarm/background-work fixes                                               | Retention progress and native Queue delivery depend on durable alarms; stream code depends on asynchronous request handling.                                      | Keep existing bounded alarm state machines and stream cancellation logic. Validate against the real runtime.                                                                                                     |
| v0.5.0 lower per-cell memory, faster KV prefix listing, transaction/SQL and recovery fixes                   | Existing storage transactions and paginated prefix scans use these paths.                                                                                         | Adopt through the upgrade. Keep bounded cleanup and paging; these still limit application memory and work.                                                                                                       |
| v0.4.1 Durable Object facets, with v0.5.0 rollback fixes                                                     | Facets could place related run/stream state under one owner, but current streams and global indexes have independent identities and lifecycles.                   | Defer. Adoption needs a data migration and contention/failure analysis; it is not a drop-in storage optimization.                                                                                                |
| v0.5.0 service-binding fetch on named entrypoints, default-binding RPC, nested RPC properties                | The companion consumer already uses default service-binding fetch, with explicit authentication and HTTP status handling. External Node clients use HTTP as well. | Keep the current bridge. RPC would require a separate internal contract and error mapping; it would not remove the public HTTP API.                                                                              |
| v0.5.0 native Workflow retention and manual deletion                                                         | These APIs govern celld's Cloudflare-compatible Workflow instances, not Vercel Workflow records in `WorkflowRunDO`.                                               | Do not substitute them for World retention. Keep run fences, catalog cleanup, stream deletion, and payload cleanup.                                                                                              |
| v0.5.0 `.dev.vars` with reload                                                                               | Useful for local credentials and retention settings.                                                                                                              | Ignore `.dev.vars` and `.celld/` in Git and document the development-only scope. The existing two-script smoke remains the complete local integration check.                                                     |
| v0.5.0 simplified telemetry configuration                                                                    | Fleet traces and logs can help diagnose Queue delays, retention alarms, and recovery.                                                                             | Recommend opt-in bucket telemetry or an existing OTLP collector. Document the new configuration without enabling telemetry by default.                                                                           |
| v0.5.0 Containers/Sandbox SDK, worker loaders with service bindings/custom outbound fetch, and prebuilt WASM | Could support a future worker-hosted execution service. Today the World stores state and delivers callbacks to an external workflow application.                  | Defer until there is a concrete execution-hosting requirement; these do not improve the current storage adapter by themselves.                                                                                   |
| v0.4.1 expanded Node/web APIs and v0.5.0 host-state isolation                                                | Broader compatibility and stronger runtime isolation.                                                                                                             | Accept runtime fixes; no new API dependency is needed in the current worker bundle.                                                                                                                              |

The native Queue limits remain 128,000 bytes per message, 86,400 seconds of
producer delay, and four days of broker retention in
[the v0.5.0 queue policy](https://github.com/denoland/celld/blob/v0.5.0/crates/logic/queue.rs).
Keep external payload storage and long-delay republishing.

Telemetry migration details are in the
[v0.5.0 telemetry documentation](https://github.com/denoland/celld/blob/v0.5.0/docs/telemetry.md).

## Repository validation

The CI/native smoke and Docker performance harness pin v0.5.0. The native
runner verifies the release asset digests on Linux x86-64 and macOS arm64.
MinIO downloads use the official GitHub release assets with the existing
checksums because the former download host returns HTTP 410.

The native smoke deploys secrets in the temporary Worker configs and runs
without a storage-probe bypass. It tests acknowledged run/stream state and
accepted Queue delivery across a process restart, cancelled stream long polls,
and multi-page retention recovery after SIGKILL. These are fresh v0.5.0
deployments; they do not establish recovery of a production v0.4.0 bucket.

Run `pnpm check` and `pnpm test:integration:celld-smoke` for the repository and
runtime checks. Run `pnpm test:perf:minio` with Docker Compose to measure this
project's workload; compare against a separately preserved v0.4.0 baseline
before claiming a performance improvement.

Validation on macOS arm64: `pnpm check` passed (24 test files, 434 tests,
worker bundle checks, and package checks). All four real v0.5.0 smoke tests passed, including persisted bucket telemetry. The Docker performance harness could not run because the local
Docker installation lacks the Compose plugin; no performance gain is claimed.
