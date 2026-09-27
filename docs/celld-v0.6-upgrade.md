# celld v0.6.0 upgrade and feature review

Reviewed September 26, 2026. The previous runtime baseline was celld v0.5.0;
v0.6.0 is the latest published runtime release, and world-celld now requires
it. The World data model, binding names, worker configuration, and node
environment variables are unchanged. The npm and runtime versions are
independent.

Sources: [v0.5.1 release](https://github.com/denoland/celld/releases/tag/v0.5.1),
[v0.6.0 release](https://github.com/denoland/celld/releases/tag/v0.6.0), and
[versioned runtime documentation](https://github.com/denoland/celld/blob/v0.6.0/docs/README.md).

Fleets still on celld v0.4.x must first apply the configuration changes in the
[v0.5.0 upgrade guide](https://github.com/ewhauser/world-celld/blob/main/docs/celld-v0.5-upgrade.md)
(`CELLD_VAR_*` removal, telemetry variables, removed tuning variables).

## Required deployment changes

1. celld defaults to `fleet` durability, which a fleet of two or more nodes
   uses unless `CELLD_DURABILITY=bucket` is set. For such a fleet, schedule
   downtime and stop **every old celld node** before starting v0.6.0. A v0.6.0
   node refuses to start when a v0.5.x follower returns the old log-tail
   format, so a fleet-durability fleet cannot mix the versions.
2. A single node, or a fleet with `CELLD_DURABILITY=bucket`, has no followers.
   celld documents a rolling update from v0.5.1 for that case. From v0.5.0,
   either roll to v0.5.1 first or use the full stop above.
3. Do not start a v0.5.x binary against the bucket after v0.6.0 has run.
   celld does not document a downgrade path.
4. Using the v0.6.0 CLI, deploy the configured Queue consumer first, then the
   primary worker. Start v0.6.0 nodes with the existing bucket, storage
   credentials, listener configuration, and node environment. Check an
   authenticated World call and a queued callback before reopening traffic.

There is no World data-model or binding-name migration in this change. Existing
native Queue attachments, Queue payload keys, and Durable Object storage retain
their identities.

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
manager. Details are in the
[v0.6.0 telemetry documentation](https://github.com/denoland/celld/blob/v0.6.0/docs/telemetry.md).

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

| Change                                                                                           | Relevance to world-celld                                                                                                                     | Decision                                                                                                                                             |
| ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| v0.6.0 ranged log-tail recovery for fleet durability                                             | Run, catalog, hook, stream, and Queue cells recover through this path after a node loss.                                                     | Adopt through the upgrade. It forces the full-stop upgrade above. The two-node failover harness covers owner loss on v0.6.0.                         |
| v0.6.0 per-cell memory: bounded SQL statement cache, one object-store client per cell            | Every World cell uses SQL-backed storage.                                                                                                    | Adopt through the upgrade. Keep bounded cleanup and paging; these still limit application memory and work.                                           |
| v0.6.0 #228 re-armed alarms fire on time despite pending handler timers                          | Retention progress and native Queue delivery depend on durable alarms.                                                                       | Adopt through the upgrade. Keep the existing bounded alarm state machines; no workaround is removed in this change.                                  |
| v0.6.0 independent facet databases and facet migration on first open                             | world-celld does not use facets.                                                                                                             | No action. Facet adoption remains deferred for the reasons in the v0.5.0 review.                                                                     |
| v0.6.0 R2 empty key segments and #232 non-ASCII `list()` keys                                    | Queue payload keys are `workflow-queue/<runId>/<messageId>` with URI-encoded, non-empty segments. The worker never lists the payload bucket. | No action. Existing payload objects keep their keys.                                                                                                 |
| v0.6.0 stricter workerd parity (`WorkerCode`, wasm modules, string exports, `transactionSync()`) | The worker bundles export only classes and functions, use no Dynamic Workers or wasm, and use async `transaction()`.                         | No action. `pnpm check` bundles both workers under workerd conditions.                                                                               |
| v0.6.0 `ctx.exports` entrypoints, Ed25519/X25519, UTF-8 `Headers`, WebSocket `wasClean`          | No current dependency.                                                                                                                       | Accept runtime fixes; no new API dependency is needed in the current worker bundle.                                                                  |
| v0.5.1 Dynamic Worker limits and Tail reports, `celld r2` CLI, Wrangler `define`/`rules`         | `celld r2` can inspect Queue payload objects under the `WORKFLOW_QUEUE_PAYLOADS` binding without a running node.                             | Optional operator tool. The remaining additions support a future worker-hosted execution service and are deferred until there is such a requirement. |
| v0.5.1 LTX page-cache caps, compaction of retained bundles, `/evict` and `/state` reporting      | Lower node memory and better operator diagnostics.                                                                                           | Adopt defaults.                                                                                                                                      |

The native Queue limits remain 128,000 bytes per message, 86,400 seconds of
producer delay, and four days of broker retention in
[the v0.6.0 queue policy](https://github.com/denoland/celld/blob/v0.6.0/crates/logic/queue.rs).
Keep external payload storage and long-delay republishing.

## Repository validation

The CI/native smoke and Docker performance harness pin v0.6.0. The native
runner verifies the release asset digests on Linux x86-64 and macOS arm64
against the digests GitHub publishes for the release.

These are fresh v0.6.0 deployments; they do not establish recovery of a
production v0.5.x bucket.

Validation on macOS arm64:

- `pnpm check` passed (25 test files, 470 tests, worker bundle checks, and
  package checks).
- `pnpm test:integration:celld-smoke` passed all 10 real v0.6.0 tests.
- `pnpm test:perf:minio` passed the queue, mixed workflow lifecycle, and
  retention workloads.
- `pnpm test:perf:minio:failover` passed: 96 of 96 accepted messages delivered
  across a primary SIGKILL, with no duplicate callbacks, no mismatched message
  IDs, and about two seconds to peer ownership.

No v0.5.0 performance baseline was preserved for this comparison, so no
performance change is claimed.
