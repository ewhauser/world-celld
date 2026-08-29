# world-celld worker

The primary celld-deployable half of `@ewhauser/world-celld`: five cell classes
(WorkflowRunDO, RunCatalogDO, HookTokenDO, HookIdDO, and StreamDO) behind an
authenticated HTTP router. Storage and control methods use fixed JSON RPC
routes; stream chunks use bounded binary batch writes and binary long-poll
reads. Queue producers use celld's native Queue binding, with run-bearing
payload bodies stored in the fleet's object store.

## Deploy

Copy this directory and the companion Queue consumer out of `node_modules` so
`celld deploy` can bundle them. Deploy the consumer first and this primary
worker last:

```sh
cp -r node_modules/@ewhauser/world-celld/celld-worker ./workflow-world
cp -r node_modules/@ewhauser/world-celld/celld-queue-worker ./workflow-world-queue
celld deploy ./workflow-world-queue --bucket s3://my-cells-bucket
celld deploy ./workflow-world --bucket s3://my-cells-bucket
```

Upgrading from the former QueueDO implementation is a hard cutover. Drain or
account for its pending messages and dead letters first; they are not migrated
to the native Queue.

Requirements:

- celld v0.4.0 (the currently tested runtime baseline).
- `esbuild` on PATH (celld shells out to it).
- A bucket with conditional-write support (celld's fencing requirement).
- `WORLD_SECRET` injected at the node level (`CELLD_VAR_WORLD_SECRET=...`) —
  the router fails closed with 503 while it is empty.

Queue payloads do not require another provider or another set of credentials.
celld maps the `WORKFLOW_QUEUE_PAYLOADS` binding into the fleet bucket under
`r2/workflow-world-queue-payloads/`. The `r2_buckets` key in `wrangler.jsonc`
names the Workers-compatible binding API; it does not require Cloudflare R2.

## Fleet-wide retention

The bundled `wrangler.jsonc` declares an hourly UTC cron trigger. It does no
catalog work by default. Set `CELLD_VAR_WORKFLOW_RETENTION_MS` on every node to
enable a maximum workflow age measured from run creation:

```sh
CELLD_VAR_WORKFLOW_RETENTION_MS=7776000000 \
celld --bucket s3://my-cells-bucket
```

`7776000000` is 90 days. The policy includes pending and running workflows as
well as terminal ones. Each cron occurrence admits at most
`WORKFLOW_RETENTION_BATCH_SIZE` runs (default `128`); the existing per-run alarm
state machine finishes bounded index, stream, object-store queue-payload, and run-payload
cleanup. Edit `triggers.crons` in the copied config if hourly discovery is not
the desired resolution.

Point the app at any node's public listener:

```ts
import { createCelldWorld } from '@ewhauser/world-celld';

const world = createCelldWorld({
  fleetUrl: 'http://fleet.internal:8080',
  secret: process.env.CELLD_WORLD_SECRET!,
});
```
