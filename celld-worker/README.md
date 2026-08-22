# world-celld worker

The celld-deployable half of `@ewhauser/world-celld`: six cell classes
(WorkflowRunDO, RunCatalogDO, HookTokenDO, HookIdDO, StreamDO, and QueueDO)
behind an authenticated HTTP router.
Storage, control, and queue methods use fixed JSON RPC routes; stream chunks use
bounded binary batch writes and binary long-poll reads.

## Deploy

Copy this directory out of `node_modules` so `celld deploy` can bundle it
(its `main` must live inside the project directory), then deploy against your
fleet bucket:

```sh
cp -r node_modules/@ewhauser/world-celld/celld-worker ./workflow-world
celld deploy ./workflow-world --bucket s3://my-cells-bucket
```

Requirements:

- celld v0.3.0 (the currently tested runtime baseline).
- `esbuild` on PATH (celld shells out to it).
- A bucket with conditional-write support (celld's fencing requirement).
- `WORLD_SECRET` injected at the node level (`CELLD_VAR_WORLD_SECRET=...`) —
  the router fails closed with 503 while it is empty.

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
state machine finishes bounded index, stream, queue, and payload cleanup.

New runs persist the application's `queueShards` placement. Set
`WORKFLOW_RETENTION_QUEUE_SHARDS` to the historical queue-shard count when
cleaning runs created before this worker version. Edit `triggers.crons` in the
copied config if hourly discovery is not the desired resolution.

Point the app at any node's public listener:

```ts
import { createCelldWorld } from '@ewhauser/world-celld';

const world = createCelldWorld({
  fleetUrl: 'http://fleet.internal:8080',
  secret: process.env.CELLD_WORLD_SECRET!,
});
```
