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

- `esbuild` on PATH (celld shells out to it).
- A bucket with conditional-write support (celld's fencing requirement).
- `WORLD_SECRET` injected at the node level (`CELLD_VAR_WORLD_SECRET=...`) —
  the router fails closed with 503 while it is empty.

Point the app at any node's public listener:

```ts
import { createCelldWorld } from '@ewhauser/world-celld';

const world = createCelldWorld({
  fleetUrl: 'http://fleet.internal:8080',
  secret: process.env.CELLD_WORLD_SECRET!,
});
```
