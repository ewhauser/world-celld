# world-celld

A [Workflow DevKit](https://useworkflow.dev) `World` backed by
[celld](https://github.com/denoland/celld).

`world-celld` stores workflow runs, hooks, and streams in celld cells. Scheduled
work is delivered with durable cell alarms. This gives Node applications a
self-hosted alternative to platform-specific Workflow backends.

> [!WARNING]
> `world-celld` is experimental. The package has not been published to npm or
> proven in production, and its API and storage layout may change before 1.0.

## What it provides

- Workflow run, step, event, and hook persistence
- Durable streams
- Delayed work, retries, deduplication, and dead-letter storage
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

Until the first npm release, clone and build this repository, then link it from
your application:

```sh
# In this repository
pnpm install
pnpm build

# In your Workflow application
pnpm add ../world-celld
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

| Option         | Environment variable  | Default                  |
| -------------- | --------------------- | ------------------------ |
| `fleetUrl`     | `CELLD_FLEET_URL`     | required                 |
| `secret`       | `CELLD_WORLD_SECRET`  | required with `fleetUrl` |
| `baseUrl`      | `WORKFLOW_BASE_URL`   | `http://localhost:$PORT` |
| `deploymentId` | `CELLD_DEPLOYMENT_ID` | `celld-default`          |
| `queueShards`  | —                     | `1`                      |
| `readPollMs`   | —                     | `250`                    |
| `rpcTimeoutMs` | —                     | `30000`                  |

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

## Operational notes

- Delivery is at least once. Workflow steps and other external side effects
  must be idempotent.
- Queue cells expose `stats`, `listDeadLetters`, `redriveDeadLetter`,
  `purgeDeadLetters`, and `rearmAlarm` through the authenticated RPC endpoint.
- A new application URL applies to newly enqueued messages. Existing messages
  retain the callback URL with which they were created.
- Fleet restarts can interrupt in-flight callbacks; expired claims are
  delivered again.

## Development

```sh
pnpm format
pnpm check
```

`pnpm check` runs formatting, linting, type checking, tests, the build, and the
worker bundle check. Oxlint runs its correctness, suspicious, and performance
categories with type-aware checks and warnings denied. The default test suite includes the upstream
`@workflow/world-testing` conformance suite and does not require celld. To run
the live fleet tests:

```sh
CELLD_FLEET_URL=http://fleet.internal:8080 \
CELLD_WORLD_SECRET=replace-with-a-secret \
pnpm test:integration
```

Bug reports and focused pull requests are welcome. Please include a regression
test for behavior changes and run the checks above before submitting a PR.

## License

Apache-2.0. Parts of the implementation are adapted from
[`vinnymac/worlds`](https://github.com/vinnymac/worlds), also under Apache-2.0.
See [`NOTICE`](./NOTICE) for details.
