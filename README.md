# @ewhauser/world-celld

A [Vercel Workflow DevKit](https://useworkflow.dev) `World` backend
implemented on [celld](https://github.com/denoland/celld) — self-hosted,
distributed Durable Objects. Run durable workflows on your own machines with
state in an S3-compatible bucket you own.

Closely mirrors
[`@fantasticfour/world-cloudflare`](https://github.com/vinnymac/worlds/tree/main/packages/world-cloudflare)
(portions vendored, Apache-2.0 — see [NOTICE](./NOTICE)), with celld cells
standing in for Cloudflare's platform services:

| world-cloudflare | world-celld |
| --- | --- |
| `WorkflowRunDO` Durable Object | `WorkflowRunDO` cell (vendored, event-sourced `applyEvent`) |
| `StreamDO` Durable Object | `StreamDO` cell (vendored) |
| Workers KV global index | `IndexDO` cell — single serialized cell, **read-after-write consistent** (KV was eventually consistent) |
| Cloudflare Queues | `QueueDO` cells — durable alarms, retries, dead-letter table |
| Worker `env` bindings | authenticated HTTP RPC to the fleet's public listener |

## Architecture

```
Node app (workflow runtime + createCelldWorld({ fleetUrl, secret }))
  │  HTTPS + Bearer, tagged-JSON RPC:  POST /v1/rpc/{binding}/{name}/{method}
  ▼
celld public listener → worker router (bearer auth, method whitelist)
  ├─ WORKFLOW_DB      → WorkflowRunDO   (one cell per run; guards + event append + entity
  │                                      mutation in one storage transaction)
  ├─ WORKFLOW_STREAMS → StreamDO        (chunk store + per-run stream registry)
  ├─ WORKFLOW_INDEX   → IndexDO         (run listing + hook token/id lookup)
  └─ WORKFLOW_QUEUE   → QueueDO         (q:<shard> cells: due index, alarm-driven delivery
                          │              loop, idempotency dedup, backoff, DLQ)
                          │ outbound fetch (x-vqs-* dialect)
                          ▼
              app's POST {baseUrl}/.well-known/workflow/v1/{flow|step}
              (the workflow runtime's createQueueHandler routes)
```

## Quick start (no fleet needed)

```bash
pnpm install && pnpm build
cd examples/demo-app && pnpm build && pnpm demo
```

The demo runs an order workflow — steps, `sleep`, an approval hook, and a
live output stream — against the in-process fleet emulation
(`@ewhauser/world-celld/testing`): the real router and real cell classes over
in-memory state, crossing the real wire protocol.

## Deploying to a celld fleet

1. **Fleet prerequisites** ([celld docs](https://github.com/denoland/celld)):
   the `celld` binary, `esbuild` on PATH, and a bucket with
   **conditional-write support** — S3, R2, GCS, Azure Blob, or Tigris.
   MinIO (community), B2, Hetzner, and DO Spaces do not qualify.

2. **Deploy the world worker.** The deployable project ships in the package:

   ```bash
   cp -r node_modules/@ewhauser/world-celld/celld-worker ./workflow-world
   celld deploy ./workflow-world --bucket s3://my-cells-bucket
   ```

3. **Start nodes** with the secret injected as a var override:

   ```bash
   CELLD_VAR_WORLD_SECRET=$(openssl rand -hex 32) \
   celld --bucket s3://my-cells-bucket --listen 0.0.0.0:8080 \
         --internal-listen 10.0.0.12:8081 --advertise 10.0.0.12:8081
   ```

4. **Point the app at the fleet:**

   ```ts
   import { createCelldWorld } from '@ewhauser/world-celld';

   const world = createCelldWorld({
     fleetUrl: 'http://fleet.internal:8080',   // or CELLD_FLEET_URL
     secret: process.env.CELLD_WORLD_SECRET!,  // or CELLD_WORLD_SECRET
     baseUrl: 'https://app.internal',          // queue cells deliver here
   });
   ```

   With the Workflow DevKit, set `WORKFLOW_TARGET_WORLD=@ewhauser/world-celld`
   and the `CELLD_*` env vars; `createWorld()` picks them up.

`baseUrl` (or `WORKFLOW_BASE_URL`) must be reachable **from the fleet**:
queue cells push `x-vqs-*` messages to
`{baseUrl}/.well-known/workflow/v1/{flow|step}`.

### Config reference

| Option / env var | Default | Meaning |
| --- | --- | --- |
| `fleetUrl` / `CELLD_FLEET_URL` | — | any fleet node's public listener |
| `secret` / `CELLD_WORLD_SECRET` | — | bearer secret (worker `WORLD_SECRET`) |
| `baseUrl` / `WORKFLOW_BASE_URL` | `http://localhost:$PORT` | app callback base URL |
| `deploymentId` / `CELLD_DEPLOYMENT_ID` | `celld-default` | reported deployment id |
| `queueShards` | `1` | number of `q:<shard>` cells (pinned at first use; changing it requires a drained fleet) |
| `readPollMs` | `250` | stream read poll interval |
| `CELLD_QUEUE_MODE=cells` | — | force the live-queue path under a test runner |

Worker vars: `WORLD_SECRET` (required), `WORKFLOW_CALLBACK_SECRET` (optional,
sent as `x-workflow-callback-secret` on deliveries), `QUEUE_MAX_ATTEMPTS` (5),
`QUEUE_MAX_INFLIGHT` (5), `QUEUE_DELIVERY_TIMEOUT_MS` (300000).

## Operations

- **Delivery semantics**: at-least-once everywhere (celld's model). Handlers
  are idempotent by construction — the runtime dedups on `idempotencyKey`
  inside the queue cell, and permanent handler errors (404/409/410/422) drop
  without burning retries.
- **Dead letters**: after `QUEUE_MAX_ATTEMPTS` transient failures a message
  moves to the cell's DLQ. Inspect and recover over RPC (bearer-authenticated):
  `stats`, `listDeadLetters`, `redriveDeadLetter`, `purgeDeadLetters` on
  `POST /v1/rpc/queue/q:0/<method>`.
- **Alarm abandonment**: celld abandons a cell alarm after six counted
  handler failures. QueueDO's alarm never throws for data conditions (backoff
  reschedules instead), and `POST /v1/rpc/queue/q:0/rearmAlarm` re-derives and
  arms the timer if it ever happens. Alert on `stats.alarmAt === null` while
  `pending > 0`.
- **Deploys restart the fleet** (celld has no staged rollout). In-flight
  deliveries die with the isolate; the inflight-deadline sweep on the next
  alarm redelivers them. App redeploys at a new URL are safe: the delivery
  target travels per message, not pinned per cell.
- **celld#144**: alarm handlers can overlap; QueueDO wraps its claim phase in
  `blockConcurrencyWhile` and keeps delivery I/O outside the gate.
- **Fleet tuning** (measured in eve-ambient's celld evaluation, re-measure in
  your infra): `CELLD_TTL_MS=5000`, `CELLD_WAKER_TICK_MS=5000`, stable node
  identities, and `CELLD_LTX_COMPACTION` enabled.
- **Security**: all state-touching routes require the bearer secret and fail
  closed (503) when unset; celld's *internal* listener exposes unauthenticated
  `/shutdown`//`/evict` — firewall it separately.

## Testing

```bash
pnpm test              # unit + wire-protocol + conformance (no celld needed)
pnpm test:integration  # against a real fleet (CELLD_FLEET_URL + CELLD_WORLD_SECRET)
```

- `test/spec.test.ts` / `test/spec-queue.test.ts` run the full
  `@workflow/world-testing` conformance suite over the real HTTP RPC protocol
  (real router + real cell classes on in-memory state), with the queue on the
  in-process pump and on live QueueDO cells respectively.
- `test/integration/fleet.test.ts` doubles as the celld spike assertions:
  DO-RPC Date/Uint8Array fidelity, storage list-option pagination, 1 MiB
  stream chunks, live alarm delivery/delay/503-redeliver, DLQ + redrive.
- `@ewhauser/world-celld/testing` exports the harness (`startHarness`,
  `startDevFleet`, `FakeFleet`) for your own tests.

## Package layout

- `.` — `createCelldWorld()` for the Node app (storage/streamer/queue over
  HTTP RPC; in-process test pump under test runners).
- `./worker` — the celld worker: 4 cell classes + the router (kept free of
  Node built-ins; CI verifies with an esbuild pass under workerd conditions).
- `./testing` — in-process fleet emulation.
- `celld-worker/` — the copy-and-deploy project directory.

## License

Apache-2.0. Portions vendored from
[vinnymac/worlds](https://github.com/vinnymac/worlds) (Apache-2.0) — see
[NOTICE](./NOTICE).
