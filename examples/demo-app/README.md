# world-celld demo

A Hono + Nitro app (the [official Workflow DevKit setup](https://useworkflow.dev))
running an order workflow — steps, `sleep`, an approval **hook**, and a live
output **stream** — on the celld world.

## One-command demo (no celld needed)

Runs against the in-process fleet emulation from
`@ewhauser/world-celld/testing` (real router, real cell classes, real wire
protocol; in-memory state):

```bash
pnpm build && pnpm demo
```

You'll see the stream tail live, the hook get approved, and the run complete:

```
[stream] order order-1042: validating
[stream] order order-1042: total USD 42.5
[stream] order order-1042: awaiting approval — POST /approvals/<token>
[demo] approving via hook token <token>
[stream] order order-1042: approved, shipping in 2s
[stream] order order-1042: shipped as ship_order-1042_xxxxxx
[demo] final status: {"runId":"wrun_...","status":"completed"}
```

## Against a real celld fleet

Deploy the world worker (see the repo README), then:

```bash
pnpm build
CELLD_FLEET_URL=http://your-fleet:8080 CELLD_WORLD_SECRET=... pnpm demo
```

Or run the app as a normal server and drive it with curl:

```bash
WORKFLOW_TARGET_WORLD=@ewhauser/world-celld \
CELLD_FLEET_URL=http://your-fleet:8080 \
CELLD_WORLD_SECRET=... \
WORKFLOW_BASE_URL=http://127.0.0.1:3000 \
PORT=3000 pnpm start
```

```bash
curl -X POST http://127.0.0.1:3000/orders/order-1
```

`WORKFLOW_BASE_URL` must be reachable **from the fleet** — queue cells push
step/flow messages back to the app at that address.

For a long-lived local emulated fleet instead: `pnpm fleet:local`.
