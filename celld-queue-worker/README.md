# world-celld Queue consumer

This companion celld v0.5.0 script consumes the native `workflow-world` Queue
and forwards each delivery to the primary `workflow-world` script through a
`QueueDeliveryRpc` named service binding. The consumer calls
`deliver(secret, envelope, attempt)` and handles `complete`, `suspend`, or `retry`
results. RPC exceptions retry without acknowledgment. The separate script keeps the broker consumer attachment independent of the
public HTTP worker.

Deploy it before the primary worker so the Queue consumer attachment exists:

```sh
cp -r node_modules/@ewhauser/world-celld/celld-queue-worker ./workflow-world-queue
celld deploy ./workflow-world-queue --bucket s3://my-cells-bucket
```

Before deploying, set `vars.WORLD_SECRET` in the copied Wrangler config to the
same bearer secret used by the primary worker. Keep that private deployment
config out of source control. v0.5.0 rejects `CELLD_VAR_*` node overrides.
Deploy the primary worker after this script so it remains the fleet's
public application. Native Queue operations are available through `celld queue
info`, `peek`, `pause`, `resume`, `purge`, and `redrive`.

The primary worker must export `QueueDeliveryRpc`, and this script's service
binding must set `entrypoint` to that name. Deploy matching worker versions
during a maintenance window; there is no fallback to `/v1/queue/deliver`.
