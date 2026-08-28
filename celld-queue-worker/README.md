# world-celld Queue consumer

This companion celld v0.4.0 script consumes the native `workflow-world` Queue
and forwards each delivery to the primary `workflow-world` script through a
service binding. It is separate because celld does not allow one script to
export both `fetch()` and a Queue consumer.

Deploy it before the primary worker so the Queue consumer attachment exists:

```sh
cp -r node_modules/@ewhauser/world-celld/celld-queue-worker ./workflow-world-queue
celld deploy ./workflow-world-queue --bucket s3://my-cells-bucket
```

Set `CELLD_VAR_WORLD_SECRET` to the same bearer secret used by the primary
worker. Deploy the primary worker after this script so it remains the fleet's
public application. Native Queue operations are available through `celld queue
info`, `peek`, `pause`, `resume`, `purge`, and `redrive`.
