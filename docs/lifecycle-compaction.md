# Expired-run lifecycle authority and derivative compaction

## Invariants

`WorkflowRunDO` owns the only permanent expiry record for a run:
`retention:tombstone`. The record contains the final cleanup accounting, so
successful cleanup deletes `retention:cleanup` instead of retaining two
authoritative-looking records.

All other expiry state is derivative:

- catalog `expired:<runId>` markers reject delayed publications until their
  authoritative publication leases expire, then a bounded alarm deletes them;
- run-local `queue-payload:<messageId>` registrations identify object-store payloads that
  retention must remove, while dedicated `queue-orphan:<messageId>` cells own
  their independent failure-cleanup alarms;
- queue idempotency claims live in dedicated `WorkflowRunDO` instances and
  expire through their cell alarms;
- hook token and hook-ID shards have no run fence. They consult the owning
  RunDO and use exact, expiring claims to prevent a released hook from being
  recreated.

celld's native Queue is intentionally not another lifecycle authority. A
retention pass cannot selectively delete one run's broker messages, and celld
retains them for a fixed four days. Instead, a broker message is a small pointer
to an object-store body. Retention removes that body and its registration. A late pointer
then receives permanent `410 QueuePayloadExpired` and is acknowledged without
calling the application or recreating the run.

`RunFenceDO`, `QueueDO`, `WORKFLOW_RUN_FENCES`, queue shards, queue receipt
fences, and hook-shard `runfence:<runId>` keys no longer exist. This remains an
intentional hard cutover; there is no decoder, dual write, or migration path for
their previous shapes.

## Native Queue and object-storage protocol

The application publishes a versioned queue envelope to the primary worker.
For a run-bearing message, the worker performs these operations in order:

1. register `{messageId, object key, orphan expiry}` in the authoritative run cell;
2. write the tagged-JSON body through the provider-neutral payload store;
3. publish a pointer envelope through celld's native Queue producer binding.

The registration rejects an already-expired run. A dedicated orphan cell removes
the object and unregisters it only after `notBefore + 5 days`, whether the run
already exists or is still in resilient start. This is one day beyond celld's fixed four-day Queue
retention, so cleanup cannot remove a body while a valid broker message can
still exist. A failure between registration, object-store write, and broker publication
therefore leaks only until the bounded orphan alarm; it cannot bypass run
expiry. Every valid workflow suspension extends the orphan deadline before its
replacement pointer is published.

celld limits one Queue message to 128,000 bytes and producer delay to 86,400
seconds. Object-store offload preserves the World's larger body contract, while the
consumer chains waits longer than one day by publishing the same stable
envelope with the remaining absolute `notBefore` deadline.

celld requires the consumer to be a script without `fetch()`. The companion
`workflow-world-queue-consumer` script therefore consumes `workflow-world` and
calls the primary `workflow-world` script through a service binding. The
primary script resolves the object-store body and invokes the application's
`/.well-known/workflow/v1/flow` endpoint.

For an `idempotencyKey`, a dedicated claim cell is named from the queue name
and key. A delivery claims its exact `messageId` for 15 minutes. A duplicate
publication with another message ID is acknowledged while the claim is live.
A valid workflow suspension (`503` plus `timeoutSeconds`) extends the claim
through the requested deadline and republishes the stable message identity.
Success and permanent application statuses (`404`, `409`, `410`, `422`) delete
the stored object, unregister it from the run, and release the claim. Ordinary
transient failures keep the payload but release the claim so the broker retry
can reacquire it. A valid `503` records `retryAt`; an early retry receives the
same suspension response instead of calling the application or losing the
future delivery.

## Retention state machine

| Phase        | Bounded work                                      | Durable result                           |
| ------------ | ------------------------------------------------- | ---------------------------------------- |
| `retained`   | wait until the pinned deadline                    | advances to `index`                      |
| `index`      | remove hook and run catalog derivatives           | advances to `streams`                    |
| `streams`    | fence registries and delete bounded chunk pages   | advances to `queues`                     |
| `queues`     | delete at most 128 stored queue bodies            | advances or repeats                      |
| `payload`    | tombstone first, then delete at most 128 run keys | repeats until only the tombstone remains |
| `tombstoned` | no further mutation                               | one permanent metadata-only record       |

Every cross-cell or object-store await is followed by a generation-checked transaction.
Concurrent pages may repeat an idempotent external delete, but only one can
advance the generation or add to cleanup accounting. Failures persist their
error and schedule capped-backoff retry. The final tombstone contains no
workflow input, output, event, step, hook, stream, or queue payload.

Fleet-wide maximum-age discovery is the deliberate cross-shard exception. One
celld cron occurrence performs a bounded creation-time merge across the 16 run
catalog shards, admits at most `WORKFLOW_RETENTION_BATCH_SIZE` runs, and then
rechecks each candidate's authoritative `createdAt` in its RunDO. Repeated cron
occurrences advance through a backlog without turning one invocation into an
unbounded namespace scan.

## Bounded lifetimes

| Bound                    |                Value | Purpose                                            |
| ------------------------ | -------------------: | -------------------------------------------------- |
| one fleet RPC attempt    |           300,000 ms | maximum accepted `rpcTimeoutMs`                    |
| idempotent fleet call    |           900,900 ms | three attempts plus two maximum retry delays       |
| run index publication    |         1,200,900 ms | apply response plus idempotent catalog publication |
| queue callback           |           300,000 ms | application delivery timeout                       |
| queue claim stale window |           900,000 ms | crash recovery and duplicate suppression           |
| native producer delay    |             86,400 s | celld v0.4.0 per-publication maximum               |
| native broker retention  |               4 days | fixed celld v0.4.0 message lifetime                |
| orphan object grace      | `notBefore + 5 days` | outlives every valid native Queue pointer          |

Queue deadlines remain fixed-width epoch-millisecond values. Validation leaves
enough headroom for the claim window before a suspension is accepted. A logical
wait can span many native publications, but each publication and claim deadline
remains representable and bounded.

## Evidence and coverage

`pnpm test:perf:index -- --disableConsoleIntercept` keeps public protocol RPCs
separate from internal Durable Object and storage work. Those measurements
cover run catalog and hook-index behavior; the former QueueDO storage-count
baseline was removed because it is not comparable to celld's native broker and
object-storage implementation.

Focused tests cover producer envelope validation, object-store offload and cleanup,
consumer success/permanent/transient decisions, suspension re-publication,
long-delay chaining, exact idempotency claims, orphan cleanup, 128-object
retention pages, generation races, maximum-age cleanup, and tombstone
non-resurrection. The required real celld v0.4.0 smoke additionally proves the
two-script Queue/service graph, delayed delivery across process loss, object-store-backed
retention cleanup, durable run/stream recovery, and resumption of multi-page
cleanup after `SIGKILL`.

These are protocol and operation-count claims. Local elapsed time and
throughput remain machine-specific performance evidence.
