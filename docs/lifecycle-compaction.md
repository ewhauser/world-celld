# Expired-run lifecycle authority and derivative compaction

## Invariants

`WorkflowRunDO` owns the only permanent expiry record for a run:
`retention:tombstone`. The record also contains the final cleanup accounting,
so successful cleanup deletes `retention:cleanup` instead of retaining two
authoritative-looking records.

All other expiry state is derivative:

- catalog `expired:<runId>` markers reject delayed publications until their
  authoritative publication leases have expired, then a bounded alarm deletes
  them;
- queue `expired-run:<runId>` markers serialize cleanup against enqueue,
  delivery retry, and redrive. The queue retains the final cumulative deletion
  receipt until RunDO durably records and acknowledges it, then deletes the
  marker after the explicit queue delivery/RPC horizon;
- hook token and hook-ID shards have no run fence at all. They consult the
  owning RunDO for reads/finalization and use exact, expiring claims to prevent
  a released hook from being recreated.

Every run-associated queue mutation consults `WorkflowRunDO.getLifecycleStatus`
when it begins, then rechecks the exact queue marker in the mutation
transaction. Claimed messages repeat the authoritative check immediately
before external delivery. Consequently an expired run remains closed before
queue cleanup reaches its shard and after the derivative marker is deleted;
there is no cached non-expired proof which can outlive a retention boundary.

## Producer and consumer map

| State                                          | Producers                                                                                          | Consumers and replay behavior                                                                                                        | Terminal state                                                                          |
| ---------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------- |
| `retention:cleanup`                            | terminal `WorkflowRunDO.applyEvent`, maximum-age cron enforcement, `scheduleCleanup`, `cleanupNow` | `retentionState`, the RunDO alarm phase machine, status/rearm RPCs, generation checks after every cross-cell await, retry accounting | folded into and deleted beside the final tombstone                                      |
| `retention:tombstone`                          | payload cleanup, before each bounded delete page and again on completion                           | every RunDO read/write guard, `getLifecycleStatus`, queue fallback, hook read/finalize fallback                                      | one permanent record per expired run                                                    |
| terminal cleanup marker                        | terminal `applyEvent`                                                                              | hook entity pages, durable hook-marker pages, wait pages, failure backoff                                                            | deleted when all terminal pages finish                                                  |
| hook entity/marker                             | hook event transaction                                                                             | terminal and retention cleanup replays; exact token and ID release batches                                                           | deleted in pages of at most 64 hooks                                                    |
| exact hook claim plus deadline                 | token/ID `reserve`                                                                                 | `finalize`/`publish`, exact cancellation release, hook cleanup, shard alarm                                                          | finalized/released immediately or alarm-deleted after its protocol lease                |
| RunDO exact cancellation                       | ambiguous `applyEvent` resolution when the hook is absent and the run has not expired              | replayed `hook_created` guard                                                                                                        | payload cleanup deletes it; resolvers never recreate it after the logical tombstone     |
| catalog expiry marker plus GC deadline         | RunDO index cleanup calling `RunCatalogDO.expireRun`                                               | catalog `upsertRun`; catalog alarm                                                                                                   | deleted after the publication horizon                                                   |
| queue run reference                            | enqueue, claim recovery, retry, DLQ transition, redrive                                            | paged `expireRun`, ack, purge                                                                                                        | deleted with its message lifecycle                                                      |
| queue exact expiry receipt                     | each queue shard's paged `expireRun`                                                               | enqueue, 503 reschedule, retry/DLQ, redrive; RunDO cumulative-count reconciliation and replay                                        | retained without a deadline until RunDO persists the final receipt                      |
| queue receipt acknowledgement plus GC deadline | RunDO after persisting the final shard receipt                                                     | idempotent `acknowledgeExpireRun`; bounded queue compaction alarm                                                                    | exact receipt and deadline are deleted; all later operations use RunDO authority        |
| stream registry/stream expiry metadata         | RunDO stream cleanup                                                                               | stream read/write guards and bounded stream cleanup replay                                                                           | retained as the stream entity's terminal metadata, not a duplicated per-shard run fence |

`RunFenceDO`, `WORKFLOW_RUN_FENCES`, and hook-shard `runfence:<runId>` keys no
longer exist. A hard cutover is intentional; there is no decoder, dual write,
or migration path for their previous shapes.

## Bounded lifetimes and work

The horizons in `src/lifecycle.ts` come from enforced protocol limits:

| Bound                     |        Value | Derivation                                                                                      |
| ------------------------- | -----------: | ----------------------------------------------------------------------------------------------- |
| one fleet RPC attempt     |   300,000 ms | maximum accepted `rpcTimeoutMs`                                                                 |
| idempotent fleet call     |   900,900 ms | three attempts plus the maximum two retry delays                                                |
| run index publication     | 1,200,900 ms | one authoritative apply response plus one idempotent catalog call                               |
| one queue delivery lease  |   330,000 ms | 300,000 ms delivery timeout plus 30,000 ms lost-claim grace                                     |
| catalog exact-fence grace | 1,200,900 ms | maximum run-index publication lifetime                                                          |
| queue receipt grace       |   330,000 ms | maximum of one mutation RPC and one delivery lease, starting only after durable acknowledgement |
| hook exact-claim lease    | 2,101,800 ms | reserve retries, one authoritative apply, finalize retries, and retry delays                    |

An operation arriving after a lease must reacquire authority. This makes a
delayed original request finite while allowing an arbitrarily late logical
replay to start a fresh request and be rejected by the tombstone.

Every compaction alarm lists at most 129 entries, mutates at most 128 items,
and re-arms for `now + 1` when a page remains. Storage failures schedule a new
alarm edge before returning or throwing, rather than relying only on the
platform's finite automatic retry ladder.

Fleet-wide maximum-age discovery is the deliberate cross-shard exception. One
celld cron occurrence performs a bounded creation-time merge across the 16 run
catalog shards, admits at most `WORKFLOW_RETENTION_BATCH_SIZE` runs, then
rechecks each candidate's authoritative `createdAt` in its RunDO. The RunDO
removes its catalog entry before the enforcement RPC returns when possible;
the remaining cleanup stays paged and alarm-driven. Repeated cron occurrences
therefore advance through a backlog without turning one invocation into an
unbounded namespace scan.

## Measured protocol and storage effects

`pnpm test:perf:index -- --disableConsoleIntercept` keeps public protocol RPCs
separate from internal Durable Object/storage work.

- Run create/update remains two public RPCs. Its catalog shard performs one
  expiry-marker read, one two-key batch put, and one transaction.
- Hook create remains three public RPCs. Each ownership domain performs two
  transactions, two batch reads, three scalar writes (record, claim, claim
  deadline), and one batch delete. Finalization makes two internal lifecycle
  reads against the authoritative RunDO instead of a separate RunFenceDO; each
  lifecycle read is one RunDO transaction and one batch storage read.
- Hook lookup remains one public RPC plus one internal authoritative RunDO
  lifecycle read (one RunDO transaction and one batch storage read).
- The retention sample performs one internal catalog expiry RPC and no
  RunFence RPC. Catalog expiry performs one marker read, two scalar writes
  (fence and GC deadline), one two-key delete, and one transaction.
- A steady-state run-associated queue enqueue performs one public RPC, one
  internal RunDO lifecycle RPC, one RunDO transaction/batch read, and one queue
  transaction with two scalar reads plus four scalar writes for
  config/message/due/run reference.
- An expired enqueue after exact-receipt compaction performs one public RPC,
  one internal RunDO lifecycle RPC, one queue transaction/scalar marker read,
  and one RunDO transaction/batch read. It performs no writes.
- A successful run-associated delivery performs one external callback fetch,
  one internal RunDO lifecycle RPC/transaction/batch read, and two queue
  transactions. Queue storage performs one message batch read, ten bounded
  index lists, one batch claim write, one batch due-index delete, and three
  scalar acknowledgement deletes.

These are operation counts, not latency or CPU claims. The suite prints local
elapsed-time smoke values separately.

## Failure and concurrency coverage

Focused fake-time tests cover late enqueue, late hook finalization, delayed
cleanup replay, callback retry, stale DLQ redrive, exact-claim expiry, DO
restart, 128-item compaction pages, partial compaction, injected alarm failure,
concurrent expiry/compaction, lost final cleanup responses, unacknowledged
receipt retention, and convergence of hundreds of exact queue receipts to no
derivative state. Existing retention tests continue to cover cleanup
generation races, paged hooks/streams/queues/payload, lost responses, and alarm
backoff. Maximum-age tests additionally cover pending, running, and terminal
runs, authoritative cutoff rechecks, bounded sweep progress, the earliest of
terminal and fleet-wide deadlines, and cleanup across persisted queue-shard
placement.
