# Index sharding evidence and routing contract

## Verified starting point

The hard cutover started from fetched `origin/main` at
`0de370be6e856aa844115095eb37fe97b929ae26` on 2026-08-18.
The following lower-risk reductions were present in that first-parent history
and in the checked-out source/tests before the sharding work began:

| Commit    | Landed reduction                                                  |
| --------- | ----------------------------------------------------------------- |
| `e895683` | `runs.list` value-bearing pages and bounded RunDO reads (#21)     |
| `4b48e9b` | unused correlation index removal (#23)                            |
| `56403e1` | bounded QueueDO deadline work and callback-body handling (#24)    |
| `86e53b6` | batched binary stream I/O and long polling (#26)                  |
| `0de370b` | bounded retention cleanup and batched DO storage operations (#27) |

`HEAD`, `origin/main`, and `FETCH_HEAD` all resolved to `0de370b` before the
measurement-only test seam was added.

## Repeatable measurement

Replay the pre-cutover workload against an exported tree of the exact recorded
base commit, without changing the current worktree, with:

```sh
pnpm test:perf:index:baseline
```

The command resolves `0de370b` to its full commit ID, exports that tree to a
temporary directory, injects only the base-compatible measurement fixture, and
runs it with the dependency installation from this checkout. It removes the
temporary tree afterward. A different auditable commit can be selected with
`node scripts/measure-index-main-baseline.mjs --ref=<commit>`.

Run the sharded workload with:

```sh
pnpm test:perf:index --disableConsoleIntercept
```

The workload uses the real HTTP router, remote client, Durable Object classes,
and storage logic on the deterministic in-process fake fleet. It is not a
production latency benchmark. Latency and throughput below are local reference
results; the public RPC and internal storage-operation counts are deterministic
assertions. Contention results inject 2 ms of work into every catalog storage
transaction so serialization is repeatable.

Both commands print one-line JSON records for fanout/storage, controlled
contention, and retention. Deterministic RPC and storage-operation shapes are
also asserted, so protocol drift fails the measurement rather than silently
changing the baseline.

### Public protocol RPCs

| Operation                           | `0de370b` singleton | Sharded worktree | Explanation                                                                                           |
| ----------------------------------- | ------------------: | ---------------: | ----------------------------------------------------------------------------------------------------- |
| run create                          |                   3 |                2 | two singleton `putOwned` calls became one catalog commit                                              |
| run attribute update                |                   3 |                2 | same batched catalog commit                                                                           |
| list one run                        |                   2 |                2 | one stateless merged-catalog request plus one RunDO read                                              |
| list N qualifying runs              |             `1 + N` |          `1 + N` | public fanout is stable and authoritative RunDO reads remain bounded at 8                             |
| hook create                         |                   3 |                3 | one reserve, one RunDO commit, one cohesive finalize request across token and ID domains              |
| hook get by token                   |                   1 |                1 | the token shard performs one internal per-run fence read                                              |
| hook get by ID                      |                   1 |                1 | the ID shard performs one internal per-run fence read                                                 |
| hook resume                         |                   1 |                1 | RunDO-only work; no index caller                                                                      |
| hook delete                         |                   2 |                2 | one RunDO commit and one cohesive parallel release request                                            |
| terminal transition with five hooks |                   4 |                2 | one RunDO commit and one cohesive fence/catalog commit; hooks are paged by the terminal cleanup alarm |

One replay of those commands measured run create at 29.67 ms versus 27.34 ms,
run update at 7.61 ms versus 6.31 ms, list-one at 4.30 ms versus 5.23 ms, hook
create at 7.17 ms versus 7.92 ms, hook delete at 4.14 ms versus 5.98 ms, and
terminal transition at 7.96 ms versus 5.77 ms. These timings include local HTTP
and are smoke references only. They are reported separately from the
deterministic RPC and storage counts and are not used to claim a production
latency change.

### Internal DO and storage work

- Run create/update changed from two transactions, two fence reads, and two
  scalar writes in the singleton to one catalog-shard transaction, one local
  expiry-fence read, and one two-key batch write. A stateless commit endpoint
  also keeps terminal fencing plus catalog publication to one public call.
- A run list performs 16 internal catalog storage lists in parallel behind one
  authenticated stateless worker request, then the same authoritative RunDO
  reads as before. For `limit=20`, public fanout remains 21; sharding does not
  reduce the internal read work.
- Hook creation uses two token-shard transactions, two hook-ID-shard
  transactions, and two internal per-run fence reads. This is more raw work
  than the old two singleton transactions: admission now reserves both token
  and hook ID before the RunDO commit. It removes unrelated-key contention
  while keeping each uniqueness invariant in one natural-key transaction.
  Finalization remains one public call whose two exact-claim writes are
  independently idempotent.
- Hook lookup keeps one public RPC but adds one internal RunFenceDO read. That
  read replaces the old singleton's co-located global terminal/expiry keys.
- Hook deletion uses one transaction in each ownership domain behind one
  public request. Releases are grouped into bounded batches of 64 per shard.
- The baseline retention sample made one public `cleanupNow` plus eight status
  polls. The sharded sample made one `cleanupNow` plus nine polls because the
  terminal state machine now has a bounded marker-replay phase. The old
  index phase made one internal `IndexDO.expireRun` call; the new phase makes
  one per-run fence call and one catalog-shard expiry call. The local
  end-to-end sample was 16.70 ms before and 19.94 ms after. This is a cleanup
  safety/storage-shape tradeoff, not a portable latency claim.

### Controlled contention

The 48-run, concurrency-16 workload with 2 ms injected transaction work gave:

| Model                                            |   elapsed | throughput |      p50 |      p95 |
| ------------------------------------------------ | --------: | ---------: | -------: | -------: |
| old global singleton, two index transactions/run | 220.22 ms |   217.96/s | 68.14 ms | 77.41 ms |
| new batched commit forced onto one catalog shard | 114.97 ms |   417.50/s | 34.38 ms | 39.38 ms |
| new batched commit distributed across 16 shards  |  35.08 ms | 1,368.14/s | 10.00 ms | 17.47 ms |

The middle row isolates most of the cohesive-RPC/storage batching benefit. The
gap between the middle and final rows is the sharding benefit under the same
RPC and storage shape. A RunDO-only `step_created` control remained index-free;
its timing is reported by the test but is not used to attribute sharding gains.

## Ownership and routing contract

Routing uses versioned FNV-1a over UTF-8 bytes. Hash collisions share a shard
but never a storage key, so they affect contention only, not correctness.

| Domain               | Key      |      Cardinality | Why                                                                                                           |
| -------------------- | -------- | ---------------: | ------------------------------------------------------------------------------------------------------------- |
| run catalog          | `runId`  |  16 fixed shards | run writes have a natural key; fixed cardinality makes ordered global/workflow listing a bounded 16-way merge |
| run lifecycle fence  | `runId`  | one cell per run | terminal/expiry state is a per-run coordination atom and avoids any global fence reader                       |
| hook token ownership | token    |  32 fixed shards | every contender for one token reaches one transaction; releases batch by shard                                |
| hook-ID ownership    | `hookId` |  32 fixed shards | every contender for one ID reaches one transaction; direct lookup and releases use that shard                 |

The shard counts and `v1` cell names are persisted routing protocol, not
runtime tuning knobs. Changing them requires another explicit hard cutover.

## Correctness boundary

There is no transaction spanning Durable Objects. The authoritative event and
entity mutation remains the WorkflowRunDO transaction. Token uniqueness is a
HookTokenDO transaction and hook-ID uniqueness is a HookIdDO transaction.
Admission reserves both natural keys in a fixed order; the claims are exact and
independently releasable. Catalog and lookup entries remain derived,
idempotent publications:

- a failed run catalog publication is repaired by idempotent `run_created`
  replay;
- a token publication followed by a failed hook-ID publication is repaired by
  hook replay using the surviving exact hook-ID claim;
- a thrown `applyEvent` RPC is resolved inside the authoritative RunDO. That
  serialized transaction either observes the committed hook or writes an
  exact cancellation fence before both external claims are released, so a
  pre-dispatch failure does not strand ownership and a delayed request cannot
  commit afterward;
- hook disposal stores a durable release marker in the RunDO transaction, so
  partial token/ID release can be replayed without another disposal event;
- normal hook disposal deletes the exact ownership records and claims; a
  delayed finalize has neither its exact live claim nor an existing owned
  record, so it cannot recreate the hook and no per-hook fence accumulates;
  terminal cleanup replays all durable hook markers and writes at most one run
  fence per touched token/ID shard;
- terminal and expiry fences hide hook records immediately even while bounded
  physical cleanup pages remain.

Focused tests cover deterministic distribution, UTF-8 byte-ordered merged
forward/reverse pagination, same-token contention, hook-ID collisions, token
reuse, exact-claim cancellation, delayed-finalization fences, catalog
resurrection prevention, partial-publication/deletion failures, lost-response
ownership, concurrent ownership, concurrent resume idempotency, terminal
marker fencing, cleanup paging, and retention retries.
