# World v8 optional feature evaluation

Evaluated on `@workflow/world@5.0.0-beta.40` and the pinned Workflow runtime
`5.0.0-beta.58`, after the World v8 upgrade in PR #90. The new tests run through
the HTTP worker router and real Durable Object classes with an in-process fleet.
They do not measure live celld or MinIO latency.

| Feature                   | Evidence                                                                                                                                                                                                                     | Decision                                                                                                                                                                                                           |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Hook resume deduplication | 32 concurrent writes with one `(runId, resumeId)` returned the same committed event. A cell restart preserved the claim, `events.list` returned the `resumeId`, and reuse with a different digest was rejected.              | Worth enabling for retry safety after an SDK-level `resumeHook()` and live celld fault test. The pinned runtime still commits the event before publishing the wake, so this is not a latency optimization.         |
| `queueBatch`              | 64 concurrent queue sends produced 64 public HTTP requests in the current adapter. The pinned runtime calls `queueBatch` for fan-out when available. celld offers `sendBatch` with limits of 100 messages and 256,000 bytes. | Prototype next for wide fan-out workloads. Preserve input-order results, per-message idempotency, payload offload, and partial-failure reporting. Compare time to first step on a real deployment before enabling. |
| Analytics namespace       | The current `runs.list({resolveData: 'none'})` uses one public list request but still reads each candidate's authoritative RunDO before stripping payload fields.                                                            | Defer until there is a listing or trace workload that needs metadata-only reads. A safe implementation requires derived indexes and explicit freshness semantics. It will not speed up workflow execution.         |
| Forced hook claim         | A competing `hook_created` with `force: true` still yields `hook_conflict`; the original token owner remains and receives no `hook_disposed`.                                                                                | Keep `hookForceClaim` off. Implementing it requires cross-run takeover ordering and refusal/redirect of in-flight resumes. There is no demonstrated use case here.                                                 |

Run the focused evaluation with
`pnpm exec vitest run test/world-v8-feature-evaluation.test.ts`. The full default
suite passed (604 tests). These results establish backend behavior and current
transport costs; they are not evidence of a production performance gain.
