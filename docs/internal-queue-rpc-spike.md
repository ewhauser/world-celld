# Internal Queue RPC spike

## Result

The complete internal delivery path works through a named `QueueDeliveryRpc`
entrypoint on celld v0.5.0. The consumer calls `deliver(secret, envelope, attempt)`;
the entrypoint invokes transport-independent delivery logic directly. There is
no HTTP wrapper or fallback. The former `/v1/queue/deliver` route returns 404.

## Contract and behavior

| Outcome                         | Consumer action                                                                                                                   |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `complete`                      | Acknowledge after successful/permanent delivery and durable cleanup, or for an already-completed claim or expired payload.        |
| `suspend` with `timeoutSeconds` | Publish the same message with a new deadline, preserving the delivery-failure count. Acknowledge only after publication succeeds. |
| `retry`                         | Retry with the existing bounded exponential delay.                                                                                |
| RPC rejection                   | Retry without acknowledgment.                                                                                                     |

The server validates the shared secret, envelope, and positive integer attempt
before accessing storage or making callbacks. The application callback still
receives its separate callback secret. Claim and orphan protection are extended
before returning a suspension. Payload deletion, unregistering, and claim
completion are awaited before returning `complete`.

Object-store and callback failures release an acquired claim before propagating
an RPC error. Non-503 transient callback bodies are cancelled immediately because
their contents are not part of the RPC result. Suspension response parsing keeps
the existing 64 KiB cap. Successful and permanent callback bodies remain unbuffered.

The `WorkerEntrypoint` base resolves to the real runtime class in the worker
build; the existing Node base module provides the structural test counterpart.
Both the package worker entry and the deployed source entry export the named
class. The consumer config selects it with `entrypoint: "QueueDeliveryRpc"`.

## Validation

- Consumer contract tests first failed against the old fetch implementation:
  six failed and five passed, showing that the RPC cases exercised a new path.
- `pnpm check`: 468 tests across 25 files, plus formatting, lint, typecheck,
  build, worker-bundle, and package checks.
- `pnpm test:integration:celld-smoke`: ten passed on macOS arm64 with celld
  v0.5.0 and MinIO, including restart/Queue recovery, retention recovery, stream
  cancellation, and persisted telemetry.
- Real RPC tests reject invalid credentials, malformed envelopes, and invalid
  attempts; the removed HTTP delivery route returns 404.
- Real callback tests preserve message identity while suspension keeps attempt 1
  and a transient failure increments it to 2.
- Unit cases cover permanent statuses, missing payloads, duplicate/active claims,
  missing secrets, bounded response parsing, storage failures, failed completion,
  invalid delays, suspension limits, and publication failure before acknowledgment.

The negative RPC tests use a temporary, bundled HTTP driver with a test-only
service binding. It is generated inside the smoke's temporary deployment and
is not part of the shipped worker. The ordinary Queue tests use the actual
companion consumer and configured named binding.

## Deployment and limits

Deploy matching versions of the consumer and primary worker during a maintenance
window, consumer first and primary last. Keep the same Queue, bucket, and Durable
Object names. An old consumer cannot use the removed HTTP delivery route, and a
new consumer requires the named export. No mixed-version fallback is provided.

Both transports use service bindings. This spike establishes correctness on one
node; it does not establish a performance gain, exactly-once application effects,
multi-node handoff, or a production migration. No production deployment or
publication was performed.
