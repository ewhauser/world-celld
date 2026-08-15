/**
 * Durable Object base class — Node variant.
 *
 * The cell classes extend `DurableObject` so celld's JS RPC works on their
 * stubs. This module is what Node builds (tests, the `./testing` in-process
 * fleet) resolve; the worker build swaps it for do-base-workerd.ts, which
 * re-exports the real `cloudflare:workers` base (see tsdown.config.ts).
 * Behaviorally identical at this layer: the base only assigns ctx/env.
 */
export class DurableObject<TEnv = unknown> {
  constructor(
    readonly ctx: DurableObjectState,
    readonly env: TEnv,
  ) {}
}
