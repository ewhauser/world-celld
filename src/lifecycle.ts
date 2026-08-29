import { MAX_QUEUE_TIMESTAMP_MS } from './validation.js';

/**
 * Hard protocol bounds used by lifecycle leases and derivative-fence cleanup.
 *
 * These are not retention TTLs. They bound how long a request which was
 * admitted before expiry may still mutate a derivative Durable Object:
 *
 * - fleet calls have at most three 5-minute attempts plus bounded retry delay;
 * - one native Queue idempotency claim stays live for at most 15 minutes
 *   without a successful delivery or explicit workflow suspension;
 * - hook admission spans reserve retries, one authoritative apply, and
 *   finalize retries before the exact claim must be reacquired.
 */
export const MAX_FLEET_RPC_TIMEOUT_MS = 5 * 60 * 1000;
export const FLEET_IDEMPOTENT_ATTEMPTS = 3;
export const FLEET_RETRY_BACKOFF_MAX_MS = 900;

export const MAX_IDEMPOTENT_RPC_LIFETIME_MS =
  FLEET_IDEMPOTENT_ATTEMPTS * MAX_FLEET_RPC_TIMEOUT_MS + FLEET_RETRY_BACKOFF_MAX_MS;
export const MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS =
  MAX_FLEET_RPC_TIMEOUT_MS + MAX_IDEMPOTENT_RPC_LIFETIME_MS;

export const MAX_QUEUE_DELIVERY_TIMEOUT_MS = 5 * 60 * 1000;
export const QUEUE_CLAIM_STALE_MS = 15 * 60 * 1000;
/** A fresh delivery edge may advance an immediately-due message by one millisecond. */
export const QUEUE_ALARM_EDGE_MS = 1;
export const MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS = QUEUE_CLAIM_STALE_MS + QUEUE_ALARM_EDGE_MS;
/** Latest admissible due time with room for a fresh queue claim. */
export const MAX_QUEUE_SCHEDULE_TIMESTAMP_MS =
  MAX_QUEUE_TIMESTAMP_MS - MAX_QUEUE_DERIVED_DEADLINE_HEADROOM_MS;

export const CATALOG_FENCE_GRACE_MS = MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS;

export const HOOK_CLAIM_LEASE_MS =
  (FLEET_IDEMPOTENT_ATTEMPTS * 2 + 1) * MAX_FLEET_RPC_TIMEOUT_MS + FLEET_RETRY_BACKOFF_MAX_MS * 2;

export const LIFECYCLE_COMPACTION_BATCH = 128;
export const LIFECYCLE_COMPACTION_RETRY_MS = 1000;
