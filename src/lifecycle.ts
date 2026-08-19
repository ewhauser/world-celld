/**
 * Hard protocol bounds used by lifecycle leases and derivative-fence cleanup.
 *
 * These are not retention TTLs. They bound how long a request which was
 * admitted before expiry may still mutate a derivative Durable Object:
 *
 * - fleet calls have at most three 5-minute attempts plus bounded retry delay;
 * - one queue delivery owns its lease for at most 5 minutes plus 30 seconds;
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
export const QUEUE_INFLIGHT_GRACE_MS = 30 * 1000;
export const MAX_QUEUE_INFLIGHT_LEASE_MS = MAX_QUEUE_DELIVERY_TIMEOUT_MS + QUEUE_INFLIGHT_GRACE_MS;

export const CATALOG_FENCE_GRACE_MS = MAX_RUN_INDEX_PUBLICATION_LIFETIME_MS;
export const QUEUE_FENCE_GRACE_MS = Math.max(MAX_FLEET_RPC_TIMEOUT_MS, MAX_QUEUE_INFLIGHT_LEASE_MS);

export const HOOK_CLAIM_LEASE_MS =
  (FLEET_IDEMPOTENT_ATTEMPTS * 2 + 1) * MAX_FLEET_RPC_TIMEOUT_MS + FLEET_RETRY_BACKOFF_MAX_MS * 2;

export const LIFECYCLE_COMPACTION_BATCH = 128;
export const LIFECYCLE_COMPACTION_RETRY_MS = 1000;
