import { rpcParse, rpcStringify } from '../codec.js';
import { FleetTransportError, reconstructError, type WireError } from './errors.js';

export interface RpcTransport {
  fleetUrl: string;
  secret: string;
  /** Injectable for tests. Default: globalThis.fetch */
  fetchImpl?: typeof fetch;
}

/** Statuses that mean "the fleet couldn't take the request" (safe to retry reads). */
const RETRYABLE_STATUSES = new Set([502, 503, 504]);
const READ_RETRIES = 2;

function delayMs(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Invoke one DO method through the worker router.
 *
 * Mutating calls run exactly once from the client's perspective: a blind
 * retry of e.g. applyEvent would turn commit-then-network-fail into a
 * spurious ENTITY_CONFLICT. The workflow runtime already gets at-least-once
 * via queue redelivery. Read-only calls retry on transport failures.
 */
export async function callDO<T>(
  transport: RpcTransport,
  binding: string,
  name: string,
  method: string,
  args: unknown[],
  opts?: { idempotent?: boolean },
): Promise<T> {
  const doFetch = transport.fetchImpl ?? fetch;
  const url = `${transport.fleetUrl.replace(/\/$/, '')}/v1/rpc/${binding}/${encodeURIComponent(
    name,
  )}/${method}`;
  const attempts = opts?.idempotent ? 1 + READ_RETRIES : 1;

  let lastError: unknown;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    if (attempt > 1) {
      await delayMs(100 * attempt + Math.random() * 200);
    }
    let response: Response;
    try {
      response = await doFetch(url, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          authorization: `Bearer ${transport.secret}`,
        },
        body: rpcStringify(args),
      });
    } catch (error) {
      lastError = new FleetTransportError(`world-celld: fleet unreachable at ${url}`, error);
      continue;
    }

    if (response.ok) {
      return rpcParse<T>(await response.text());
    }

    const text = await response.text();
    let wire: WireError | undefined;
    try {
      wire = (JSON.parse(text) as { error?: WireError }).error;
    } catch {
      wire = { message: text || `HTTP ${response.status}` };
    }

    if (RETRYABLE_STATUSES.has(response.status) && attempt < attempts) {
      lastError = reconstructError(wire, response.status);
      continue;
    }

    throw reconstructError(wire, response.status);
  }

  throw lastError instanceof Error
    ? lastError
    : new FleetTransportError('world-celld: rpc failed', lastError);
}
