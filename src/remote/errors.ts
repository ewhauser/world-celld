import { WorkflowWorldError } from '@workflow/errors';

export interface WireError {
  name?: string;
  message?: string;
  status?: number;
}

/**
 * Reconstruct a typed error from a router error payload. `@workflow/errors`
 * matches with name-based `.is()` checks, so a WorkflowWorldError with the
 * original name (and status) restored behaves identically to the source
 * error for every consumer in the workflow runtime.
 */
export function reconstructError(wire: WireError | undefined, httpStatus: number): Error {
  const message = wire?.message ?? `world-celld rpc failed with HTTP ${httpStatus}`;
  const error = new WorkflowWorldError(message, {
    status: wire?.status ?? httpStatus,
  });
  if (wire?.name) {
    error.name = wire.name;
  }
  return error;
}

/** Transport-level failure (fleet unreachable, malformed response). */
export class FleetTransportError extends Error {
  constructor(
    message: string,
    readonly cause?: unknown,
  ) {
    super(message);
    this.name = 'FleetTransportError';
  }
}
