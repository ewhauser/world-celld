import type { Event, EventResult } from '@workflow/world';

interface ResumeWorld {
  events: {
    create(
      runId: string,
      data: {
        eventType: 'hook_received' | 'run_started';
        correlationId?: string;
        eventData?: { token: string; payload: unknown };
      },
      params?: { resumeId: string; resumePayloadDigest: string; preloadEvents: true },
    ): Promise<EventResult>;
    list(params: { runId: string }): Promise<unknown>;
  };
}

/** The usability check @workflow/core applies to a hook_received preload. */
function isUsableReplayPreload(result: EventResult, resumeId: string): boolean {
  const events: Event[] | undefined = result.events;
  return (
    result.run?.startedAt !== undefined &&
    result.event !== undefined &&
    events !== undefined &&
    events.length > 0 &&
    result.cursor != null &&
    !result.hasMore &&
    typeof result.maxEvents === 'number' &&
    events.some((event) => event.eventType === 'run_created') &&
    events.some((event) => event.eventType === 'run_started') &&
    events.some(
      (event) =>
        event.eventType === 'hook_received' &&
        (event as { resumeId?: string }).resumeId === resumeId,
    )
  );
}

/**
 * Mirrors @workflow/core's lazy hook resume setup: an idempotent
 * hook_received re-ensure with preloadEvents, then the generic run_started
 * setup (and events.list when that returns no log) unless the response is a
 * usable replay preload.
 */
export async function lazyHookResumeSetup(
  world: ResumeWorld,
  input: { runId: string; hookId: string; token: string; resumeId: string; payload: unknown },
): Promise<{ result: EventResult; usablePreload: boolean }> {
  const result = await world.events.create(
    input.runId,
    {
      eventType: 'hook_received',
      correlationId: input.hookId,
      eventData: { token: input.token, payload: input.payload },
    },
    { resumeId: input.resumeId, resumePayloadDigest: 'perf-digest', preloadEvents: true },
  );
  const usablePreload = isUsableReplayPreload(result, input.resumeId);
  if (!usablePreload) {
    const started = await world.events.create(input.runId, { eventType: 'run_started' });
    if (started.events === undefined) await world.events.list({ runId: input.runId });
  }
  return { result, usablePreload };
}
