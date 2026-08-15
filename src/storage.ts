import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  RunNotSupportedError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type {
  CreateEventParams,
  CreateEventRequest,
  Event,
  EventResult,
  GetEventParams,
  GetHookParams,
  GetStepParams,
  GetWorkflowRunParams,
  Hook,
  ListEventsParams,
  ListHooksParams,
  ListWorkflowRunStepsParams,
  ListWorkflowRunsParams,
  PaginatedResponse,
  ResolveData,
  RunCreatedEventRequest,
  Step,
  StepWithoutData,
  Storage,
  WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';
import {
  EventSchema,
  HookSchema,
  SPEC_VERSION_CURRENT,
  StepSchema,
  WorkflowRunSchema,
} from '@workflow/world';
import { parse, stringify } from './vendor/shared/index.js';
import { monotonicFactory } from 'ulid';
import type { ApplyEventFailure, ApplyEventOutcome, ApplyEventRequest } from './apply-event.js';
import type { HookTokenOwner, IndexNamespace } from './config.js';
import { compact } from './util.js';

/**
 * RPC surface of WorkflowRunDO used by the storage layer. Declared
 * structurally to avoid a circular type reference on the DO class.
 */
export interface WorkflowRunDOStub {
  applyEvent(request: ApplyEventRequest): Promise<ApplyEventOutcome>;
  getRun(): Promise<WorkflowRun | null>;
  getStep(stepId: string): Promise<Step | null>;
  getEvent(eventId: string): Promise<Event | null>;
  listEvents(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<{ data: Event[]; cursor: string | null; hasMore: boolean }>;
  listSteps(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<{ data: Step[]; cursor: string | null; hasMore: boolean }>;
  listHooks(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<{ data: Hook[]; cursor: string | null; hasMore: boolean }>;
}

export interface WorkflowRunDONamespace {
  idFromName(name: string): DurableObjectIdLike;
  get(id: DurableObjectIdLike): WorkflowRunDOStub;
}

export interface DurableObjectIdLike {
  toString(): string;
}

export interface CloudflareStorageConfig {
  env: {
    WORKFLOW_DB: WorkflowRunDONamespace;
    WORKFLOW_INDEX: IndexNamespace;
  };
  deploymentId: string;
}

function sortableTimestamp(date: Date): string {
  return date.getTime().toString().padStart(13, '0');
}

function workflowRunIndexKey(run: WorkflowRun): string {
  return `run:${run.workflowName}:${sortableTimestamp(run.createdAt)}:${run.runId}`;
}

function globalRunIndexKey(run: WorkflowRun): string {
  return `runall:${sortableTimestamp(run.createdAt)}:${run.runId}`;
}

function hookOwner(hook: Pick<Hook, 'runId' | 'hookId'>): HookTokenOwner {
  return { runId: hook.runId, hookId: hook.hookId };
}

/**
 * Filter data based on ResolveData parameter.
 * When resolveData is 'none', strips specified keys to reduce data transfer.
 */
function filterData<T extends object>(
  data: T,
  resolveData: ResolveData | undefined,
  keysToStrip: (keyof T)[],
): T {
  if (resolveData === 'none') {
    const newData = { ...data };
    for (const key of keysToStrip) {
      if (key in newData) {
        delete newData[key];
      }
    }
    return newData;
  }
  return data;
}

/**
 * Filter hook data based on resolveData parameter
 */
function filterHookData(hook: Hook, resolveData: ResolveData): Hook {
  if (resolveData === 'none' && 'metadata' in hook) {
    const { metadata: _, ...rest } = hook;
    return { metadata: undefined, ...rest };
  }
  return hook;
}

/**
 * Convert a structured applyEvent failure back into the typed error the
 * runtime matches on (via error-name based `.is()` checks). Typed errors
 * cannot cross the DO RPC boundary intact, which is why the DO returns
 * outcome objects instead of throwing.
 */
function throwOutcomeError(
  outcome: ApplyEventFailure,
  runId: string,
  data: CreateEventRequest | RunCreatedEventRequest,
): never {
  switch (outcome.code) {
    case 'RUN_NOT_FOUND':
      throw new WorkflowRunNotFoundError(runId);
    case 'STEP_NOT_FOUND':
      throw new WorkflowWorldError(outcome.message, { status: 404 });
    case 'HOOK_NOT_FOUND':
      throw new HookNotFoundError(data.correlationId ?? runId);
    case 'ENTITY_CONFLICT':
      throw new EntityConflictError(outcome.message);
    case 'RUN_EXPIRED':
      throw new RunExpiredError(outcome.message);
    case 'TOO_EARLY':
      throw new TooEarlyError(outcome.message, { retryAfter: outcome.retryAfterSeconds });
    case 'RUN_NOT_SUPPORTED':
      throw new RunNotSupportedError(outcome.runSpecVersion ?? 0, SPEC_VERSION_CURRENT);
    case 'LEGACY_RUN_NOT_SUPPORTED':
      throw new WorkflowWorldError(outcome.message, { status: 422 });
  }
}

const parseRun = (run: WorkflowRun): WorkflowRun => WorkflowRunSchema.parse(compact(run));
const parseStep = (step: Step): Step => StepSchema.parse(compact(step));
const parseHook = (hook: Hook): Hook => HookSchema.parse(compact(hook));
const parseEvent = (event: Event): Event => EventSchema.parse(compact(event));

export function createStorage(config: CloudflareStorageConfig): Storage {
  const { env } = config;
  const ulid = monotonicFactory();

  // Helper to get or create a DO for a run
  const getRunDO = (runId: string): WorkflowRunDOStub => {
    const id = env.WORKFLOW_DB.idFromName(runId);
    return env.WORKFLOW_DB.get(id);
  };

  const runsGet = async (
    runId: string,
    params?: GetWorkflowRunParams,
  ): Promise<WorkflowRun | WorkflowRunWithoutData> => {
    const stub = getRunDO(runId);
    const run = await stub.getRun();

    if (!run) {
      throw new WorkflowRunNotFoundError(runId);
    }

    return filterData(parseRun(run), params?.resolveData, ['input', 'output']);
  };

  return {
    runs: {
      get: runsGet,

      async list(
        params?: ListWorkflowRunsParams,
      ): Promise<PaginatedResponse<WorkflowRun | WorkflowRunWithoutData>> {
        const limit = params?.pagination?.limit ?? 20;
        const prefix = params?.workflowName ? `run:${params.workflowName}:` : 'runall:';
        const reverse = params?.pagination?.sortOrder === 'desc';
        const matches: Array<{
          key: string;
          run: WorkflowRun | WorkflowRunWithoutData;
        }> = [];
        let scanCursor = params?.pagination?.cursor;
        let exhausted = false;

        // Keep scanning index pages until the requested page is full. This
        // prevents status filters and stale derived entries from producing
        // short pages or cursors that skip matching runs.
        while (matches.length <= limit && !exhausted) {
          const kvList = await env.WORKFLOW_INDEX.list({
            prefix,
            limit: Math.min(1000, Math.max(50, limit)),
            cursor: scanCursor,
            reverse,
          });
          if (kvList.keys.length === 0) {
            exhausted = true;
            break;
          }

          for (const key of kvList.keys) {
            const meta = await env.WORKFLOW_INDEX.get(key.name);
            if (!meta) continue;
            const { runId } = JSON.parse(meta) as { runId: string };
            try {
              const run = await runsGet(runId, { resolveData: params?.resolveData });
              if (!params?.status || run.status === params.status) {
                matches.push({ key: key.name, run });
                if (matches.length > limit) break;
              }
            } catch (error) {
              if (!WorkflowRunNotFoundError.is(error)) throw error;
            }
          }

          exhausted = kvList.list_complete;
          scanCursor = kvList.cursor ?? kvList.keys.at(-1)?.name;
        }

        const hasMore = matches.length > limit;
        const page = matches.slice(0, limit);

        return {
          data: page.map(({ run }) => run),
          cursor: hasMore ? (page.at(-1)?.key ?? null) : null,
          hasMore,
        };
      },
    } as Storage['runs'],

    events: {
      async create(
        runId: string | null,
        data: RunCreatedEventRequest | CreateEventRequest,
        _params?: CreateEventParams,
      ): Promise<EventResult> {
        // For run_created events, generate a runId server-side if absent.
        let effectiveRunId: string;
        if (data.eventType === 'run_created' && (!runId || runId === '')) {
          effectiveRunId = `wrun_${ulid()}`;
        } else if (!runId) {
          throw new WorkflowWorldError('runId is required for non-run_created events', {
            status: 400,
          });
        } else {
          effectiveRunId = runId;
        }

        const stub = getRunDO(effectiveRunId);

        let tokenHolder: ApplyEventRequest['tokenHolder'];
        let hookReservation: HookTokenOwner | undefined;
        if (data.eventType === 'hook_created') {
          hookReservation = {
            runId: effectiveRunId,
            hookId: data.correlationId,
          };
          const reservation = await env.WORKFLOW_INDEX.reserveHookToken(
            data.eventData.token,
            hookReservation,
          );
          tokenHolder = reservation.claimed ? null : reservation.holder;
        }

        // Guards, event append, and entity mutation run in ONE DO storage
        // transaction (see apply-event.ts). The event is schema-validated
        // before anything is persisted.
        let outcome: ApplyEventOutcome;
        try {
          outcome = await stub.applyEvent({ runId: effectiveRunId, data, tokenHolder });
        } catch (error) {
          if (hookReservation && data.eventType === 'hook_created') {
            await env.WORKFLOW_INDEX.releaseHookToken(data.eventData.token, hookReservation);
          }
          throw error;
        }

        if (!outcome.ok) {
          if (hookReservation && data.eventType === 'hook_created') {
            await env.WORKFLOW_INDEX.releaseHookToken(data.eventData.token, hookReservation);
          }
          throwOutcomeError(outcome, effectiveRunId, data);
        }

        // Derived indexes are deliberately rewritten on idempotent replay so
        // a committed run or hook can repair an interrupted index update.
        if (outcome.run) {
          const meta = JSON.stringify({
            runId: effectiveRunId,
            createdAt: outcome.run.createdAt.toISOString(),
            status: outcome.run.status,
          });
          await env.WORKFLOW_INDEX.put(workflowRunIndexKey(outcome.run), meta);
          await env.WORKFLOW_INDEX.put(globalRunIndexKey(outcome.run), meta);
        }
        if (outcome.hookToIndex) {
          const serialized = stringify(outcome.hookToIndex);
          await env.WORKFLOW_INDEX.finalizeHookIndexes(
            outcome.hookToIndex.token,
            outcome.hookToIndex.hookId,
            serialized,
            hookOwner(outcome.hookToIndex),
          );
        } else if (hookReservation && data.eventType === 'hook_created') {
          await env.WORKFLOW_INDEX.releaseHookToken(data.eventData.token, hookReservation);
        }
        for (const released of outcome.releasedHooks) {
          await env.WORKFLOW_INDEX.deleteHookIndexes(released.token, released.hookId, {
            runId: effectiveRunId,
            hookId: released.hookId,
          });
        }
        if (outcome.event?.correlationId) {
          const correlationKey = `correlation:${encodeURIComponent(
            outcome.event.correlationId,
          )}:${sortableTimestamp(outcome.event.createdAt)}:${outcome.event.eventId}:${effectiveRunId}`;
          await env.WORKFLOW_INDEX.put(
            correlationKey,
            JSON.stringify({ runId: effectiveRunId, eventId: outcome.event.eventId }),
          );
        }

        return {
          event: outcome.event,
          run: outcome.run,
          step: outcome.step,
          hook: outcome.hook,
          events: outcome.events,
        };
      },

      async get(runId: string, eventId: string, _params?: GetEventParams): Promise<Event> {
        const stub = getRunDO(runId);
        const event = await stub.getEvent(eventId);

        if (!event) {
          throw new WorkflowWorldError(`Event not found: ${eventId}`, {
            status: 404,
          });
        }

        return parseEvent(event);
      },

      async list(params: ListEventsParams): Promise<PaginatedResponse<Event>> {
        const { runId } = params;
        const limit = params?.pagination?.limit ?? 100;

        const stub = getRunDO(runId);
        const result = await stub.listEvents({
          limit,
          cursor: params?.pagination?.cursor || undefined,
          sortOrder: params.pagination?.sortOrder || 'asc',
        });

        return {
          data: result.data.map(parseEvent),
          cursor: result.cursor,
          hasMore: result.hasMore,
        };
      },

      async listByCorrelationId(params) {
        const limit = params.pagination?.limit ?? 100;
        const prefix = `correlation:${encodeURIComponent(params.correlationId)}:`;
        const listed = await env.WORKFLOW_INDEX.list({
          prefix,
          limit,
          cursor: params.pagination?.cursor,
          reverse: params.pagination?.sortOrder === 'desc',
        });
        const events = await Promise.all(
          listed.keys.map(async ({ name }) => {
            const raw = await env.WORKFLOW_INDEX.get(name);
            if (!raw) return null;
            const { runId, eventId } = JSON.parse(raw) as { runId: string; eventId: string };
            return getRunDO(runId).getEvent(eventId);
          }),
        );
        return {
          data: events.filter((event): event is Event => event !== null).map(parseEvent),
          cursor: listed.list_complete ? null : (listed.cursor ?? null),
          hasMore: !listed.list_complete,
        };
      },
    },

    steps: {
      async get(runId: string | undefined, stepId: string, params?: GetStepParams) {
        if (!runId) {
          throw new WorkflowWorldError('runId is required for Cloudflare step lookup', {
            status: 400,
          });
        }
        const stub = getRunDO(runId);
        const step = await stub.getStep(stepId);

        if (!step) {
          throw new WorkflowWorldError(`Step not found: ${stepId}`, {
            status: 404,
          });
        }

        return filterData(parseStep(step), params?.resolveData, ['input', 'output']);
      },

      async list(
        params: ListWorkflowRunStepsParams,
      ): Promise<PaginatedResponse<Step | StepWithoutData>> {
        const { runId } = params;
        const limit = params?.pagination?.limit ?? 20;

        const stub = getRunDO(runId);
        const result = await stub.listSteps({
          limit,
          cursor: params?.pagination?.cursor || undefined,
          sortOrder: params?.pagination?.sortOrder ?? 'asc',
        });

        return {
          data: result.data.map((s) =>
            filterData(parseStep(s), params?.resolveData, ['input', 'output']),
          ),
          cursor: result.cursor,
          hasMore: result.hasMore,
        };
      },
    } as Storage['steps'],

    hooks: {
      async get(hookId: string, params?: GetHookParams) {
        const raw = await env.WORKFLOW_INDEX.get(`hookid:${hookId}`);

        if (!raw) {
          throw new HookNotFoundError(hookId);
        }

        const hook = parseHook(parse<Hook>(raw));
        return filterHookData(hook, params?.resolveData ?? 'all');
      },

      async getByToken(token: string, params?: GetHookParams) {
        const raw = await env.WORKFLOW_INDEX.get(`hook:${token}`);

        if (!raw) {
          throw new HookNotFoundError(token);
        }

        const hook = parseHook(parse<Hook>(raw));
        return filterHookData(hook, params?.resolveData ?? 'all');
      },

      async list(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
        if (!params.runId) {
          throw new WorkflowWorldError('runId is required for listing hooks', {
            status: 400,
          });
        }
        const runId = params.runId;
        const limit = params?.pagination?.limit ?? 100;

        const stub = getRunDO(runId);
        const result = await stub.listHooks({
          limit,
          cursor: params?.pagination?.cursor || undefined,
          sortOrder: params?.pagination?.sortOrder ?? 'asc',
        });

        return {
          data: result.data.map((h) => filterHookData(parseHook(h), params?.resolveData ?? 'all')),
          cursor: result.cursor,
          hasMore: result.hasMore,
        };
      },
    },
  };
}
