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
  isTerminalWorkflowRunStatus,
  SPEC_VERSION_CURRENT,
  StepSchema,
  WorkflowRunSchema,
} from '@workflow/world';
import { parse, stringify } from './vendor/shared/index.js';
import { monotonicFactory } from 'ulid';
import type { ApplyEventFailure, ApplyEventOutcome, ApplyEventRequest } from './apply-event.js';
import type { HookTokenOwner, IndexNamespace } from './config.js';
import { compact } from './util.js';
import { globalRunIndexKey, type RunReadOutcome, workflowRunIndexKey } from './retention.js';

/**
 * RPC surface of WorkflowRunDO used by the storage layer. Declared
 * structurally to avoid a circular type reference on the DO class.
 */
export interface WorkflowRunDOStub {
  applyEvent(request: ApplyEventRequest): Promise<ApplyEventOutcome>;
  getRun(): Promise<RunReadOutcome<WorkflowRun | null>>;
  getStep(stepId: string): Promise<RunReadOutcome<Step | null>>;
  getEvent(eventId: string): Promise<RunReadOutcome<Event | null>>;
  listEvents(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Event[]; cursor: string | null; hasMore: boolean }>>;
  listSteps(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Step[]; cursor: string | null; hasMore: boolean }>>;
  listHooks(params?: {
    limit?: number;
    cursor?: string;
    sortOrder?: 'asc' | 'desc';
  }): Promise<RunReadOutcome<{ data: Hook[]; cursor: string | null; hasMore: boolean }>>;
  getCleanupStatus(): Promise<import('./retention.js').CleanupRecord | null>;
  scheduleCleanup(
    request: import('./retention.js').ScheduleCleanupRequest,
  ): Promise<import('./retention.js').CleanupRecord | null>;
  cleanupNow(
    request: import('./retention.js').ScheduleCleanupRequest,
  ): Promise<import('./retention.js').CleanupRecord | null>;
  rearmCleanup(): Promise<import('./retention.js').CleanupRecord | null>;
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
  runRetentionMs?: number;
  queueShards?: number;
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
    case 'WAIT_NOT_FOUND':
      throw new WorkflowWorldError(outcome.message, { status: 404 });
    case 'ENTITY_CONFLICT':
      throw new EntityConflictError(outcome.message);
    case 'RUN_EXPIRED':
      throw new RunExpiredError(outcome.message);
    case 'TOO_EARLY':
      throw new TooEarlyError(outcome.message, { retryAfter: outcome.retryAfterSeconds });
    case 'RUN_NOT_SUPPORTED':
      throw new RunNotSupportedError(outcome.runSpecVersion ?? 0, SPEC_VERSION_CURRENT);
  }
}

const parseRun = (run: WorkflowRun): WorkflowRun => WorkflowRunSchema.parse(compact(run));
const parseStep = (step: Step): Step => StepSchema.parse(compact(step));
const parseHook = (hook: Hook): Hook => HookSchema.parse(compact(hook));
const parseEvent = (event: Event): Event => EventSchema.parse(compact(event));

function unwrapRead<T>(outcome: RunReadOutcome<T>): T {
  if (!outcome.ok) {
    throw new RunExpiredError(outcome.message);
  }
  return outcome.value;
}

/** Bound cross-run fanout so large list pages cannot create an RPC burst. */
const RUN_LIST_CONCURRENCY = 8;

interface RunIndexMetadata {
  runId: string;
  /** Optional so indexes written by older deployments remain readable. */
  status?: WorkflowRun['status'];
}

/**
 * Run statuses only move pending -> running -> terminal, and terminal statuses
 * are immutable. An older index value may therefore safely exclude a filter
 * only when it is already terminal, or when the caller asks for pending and
 * the index has advanced beyond pending. Earlier non-terminal metadata cannot
 * exclude a later status because a post-commit index write may need replay.
 */
function indexStatusExcludes(
  indexed: WorkflowRun['status'] | undefined,
  requested: WorkflowRun['status'] | undefined,
): boolean {
  if (indexed === undefined || requested === undefined || indexed === requested) return false;
  return isTerminalWorkflowRunStatus(indexed) || requested === 'pending';
}

export function createStorage(config: CloudflareStorageConfig): Storage {
  const { env } = config;
  const cleanup = {
    retentionMs: config.runRetentionMs ?? 0,
    queueShards: config.queueShards ?? 1,
  };
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
    const run = unwrapRead(await stub.getRun());

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

          // IndexDO returns values with the page. Resolve missing values in
          // bounded batches as a compatibility fallback for older/custom
          // KV-like adapters instead of restoring serial per-key reads.
          const candidates: Array<{ key: string; metadata: RunIndexMetadata }> = [];
          for (let offset = 0; offset < kvList.keys.length; offset += RUN_LIST_CONCURRENCY) {
            const batch = kvList.keys.slice(offset, offset + RUN_LIST_CONCURRENCY);
            const values = await Promise.all(
              batch.map(async (key) => ({
                key: key.name,
                value: key.value ?? (await env.WORKFLOW_INDEX.get(key.name)),
              })),
            );
            for (const { key, value } of values) {
              if (!value) continue;
              const metadata = JSON.parse(value) as RunIndexMetadata;
              // Use monotonic status metadata as a conservative prefilter,
              // then still verify every candidate against the authoritative
              // RunDO below. Earlier metadata cannot exclude a later status.
              if (indexStatusExcludes(metadata.status, params?.status)) {
                continue;
              }
              candidates.push({ key, metadata });
            }
          }

          // Fetch enough candidates to prove the requested page and hasMore,
          // retaining index order while limiting concurrent cross-run RPCs.
          for (let offset = 0; offset < candidates.length && matches.length <= limit;) {
            const needed = limit + 1 - matches.length;
            const batch = candidates.slice(offset, offset + Math.min(RUN_LIST_CONCURRENCY, needed));
            const resolved = await Promise.all(
              batch.map(async ({ key, metadata }) => {
                try {
                  const run = await runsGet(metadata.runId, {
                    resolveData: params?.resolveData,
                  });
                  return !params?.status || run.status === params.status ? { key, run } : null;
                } catch (error) {
                  if (!WorkflowRunNotFoundError.is(error) && !RunExpiredError.is(error)) {
                    throw error;
                  }
                  return null;
                }
              }),
            );
            for (const match of resolved) {
              if (match) matches.push(match);
            }
            offset += batch.length;
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
        params?: CreateEventParams,
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
          outcome = await stub.applyEvent({
            runId: effectiveRunId,
            data,
            params,
            tokenHolder,
            cleanup,
          });
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
          await env.WORKFLOW_INDEX.putOwned(effectiveRunId, workflowRunIndexKey(outcome.run), meta);
          await env.WORKFLOW_INDEX.putOwned(effectiveRunId, globalRunIndexKey(outcome.run), meta);
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

        const eventPage =
          outcome.events === undefined
            ? {}
            : {
                events: outcome.events,
                cursor: outcome.cursor ?? null,
                hasMore: outcome.hasMore ?? false,
              };
        return {
          event: outcome.event,
          run: outcome.run,
          step: outcome.step,
          hook: outcome.hook,
          wait: outcome.wait,
          stepCreated: outcome.stepCreated,
          maxEvents: outcome.maxEvents,
          ...eventPage,
        };
      },

      async get(runId: string, eventId: string, _params?: GetEventParams): Promise<Event> {
        const stub = getRunDO(runId);
        const event = unwrapRead(await stub.getEvent(eventId));

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
        const result = unwrapRead(
          await stub.listEvents({
            limit,
            cursor: params?.pagination?.cursor || undefined,
            sortOrder: params.pagination?.sortOrder || 'asc',
          }),
        );

        return {
          data: result.data.map(parseEvent),
          cursor: result.cursor,
          hasMore: result.hasMore,
        };
      },

      async listByCorrelationId(params) {
        const limit = params.pagination?.limit ?? 100;
        const stub = getRunDO(params.runId);
        const matches: Event[] = [];
        let scanCursor = params.pagination?.cursor;
        let exhausted = false;

        // Workflow 5 scopes correlation IDs to a run. Scan that run's dense
        // event log rather than the global derived index so identical step or
        // wait IDs in different runs can never leak into this result.
        while (matches.length <= limit && !exhausted) {
          const page = unwrapRead(
            await stub.listEvents({
              limit: Math.min(1000, Math.max(50, limit + 1)),
              cursor: scanCursor,
              sortOrder: params.pagination?.sortOrder ?? 'asc',
            }),
          );
          for (const event of page.data) {
            if (event.correlationId === params.correlationId) {
              matches.push(event);
              if (matches.length > limit) break;
            }
          }
          exhausted = !page.hasMore;
          scanCursor = page.cursor ?? page.data.at(-1)?.eventId;
          if (page.data.length === 0) exhausted = true;
        }

        const hasMore = matches.length > limit;
        const data = matches.slice(0, limit);
        return {
          data: data.map(parseEvent),
          cursor: hasMore ? (data.at(-1)?.eventId ?? null) : null,
          hasMore,
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
        const step = unwrapRead(await stub.getStep(stepId));

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
        const result = unwrapRead(
          await stub.listSteps({
            limit,
            cursor: params?.pagination?.cursor || undefined,
            sortOrder: params?.pagination?.sortOrder ?? 'asc',
          }),
        );

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
        const result = unwrapRead(
          await stub.listHooks({
            limit,
            cursor: params?.pagination?.cursor || undefined,
            sortOrder: params?.pagination?.sortOrder ?? 'asc',
          }),
        );

        return {
          data: result.data.map((h) => filterHookData(parseHook(h), params?.resolveData ?? 'all')),
          cursor: result.cursor,
          hasMore: result.hasMore,
        };
      },
    },
  };
}
