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
  CreateEventSchema,
  EventSchema,
  HookSchema,
  isTerminalWorkflowRunStatus,
  SPEC_VERSION_CURRENT,
  StepSchema,
  WaitSchema,
  WorkflowRunSchema,
} from '@workflow/world';
import { parse, stringify } from './vendor/shared/index.js';
import { monotonicFactory } from 'ulid';
import type {
  ApplyEventErrorCode,
  ApplyEventFailure,
  ApplyEventOutcome,
  ApplyEventRequest,
  ApplyEventSuccess,
} from './apply-event.js';
import type { HookTokenOwner, IndexNamespace } from './config.js';
import type { HookReservation } from './indexes.js';
import { FleetTransportError } from './remote/errors.js';
import { compact } from './util.js';
import type { RunReadOutcome } from './retention.js';

/**
 * RPC surface of WorkflowRunDO used by the storage layer. Declared
 * structurally to avoid a circular type reference on the DO class.
 */
export interface WorkflowRunDOStub {
  applyEvent(request: ApplyEventRequest): Promise<ApplyEventOutcome>;
  resolveHookTokenClaim(request: {
    hookId: string;
    token: string;
    claimId: string;
  }): Promise<{ committed: boolean }>;
  getLifecycleStatus(): Promise<import('./retention.js').RunLifecycleStatus>;
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

const APPLY_EVENT_ERROR_CODES = new Set<string>([
  'RUN_NOT_FOUND',
  'STEP_NOT_FOUND',
  'HOOK_NOT_FOUND',
  'WAIT_NOT_FOUND',
  'ENTITY_CONFLICT',
  'HOOK_CLAIM_CANCELLED',
  'RUN_EXPIRED',
  'TOO_EARLY',
  'RUN_NOT_SUPPORTED',
] satisfies ApplyEventErrorCode[]);

interface ApplyEventWireFailure {
  ok: false;
  code: string;
  message: string;
  status?: number;
  retryAfter?: number;
  retryAfterSeconds?: number;
  runSpecVersion?: number;
  worldSpecVersion?: number;
  details?: unknown;
}

type ParsedApplyEventOutcome =
  | { kind: 'success'; outcome: ApplyEventSuccess }
  | { kind: 'known-failure'; outcome: ApplyEventFailure & ApplyEventWireFailure }
  | { kind: 'unknown-failure'; outcome: ApplyEventWireFailure };

function malformedApplyEventOutcome(reason: string): never {
  throw new FleetTransportError(`world-celld: malformed applyEvent outcome: ${reason}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function validateOptionalNumber(
  outcome: Record<string, unknown>,
  key: 'status' | 'retryAfter' | 'retryAfterSeconds' | 'runSpecVersion' | 'worldSpecVersion',
  integer: boolean,
): void {
  const value = outcome[key];
  if (
    value !== undefined &&
    (typeof value !== 'number' ||
      !Number.isFinite(value) ||
      (integer && !Number.isSafeInteger(value)))
  ) {
    malformedApplyEventOutcome(`${key} must be ${integer ? 'a safe integer' : 'a finite number'}`);
  }
}

function parseSuccessEntity<T>(
  outcome: Record<string, unknown>,
  key: 'event' | 'run' | 'step' | 'hook' | 'wait' | 'hookToIndex',
  schema: {
    safeParse(value: unknown): { success: true; data: T } | { success: false };
  },
): T | undefined {
  const value = outcome[key];
  if (value === undefined) return undefined;
  if (!isRecord(value)) malformedApplyEventOutcome(`${key} is invalid`);
  const result = schema.safeParse(compact(value));
  if (!result.success) malformedApplyEventOutcome(`${key} is invalid`);
  return result.data;
}

/**
 * Validate the untrusted value decoded from the remote applyEvent RPC.
 * Unknown failure codes are intentionally valid and classified separately so
 * an older client can reconstruct their wire metadata without treating them
 * as success. All fields consumed by the success path are checked before any
 * derivative index operation can run.
 */
function parseApplyEventOutcome(value: unknown): ParsedApplyEventOutcome {
  if (!isRecord(value)) malformedApplyEventOutcome('response must be an object');

  if (value.ok === false) {
    if (typeof value.code !== 'string' || value.code.length === 0) {
      malformedApplyEventOutcome('failure code must be a non-empty string');
    }
    if (typeof value.message !== 'string') {
      malformedApplyEventOutcome('failure message must be a string');
    }
    validateOptionalNumber(value, 'status', true);
    validateOptionalNumber(value, 'retryAfter', false);
    validateOptionalNumber(value, 'retryAfterSeconds', false);
    validateOptionalNumber(value, 'runSpecVersion', true);
    validateOptionalNumber(value, 'worldSpecVersion', true);

    const outcome = value as unknown as ApplyEventWireFailure;
    return APPLY_EVENT_ERROR_CODES.has(outcome.code)
      ? {
          kind: 'known-failure',
          outcome: outcome as ApplyEventFailure & ApplyEventWireFailure,
        }
      : { kind: 'unknown-failure', outcome };
  }

  if (value.ok !== true) malformedApplyEventOutcome('ok must be true or false');
  if (!Array.isArray(value.releasedHooks)) {
    malformedApplyEventOutcome('releasedHooks must be an array');
  }
  for (const releasedHook of value.releasedHooks) {
    if (
      !isRecord(releasedHook) ||
      typeof releasedHook.hookId !== 'string' ||
      releasedHook.hookId.length === 0 ||
      typeof releasedHook.token !== 'string' ||
      releasedHook.token.length === 0
    ) {
      malformedApplyEventOutcome('releasedHooks entries must contain hookId and token strings');
    }
  }

  const event = parseSuccessEntity(value, 'event', EventSchema);
  const run = parseSuccessEntity(value, 'run', WorkflowRunSchema);
  const step = parseSuccessEntity(value, 'step', StepSchema);
  const hook = parseSuccessEntity(value, 'hook', HookSchema);
  const wait = parseSuccessEntity(value, 'wait', WaitSchema);
  const hookToIndex = parseSuccessEntity(value, 'hookToIndex', HookSchema);

  if (value.stepCreated !== undefined && value.stepCreated !== true) {
    malformedApplyEventOutcome('stepCreated must be true when present');
  }
  if (value.runCreated !== undefined) {
    if (
      !isRecord(value.runCreated) ||
      typeof value.runCreated.workflowName !== 'string' ||
      !(value.runCreated.createdAt instanceof Date) ||
      Number.isNaN(value.runCreated.createdAt.getTime())
    ) {
      malformedApplyEventOutcome('runCreated is invalid');
    }
  }
  let events: Event[] | undefined;
  if (value.events !== undefined) {
    if (!Array.isArray(value.events)) malformedApplyEventOutcome('events must be an array');
    events = value.events.map((candidate) => {
      if (!isRecord(candidate)) malformedApplyEventOutcome('events contains an invalid event');
      const result = EventSchema.safeParse(compact(candidate));
      if (!result.success) {
        malformedApplyEventOutcome('events contains an invalid event');
      }
      return result.data;
    });
    if (value.cursor !== null && typeof value.cursor !== 'string') {
      malformedApplyEventOutcome('cursor is required when events are present');
    }
    if (typeof value.hasMore !== 'boolean') {
      malformedApplyEventOutcome('hasMore is required when events are present');
    }
  }
  if (value.cursor !== undefined && value.cursor !== null && typeof value.cursor !== 'string') {
    malformedApplyEventOutcome('cursor must be a string or null');
  }
  if (value.hasMore !== undefined && typeof value.hasMore !== 'boolean') {
    malformedApplyEventOutcome('hasMore must be a boolean');
  }
  if (
    value.maxEvents !== undefined &&
    (typeof value.maxEvents !== 'number' ||
      !Number.isSafeInteger(value.maxEvents) ||
      value.maxEvents < 0)
  ) {
    malformedApplyEventOutcome('maxEvents must be a non-negative safe integer');
  }
  if (run !== undefined && value.indexPublicationExpiresAt === undefined) {
    malformedApplyEventOutcome('indexPublicationExpiresAt is required when run is present');
  }
  if (
    value.indexPublicationExpiresAt !== undefined &&
    (typeof value.indexPublicationExpiresAt !== 'number' ||
      !Number.isFinite(value.indexPublicationExpiresAt))
  ) {
    malformedApplyEventOutcome('indexPublicationExpiresAt must be a finite number');
  }

  return {
    kind: 'success',
    outcome: {
      ...value,
      event,
      run,
      step,
      hook,
      wait,
      hookToIndex,
      events,
      releasedHooks: value.releasedHooks.map((releasedHook) => ({
        hookId: (releasedHook as Record<string, unknown>).hookId as string,
        token: (releasedHook as Record<string, unknown>).token as string,
      })),
    } as ApplyEventSuccess,
  };
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
  outcome: ApplyEventWireFailure,
  runId: string,
  data: CreateEventRequest | RunCreatedEventRequest,
): never {
  const withCode = <T extends Error>(error: T): T => {
    Object.assign(error, { code: outcome.code });
    return error;
  };

  switch (outcome.code) {
    case 'RUN_NOT_FOUND':
      throw withCode(new WorkflowRunNotFoundError(runId));
    case 'STEP_NOT_FOUND':
      throw new WorkflowWorldError(outcome.message, { status: 404, code: outcome.code });
    case 'HOOK_NOT_FOUND':
      throw withCode(new HookNotFoundError(data.correlationId ?? runId));
    case 'WAIT_NOT_FOUND':
      throw new WorkflowWorldError(outcome.message, { status: 404, code: outcome.code });
    case 'ENTITY_CONFLICT':
      throw withCode(new EntityConflictError(outcome.message));
    case 'HOOK_CLAIM_CANCELLED':
      throw new WorkflowWorldError(outcome.message, { status: 503, code: outcome.code });
    case 'RUN_EXPIRED':
      throw withCode(new RunExpiredError(outcome.message));
    case 'TOO_EARLY':
      throw withCode(new TooEarlyError(outcome.message, { retryAfter: outcome.retryAfterSeconds }));
    case 'RUN_NOT_SUPPORTED':
      throw withCode(new RunNotSupportedError(outcome.runSpecVersion ?? 0, SPEC_VERSION_CURRENT));
    default: {
      const error = new WorkflowWorldError(outcome.message, {
        status: outcome.status,
        code: outcome.code,
        retryAfter: outcome.retryAfter,
      });
      for (const key of [
        'retryAfterSeconds',
        'runSpecVersion',
        'worldSpecVersion',
        'details',
      ] as const) {
        if (Object.hasOwn(outcome, key)) Object.assign(error, { [key]: outcome[key] });
      }
      throw error;
    }
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
  status: WorkflowRun['status'];
}

/**
 * Run statuses only move pending -> running -> terminal, and terminal statuses
 * are immutable. An older index value may therefore safely exclude a filter
 * only when it is already terminal, or when the caller asks for pending and
 * the index has advanced beyond pending. Earlier non-terminal metadata cannot
 * exclude a later status because a post-commit index write may need replay.
 */
function indexStatusExcludes(
  indexed: WorkflowRun['status'],
  requested: WorkflowRun['status'] | undefined,
): boolean {
  if (requested === undefined || indexed === requested) return false;
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
          const kvList = await env.WORKFLOW_INDEX.listRuns({
            prefix,
            limit: Math.min(1000, Math.max(50, limit)),
            cursor: scanCursor,
            reverse,
          });
          if (kvList.keys.length === 0) {
            exhausted = true;
            break;
          }

          const candidates: Array<{ key: string; metadata: RunIndexMetadata }> = [];
          for (const key of kvList.keys) {
            const metadata = JSON.parse(key.value) as RunIndexMetadata;
            // Use monotonic status metadata as a conservative prefilter,
            // then still verify every candidate against the authoritative
            // RunDO below. Earlier metadata cannot exclude a later status.
            if (indexStatusExcludes(metadata.status, params?.status)) {
              continue;
            }
            candidates.push({ key: key.name, metadata });
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
        // Parse the public runtime value before run routing or hook admission.
        // The canonical Workflow schema strips unknown fields and rejects bad
        // discriminants/missing/wrong-typed fields without any external side
        // effects.
        data = CreateEventSchema.parse(compact(data));
        if (runId !== null && typeof runId !== 'string') {
          throw new WorkflowWorldError('runId must be a string or null', { status: 400 });
        }

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
        let hookAdmission:
          | { owner: HookTokenOwner; reservation: HookReservation; token: string }
          | undefined;
        if (data.eventType === 'hook_created') {
          const owner = {
            runId: effectiveRunId,
            hookId: data.correlationId,
          };
          const admission = await env.WORKFLOW_INDEX.reserveHook(data.eventData.token, owner);
          if (admission.admitted) {
            tokenHolder = null;
            hookAdmission = {
              owner,
              reservation: admission.reservation,
              token: data.eventData.token,
            };
          } else {
            tokenHolder = admission.holder;
          }
        }

        // Guards, event append, and entity mutation run in ONE DO storage
        // transaction (see apply-event.ts). The event is schema-validated
        // before anything is persisted.
        // A thrown RPC is commit-ambiguous. Resolve it inside the authoritative
        // RunDO: that transaction either observes the committed hook or fences
        // this exact reservation before its token/ID claims are released.
        let parsedOutcome: ParsedApplyEventOutcome;
        try {
          const wireOutcome: unknown = await stub.applyEvent({
            runId: effectiveRunId,
            data,
            params,
            tokenHolder,
            hookClaimId: hookAdmission?.reservation.claimId,
            cleanup,
          });
          parsedOutcome = parseApplyEventOutcome(wireOutcome);
        } catch (error) {
          if (hookAdmission) {
            let resolution: { committed: boolean };
            try {
              resolution = await stub.resolveHookTokenClaim({
                hookId: hookAdmission.owner.hookId,
                token: hookAdmission.token,
                claimId: hookAdmission.reservation.claimId,
              });
            } catch {
              // Resolution is itself ambiguous, so fail closed and leave both
              // claims for an exact same-reservation retry.
              throw error;
            }
            if (!resolution.committed) {
              await env.WORKFLOW_INDEX.releaseHookReservation(
                hookAdmission.token,
                hookAdmission.owner,
                hookAdmission.reservation,
              );
            }
          }
          throw error;
        }

        if (parsedOutcome.kind !== 'success') {
          if (hookAdmission) {
            await env.WORKFLOW_INDEX.releaseHookReservation(
              hookAdmission.token,
              hookAdmission.owner,
              hookAdmission.reservation,
            );
          }
          throwOutcomeError(parsedOutcome.outcome, effectiveRunId, data);
        }
        const outcome = parsedOutcome.outcome;

        // Derived indexes are deliberately rewritten on idempotent replay so
        // a committed run or hook can repair an interrupted index update.
        if (outcome.run) {
          const meta = JSON.stringify({
            runId: effectiveRunId,
            createdAt: outcome.run.createdAt.toISOString(),
            status: outcome.run.status,
          });
          if (outcome.indexPublicationExpiresAt === undefined) {
            throw new Error('world-celld: authoritative run mutation omitted its index lease');
          }
          await env.WORKFLOW_INDEX.commitRun(outcome.run, meta, outcome.indexPublicationExpiresAt);
        }
        if (outcome.hookToIndex) {
          const serialized = stringify(outcome.hookToIndex);
          await env.WORKFLOW_INDEX.finalizeHookIndexes(
            outcome.hookToIndex.token,
            outcome.hookToIndex.hookId,
            serialized,
            hookOwner(outcome.hookToIndex),
            hookAdmission?.reservation,
          );
        } else if (hookAdmission) {
          await env.WORKFLOW_INDEX.releaseHookReservation(
            hookAdmission.token,
            hookAdmission.owner,
            hookAdmission.reservation,
          );
        }
        if (outcome.releasedHooks.length > 0) {
          await env.WORKFLOW_INDEX.releaseHookIndexes({
            runId: effectiveRunId,
            hooks: outcome.releasedHooks,
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
        const raw = await env.WORKFLOW_INDEX.getHookById(hookId);

        if (!raw) {
          throw new HookNotFoundError(hookId);
        }

        const hook = parseHook(parse<Hook>(raw));
        return filterHookData(hook, params?.resolveData ?? 'all');
      },

      async getByToken(token: string, params?: GetHookParams) {
        const raw = await env.WORKFLOW_INDEX.getHookByToken(token);

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
