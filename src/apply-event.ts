/**
 * Transactional event-application core shared by the real WorkflowRunDO and
 * the in-memory test mocks.
 *
 * All guard checks, the event append, and the entity mutation happen against a
 * single {@link EventStore}. The Durable Object wraps a call to
 * {@link applyEvent} in `ctx.storage.transaction()`, so the whole operation is
 * atomic; the mocks run it against an in-memory store.
 *
 * Guard semantics are ported from the upstream `@workflow/world-local` /
 * `@workflow/world-postgres` reference implementations (Workflow 5 contract):
 * - run_started on a terminal run -> RunExpiredError
 * - terminal transitions on a terminal run -> EntityConflictError
 * - run_cancelled is idempotent on an already-cancelled run
 * - duplicate step_created / hook_created -> EntityConflictError
 * - step events on a terminal step -> EntityConflictError
 * - step_started before step.retryAfter -> TooEarlyError
 * - error/output/completedAt are cleared whenever a run re-enters a
 *   non-final status (WorkflowRunSchema is a discriminated union)
 *
 * Errors are returned as structured outcomes (never thrown) because custom
 * error classes do not survive the Durable Object RPC boundary with their
 * `name` intact — and `@workflow/errors` matches errors by name via `.is()`.
 * The storage layer converts outcomes back into the typed errors.
 */

import type {
  CreateEventParams,
  CreateEventRequest,
  Event,
  Hook,
  RunCreatedEventRequest,
  Step,
  Wait,
  WorkflowRun,
} from '@workflow/world';
import {
  applyAttributeChanges,
  eventIdToSlot,
  EventSchema,
  getMaxEventsPerRun,
  HookSchema,
  isChildEntityCreationEvent,
  isTerminalStepStatus,
  isTerminalWorkflowRunStatus,
  SPEC_VERSION_CURRENT,
  slotToEventId,
  StepSchema,
  validateAttributeChanges,
  WaitSchema,
  WorkflowRunSchema,
} from '@workflow/world';
import { compact } from './util.js';
import type { ScheduleCleanupRequest } from './retention.js';

/** Storage key for the run entity. */
const RUN_KEY = 'run';
/** Key prefixes for per-entity storage. */
export const EVENT_KEY_PREFIX = 'event:';
export const STEP_KEY_PREFIX = 'step:';
export const HOOK_KEY_PREFIX = 'hook:';
export const WAIT_KEY_PREFIX = 'wait:';
export const STEP_CREATED_KEY_PREFIX = 'stepcreated:';
export const HOOK_CREATED_KEY_PREFIX = 'hookcreated:';
/**
 * Marker recording that a hook_created event was committed for a hookId.
 * Mirrors postgres' `workflow_events_entity_creation_unique` partial index:
 * it outlives hook disposal so a replayed hook_created after dispose is
 * rejected as a duplicate instead of resurrecting the hook.
 */
const HOOK_EVENT_MARKER_PREFIX = 'hookevent:';
/** Durable repair record for idempotent post-commit hook-index release. */
const HOOK_RELEASE_MARKER_PREFIX = 'hookreleased:';
/** Exact ambiguous reservations canceled by a serialized RunDO resolution. */
export const HOOK_CLAIM_CANCELLATION_PREFIX = 'hookclaimcancelled:';
/** Workflow-authored attr_set correlation claims. */
const ATTRIBUTE_EVENT_MARKER_PREFIX = 'attrevent:';
/** Durable lazy-hook resume claims, scoped by the containing run DO. */
const HOOK_RESUME_CLAIM_PREFIX = 'hookresume:';

interface HookResumeClaim {
  hookId: string;
  eventId: string;
  payloadDigest?: string;
}

export interface EventStoreListOptions {
  prefix: string;
  /** Exclusive lower bound (full key). */
  startAfter?: string;
  /** Exclusive upper bound (full key). */
  end?: string;
  limit?: number;
  reverse?: boolean;
}

/**
 * Minimal transactional key-value surface, satisfied by
 * `DurableObjectTransaction` / `DurableObjectStorage` and by the in-memory
 * test-mock store. Keys list in lexicographic order (reversed when
 * `reverse: true`).
 */
export interface EventStore {
  get<T>(key: string): Promise<T | undefined>;
  /** Optional native multi-key read (128 keys per Durable Object call). */
  getMany?<T>(keys: string[]): Promise<Map<string, T>>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<boolean>;
  /** Optional native multi-key delete. */
  deleteMany?(keys: string[]): Promise<number>;
  list<T>(options: EventStoreListOptions): Promise<Map<string, T>>;
  /**
   * Celld-only capability: terminal entity cleanup is persisted and paged by
   * WorkflowRunDO alarms instead of running inside the event transaction.
   */
  deferTerminalCleanup?: boolean;
}

const STORAGE_BATCH_SIZE = 128;
const MAX_ENTITY_PAGE_SIZE = 1000;

async function getMany<T>(store: EventStore, keys: string[]): Promise<Map<string, T>> {
  if (store.getMany) {
    const result = new Map<string, T>();
    for (let offset = 0; offset < keys.length; offset += STORAGE_BATCH_SIZE) {
      const page = await store.getMany<T>(keys.slice(offset, offset + STORAGE_BATCH_SIZE));
      for (const entry of page) result.set(...entry);
    }
    return result;
  }
  const values = await Promise.all(keys.map((key) => store.get<T>(key)));
  return new Map(
    keys.flatMap((key, index) =>
      values[index] === undefined ? [] : ([[key, values[index] as T]] as Array<[string, T]>),
    ),
  );
}

async function deleteMany(store: EventStore, keys: string[]): Promise<number> {
  if (store.deleteMany) {
    let deleted = 0;
    for (let offset = 0; offset < keys.length; offset += STORAGE_BATCH_SIZE) {
      deleted += await store.deleteMany(keys.slice(offset, offset + STORAGE_BATCH_SIZE));
    }
    return deleted;
  }
  const results = await Promise.all(keys.map((key) => store.delete(key)));
  return results.filter(Boolean).length;
}

export interface ApplyEventRequest {
  /** The effective run id (already resolved / generated by the caller). */
  runId: string;
  data: CreateEventRequest | RunCreatedEventRequest;
  /** Workflow 5 event-log position and preload options. */
  params?: CreateEventParams;
  /**
   * Current holder of the hook token per the sharded token-ownership index
   * (hook_created only). `null` means the token is unclaimed.
   */
  tokenHolder?: { runId: string; hookId: string } | null;
  /** Exact token/ID reservation fenced on ambiguous-delivery resolution. */
  hookClaimId?: string;
  /** Internal world-celld retention policy captured with the event. */
  cleanup?: ScheduleCleanupRequest;
}

export type ApplyEventErrorCode =
  | 'RUN_NOT_FOUND'
  | 'STEP_NOT_FOUND'
  | 'HOOK_NOT_FOUND'
  | 'WAIT_NOT_FOUND'
  | 'ENTITY_CONFLICT'
  | 'HOOK_CLAIM_CANCELLED'
  | 'RUN_EXPIRED'
  | 'TOO_EARLY'
  | 'RUN_NOT_SUPPORTED';

export interface ApplyEventFailure {
  ok: false;
  code: ApplyEventErrorCode;
  message: string;
  /** Seconds until retry is allowed (TOO_EARLY only). */
  retryAfterSeconds?: number;
  /** The run's spec version (RUN_NOT_SUPPORTED only). */
  runSpecVersion?: number;
}

export interface ApplyEventSuccess {
  ok: true;
  /** Absent for idempotent replays that record no new event. */
  event?: Event;
  run?: WorkflowRun;
  step?: Step;
  hook?: Hook;
  wait?: Wait;
  /** True when a lazy step_started atomically created its step. */
  stepCreated?: true;
  /** Hooks whose token and ID indexes must be released. */
  releasedHooks: Array<{ hookId: string; token: string }>;
  /** Set when a run entity was created (run_created or resilient bootstrap). */
  runCreated?: { workflowName: string; createdAt: Date };
  /** Set when the sharded hook indexes must be (re)written. */
  hookToIndex?: Hook;
  /** All events (ascending), preloaded for run_started responses. */
  events?: Event[];
  /** Pagination metadata required whenever events are returned. */
  cursor?: string | null;
  hasMore?: boolean;
  /** Server-owned event ceiling advertised on run lifecycle writes. */
  maxEvents?: number;
  /** Deadline for publishing this authoritative mutation to derivative indexes. */
  indexPublicationExpiresAt?: number;
}

export type ApplyEventOutcome = ApplyEventSuccess | ApplyEventFailure;

interface ApplyEventContext extends ApplyEventRequest {
  nextEventId(): string;
  now: Date;
}

function failure(
  code: ApplyEventErrorCode,
  message: string,
  extra?: Pick<ApplyEventFailure, 'retryAfterSeconds' | 'runSpecVersion'>,
): ApplyEventFailure {
  return { ok: false, code, message, ...extra };
}

export function hookClaimCancellationKey(claimId: string): string {
  return `${HOOK_CLAIM_CANCELLATION_PREFIX}${encodeURIComponent(claimId)}`;
}

function readStringProp(value: unknown, key: string): string | undefined {
  if (value !== null && typeof value === 'object') {
    const prop = (value as Record<string, unknown>)[key];
    if (typeof prop === 'string') return prop;
  }
  return undefined;
}

/**
 * Map a failure eventData.error (which may be a string, an Error-shaped
 * object, or anything else) to the structured error stored on entities.
 * Matches upstream: string errors keep their text, errorCode is preserved.
 */
function toStructuredError(
  error: unknown,
  opts?: { stack?: string; code?: string },
): { message: string; stack?: string; code?: string } {
  const message =
    typeof error === 'string' ? error : (readStringProp(error, 'message') ?? 'Unknown error');
  return {
    message,
    stack: opts?.stack ?? readStringProp(error, 'stack'),
    code: opts?.code ?? readStringProp(error, 'code'),
  };
}

/**
 * Shared cursor pagination over a key prefix. Fetches `limit + 1` entries to
 * detect `hasMore` without ever skipping the boundary entry (the cursor is
 * derived from the last *returned* item, not the peeked one).
 */
export async function listByPrefix<T>(
  store: EventStore,
  prefix: string,
  params: { limit: number; cursor?: string; sortOrder?: 'asc' | 'desc' },
  getId: (item: T) => string,
): Promise<{ data: T[]; cursor: string | null; hasMore: boolean }> {
  const { limit, cursor, sortOrder = 'asc' } = params;
  const options: EventStoreListOptions =
    sortOrder === 'desc'
      ? { prefix, reverse: true, limit: limit + 1, end: cursor ? `${prefix}${cursor}` : undefined }
      : { prefix, limit: limit + 1, startAfter: cursor ? `${prefix}${cursor}` : undefined };
  const entries = await store.list<T>(options);
  const values = Array.from(entries.values());
  const hasMore = values.length > limit;
  const data = values.slice(0, limit);
  const last = data.at(-1);
  return {
    data,
    cursor: hasMore && last !== undefined ? getId(last) : null,
    hasMore,
  };
}

function creationIndexKey(prefix: string, createdAt: Date, id: string): string {
  return `${prefix}${createdAt.toISOString()}:${id}`;
}

/**
 * Cursor pagination over an explicit creation-time index. Entity ids are
 * values rather than key suffixes, so caller-supplied ids cannot disturb the
 * requested ordering.
 */
export async function listByCreationTime<T>(
  store: EventStore,
  indexPrefix: string,
  entityPrefix: string,
  params: { limit: number; cursor?: string; sortOrder?: 'asc' | 'desc' },
): Promise<{ data: T[]; cursor: string | null; hasMore: boolean }> {
  const requestedLimit = Number.isFinite(params.limit) ? Math.floor(params.limit) : 1;
  const limit = Math.min(MAX_ENTITY_PAGE_SIZE, Math.max(1, requestedLimit));
  const { cursor, sortOrder = 'asc' } = params;
  const entries = await store.list<string>(
    sortOrder === 'desc'
      ? {
          prefix: indexPrefix,
          reverse: true,
          limit: limit + 1,
          end: cursor ? `${indexPrefix}${cursor}` : undefined,
        }
      : {
          prefix: indexPrefix,
          limit: limit + 1,
          startAfter: cursor ? `${indexPrefix}${cursor}` : undefined,
        },
  );
  const page = Array.from(entries.entries()).slice(0, limit);
  const entityKeys = page.map(([, id]) => `${entityPrefix}${id}`);
  const entities = await getMany<T>(store, entityKeys);
  const data = entityKeys.flatMap((key) => {
    const item = entities.get(key);
    return item === undefined ? [] : [item];
  });
  const hasMore = entries.size > limit;
  const lastKey = page.at(-1)?.[0];
  return {
    data,
    cursor: hasMore && lastKey ? lastKey.slice(indexPrefix.length) : null,
    hasMore,
  };
}

/** Delete all hook entities for the run, returning the released tokens. */
async function releaseAllHooks(
  store: EventStore,
): Promise<Array<{ hookId: string; token: string }>> {
  if (store.deferTerminalCleanup) return [];
  const hooks = await store.list<Hook>({ prefix: HOOK_KEY_PREFIX });
  const released: Array<{ hookId: string; token: string }> = [];
  const keys: string[] = [];
  for (const hook of hooks.values()) {
    keys.push(`${HOOK_KEY_PREFIX}${hook.hookId}`);
    keys.push(creationIndexKey(HOOK_CREATED_KEY_PREFIX, hook.createdAt, hook.hookId));
    released.push({ hookId: hook.hookId, token: hook.token });
  }
  await deleteMany(store, keys);
  return released;
}

/** Delete all persisted waits once their owning run becomes terminal. */
async function releaseAllWaits(store: EventStore): Promise<void> {
  if (store.deferTerminalCleanup) return;
  const waits = await store.list<Wait>({ prefix: WAIT_KEY_PREFIX });
  await deleteMany(store, Array.from(waits.keys()));
}

export async function applyEvent(
  store: EventStore,
  ctx: ApplyEventContext,
): Promise<ApplyEventOutcome> {
  const { runId, data, now } = ctx;
  const specVersion = data.specVersion ?? SPEC_VERSION_CURRENT;

  if (specVersion !== SPEC_VERSION_CURRENT) {
    return failure(
      'RUN_NOT_SUPPORTED',
      `Run "${runId}" uses unsupported specVersion ${specVersion}; this world requires ${SPEC_VERSION_CURRENT}`,
      { runSpecVersion: specVersion },
    );
  }

  /** Build and validate the stored event BEFORE anything is persisted. */
  const buildEvent = (record: Record<string, unknown>): Event => {
    const withMeta: Record<string, unknown> = {
      ...record,
      runId,
      eventId: ctx.nextEventId(),
      createdAt: now,
      occurredAt: ctx.params?.occurredAt,
      resumeId: ctx.params?.resumeId,
      specVersion,
    };
    if (record.eventType !== 'hook_received') {
      delete withMeta.resumeId;
    }
    if (record.eventType === 'run_started') {
      delete withMeta.eventData;
    }
    if (
      record.eventType === 'step_started' &&
      withMeta.eventData !== null &&
      typeof withMeta.eventData === 'object'
    ) {
      const { input: _input, ...eventData } = withMeta.eventData as Record<string, unknown>;
      withMeta.eventData = eventData;
    }
    return EventSchema.parse(compact(withMeta));
  };

  const putEvent = async (event: Event): Promise<void> => {
    await store.put(`${EVENT_KEY_PREFIX}${event.eventId}`, event);
  };

  // ============================================================
  // VALIDATION: current run state (skipped for run_created and for
  // step_completed / step_retrying, matching upstream — those only operate
  // on running steps regardless of run state).
  // ============================================================
  let currentRun: WorkflowRun | undefined;
  const skipRunValidation =
    data.eventType === 'run_created' ||
    data.eventType === 'step_completed' ||
    data.eventType === 'step_retrying';
  if (!skipRunValidation) {
    currentRun = await store.get<WorkflowRun>(RUN_KEY);
  }

  // ============================================================
  // RESILIENT START: run_started on a non-existent run bootstraps the run
  // from the message's runInput (spec >= 3 queue transport).
  // ============================================================
  let bootstrapped: ApplyEventSuccess['runCreated'];
  if (data.eventType === 'run_started' && !currentRun && data.eventData) {
    const {
      deploymentId,
      workflowName,
      input,
      executionContext,
      attributes,
      allowReservedAttributes,
      encryptionPublicKey,
    } = data.eventData;
    if (deploymentId && workflowName && input !== undefined) {
      const attributeChanges = Object.entries(attributes ?? {}).map(([key, value]) => ({
        key,
        value,
      }));
      validateAttributeChanges(attributeChanges, { allowReservedAttributes });
      const createdRun = WorkflowRunSchema.parse(
        compact({
          runId,
          deploymentId,
          workflowName,
          specVersion,
          executionContext,
          attributes,
          encryptionPublicKey,
          input,
          status: 'pending',
          output: undefined,
          error: undefined,
          startedAt: undefined,
          completedAt: undefined,
          createdAt: now,
          updatedAt: now,
        }),
      );
      const runCreatedEvent = buildEvent({
        eventType: 'run_created',
        eventData: {
          deploymentId,
          workflowName,
          input,
          executionContext,
          attributes,
          allowReservedAttributes,
          encryptionPublicKey,
        },
      });
      await store.put(RUN_KEY, createdRun);
      await putEvent(runCreatedEvent);
      currentRun = createdRun;
      bootstrapped = { workflowName, createdAt: now };
    }
  }

  // ============================================================
  // WORKFLOW 5 PROTOCOL BOUNDARY
  // ============================================================
  if (currentRun && currentRun.specVersion !== SPEC_VERSION_CURRENT) {
    return failure(
      'RUN_NOT_SUPPORTED',
      `Run "${runId}" uses unsupported specVersion ${currentRun.specVersion}; this world requires ${SPEC_VERSION_CURRENT}`,
      { runSpecVersion: currentRun.specVersion },
    );
  }

  // ============================================================
  // GUARDS: terminal run state
  // ============================================================
  if (currentRun && isTerminalWorkflowRunStatus(currentRun.status)) {
    // Idempotent: run_cancelled on an already-cancelled run records the event
    // and returns the current state.
    if (data.eventType === 'run_cancelled' && currentRun.status === 'cancelled') {
      const event = buildEvent({ ...data });
      await putEvent(event);
      return { ok: true, event, run: currentRun, releasedHooks: [] };
    }
    if (data.eventType === 'run_started') {
      return failure(
        'RUN_EXPIRED',
        `Workflow run "${runId}" is already in terminal state "${currentRun.status}"`,
      );
    }
    if (
      data.eventType === 'run_completed' ||
      data.eventType === 'run_failed' ||
      data.eventType === 'run_cancelled'
    ) {
      return failure(
        'ENTITY_CONFLICT',
        `Cannot transition run from terminal state "${currentRun.status}"`,
      );
    }
    if (isChildEntityCreationEvent(data)) {
      return failure(
        'ENTITY_CONFLICT',
        `Cannot create new entities on run in terminal state "${currentRun.status}"`,
      );
    }
    if (data.eventType === 'attr_set') {
      return failure(
        'ENTITY_CONFLICT',
        `Cannot set attributes on run in terminal state "${currentRun.status}"`,
      );
    }
  }

  // ============================================================
  // GUARDS: step ordering and terminal state
  // ============================================================
  let validatedStep: Step | undefined;
  if (
    data.eventType === 'step_started' ||
    data.eventType === 'step_completed' ||
    data.eventType === 'step_failed' ||
    data.eventType === 'step_retrying'
  ) {
    const lazyStepStart = data.eventType === 'step_started' && isChildEntityCreationEvent(data);
    validatedStep = await store.get<Step>(`${STEP_KEY_PREFIX}${data.correlationId}`);
    if (!validatedStep) {
      if (!lazyStepStart) {
        return failure('STEP_NOT_FOUND', `Step "${data.correlationId}" not found`);
      }
    } else if (lazyStepStart) {
      return failure('ENTITY_CONFLICT', `Step "${data.correlationId}" already exists`);
    }
    if (validatedStep && isTerminalStepStatus(validatedStep.status)) {
      return failure(
        'ENTITY_CONFLICT',
        `Cannot modify step in terminal state "${validatedStep.status}"`,
      );
    }
    // On terminal runs, only in-flight steps may still record progress.
    if (
      currentRun &&
      isTerminalWorkflowRunStatus(currentRun.status) &&
      validatedStep &&
      validatedStep.status !== 'running'
    ) {
      return failure(
        'RUN_EXPIRED',
        `Cannot modify non-running step on run in terminal state "${currentRun.status}"`,
      );
    }
  }

  // ============================================================
  // GUARDS: hook event ordering
  // ============================================================
  if (data.eventType === 'hook_received' && ctx.params?.resumeId) {
    const claim = await store.get<HookResumeClaim>(
      `${HOOK_RESUME_CLAIM_PREFIX}${ctx.params.resumeId}`,
    );
    if (claim) {
      if (claim.hookId !== data.correlationId) {
        return failure(
          'ENTITY_CONFLICT',
          `hook_received resumeId "${ctx.params.resumeId}" already recorded for a different hook`,
        );
      }
      if (
        claim.payloadDigest &&
        ctx.params.resumePayloadDigest &&
        claim.payloadDigest !== ctx.params.resumePayloadDigest
      ) {
        return failure(
          'ENTITY_CONFLICT',
          `hook_received resumeId "${ctx.params.resumeId}" already recorded with a different payload`,
        );
      }
      const committed = await store.get<Event>(`${EVENT_KEY_PREFIX}${claim.eventId}`);
      if (
        committed?.eventType === 'hook_received' &&
        committed.correlationId === claim.hookId &&
        committed.resumeId === ctx.params.resumeId
      ) {
        return { ok: true, event: committed, releasedHooks: [] };
      }
      if (committed) {
        return failure(
          'ENTITY_CONFLICT',
          `hook_received resumeId "${ctx.params.resumeId}" points to a different event`,
        );
      }
    }
  }
  if (
    (data.eventType === 'hook_disposed' || data.eventType === 'hook_received') &&
    data.correlationId
  ) {
    const existingHook = await store.get<Hook>(`${HOOK_KEY_PREFIX}${data.correlationId}`);
    if (!existingHook) {
      if (data.eventType === 'hook_disposed') {
        const released = await store.get<{ hookId: string; token: string }>(
          `${HOOK_RELEASE_MARKER_PREFIX}${data.correlationId}`,
        );
        if (released) return { ok: true, releasedHooks: [released] };
      }
      return failure('HOOK_NOT_FOUND', `Hook "${data.correlationId}" not found`);
    }
  }

  // Non-run_created events require the run to exist (after the resilient
  // start bootstrap had its chance). step_completed / step_retrying skip the
  // run read entirely — their step guard above already proves the run exists.
  if (!skipRunValidation && data.eventType !== 'run_started' && !currentRun) {
    return failure('RUN_NOT_FOUND', `Workflow run "${runId}" not found`);
  }

  // ============================================================
  // EVENT + ENTITY WRITES (single transaction)
  // ============================================================
  switch (data.eventType) {
    case 'run_created': {
      const existing = await store.get<WorkflowRun>(RUN_KEY);
      if (existing) {
        // Idempotent replay: return the existing run without appending a
        // duplicate run_created event to the log.
        return { ok: true, run: existing, releasedHooks: [] };
      }
      const attributeChanges = Object.entries(data.eventData.attributes ?? {}).map(
        ([key, value]) => ({ key, value }),
      );
      validateAttributeChanges(attributeChanges, {
        allowReservedAttributes: data.eventData.allowReservedAttributes,
      });
      const run = WorkflowRunSchema.parse(
        compact({
          runId,
          deploymentId: data.eventData.deploymentId,
          workflowName: data.eventData.workflowName,
          specVersion,
          executionContext: data.eventData.executionContext,
          attributes: data.eventData.attributes,
          encryptionPublicKey: data.eventData.encryptionPublicKey,
          input: data.eventData.input,
          status: 'pending',
          output: undefined,
          error: undefined,
          startedAt: undefined,
          completedAt: undefined,
          createdAt: now,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(RUN_KEY, run);
      await putEvent(event);
      return {
        ok: true,
        event,
        run,
        releasedHooks: [],
        runCreated: { workflowName: data.eventData.workflowName, createdAt: now },
      };
    }

    case 'run_started': {
      if (!currentRun) {
        return failure('RUN_NOT_FOUND', `Workflow run "${runId}" not found`);
      }
      // Idempotent for concurrent invocations / queue redeliveries: if the
      // run is already running this is a replay — no duplicate event.
      if (currentRun.status === 'running') {
        const result: ApplyEventSuccess = {
          ok: true,
          run: currentRun,
          releasedHooks: [],
          maxEvents: getMaxEventsPerRun(),
        };
        if (ctx.params?.skipPreload) return result;
        const allEvents = await listByPrefix<Event>(
          store,
          EVENT_KEY_PREFIX,
          { limit: getMaxEventsPerRun(), sortOrder: 'asc' },
          (event) => event.eventId,
        );
        return {
          ...result,
          events: allEvents.data,
          cursor: allEvents.cursor,
          hasMore: allEvents.hasMore,
        };
      }
      const run = WorkflowRunSchema.parse(
        compact({
          ...currentRun,
          status: 'running',
          output: undefined,
          error: undefined,
          completedAt: undefined,
          startedAt: currentRun.startedAt ?? now,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(RUN_KEY, run);
      await putEvent(event);
      // Preload all events so the runtime can skip the initial events.list.
      const result: ApplyEventSuccess = {
        ok: true,
        event,
        run,
        releasedHooks: [],
        runCreated: bootstrapped,
        maxEvents: getMaxEventsPerRun(),
      };
      if (ctx.params?.skipPreload) return result;
      const allEvents = await listByPrefix<Event>(
        store,
        EVENT_KEY_PREFIX,
        { limit: getMaxEventsPerRun(), sortOrder: 'asc' },
        (e) => e.eventId,
      );
      return {
        ...result,
        events: allEvents.data,
        cursor: allEvents.cursor,
        hasMore: allEvents.hasMore,
      };
    }

    case 'run_completed': {
      if (!currentRun) {
        return failure('RUN_NOT_FOUND', `Workflow run "${runId}" not found`);
      }
      const run = WorkflowRunSchema.parse(
        compact({
          ...currentRun,
          status: 'completed',
          output: data.eventData.output,
          error: undefined,
          completedAt: now,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(RUN_KEY, run);
      await putEvent(event);
      const releasedHooks = await releaseAllHooks(store);
      await releaseAllWaits(store);
      return { ok: true, event, run, releasedHooks };
    }

    case 'run_failed': {
      if (!currentRun) {
        return failure('RUN_NOT_FOUND', `Workflow run "${runId}" not found`);
      }
      const run = WorkflowRunSchema.parse(
        compact({
          ...currentRun,
          status: 'failed',
          output: undefined,
          error: toStructuredError(data.eventData.error, {
            code: data.eventData.errorCode ?? readStringProp(data.eventData.error, 'code'),
          }),
          completedAt: now,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(RUN_KEY, run);
      await putEvent(event);
      const releasedHooks = await releaseAllHooks(store);
      await releaseAllWaits(store);
      return { ok: true, event, run, releasedHooks };
    }

    case 'run_cancelled': {
      if (!currentRun) {
        return failure('RUN_NOT_FOUND', `Workflow run "${runId}" not found`);
      }
      const run = WorkflowRunSchema.parse(
        compact({
          ...currentRun,
          status: 'cancelled',
          output: undefined,
          error: undefined,
          completedAt: now,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(RUN_KEY, run);
      await putEvent(event);
      const releasedHooks = await releaseAllHooks(store);
      await releaseAllWaits(store);
      return { ok: true, event, run, releasedHooks };
    }

    case 'attr_set': {
      if (!currentRun) {
        return failure('RUN_NOT_FOUND', `Workflow run "${runId}" not found`);
      }
      validateAttributeChanges(data.eventData.changes, {
        existingKeys: Object.keys(currentRun.attributes),
        allowReservedAttributes: data.eventData.allowReservedAttributes,
      });
      const markerKey =
        data.correlationId && data.eventData.writer.type === 'workflow'
          ? `${ATTRIBUTE_EVENT_MARKER_PREFIX}${data.correlationId}`
          : undefined;
      if (markerKey && (await store.get<string>(markerKey)) !== undefined) {
        return failure('ENTITY_CONFLICT', `Attribute event "${data.correlationId}" already exists`);
      }
      const run = WorkflowRunSchema.parse(
        compact({
          ...currentRun,
          attributes: applyAttributeChanges(currentRun.attributes, data.eventData.changes),
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(RUN_KEY, run);
      if (markerKey) await store.put(markerKey, event.eventId);
      await putEvent(event);
      return { ok: true, event, run, releasedHooks: [] };
    }

    case 'step_created': {
      const stepKey = `${STEP_KEY_PREFIX}${data.correlationId}`;
      const existing = await store.get<Step>(stepKey);
      if (existing) {
        // Core catches EntityConflictError for exactly this replay case
        // ("Step already exists, continuing").
        return failure('ENTITY_CONFLICT', `Step "${data.correlationId}" already exists`);
      }
      const step = StepSchema.parse(
        compact({
          runId,
          stepId: data.correlationId,
          stepName: data.eventData.stepName,
          status: 'pending',
          input: data.eventData.input,
          output: undefined,
          error: undefined,
          attempt: 0,
          startedAt: undefined,
          completedAt: undefined,
          retryAfter: undefined,
          createdAt: now,
          updatedAt: now,
          specVersion,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(stepKey, step);
      await store.put(
        creationIndexKey(STEP_CREATED_KEY_PREFIX, step.createdAt, step.stepId),
        step.stepId,
      );
      await putEvent(event);
      return { ok: true, event, step, releasedHooks: [] };
    }

    case 'step_started': {
      let step = validatedStep;
      let stepCreated = false;
      if (!step && data.eventData?.input !== undefined && data.eventData.stepName) {
        step = StepSchema.parse(
          compact({
            runId,
            stepId: data.correlationId,
            stepName: data.eventData.stepName,
            status: 'pending',
            input: data.eventData.input,
            attempt: 0,
            createdAt: now,
            updatedAt: now,
            specVersion,
          }),
        );
        const createdEvent = buildEvent({
          eventType: 'step_created',
          correlationId: data.correlationId,
          eventData: {
            stepName: data.eventData.stepName,
            workflowName: data.eventData.workflowName,
            input: data.eventData.input,
          },
        });
        await store.put(`${STEP_KEY_PREFIX}${data.correlationId}`, step);
        await store.put(
          creationIndexKey(STEP_CREATED_KEY_PREFIX, step.createdAt, step.stepId),
          step.stepId,
        );
        await putEvent(createdEvent);
        stepCreated = true;
      }
      if (!step) {
        return failure('STEP_NOT_FOUND', `Step "${data.correlationId}" not found`);
      }
      if (step.retryAfter && step.retryAfter.getTime() > now.getTime()) {
        return failure(
          'TOO_EARLY',
          `Cannot start step "${data.correlationId}": retryAfter timestamp has not been reached yet`,
          {
            retryAfterSeconds: Math.ceil((step.retryAfter.getTime() - now.getTime()) / 1000),
          },
        );
      }
      const updated = StepSchema.parse(
        compact({
          ...step,
          status: 'running',
          // Only set startedAt on the first start; increment attempt each start.
          startedAt: step.startedAt ?? now,
          attempt: step.attempt + 1,
          retryAfter: undefined,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(`${STEP_KEY_PREFIX}${data.correlationId}`, updated);
      await putEvent(event);
      return {
        ok: true,
        event,
        step: updated,
        releasedHooks: [],
        ...(stepCreated ? { stepCreated: true } : {}),
      };
    }

    case 'step_completed': {
      const step = validatedStep;
      if (!step) {
        return failure('STEP_NOT_FOUND', `Step "${data.correlationId}" not found`);
      }
      const updated = StepSchema.parse(
        compact({
          ...step,
          status: 'completed',
          output: data.eventData.result,
          completedAt: now,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(`${STEP_KEY_PREFIX}${data.correlationId}`, updated);
      await putEvent(event);
      return { ok: true, event, step: updated, releasedHooks: [] };
    }

    case 'step_failed': {
      const step = validatedStep;
      if (!step) {
        return failure('STEP_NOT_FOUND', `Step "${data.correlationId}" not found`);
      }
      const updated = StepSchema.parse(
        compact({
          ...step,
          status: 'failed',
          error: toStructuredError(data.eventData.error),
          completedAt: now,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(`${STEP_KEY_PREFIX}${data.correlationId}`, updated);
      await putEvent(event);
      return { ok: true, event, step: updated, releasedHooks: [] };
    }

    case 'step_retrying': {
      const step = validatedStep;
      if (!step) {
        return failure('STEP_NOT_FOUND', `Step "${data.correlationId}" not found`);
      }
      const updated = StepSchema.parse(
        compact({
          ...step,
          status: 'pending',
          error: toStructuredError(data.eventData.error),
          retryAfter: data.eventData.retryAfter ? new Date(data.eventData.retryAfter) : undefined,
          updatedAt: now,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(`${STEP_KEY_PREFIX}${data.correlationId}`, updated);
      await putEvent(event);
      return { ok: true, event, step: updated, releasedHooks: [] };
    }

    case 'hook_created': {
      const hookId = data.correlationId;
      const token = data.eventData.token;
      const holder = ctx.tokenHolder ?? null;
      const markerKey = `${HOOK_EVENT_MARKER_PREFIX}${hookId}`;
      const hookKey = `${HOOK_KEY_PREFIX}${hookId}`;

      if (
        ctx.hookClaimId &&
        (await store.get(hookClaimCancellationKey(ctx.hookClaimId))) !== undefined
      ) {
        return failure(
          'HOOK_CLAIM_CANCELLED',
          `Hook reservation for "${hookId}" was canceled before commit`,
        );
      }

      if (holder && (holder.runId !== runId || holder.hookId !== hookId)) {
        // A different (runId, hookId) holds this token. Record a
        // hook_conflict event carrying conflictingRunId so the claiming
        // workflow fails deterministically when the hook is awaited. The
        // legitimate holder's KV index entry is left untouched.
        const event = buildEvent({
          eventType: 'hook_conflict',
          correlationId: hookId,
          eventData: { token, conflictingRunId: holder.runId },
        });
        await putEvent(event);
        return { ok: true, event, releasedHooks: [] };
      }

      const existingMarker = await store.get<string>(markerKey);
      if (existingMarker !== undefined) {
        const existingHook = await store.get<Hook>(hookKey);
        if (existingHook && !holder) {
          // Crash orphan: hook + event committed but one or both sharded
          // index writes were lost. Complete the partial write by re-indexing.
          return { ok: true, hook: existingHook, hookToIndex: existingHook, releasedHooks: [] };
        }
        // Fully-committed duplicate (or re-create after disposal): reject so
        // the runtime's concurrent-replay catch path can swallow it.
        return failure('ENTITY_CONFLICT', `Hook "${hookId}" already created`);
      }

      const hook = HookSchema.parse(
        compact({
          runId,
          hookId,
          token,
          metadata: data.eventData.metadata,
          ownerId: '',
          projectId: '',
          environment: '',
          createdAt: now,
          specVersion,
          isWebhook: data.eventData.isWebhook ?? false,
          isSystem: data.eventData.isSystem,
          tokenRetentionUntil: data.eventData.tokenRetentionUntil,
        }),
      );
      const event = buildEvent({ ...data });
      await store.put(hookKey, hook);
      await store.put(
        creationIndexKey(HOOK_CREATED_KEY_PREFIX, hook.createdAt, hook.hookId),
        hook.hookId,
      );
      await store.put(markerKey, event.eventId);
      await putEvent(event);
      return { ok: true, event, hook, hookToIndex: hook, releasedHooks: [] };
    }

    case 'hook_disposed': {
      const hookKey = `${HOOK_KEY_PREFIX}${data.correlationId}`;
      // Existence was validated above; read again for the token.
      const hook = await store.get<Hook>(hookKey);
      if (!hook) {
        return failure('HOOK_NOT_FOUND', `Hook "${data.correlationId}" not found`);
      }
      await store.delete(hookKey);
      await store.delete(creationIndexKey(HOOK_CREATED_KEY_PREFIX, hook.createdAt, hook.hookId));
      await store.put(`${HOOK_RELEASE_MARKER_PREFIX}${hook.hookId}`, {
        hookId: hook.hookId,
        token: hook.token,
      });
      const event = buildEvent({ ...data });
      await putEvent(event);
      return {
        ok: true,
        event,
        releasedHooks: [{ hookId: hook.hookId, token: hook.token }],
      };
    }

    case 'wait_created': {
      const waitKey = `${WAIT_KEY_PREFIX}${data.correlationId}`;
      if ((await store.get<Wait>(waitKey)) !== undefined) {
        return failure('ENTITY_CONFLICT', `Wait "${data.correlationId}" already exists`);
      }
      const wait = WaitSchema.parse({
        waitId: `${runId}-${data.correlationId}`,
        runId,
        status: 'waiting',
        resumeAt: data.eventData.resumeAt,
        createdAt: now,
        updatedAt: now,
        specVersion,
      });
      const event = buildEvent({ ...data });
      await store.put(waitKey, wait);
      await putEvent(event);
      return { ok: true, event, wait, releasedHooks: [] };
    }

    case 'wait_completed': {
      const waitKey = `${WAIT_KEY_PREFIX}${data.correlationId}`;
      const existing = await store.get<Wait>(waitKey);
      if (!existing) {
        return failure('WAIT_NOT_FOUND', `Wait "${data.correlationId}" not found`);
      }
      if (existing.status === 'completed') {
        return failure('ENTITY_CONFLICT', `Wait "${data.correlationId}" already completed`);
      }
      const event = buildEvent({ ...data });
      const wait = WaitSchema.parse({
        ...existing,
        status: 'completed',
        resumeAt: data.eventData?.resumeAt ?? existing.resumeAt,
        completedAt: now,
        updatedAt: now,
      });
      await store.put(waitKey, wait);
      await putEvent(event);
      return { ok: true, event, wait, releasedHooks: [] };
    }

    // hook_received and hook_conflict are event-only at the entity level.
    default: {
      const event = buildEvent({ ...data });
      await putEvent(event);
      if (data.eventType === 'hook_received' && ctx.params?.resumeId) {
        await store.put<HookResumeClaim>(`${HOOK_RESUME_CLAIM_PREFIX}${ctx.params.resumeId}`, {
          hookId: data.correlationId,
          eventId: event.eventId,
          ...(ctx.params.resumePayloadDigest
            ? { payloadDigest: ctx.params.resumePayloadDigest }
            : {}),
        });
      }
      return { ok: true, event, releasedHooks: [] };
    }
  }
}

/**
 * Complete Workflow 5's event-page response after an atomic apply. A celld
 * run is a single serialized writer, so the committed sequence is dense; a
 * stale eventCount therefore needs only the slots between the requested
 * position and the event this call committed.
 */
export async function finalizeEventPage(
  store: EventStore,
  outcome: ApplyEventOutcome,
  params?: CreateEventParams,
): Promise<ApplyEventOutcome> {
  if (!outcome.ok) return outcome;
  const result: ApplyEventSuccess =
    outcome.run && outcome.maxEvents === undefined
      ? { ...outcome, maxEvents: getMaxEventsPerRun() }
      : outcome;
  if (!result.event) return result;

  if (typeof params?.sinceCursor === 'string') {
    const page = await listByPrefix<Event>(
      store,
      EVENT_KEY_PREFIX,
      { limit: 100, cursor: params.sinceCursor, sortOrder: 'asc' },
      (event) => event.eventId,
    );
    return { ...result, events: page.data, cursor: page.cursor, hasMore: page.hasMore };
  }

  if (params?.eventCount === undefined) return result;
  const committedSlot = eventIdToSlot(result.event.eventId);
  if (committedSlot === null || committedSlot <= params.eventCount + 1) return result;

  const skippedCount = committedSlot - params.eventCount - 1;
  const page = await listByPrefix<Event>(
    store,
    EVENT_KEY_PREFIX,
    {
      limit: skippedCount,
      cursor: params.eventCount > 0 ? slotToEventId(params.eventCount) : undefined,
      sortOrder: 'asc',
    },
    (event) => event.eventId,
  );
  const events = page.data.filter((event) => event.eventId < result.event!.eventId);
  return {
    ...result,
    events,
    cursor: null,
    hasMore: events.length < skippedCount,
  };
}
