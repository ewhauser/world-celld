import { HookSchema, WorkflowRunSchema, type WorkflowRun } from '@workflow/world';
import type { HookTokenOwner } from '../config.js';
import type { HookReservation, IndexListOptions } from '../indexes.js';
import type { ExpireRunIndexesRequest, ReleaseHookIndexesRequest } from '../retention.js';
import { isRecord } from '../validation.js';
import { parse } from '../vendor/shared/index.js';

export type IndexOperation =
  | 'runs.list'
  | 'runs.commit'
  | 'runs.expire'
  | 'hooks.reserve'
  | 'hooks.finalize'
  | 'hooks.release-reservation'
  | 'hooks.release';

export type ValidatedIndexRequest =
  | { operation: 'runs.list'; args: [IndexListOptions | undefined] }
  | { operation: 'runs.commit'; args: [WorkflowRun, string, number] }
  | { operation: 'runs.expire'; args: [ExpireRunIndexesRequest] }
  | { operation: 'hooks.reserve'; args: [string, HookTokenOwner] }
  | {
      operation: 'hooks.finalize';
      args: [string, string, string, HookTokenOwner, HookReservation | undefined];
    }
  | {
      operation: 'hooks.release-reservation';
      args: [string, HookTokenOwner, HookReservation];
    }
  | { operation: 'hooks.release'; args: [ReleaseHookIndexesRequest] };

function invalid(message: string): never {
  const error = new Error(`invalid index RPC arguments: ${message}`);
  error.name = 'RequestValidationError';
  throw error;
}

function exactArgs(args: unknown[], count: number): void {
  if (args.length !== count) invalid(`expected ${count} argument(s)`);
}

function nonEmptyString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.length === 0)
    invalid(`${name} must be a non-empty string`);
  return value;
}

function stringValue(value: unknown, name: string): string {
  if (typeof value !== 'string') invalid(`${name} must be a string`);
  return value;
}

function nonNegativeSafeInteger(value: unknown, name: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    invalid(`${name} must be a non-negative safe integer`);
  }
  return value as number;
}

function positiveSafeInteger(value: unknown, name: string): number {
  const parsed = nonNegativeSafeInteger(value, name);
  if (parsed === 0) invalid(`${name} must be a positive safe integer`);
  return parsed;
}

function owner(value: unknown, name = 'owner'): HookTokenOwner {
  if (!isRecord(value)) invalid(`${name} must be an object`);
  return {
    runId: nonEmptyString(value.runId, `${name}.runId`),
    hookId: nonEmptyString(value.hookId, `${name}.hookId`),
  };
}

function reservation(value: unknown): HookReservation {
  if (!isRecord(value)) invalid('reservation must be an object');
  const parsed: HookReservation = {
    claimId: nonEmptyString(value.claimId, 'reservation.claimId'),
  };
  if (value.tokenClaimId !== undefined) {
    parsed.tokenClaimId = nonEmptyString(value.tokenClaimId, 'reservation.tokenClaimId');
  }
  if (value.hookIdClaimId !== undefined) {
    parsed.hookIdClaimId = nonEmptyString(value.hookIdClaimId, 'reservation.hookIdClaimId');
  }
  return parsed;
}

function hooks(value: unknown): Array<{ hookId: string; token: string }> {
  if (!Array.isArray(value)) invalid('hooks must be an array');
  return value.map((entry, index) => {
    if (!isRecord(entry)) invalid(`hooks[${index}] must be an object`);
    return {
      hookId: nonEmptyString(entry.hookId, `hooks[${index}].hookId`),
      token: nonEmptyString(entry.token, `hooks[${index}].token`),
    };
  });
}

function exactProperties(value: Record<string, unknown>, allowed: string[], name: string): void {
  const unexpected = Object.keys(value).find((key) => !allowed.includes(key));
  if (unexpected !== undefined) invalid(`${name}.${unexpected} is not allowed`);
}

function releaseRequest(value: unknown): ReleaseHookIndexesRequest {
  if (!isRecord(value)) invalid('release request must be an object');
  return {
    runId: nonEmptyString(value.runId, 'request.runId'),
    hooks: hooks(value.hooks),
  };
}

function expireRequest(value: unknown): ExpireRunIndexesRequest {
  if (!isRecord(value)) invalid('expiry request must be an object');
  exactProperties(value, ['runId', 'hooks', 'expiredAt'], 'request');
  return {
    runId: nonEmptyString(value.runId, 'request.runId'),
    hooks: hooks(value.hooks),
    expiredAt: nonNegativeSafeInteger(value.expiredAt, 'request.expiredAt'),
  };
}

function listOptions(value: unknown): IndexListOptions | undefined {
  if (value === undefined || value === null) return undefined;
  if (!isRecord(value)) invalid('list options must be an object');
  const parsed: IndexListOptions = {};
  for (const name of ['prefix', 'cursor', 'end'] as const) {
    if (value[name] !== undefined) parsed[name] = stringValue(value[name], `options.${name}`);
  }
  if (value.limit !== undefined) {
    const limit = value.limit;
    if (!Number.isSafeInteger(limit) || (limit as number) < 1 || (limit as number) > 1000) {
      invalid('options.limit must be an integer between 1 and 1000');
    }
    parsed.limit = limit as number;
  }
  if (value.reverse !== undefined) {
    if (typeof value.reverse !== 'boolean') invalid('options.reverse must be a boolean');
    parsed.reverse = value.reverse;
  }
  return parsed;
}

function runCommit(args: unknown[]): ValidatedIndexRequest {
  exactArgs(args, 3);
  const parsedRun = WorkflowRunSchema.safeParse(args[0]);
  if (!parsedRun.success) invalid('run does not match WorkflowRunSchema');
  const run = parsedRun.data;
  nonEmptyString(run.runId, 'run.runId');
  nonEmptyString(run.workflowName, 'run.workflowName');
  nonEmptyString(run.deploymentId, 'run.deploymentId');
  const serializedMetadata = nonEmptyString(args[1], 'serializedMetadata');
  let metadata: unknown;
  try {
    metadata = JSON.parse(serializedMetadata);
  } catch {
    invalid('serializedMetadata must be valid JSON');
  }
  if (
    !isRecord(metadata) ||
    metadata.runId !== run.runId ||
    metadata.status !== run.status ||
    metadata.createdAt !== run.createdAt.toISOString()
  ) {
    invalid('serializedMetadata must match the committed run');
  }
  const publicationExpiresAt = positiveSafeInteger(args[2], 'publicationExpiresAt');
  return { operation: 'runs.commit', args: [run, serializedMetadata, publicationExpiresAt] };
}

function hookFinalize(args: unknown[]): ValidatedIndexRequest {
  if (args.length !== 4 && args.length !== 5) invalid('expected 4 or 5 arguments');
  const token = nonEmptyString(args[0], 'token');
  const hookId = nonEmptyString(args[1], 'hookId');
  const serializedHook = nonEmptyString(args[2], 'serializedHook');
  const hookOwner = owner(args[3]);
  if (hookOwner.hookId !== hookId) invalid('owner.hookId must match hookId');
  let decodedHook: unknown;
  try {
    decodedHook = parse<unknown>(serializedHook);
  } catch {
    invalid('serializedHook must be valid tagged JSON');
  }
  const parsedHook = HookSchema.safeParse(decodedHook);
  if (!parsedHook.success) invalid('serializedHook does not match HookSchema');
  if (
    parsedHook.data.runId !== hookOwner.runId ||
    parsedHook.data.hookId !== hookId ||
    parsedHook.data.token !== token ||
    !parsedHook.data.runId ||
    !parsedHook.data.hookId ||
    !parsedHook.data.token
  ) {
    invalid('serializedHook must match token, hookId, and owner');
  }
  const hookReservation =
    args[4] === undefined || args[4] === null ? undefined : reservation(args[4]);
  return {
    operation: 'hooks.finalize',
    args: [token, hookId, serializedHook, hookOwner, hookReservation],
  };
}

export function validateIndexRequest(
  operation: IndexOperation,
  args: unknown[],
): ValidatedIndexRequest {
  switch (operation) {
    case 'runs.list':
      if (args.length > 1) invalid('expected at most 1 argument');
      return { operation, args: [listOptions(args[0])] };
    case 'runs.commit':
      return runCommit(args);
    case 'runs.expire':
      exactArgs(args, 1);
      return { operation, args: [expireRequest(args[0])] };
    case 'hooks.reserve':
      exactArgs(args, 2);
      return {
        operation,
        args: [nonEmptyString(args[0], 'token'), owner(args[1])],
      };
    case 'hooks.finalize':
      return hookFinalize(args);
    case 'hooks.release-reservation':
      exactArgs(args, 3);
      return {
        operation,
        args: [nonEmptyString(args[0], 'token'), owner(args[1]), reservation(args[2])],
      };
    case 'hooks.release':
      exactArgs(args, 1);
      return { operation, args: [releaseRequest(args[0])] };
    default:
      return invalid('unknown operation');
  }
}

export const INDEX_OPERATIONS = new Set<IndexOperation>([
  'runs.list',
  'runs.commit',
  'runs.expire',
  'hooks.reserve',
  'hooks.finalize',
  'hooks.release-reservation',
  'hooks.release',
]);
