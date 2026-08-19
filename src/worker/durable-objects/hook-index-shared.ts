import type { Hook } from '@workflow/world';
import type { HookTokenOwner } from '../../config.js';
import { runFenceCellName, type CellNamespaceLike, type RunFenceStub } from '../../indexes.js';
import { parse } from '../../vendor/shared/index.js';

export interface HookIndexEnv {
  WORKFLOW_RUN_FENCES?: CellNamespaceLike<RunFenceStub>;
}

export function sameOwner(left: HookTokenOwner, right: HookTokenOwner): boolean {
  return left.runId === right.runId && left.hookId === right.hookId;
}

export function ownerFromSerializedHook(raw: string): HookTokenOwner {
  const hook = parse<Hook>(raw);
  return { runId: hook.runId, hookId: hook.hookId };
}

export function indexKey(prefix: string, value: string): string {
  return `${prefix}:${encodeURIComponent(value)}`;
}

/** Compact shard-local fence for every hook owned by one terminal or expired run. */
export function shardRunFenceKey(runId: string): string {
  return `runfence:${encodeURIComponent(runId)}`;
}

export async function runIsActive(env: HookIndexEnv, runId: string): Promise<boolean> {
  const namespace = env.WORKFLOW_RUN_FENCES;
  if (!namespace) throw new Error('world-celld hook index missing WORKFLOW_RUN_FENCES binding');
  const target = namespace.get(namespace.idFromName(runFenceCellName(runId)));
  return (await target.getStatus()) === 'active';
}
