import type { Hook } from '@workflow/world';
import type { HookTokenOwner } from '../../config.js';
import type { CellNamespaceLike } from '../../indexes.js';
import type { RunLifecycleStatus } from '../../retention.js';
import { parse } from '../../vendor/shared/index.js';

interface RunLifecycleStub {
  getLifecycleStatus(): Promise<RunLifecycleStatus>;
}

export interface HookIndexEnv {
  WORKFLOW_DB?: CellNamespaceLike<RunLifecycleStub>;
  /** Test seam; celld deployments use Date.now(). */
  clock?: () => number;
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

export async function runIsActive(env: HookIndexEnv, runId: string): Promise<boolean> {
  const namespace = env.WORKFLOW_DB;
  if (!namespace) throw new Error('world-celld hook index missing WORKFLOW_DB binding');
  const target = namespace.get(namespace.idFromName(runId));
  return (await target.getLifecycleStatus()) === 'active';
}
