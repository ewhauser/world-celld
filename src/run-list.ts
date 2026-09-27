import { isTerminalWorkflowRunStatus, type WorkflowRun } from '@workflow/world';
import type { IndexListOptions, IndexListPage } from './indexes.js';

/** Bound cross-run fanout so large list pages cannot create an RPC burst. */
export const RUN_LIST_CONCURRENCY = 8;
/** Largest page the fleet-side list accepts, matching the catalog page limit. */
export const MAX_RUN_LIST_LIMIT = 1000;

export type RunStatus = WorkflowRun['status'];

/** Wire shape of a run listing, shared by the client and the worker router. */
export interface RunListRequest {
  workflowName?: string;
  statuses?: RunStatus[];
  limit: number;
  cursor?: string;
  sortOrder?: 'asc' | 'desc';
  /** 'none' strips run input and output from the returned page. */
  resolveData?: 'none' | 'all';
}

export interface RunListPage {
  data: WorkflowRun[];
  cursor: string | null;
  hasMore: boolean;
}

export interface RunListDependencies {
  listRuns(options: IndexListOptions): Promise<IndexListPage>;
  /** The authoritative run, or null when it does not exist or has expired. */
  readRun(runId: string): Promise<WorkflowRun | null>;
}

interface RunIndexMetadata {
  runId: string;
  status: RunStatus;
}

/**
 * Run statuses only move pending -> running -> terminal, and terminal statuses
 * are immutable. An older index value may therefore safely exclude a filter
 * only when it is already terminal, or when the caller asks for pending and
 * the index has advanced beyond pending. Earlier non-terminal metadata cannot
 * exclude a later status because a post-commit index write may need replay.
 */
function indexStatusExcludes(indexed: RunStatus, requested: RunStatus | undefined): boolean {
  if (requested === undefined || indexed === requested) return false;
  return isTerminalWorkflowRunStatus(indexed) || requested === 'pending';
}

export function stripRunData(run: WorkflowRun): WorkflowRun {
  const { input: _input, output: _output, ...rest } = run;
  return rest;
}

/**
 * Page through the run catalog, verifying every candidate against its
 * authoritative run cell. Runs on the client against remote bindings, or in
 * the worker router against fleet bindings.
 */
export async function listRunsPage(
  deps: RunListDependencies,
  request: RunListRequest,
): Promise<RunListPage> {
  const { statuses, limit } = request;
  const prefix = request.workflowName ? `run:${request.workflowName}:` : 'runall:';
  const reverse = request.sortOrder === 'desc';
  const matches: Array<{ key: string; run: WorkflowRun }> = [];
  let scanCursor = request.cursor;
  let exhausted = false;

  // Keep scanning index pages until the requested page is full. This
  // prevents status filters and stale derived entries from producing
  // short pages or cursors that skip matching runs.
  while (matches.length <= limit && !exhausted) {
    const kvList = await deps.listRuns({
      prefix,
      limit: Math.min(MAX_RUN_LIST_LIMIT, Math.max(50, limit)),
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
      // run below. Earlier metadata cannot exclude a later status.
      if (statuses?.every((status) => indexStatusExcludes(metadata.status, status))) {
        continue;
      }
      candidates.push({ key: key.name, metadata });
    }

    // Fetch enough candidates to prove the requested page and hasMore,
    // retaining index order while limiting concurrent cross-run reads.
    for (let offset = 0; offset < candidates.length && matches.length <= limit;) {
      const needed = limit + 1 - matches.length;
      const batch = candidates.slice(offset, offset + Math.min(RUN_LIST_CONCURRENCY, needed));
      const resolved = await Promise.all(
        batch.map(async ({ key, metadata }) => {
          const run = await deps.readRun(metadata.runId);
          return run && (statuses === undefined || statuses.includes(run.status))
            ? { key, run }
            : null;
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
    data: page.map(({ run }) => (request.resolveData === 'none' ? stripRunData(run) : run)),
    cursor: hasMore ? (page.at(-1)?.key ?? null) : null,
    hasMore,
  };
}
