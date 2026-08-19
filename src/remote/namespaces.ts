/**
 * HTTP-backed implementations of the structural namespace interfaces the
 * vendored storage/streamer/queue layers consume. `get(id)` returns a stub
 * whose method calls become `POST /v1/rpc/{binding}/{name}/{method}` against
 * the fleet — the celld stand-in for Cloudflare's `env` bindings.
 */
import type { CelldWorldEnv } from '../config.js';
import {
  createWorkflowIndex,
  type HookIdShardStub,
  type HookTokenShardStub,
  type RunCatalogShardStub,
  type RunFenceStub,
} from '../indexes.js';
import type { QueueCellStub } from '../queue.js';
import type { WorkflowRunDOStub } from '../storage.js';
import type { StreamDOStub } from '../streamer.js';
import { callDO, callFleetRoute, type RpcTransport } from './rpc-client.js';
import { readStreamChunks, writeStreamChunks } from './stream-client.js';

interface MethodSpec {
  methods: readonly string[];
  mutating: ReadonlySet<string>;
}

const RUNS: MethodSpec = {
  methods: [
    'applyEvent',
    'getRun',
    'getStep',
    'getEvent',
    'listEvents',
    'listSteps',
    'listHooks',
    'getCleanupStatus',
    'scheduleCleanup',
    'cleanupNow',
    'rearmCleanup',
    'resolveHookTokenClaim',
  ],
  mutating: new Set(['applyEvent', 'scheduleCleanup', 'cleanupNow', 'rearmCleanup']),
};

const STREAMS: MethodSpec = {
  methods: [
    'closeStream',
    'failStream',
    'registerStream',
    'listStreams',
    'expireRegistry',
    'finalizeRegistry',
    'expireStream',
  ],
  mutating: new Set([
    'closeStream',
    'failStream',
    'registerStream',
    'expireRegistry',
    'finalizeRegistry',
    'expireStream',
  ]),
};

function makeStreamNamespace(transport: RpcTransport) {
  return {
    idFromName(name: string) {
      return { toString: () => name };
    },
    get(id: { toString(): string }): StreamDOStub {
      const name = id.toString();
      const control = makeStub<StreamDOStub>(transport, 'streams', name, STREAMS);
      control.writeChunks = (runId, chunks) => writeStreamChunks(transport, name, runId, chunks);
      control.readChunks = (request, signal) => readStreamChunks(transport, name, request, signal);
      return control;
    },
  };
}

const QUEUE: MethodSpec = {
  methods: [
    'enqueue',
    'stats',
    'listDeadLetters',
    'redriveDeadLetter',
    'purgeDeadLetters',
    'rearmAlarm',
    'expireRun',
  ],
  mutating: new Set([
    'enqueue',
    'redriveDeadLetter',
    'purgeDeadLetters',
    'rearmAlarm',
    'expireRun',
  ]),
};

function makeStub<T>(transport: RpcTransport, binding: string, name: string, spec: MethodSpec): T {
  const stub: Record<string, (...args: unknown[]) => Promise<unknown>> = {};
  for (const method of spec.methods) {
    stub[method] = (...args: unknown[]) =>
      callDO(transport, binding, name, method, args, {
        idempotent: !spec.mutating.has(method),
      });
  }
  return stub as T;
}

function makeNamespace<T>(transport: RpcTransport, binding: string, spec: MethodSpec) {
  return {
    idFromName(name: string) {
      return { toString: () => name };
    },
    get(id: { toString(): string }): T {
      return makeStub<T>(transport, binding, id.toString(), spec);
    },
  };
}

const RUN_CATALOG: MethodSpec = {
  methods: ['upsertRun', 'list', 'expireRun'],
  // Every method is idempotent; transport retries cannot duplicate effects.
  mutating: new Set(),
};

const RUN_FENCES: MethodSpec = {
  methods: ['getStatus', 'fenceTerminal', 'fenceExpired'],
  mutating: new Set(),
};

const HOOK_TOKENS: MethodSpec = {
  methods: ['get', 'reserve', 'finalize', 'releaseClaim', 'releaseBatch'],
  mutating: new Set(),
};

const HOOK_IDS: MethodSpec = {
  methods: ['get', 'reserve', 'publish', 'releaseClaim', 'releaseBatch'],
  mutating: new Set(),
};

export function createRemoteEnv(transport: RpcTransport): CelldWorldEnv {
  const runCatalog = makeNamespace<RunCatalogShardStub>(transport, 'run-catalog', RUN_CATALOG);
  const runFences = makeNamespace<RunFenceStub>(transport, 'run-fences', RUN_FENCES);
  const hookTokens = makeNamespace<HookTokenShardStub>(transport, 'hook-tokens', HOOK_TOKENS);
  const hookIds = makeNamespace<HookIdShardStub>(transport, 'hook-ids', HOOK_IDS);
  const workflowIndex = createWorkflowIndex({ runCatalog, runFences, hookTokens, hookIds });
  return {
    WORKFLOW_DB: makeNamespace<WorkflowRunDOStub>(transport, 'runs', RUNS),
    WORKFLOW_STREAMS: makeStreamNamespace(transport),
    WORKFLOW_INDEX: {
      ...workflowIndex,
      commitRun: (run, serializedMetadata) =>
        callFleetRoute(transport, '/v1/index/runs/commit', [run, serializedMetadata], {
          idempotent: true,
        }),
      listRuns: (options) =>
        callFleetRoute(transport, '/v1/index/runs/list', [options], { idempotent: true }),
      expireRun: (request) =>
        callFleetRoute(transport, '/v1/index/runs/expire', [request], { idempotent: true }),
      reserveHook: (token, owner) =>
        callFleetRoute(transport, '/v1/index/hooks/reserve', [token, owner], {
          idempotent: true,
        }),
      finalizeHookIndexes: (token, hookId, serializedHook, owner, reservation) =>
        callFleetRoute(
          transport,
          '/v1/index/hooks/finalize',
          [token, hookId, serializedHook, owner, reservation],
          { idempotent: true },
        ),
      releaseHookReservation: (token, owner, reservation) =>
        callFleetRoute(
          transport,
          '/v1/index/hooks/release-reservation',
          [token, owner, reservation],
          { idempotent: true },
        ),
      releaseHookIndexes: (request) =>
        callFleetRoute(transport, '/v1/index/hooks/release', [request], { idempotent: true }),
    },
    WORKFLOW_QUEUE: makeNamespace<QueueCellStub>(transport, 'queue', QUEUE),
  };
}
