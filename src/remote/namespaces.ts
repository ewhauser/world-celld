/**
 * HTTP-backed implementations of the structural namespace interfaces the
 * vendored storage/streamer/queue layers consume. `get(id)` returns a stub
 * whose method calls become `POST /v1/rpc/{binding}/{name}/{method}` against
 * the fleet — the celld stand-in for Cloudflare's `env` bindings.
 */
import type { CelldWorldEnv, HookTokenOwner, IndexNamespace } from '../config.js';
import type { QueueCellStub } from '../queue.js';
import type { WorkflowRunDOStub } from '../storage.js';
import type { StreamDOStub } from '../streamer.js';
import { callDO, type RpcTransport } from './rpc-client.js';

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
  ],
  mutating: new Set(['applyEvent', 'scheduleCleanup', 'cleanupNow', 'rearmCleanup']),
};

const STREAMS: MethodSpec = {
  methods: [
    'writeChunk',
    'closeStream',
    'getChunks',
    'getInfo',
    'registerStream',
    'listStreams',
    'expireRegistry',
    'finalizeRegistry',
    'expireStream',
  ],
  mutating: new Set([
    'writeChunk',
    'closeStream',
    'registerStream',
    'expireRegistry',
    'finalizeRegistry',
    'expireStream',
  ]),
};

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

/** The IndexDO is a single well-known cell. */
const INDEX_CELL_NAME = 'index';

function makeIndexNamespace(transport: RpcTransport): IndexNamespace {
  const call = <T>(method: string, args: unknown[], idempotent: boolean) =>
    callDO<T>(transport, 'index', INDEX_CELL_NAME, method, args, { idempotent });
  return {
    get: (key) => call<string | null>('get', [key], true),
    put: (key, value) => call<void>('put', [key, value], false),
    delete: (key) => call<void>('delete', [key], false),
    putOwned: (runId, key, value) =>
      call<{ stored: boolean }>('putOwned', [runId, key, value], false),
    expireRun: (request) => call('expireRun', [request], false),
    list: (options) => call('list', [options], true),
    reserveHookToken: (token: string, owner: HookTokenOwner) =>
      call('reserveHookToken', [token, owner], false),
    finalizeHookIndexes: (token, hookId, serializedHook, owner) =>
      call<void>('finalizeHookIndexes', [token, hookId, serializedHook, owner], false),
    releaseHookToken: (token, owner) => call<void>('releaseHookToken', [token, owner], false),
    deleteHookIndexes: (token, hookId, owner) =>
      call<void>('deleteHookIndexes', [token, hookId, owner], false),
  };
}

export function createRemoteEnv(transport: RpcTransport): CelldWorldEnv {
  return {
    WORKFLOW_DB: makeNamespace<WorkflowRunDOStub>(transport, 'runs', RUNS),
    WORKFLOW_STREAMS: makeNamespace<StreamDOStub>(transport, 'streams', STREAMS),
    WORKFLOW_INDEX: makeIndexNamespace(transport),
    WORKFLOW_QUEUE: makeNamespace<QueueCellStub>(transport, 'queue', QUEUE),
  };
}
