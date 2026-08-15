/**
 * In-process fake celld fleet (the eve-ambient celld-harness pattern):
 * instantiate the REAL cell classes against Map-backed fake DO state, with a
 * virtual-clock alarm dispatcher. Only the runtime plumbing is faked — every
 * storage/alarm/lifecycle behavior under test is the real class's code.
 */

export interface FakeListOptions {
  prefix?: string;
  start?: string;
  startAfter?: string;
  end?: string;
  limit?: number;
  reverse?: boolean;
}

export interface FakeStorageMutation {
  operation: 'put' | 'delete';
  key: string;
  value?: unknown;
}

function applyListOptions(keys: string[], options: FakeListOptions): string[] {
  let result = keys.toSorted();
  if (options.prefix !== undefined) {
    const prefix = options.prefix;
    result = result.filter((k) => k.startsWith(prefix));
  }
  if (options.start !== undefined) {
    const bound = options.start;
    result = result.filter((k) => k >= bound);
  }
  if (options.startAfter !== undefined) {
    const bound = options.startAfter;
    result = result.filter((k) => k > bound);
  }
  if (options.end !== undefined) {
    const bound = options.end;
    result = result.filter((k) => k < bound);
  }
  if (options.reverse) {
    result = result.toReversed();
  }
  if (options.limit !== undefined) {
    result = result.slice(0, options.limit);
  }
  return result;
}

/**
 * Fake DurableObjectStorage over a Map. Transactions stage writes and commit
 * on success (discard on throw), with reads seeing staged state — the
 * semantics apply-event.ts relies on.
 */
export class FakeStorage {
  data = new Map<string, unknown>();
  alarmAt: number | null = null;
  private mutationFailure?: {
    predicate: (mutation: FakeStorageMutation) => boolean;
    error: Error;
  };

  constructor(private clock: () => number = () => Date.now()) {}

  async get<T>(key: string): Promise<T | undefined> {
    return this.data.get(key) as T | undefined;
  }

  async put<T>(key: string, value: T): Promise<void> {
    this.maybeFailMutation({ operation: 'put', key, value });
    // Match structured-clone persistence: mutations to the caller's object
    // after put() must not leak into storage.
    this.data.set(key, structuredClone(value));
  }

  async delete(key: string): Promise<boolean> {
    this.maybeFailMutation({ operation: 'delete', key });
    return this.data.delete(key);
  }

  async deleteAll(): Promise<void> {
    this.data.clear();
  }

  async list<T>(options: FakeListOptions = {}): Promise<Map<string, T>> {
    const keys = applyListOptions(Array.from(this.data.keys()), options);
    return new Map(keys.map((k) => [k, this.data.get(k) as T]));
  }

  async transaction<T>(cb: (txn: FakeTransaction) => Promise<T>): Promise<T> {
    const txn = new FakeTransaction(this);
    const result = await cb(txn);
    txn.commit();
    return result;
  }

  async getAlarm(): Promise<number | null> {
    return this.alarmAt;
  }

  async setAlarm(at: number | Date): Promise<void> {
    this.alarmAt = typeof at === 'number' ? at : at.getTime();
  }

  async deleteAlarm(): Promise<void> {
    this.alarmAt = null;
  }

  now(): number {
    return this.clock();
  }

  /** Inject one matching write failure, including writes inside transactions. */
  failNextMutation(
    predicate: (mutation: FakeStorageMutation) => boolean,
    error = new Error('injected fake storage mutation failure'),
  ): void {
    this.mutationFailure = { predicate, error };
  }

  /** @internal Shared with FakeTransaction so failure injection survives a transaction fix. */
  maybeFailMutation(mutation: FakeStorageMutation): void {
    if (!this.mutationFailure?.predicate(mutation)) return;
    const { error } = this.mutationFailure;
    this.mutationFailure = undefined;
    throw error;
  }
}

class FakeTransaction {
  private staged = new Map<string, unknown>();
  private deleted = new Set<string>();
  private stagedAlarm: number | null | undefined;

  constructor(private storage: FakeStorage) {}

  async get<T>(key: string): Promise<T | undefined> {
    if (this.deleted.has(key)) return undefined;
    if (this.staged.has(key)) return this.staged.get(key) as T;
    return this.storage.data.get(key) as T | undefined;
  }

  async put<T>(key: string, value: T): Promise<void> {
    this.storage.maybeFailMutation({ operation: 'put', key, value });
    this.deleted.delete(key);
    this.staged.set(key, structuredClone(value));
  }

  async delete(key: string): Promise<boolean> {
    this.storage.maybeFailMutation({ operation: 'delete', key });
    const existed = (!this.deleted.has(key) && this.staged.has(key)) || this.storage.data.has(key);
    this.staged.delete(key);
    this.deleted.add(key);
    return existed;
  }

  async list<T>(options: FakeListOptions = {}): Promise<Map<string, T>> {
    const merged = new Map(this.storage.data);
    for (const key of this.deleted) merged.delete(key);
    for (const [key, value] of this.staged) merged.set(key, value);
    const keys = applyListOptions(Array.from(merged.keys()), options);
    return new Map(keys.map((k) => [k, merged.get(k) as T]));
  }

  async getAlarm(): Promise<number | null> {
    return this.stagedAlarm === undefined ? this.storage.alarmAt : this.stagedAlarm;
  }

  async setAlarm(at: number | Date): Promise<void> {
    this.stagedAlarm = typeof at === 'number' ? at : at.getTime();
  }

  async deleteAlarm(): Promise<void> {
    this.stagedAlarm = null;
  }

  commit(): void {
    for (const key of this.deleted) this.storage.data.delete(key);
    for (const [key, value] of this.staged) this.storage.data.set(key, value);
    if (this.stagedAlarm !== undefined) this.storage.alarmAt = this.stagedAlarm;
  }
}

type CellClass = new (
  ctx: unknown,
  env: unknown,
) => {
  alarm?(info?: { retryCount?: number }): Promise<void>;
};

interface CellSlot {
  instance: InstanceType<CellClass>;
  storage: FakeStorage;
  pendingWaits: Promise<unknown>[];
  alarmRetryCount: number;
  alarmAbandoned: boolean;
}

/** celld abandons an alarm after six counted handler failures. */
const ALARM_FAILURE_LIMIT = 6;

/**
 * A fake fleet: one namespace per binding, each lazily instantiating the real
 * cell class with a fake ctx. `fireDueAlarms()` drives alarm dispatch
 * deterministically (alarms never fire on their own), reproducing celld's
 * retry ladder including the six-failure abandonment.
 */
export class FakeFleet {
  private cells = new Map<string, CellSlot>();
  now: number;

  constructor(
    private classes: Record<string, CellClass>,
    private cellEnv: Record<string, unknown> = {},
    startTime = Date.now(),
  ) {
    this.now = startTime;
  }

  namespace(bindingKey: string) {
    return {
      idFromName(name: string) {
        return { toString: () => name };
      },
      get: (id: { toString(): string }) => this.cell(bindingKey, id.toString()).instance,
    };
  }

  cell(bindingKey: string, name: string): CellSlot {
    const key = `${bindingKey}\0${name}`;
    let slot = this.cells.get(key);
    if (!slot) {
      const CellCtor = this.classes[bindingKey];
      if (!CellCtor) throw new Error(`no cell class registered for binding: ${bindingKey}`);
      const storage = new FakeStorage(() => this.now);
      const pendingWaits: Promise<unknown>[] = [];
      const ctx = {
        storage,
        id: { toString: () => name, name },
        waitUntil(promise: Promise<unknown>) {
          pendingWaits.push(promise);
        },
        async blockConcurrencyWhile<T>(cb: () => Promise<T>): Promise<T> {
          return cb();
        },
      };
      slot = {
        instance: new CellCtor(ctx, this.cellEnv),
        storage,
        pendingWaits,
        alarmRetryCount: 0,
        alarmAbandoned: false,
      };
      this.cells.set(key, slot);
    }
    return slot;
  }

  advance(ms: number): void {
    this.now += ms;
  }

  /**
   * Dispatch every alarm due at the virtual current time, awaiting waitUntil
   * work. A throwing handler re-arms per celld's ladder (2s doubling) and
   * abandons the alarm after the sixth counted failure — the cell keeps its
   * state but has no timer.
   */
  async fireDueAlarms(): Promise<void> {
    for (const slot of this.cells.values()) {
      const due = slot.storage.alarmAt;
      if (due === null || due > this.now || slot.alarmAbandoned) continue;
      if (typeof slot.instance.alarm !== 'function') continue;

      slot.storage.alarmAt = null;
      try {
        await slot.instance.alarm({ retryCount: slot.alarmRetryCount });
        slot.alarmRetryCount = 0;
      } catch {
        slot.alarmRetryCount += 1;
        if (slot.alarmRetryCount >= ALARM_FAILURE_LIMIT) {
          slot.alarmAbandoned = true;
        } else if (slot.storage.alarmAt === null) {
          slot.storage.alarmAt = this.now + 2000 * 2 ** (slot.alarmRetryCount - 1);
        }
      }
      await this.drainWaits(slot);
    }
  }

  private async drainWaits(slot: CellSlot): Promise<void> {
    while (slot.pendingWaits.length > 0) {
      const batch = slot.pendingWaits.splice(0);
      await Promise.allSettled(batch);
    }
  }

  /** Await all waitUntil work across the fleet (delivery fetches, etc.). */
  async settle(): Promise<void> {
    for (const slot of this.cells.values()) {
      await this.drainWaits(slot);
    }
  }
}
