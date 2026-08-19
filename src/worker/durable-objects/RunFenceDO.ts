import type { RunFenceStatus } from '../../indexes.js';
import { DurableObject } from '../do-base.js';

interface StoredRunFence {
  status: Exclude<RunFenceStatus, 'active'>;
  at: number;
}

/** Per-run lifecycle fence used by hook shards without a global lookup cell. */
export class RunFenceDO extends DurableObject {
  async getStatus(): Promise<RunFenceStatus> {
    return (await this.ctx.storage.get<StoredRunFence>('fence'))?.status ?? 'active';
  }

  async fenceTerminal(at: number): Promise<RunFenceStatus> {
    return await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<StoredRunFence>('fence');
      if (current) return current.status;
      await txn.put('fence', { status: 'terminal', at } satisfies StoredRunFence);
      return 'terminal';
    });
  }

  async fenceExpired(at: number): Promise<RunFenceStatus> {
    return await this.ctx.storage.transaction(async (txn) => {
      const current = await txn.get<StoredRunFence>('fence');
      if (current?.status === 'expired') return current.status;
      await txn.put('fence', { status: 'expired', at } satisfies StoredRunFence);
      return 'expired';
    });
  }
}
