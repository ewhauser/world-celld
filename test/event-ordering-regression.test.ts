import { slotToEventId } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import { FakeStorage } from '../src/testing/fake-cell.js';

import { WorkflowRunDO } from '../src/worker/durable-objects/WorkflowRunDO.js';

describe('WorkflowRunDO event ordering regression', () => {
  it('preserves commit order after the object is evicted and recreated', async () => {
    const storage = new FakeStorage();
    const ctx = {
      storage,
      id: { toString: () => 'wrun_eviction_order', name: 'wrun_eviction_order' },
      waitUntil: () => undefined,
      blockConcurrencyWhile: async <T>(callback: () => Promise<T>) => callback(),
    };

    const firstInstance = new WorkflowRunDO(ctx as DurableObjectState, {});
    const created = await firstInstance.applyEvent({
      runId: 'wrun_eviction_order',
      data: {
        eventType: 'run_created',
        eventData: {
          deploymentId: 'regression-tests',
          workflowName: 'eviction-order',
          input: [],
        },
      },
    });
    expect(created.ok).toBe(true);

    // A fresh class instance over the same durable storage models eviction.
    const secondInstance = new WorkflowRunDO(ctx as DurableObjectState, {});
    const started = await secondInstance.applyEvent({
      runId: 'wrun_eviction_order',
      data: { eventType: 'run_started' },
    });
    expect(started.ok).toBe(true);

    const events = await secondInstance.listEvents({ sortOrder: 'asc' });
    expect(events.ok).toBe(true);
    if (!events.ok) throw new Error(events.message);
    expect(events.value.data.map((event) => event.eventType)).toEqual([
      'run_created',
      'run_started',
    ]);
    expect(events.value.data.map((event) => event.eventId)).toEqual([
      slotToEventId(1),
      slotToEventId(2),
    ]);
  });
});
