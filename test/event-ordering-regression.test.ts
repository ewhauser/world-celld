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

  it('migrates pre-Workflow-5 event IDs to dense slots before accepting new writes', async () => {
    const storage = new FakeStorage();
    const createdAt = new Date('2026-08-15T00:00:00.000Z');
    storage.data.set('schema_version', 3);
    storage.data.set('event_sequence', 2);
    storage.data.set('run', {
      runId: 'wrun_slot_migration',
      deploymentId: 'regression-tests',
      workflowName: 'slot-migration',
      specVersion: 4,
      input: [],
      status: 'pending',
      createdAt,
      updatedAt: createdAt,
    });
    storage.data.set('event:wevt_z00000000000000000001', {
      runId: 'wrun_slot_migration',
      eventId: 'wevt_z00000000000000000001',
      eventType: 'run_created',
      eventData: {
        deploymentId: 'regression-tests',
        workflowName: 'slot-migration',
        input: [],
      },
      specVersion: 4,
      createdAt,
    });
    storage.data.set('event:wevt_z00000000000000000002', {
      runId: 'wrun_slot_migration',
      eventId: 'wevt_z00000000000000000002',
      eventType: 'run_started',
      specVersion: 4,
      createdAt: new Date(createdAt.getTime() + 1),
    });

    const ctx = {
      storage,
      id: { toString: () => 'wrun_slot_migration', name: 'wrun_slot_migration' },
      waitUntil: () => undefined,
      blockConcurrencyWhile: async <T>(callback: () => Promise<T>) => callback(),
    };
    const instance = new WorkflowRunDO(ctx as DurableObjectState, {});

    const migrated = await instance.listEvents({ sortOrder: 'asc' });
    expect(migrated.ok).toBe(true);
    if (!migrated.ok) throw new Error(migrated.message);
    expect(migrated.value.data.map((event) => event.eventId)).toEqual([
      slotToEventId(1),
      slotToEventId(2),
    ]);

    const started = await instance.applyEvent({
      runId: 'wrun_slot_migration',
      data: { eventType: 'run_started' },
    });
    expect(started.ok).toBe(true);
    const events = await instance.listEvents({ sortOrder: 'asc' });
    expect(events.ok).toBe(true);
    if (!events.ok) throw new Error(events.message);
    expect(events.value.data.map((event) => event.eventId)).toEqual([
      slotToEventId(1),
      slotToEventId(2),
      slotToEventId(3),
    ]);
  });
});
