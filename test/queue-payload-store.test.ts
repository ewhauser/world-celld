import { describe, expect, it, vi } from 'vitest';
import { createQueuePayloadStore } from '../src/worker/queue-payload-store.js';

type AdapterBinding = Parameters<typeof createQueuePayloadStore>[0];

describe('QueuePayloadStore object-storage adapter', () => {
  it('translates provider-neutral reads and writes to the celld binding', async () => {
    const put = vi
      .fn<AdapterBinding['put']>()
      .mockResolvedValue({ key: 'ignored-platform-result' });
    const get = vi
      .fn<AdapterBinding['get']>()
      .mockResolvedValueOnce({ text: async () => 'payload' })
      .mockResolvedValueOnce(null);
    const remove = vi.fn<AdapterBinding['delete']>().mockResolvedValue(undefined);
    const store = createQueuePayloadStore({ put, get, delete: remove });

    await store.write('workflow-queue/run/message', 'payload', {
      runId: 'run',
      messageId: 'message',
    });
    await expect(store.read('workflow-queue/run/message')).resolves.toBe('payload');
    await expect(store.read('workflow-queue/run/missing')).resolves.toBeNull();
    await store.delete(['workflow-queue/run/message']);

    expect(put).toHaveBeenCalledWith('workflow-queue/run/message', 'payload', {
      customMetadata: { runId: 'run', messageId: 'message' },
    });
    expect(remove).toHaveBeenCalledWith(['workflow-queue/run/message']);
  });
});
