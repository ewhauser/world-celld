/**
 * Provider-neutral storage used for run-bearing native Queue payloads.
 *
 * celld v0.4 exposes a namespace in the fleet's existing object store through
 * the Workers R2-compatible binding shape. Keep that platform shape here so
 * queue and retention code depend only on QueuePayloadStore.
 */
export interface QueuePayloadStore {
  write(key: string, value: string, metadata: { runId: string; messageId: string }): Promise<void>;
  read(key: string): Promise<string | null>;
  delete(keys: string | string[]): Promise<void>;
}

export interface QueuePayloadObjectStorageBinding {
  put(
    key: string,
    value: string,
    options?: { customMetadata?: Record<string, string> },
  ): Promise<unknown>;
  get(key: string): Promise<{ text(): Promise<string> } | null>;
  delete(keys: string | string[]): Promise<void>;
}

export function createQueuePayloadStore(
  binding: QueuePayloadObjectStorageBinding,
): QueuePayloadStore {
  return {
    async write(key, value, metadata) {
      await binding.put(key, value, { customMetadata: metadata });
    },
    async read(key) {
      return (await binding.get(key))?.text() ?? null;
    },
    async delete(keys) {
      await binding.delete(keys);
    },
  };
}

export async function deleteQueuePayloadObjects(
  binding: Pick<QueuePayloadObjectStorageBinding, 'delete'>,
  keys: string | string[],
): Promise<void> {
  await binding.delete(keys);
}
