/**
 * In-process celld-world emulation for tests and local development: the real
 * worker router and the real cell classes over Map-backed fake state, behind
 * node:http — the full wire protocol without a celld fleet.
 *
 * `startDevFleet()` additionally drives cell alarms and native Queue
 * deliveries on real time. Publications remain available in `queueMessages`
 * for local inspection.
 */
export { FakeFleet, FakeStorage } from './fake-cell.js';
export { startHarness, type Harness, type HarnessOptions } from './http-harness.js';

import { startHarness, type Harness, type HarnessOptions } from './http-harness.js';
import queueConsumer from '../worker/queue-consumer.js';

export interface DevFleet extends Harness {
  stop(): Promise<void>;
}

/** Alarm-pump interval for the dev fleet (real time). */
const PUMP_INTERVAL_MS = 25;

export async function startDevFleet(options: HarnessOptions = {}): Promise<DevFleet> {
  const harness = await startHarness(options);
  type PendingMessage = { body: string; attempts: number; dueAt: number };
  const pending: PendingMessage[] = [];
  const deliveries = new Set<Promise<void>>();
  let publicationCursor = 0;
  let stopped = false;

  const queueEnv = {
    WORKFLOW_QUEUE: {
      async send(body: string, sendOptions?: { delaySeconds?: number }) {
        harness.queueMessages.push(body);
        harness.queuePublications.push({ body, delaySeconds: sendOptions?.delaySeconds ?? 0 });
      },
    },
    WORLD_SERVICE: {
      deliver: (
        secret: string,
        envelope: Parameters<Harness['deliverQueueMessage']>[1],
        attempt: number,
      ) => harness.deliverQueueMessage(secret, envelope, attempt),
    },
    WORLD_SECRET: options.secret,
  };

  function dispatch(message: PendingMessage): void {
    let settled = false;
    const delivery = queueConsumer
      .queue(
        {
          messages: [
            {
              body: message.body,
              attempts: message.attempts,
              ack() {
                settled = true;
              },
              retry(retryOptions?: { delaySeconds?: number }) {
                settled = true;
                if (!stopped) {
                  pending.push({
                    body: message.body,
                    attempts: message.attempts + 1,
                    dueAt: Date.now() + (retryOptions?.delaySeconds ?? 0) * 1000,
                  });
                }
              },
            },
          ],
        },
        queueEnv,
      )
      .then(() => {
        if (!settled) {
          console.error('[world-celld dev fleet] Queue delivery was not settled');
          if (!stopped) {
            pending.push({
              body: message.body,
              attempts: message.attempts + 1,
              dueAt: Date.now() + 1000,
            });
          }
        }
        return undefined;
      })
      .catch((error: unknown) => {
        console.error('[world-celld dev fleet] Queue consumer failed', error);
        if (!settled && !stopped) {
          pending.push({
            body: message.body,
            attempts: message.attempts + 1,
            dueAt: Date.now() + 1000,
          });
        }
      });
    deliveries.add(delivery);
    void delivery.finally(() => deliveries.delete(delivery));
  }

  const queuePump = setInterval(() => {
    if (stopped) return;
    while (publicationCursor < harness.queuePublications.length) {
      const publication = harness.queuePublications[publicationCursor++];
      pending.push({
        body: publication.body,
        attempts: 1,
        dueAt: Date.now() + publication.delaySeconds * 1000,
      });
    }
    const now = Date.now();
    for (let index = 0; index < pending.length;) {
      if (pending[index].dueAt <= now) {
        dispatch(pending.splice(index, 1)[0]);
      } else {
        index++;
      }
    }
  }, PUMP_INTERVAL_MS);
  queuePump.unref?.();

  let pumping = false;
  const pump = setInterval(async () => {
    if (pumping) return;
    pumping = true;
    try {
      harness.fleet.now = Date.now();
      await harness.fleet.fireDueAlarms();
    } catch (error) {
      console.error('[world-celld dev fleet] alarm pump error:', error);
    } finally {
      pumping = false;
    }
  }, PUMP_INTERVAL_MS);
  // Don't hold the process open just for the pump.
  pump.unref?.();

  return {
    ...harness,
    async stop() {
      stopped = true;
      clearInterval(queuePump);
      clearInterval(pump);
      await Promise.allSettled(deliveries);
      await harness.close();
    },
  };
}
