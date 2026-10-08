// A/B control belongs to this fixture; the published World has no batch switch.
import { createWorld as createCelldWorld } from '../../../dist/index.js';
export function createWorld() {
  const world = createCelldWorld();
  const metrics = (globalThis.qualificationQueueMetrics ??= {
    singles: 0,
    batches: 0,
    entries: 0,
  });
  const queue = world.queue.bind(world);
  world.queue = (...args) => {
    metrics.singles++;
    return queue(...args);
  };
  if (process.env.QUALIFICATION_QUEUE_PATH === 'single') {
    delete world.queueBatch;
  } else {
    const batch = world.queueBatch.bind(world);
    world.queueBatch = async (...args) => {
      metrics.batches++;
      metrics.entries += args[1].length;
      const results = await batch(...args);
      return results;
    };
  }
  return world;
}
