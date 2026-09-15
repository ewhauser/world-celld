// Companion native Queue consumer. It intentionally exports no fetch handler:
// Keep the Queue consumer attachment independent of the public HTTP worker.
export { default } from '@ewhauser/world-celld/queue-consumer';
