// Companion native Queue consumer. It intentionally exports no fetch handler:
// celld v0.4.0 requires Queue consumers and HTTP Workers to be separate scripts.
export { default } from '@ewhauser/world-celld/queue-consumer';
