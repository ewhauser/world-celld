/**
 * Durable Object base class — workerd/celld variant. The worker build aliases
 * './do-base.js' to this module so deployed cells extend the REAL runtime
 * base class (celld's JS RPC on DO stubs requires `extends DurableObject`).
 */
export { DurableObject } from 'cloudflare:workers';
