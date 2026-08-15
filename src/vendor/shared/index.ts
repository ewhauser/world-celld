/**
 * Vendored subset of @fantasticfour/shared from vinnymac/worlds (Apache-2.0),
 * which is not published to npm. Only the modules world-cloudflare's sources
 * consume are vendored: the tagged-JSON codec, the debug logger, and compact().
 * See NOTICE at the repo root.
 */
export { createDebugLogger } from './debug.js';
export {
  DATE_FIELDS,
  dateReviver,
  uint8ArrayReplacer,
  uint8ArrayReviver,
  stringify,
  parse,
  deepClone,
} from './serialization.js';
export { compact, Mutex, Rc } from './util.js';
