/**
 * Creates a namespaced debug logger.
 * Logs to stderr to avoid corrupting CLI JSON output on stdout.
 *
 * Control via WORKFLOW_DEBUG environment variable:
 *   WORKFLOW_DEBUG=1                       // Enable all
 *   WORKFLOW_DEBUG=mysql-world             // Specific namespace
 *   WORKFLOW_DEBUG=mysql-world,redis-world // Multiple namespaces
 *
 * Vendored from vinnymac/worlds packages/shared, modified: guarded for
 * non-Node runtimes (celld/workerd), where `process` may be absent or
 * stubbed — falls back to console.error.
 */
export function createDebugLogger(namespace: string) {
  return (...args: unknown[]) => {
    const debug = typeof process === 'undefined' ? undefined : process.env?.WORKFLOW_DEBUG;
    if (!debug) return;

    const enabled =
      debug === '1' ||
      debug === 'true' ||
      debug === '*' ||
      debug.split(',').some((ns: string) => ns.trim() === namespace);

    if (!enabled) return;

    const timestamp = new Date().toISOString();
    const prefix = `[${timestamp}] [${namespace}]`;
    const message = args
      .map((arg) => (typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)))
      .join(' ');

    if (typeof process.stderr?.write === 'function') {
      process.stderr.write(`${prefix} ${message}\n`);
    } else {
      console.error(`${prefix} ${message}`);
    }
  };
}
