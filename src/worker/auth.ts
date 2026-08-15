/**
 * Bearer-token auth for the worker router.
 *
 * Fails closed: when the WORLD_SECRET var is not configured, every protected
 * route returns 503 instead of forwarding unauthenticated traffic (the
 * eve-ambient celld-worker pattern).
 */

/**
 * Constant-time string comparison via SHA-256 digests (digest-then-compare
 * is length-independent). crypto.subtle is global in both workerd and Node.
 */
async function timingSafeEqual(a: string, b: string): Promise<boolean> {
  const enc = new TextEncoder();
  const [da, db] = await Promise.all([
    crypto.subtle.digest('SHA-256', enc.encode(a)),
    crypto.subtle.digest('SHA-256', enc.encode(b)),
  ]);
  const va = new Uint8Array(da);
  const vb = new Uint8Array(db);
  let diff = 0;
  for (let i = 0; i < va.length; i++) {
    diff |= va[i] ^ vb[i];
  }
  return diff === 0;
}

export type AuthResult = { ok: true } | { ok: false; response: Response };

export async function authenticate(
  request: Request,
  secret: string | undefined,
): Promise<AuthResult> {
  if (!secret) {
    return {
      ok: false,
      response: Response.json(
        { error: { name: 'WorldNotConfigured', message: 'WORLD_SECRET is not configured' } },
        { status: 503 },
      ),
    };
  }

  const header = request.headers.get('authorization') ?? '';
  const token = header.startsWith('Bearer ') ? header.slice('Bearer '.length) : '';
  if (!token || !(await timingSafeEqual(token, secret))) {
    return {
      ok: false,
      response: Response.json(
        { error: { name: 'Unauthorized', message: 'invalid or missing bearer token' } },
        { status: 401 },
      ),
    };
  }

  return { ok: true };
}
