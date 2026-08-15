import { createHook, getWritable, sleep } from 'workflow';

async function progress(message: string) {
  'use step';
  const writer = getWritable<string>().getWriter();
  await writer.write(`${message}\n`);
  writer.releaseLock();
}

async function validateOrder(orderId: string) {
  'use step';
  // Pretend to price the order.
  return { total: 42.5, currency: 'USD', orderId };
}

async function shipOrder(orderId: string) {
  'use step';
  return `ship_${orderId}_${Math.random().toString(36).slice(2, 8)}`;
}

/**
 * Demo workflow exercising the three world primitives on celld:
 * steps + sleep (queue cells), a hook (index + run cells), and the run's
 * output stream (stream cells).
 */
export async function processOrder(orderId: string) {
  'use workflow';

  await progress(`order ${orderId}: validating`);
  const quote = await validateOrder(orderId);
  await progress(`order ${orderId}: total ${quote.currency} ${quote.total}`);

  using approval = createHook<{ approved: boolean; comment?: string }>();
  await progress(`order ${orderId}: awaiting approval — POST /approvals/${approval.token}`);

  const decision = await approval;
  if (!decision.approved) {
    await progress(`order ${orderId}: rejected (${decision.comment ?? 'no comment'})`);
    return { status: 'rejected' as const, orderId };
  }

  await progress(`order ${orderId}: approved, shipping in 2s`);
  await sleep('2s');

  const shipmentId = await shipOrder(orderId);
  await progress(`order ${orderId}: shipped as ${shipmentId}`);

  return { status: 'shipped' as const, orderId, shipmentId, total: quote.total };
}
