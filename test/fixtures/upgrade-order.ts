import { createHook, getWritable, sleep } from 'workflow';
import { getRun } from 'workflow/api';

async function progress(message: string) {
  'use step';
  const writer = getWritable<string>().getWriter();
  await writer.write(`${message}\n`);
  writer.releaseLock();
}

async function validateOrder(orderId: string) {
  'use step';
  return { total: 42.5, currency: 'USD', orderId };
}

async function shipOrder(orderId: string) {
  'use step';
  return `ship_${orderId}_${Math.random().toString(36).slice(2, 8)}`;
}

// This replaces only the demo workflow in temporary beta/stable smoke builds.
// The order ID is a separate, pending run ID, so these Run API calls have a
// stable target on both sides of the restart.
async function probeDependencySteps(runId: string, phase: string) {
  'use step';
  const run = getRun(runId);
  if (!(await run.exists)) throw new Error(`${phase}: observed run missing`);
  if ((await run.status) !== 'pending') throw new Error(`${phase}: observed run changed`);
  if (!(await run.createdAt)) throw new Error(`${phase}: missing creation time`);
  if ((await run.workflowName) !== 'upgrade-observed') {
    throw new Error(`${phase}: observed workflow name changed`);
  }
}

export async function processOrder(orderId: string) {
  'use workflow';

  await probeDependencySteps(orderId, 'before-hook');
  await progress('before-hook: dependency steps replayed');
  await progress(`order ${orderId}: validating`);
  const quote = await validateOrder(orderId);
  await progress(`order ${orderId}: total ${quote.currency} ${quote.total}`);

  using approval = createHook<{ approved: boolean; comment?: string }>();
  await progress(`order ${orderId}: awaiting approval — POST /approvals/${approval.token}`);

  const decision = await approval;
  if (!decision.approved) return { status: 'rejected' as const, orderId };

  await probeDependencySteps(orderId, 'after-hook');
  await progress('after-hook: dependency steps replayed');
  await progress(`order ${orderId}: approved, shipping in 2s`);
  await sleep('2s');
  const shipmentId = await shipOrder(orderId);
  await progress(`order ${orderId}: shipped as ${shipmentId}`);
  return { status: 'shipped' as const, orderId, shipmentId, total: quote.total };
}
