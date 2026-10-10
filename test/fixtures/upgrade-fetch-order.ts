import { createHook, fetch as workflowFetch, getWritable, sleep } from 'workflow';

async function progress(message: string) {
  'use step';
  const writer = getWritable<string>().getWriter();
  await writer.write(`${message}\n`);
  writer.releaseLock();
}

async function shipOrder(orderId: string) {
  'use step';
  return `ship_${orderId}`;
}

// Compiled once with the pinned beta runtime. Both sides of the celld restart
// execute this same output artifact, including its version-qualified fetch ID.
export async function processOrder(orderId: string) {
  'use workflow';

  const fetched = await workflowFetch(`http://${orderId}/probe`);
  if ((await fetched.text()) !== 'probe-ok') throw new Error('probe fetch changed');
  await progress('beta fetch completed');

  using approval = createHook<{ approved: boolean }>();
  await progress(`awaiting approval — POST /approvals/${approval.token}`);
  const decision = await approval;
  if (!decision.approved) return { status: 'rejected' as const };

  await sleep('2s');
  const shipmentId = await shipOrder(orderId);
  await progress(`shipped as ${shipmentId}`);
  return { status: 'shipped' as const, shipmentId };
}
