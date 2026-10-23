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

// Compile this source under both the 5.1 and 5.2 runtimes. The block-local
// steps exercise nested step identity across the toolchain upgrade. An in-flight
// 5.1 run must continue on its original compiled artifact.
export async function processOrder(orderId: string) {
  'use workflow';

  const fetched = await workflowFetch(`http://${orderId}/probe`);
  if ((await fetched.text()) !== 'probe-ok') throw new Error('probe fetch changed');
  {
    const checkpointBefore = async () => {
      'use step';
      return `before-${orderId}`;
    };
    await checkpointBefore();
  }
  await progress('nested fetch completed');

  using approval = createHook<{ approved: boolean }>();
  await progress(`awaiting approval — POST /approvals/${approval.token}`);
  const decision = await approval;
  if (!decision.approved) return { status: 'rejected' as const };

  {
    const checkpointAfter = async () => {
      'use step';
      return `after-${orderId}`;
    };
    await checkpointAfter();
  }
  await sleep('2s');
  const shipmentId = await shipOrder(orderId);
  await progress(`shipped as ${shipmentId}`);
  return { status: 'shipped' as const, shipmentId };
}
