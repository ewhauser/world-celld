async function branch(index: number, payload: string) {
  'use step';
  const startedAt = Date.now();
  await new Promise((resolve) => setTimeout(resolve, 100));
  return { index, startedAt, finishedAt: Date.now(), bytes: payload.length };
}

/** Compiled Workflow 5.0.1 fan-out fixture for the batch benchmark. */
export async function fanout(width: number) {
  'use workflow';
  const payload = 'x'.repeat(1024);
  const branches = await Promise.all(
    Array.from({ length: width }, (_, index) => branch(index, payload)),
  );
  return { branches, joinedAt: Date.now() };
}
