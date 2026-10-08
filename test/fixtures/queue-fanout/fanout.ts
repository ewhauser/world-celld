import { createHash } from 'node:crypto';
import { getStepMetadata, RetryableError } from 'workflow';

async function branch(index: number, input: string, retry: boolean) {
  'use step';
  const startedAt = Date.now();
  const { attempt } = getStepMetadata();
  if (retry && index === 0 && attempt === 1) {
    throw new RetryableError('qualification retry', { retryAfter: '1s' });
  }
  return {
    index,
    length: input.length,
    digest: createHash('sha256').update(input).digest('hex'),
    attempt,
    startedAt,
  };
}

export async function fanout(count: number, input: string, retry = false) {
  'use workflow';
  const results = await Promise.all(
    Array.from({ length: count }, (_, index) => branch(index, input, retry)),
  );
  return { results, sum: results.reduce((total, result) => total + result.index, 0) };
}
