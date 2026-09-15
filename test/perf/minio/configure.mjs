import { readFile, writeFile } from 'node:fs/promises';

// Test-only credentials belong in the deployment, not the celld node environment.
for (const directory of ['celld-worker', 'celld-queue-worker']) {
  const path = `${directory}/wrangler.jsonc`;
  const source = await readFile(path, 'utf8');
  const placeholder = '"WORLD_SECRET": ""';
  if (!source.includes(placeholder)) throw new Error(`${path}: missing secret placeholder`);
  await writeFile(path, source.replace(placeholder, '"WORLD_SECRET": "world-celld-perf-secret"'));
}
