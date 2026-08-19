import { execFileSync, spawnSync } from 'node:child_process';
import { cp, mkdtemp, mkdir, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const requestedRef = process.argv.find((argument) => argument.startsWith('--ref='))?.slice(6);
const ref = requestedRef || '0de370b';
const commit = execFileSync('git', ['rev-parse', '--verify', `${ref}^{commit}`], {
  cwd: root,
  encoding: 'utf8',
}).trim();
const temporaryRoot = await mkdtemp(join(tmpdir(), 'worlds-celld-index-baseline-'));

try {
  const archive = execFileSync('git', ['archive', '--format=tar', commit], {
    cwd: root,
    maxBuffer: 128 * 1024 * 1024,
  });
  execFileSync('tar', ['-xf', '-', '-C', temporaryRoot], { input: archive });
  await mkdir(join(temporaryRoot, 'test', 'perf'), { recursive: true });
  await cp(
    join(root, 'test', 'perf', 'index-main-baseline.test.ts'),
    join(temporaryRoot, 'test', 'perf', 'index-main-baseline.test.ts'),
  );
  await cp(
    join(root, 'vitest.index-main-baseline.config.ts'),
    join(temporaryRoot, 'vitest.index-main-baseline.config.ts'),
  );
  await symlink(join(root, 'node_modules'), join(temporaryRoot, 'node_modules'), 'dir');

  const result = spawnSync(
    join(root, 'node_modules', '.bin', 'vitest'),
    ['run', '--config', 'vitest.index-main-baseline.config.ts', '--disableConsoleIntercept'],
    {
      cwd: temporaryRoot,
      env: { ...process.env, INDEX_BASELINE_REF: commit },
      stdio: 'inherit',
    },
  );
  if (result.error) throw result.error;
  process.exitCode = result.status ?? 1;
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
