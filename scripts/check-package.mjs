#!/usr/bin/env node

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const PACKAGE_NAME = '@ewhauser/world-celld';
const tempRoot = mkdtempSync(join(tmpdir(), 'world-celld-package-'));
const packDirectory = join(tempRoot, 'pack');
const extractDirectory = join(tempRoot, 'extract');
const consumerDirectory = join(tempRoot, 'consumer');

mkdirSync(packDirectory);
mkdirSync(extractDirectory);
mkdirSync(consumerDirectory);

function run(command, args, options = {}) {
  return execFileSync(command, args, {
    cwd: resolve('.'),
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'inherit'],
    ...options,
  });
}

try {
  const output = run('npm', [
    'pack',
    '--json',
    '--ignore-scripts',
    '--pack-destination',
    packDirectory,
  ]);
  const [pack] = JSON.parse(output);
  assert(pack, 'npm pack did not return package metadata');
  assert.equal(pack.name, PACKAGE_NAME);

  const paths = new Set(pack.files.map(({ path }) => path));
  const requiredPaths = [
    'CHANGELOG.md',
    'LICENSE',
    'NOTICE',
    'README.md',
    'package.json',
    'celld-worker/README.md',
    'celld-worker/worker.ts',
    'celld-worker/wrangler.jsonc',
    'dist/index.d.ts',
    'dist/index.js',
    'dist/testing.d.ts',
    'dist/testing.js',
    'dist/worker.d.ts',
    'dist/worker.js',
  ];
  for (const path of requiredPaths) {
    assert(paths.has(path), `published package is missing ${path}`);
  }

  const exactPublishedPaths = new Set([
    'CHANGELOG.md',
    'LICENSE',
    'NOTICE',
    'README.md',
    'package.json',
    'celld-worker/README.md',
    'celld-worker/worker.ts',
    'celld-worker/wrangler.jsonc',
  ]);
  const unexpectedPath = pack.files.find(
    ({ path }) =>
      !exactPublishedPaths.has(path) &&
      !/^dist\/[A-Za-z0-9._-]+\.(?:d\.ts(?:\.map)?|js(?:\.map)?)$/.test(path),
  );
  assert(!unexpectedPath, `published package contains unexpected path ${unexpectedPath?.path}`);

  const unexpectedTypeScript = pack.files.find(
    ({ path }) =>
      path.endsWith('.ts') && !path.endsWith('.d.ts') && path !== 'celld-worker/worker.ts',
  );
  assert(
    !unexpectedTypeScript,
    `published package contains unexpected TypeScript source ${unexpectedTypeScript?.path}`,
  );

  const tarball = join(packDirectory, pack.filename);
  run('tar', ['-xzf', tarball, '-C', extractDirectory], {
    cwd: tempRoot,
  });
  const manifest = JSON.parse(readFileSync(join(extractDirectory, 'package/package.json'), 'utf8'));
  assert.equal(manifest.name, PACKAGE_NAME);
  assert.equal(manifest.version, pack.version);
  assert.equal(manifest.license, 'Apache-2.0');
  assert.equal(manifest.publishConfig?.access, 'public');
  assert.equal(manifest.publishConfig?.registry, 'https://registry.npmjs.org/');
  assert.equal(manifest.repository?.url, 'https://github.com/ewhauser/world-celld.git');
  assert.equal(manifest.sideEffects, false);

  for (const lifecycle of ['preinstall', 'install', 'postinstall', 'prepare']) {
    assert(!manifest.scripts?.[lifecycle], `published package defines ${lifecycle}`);
  }

  const dependencyGroups = [
    manifest.dependencies,
    manifest.optionalDependencies,
    manifest.peerDependencies,
    manifest.bundledDependencies,
  ];
  for (const dependencies of dependencyGroups) {
    for (const [name, specifier] of Object.entries(dependencies ?? {})) {
      assert(
        typeof specifier !== 'string' ||
          !/^(?:catalog:|file:|git(?:\+|:)|https?:|link:|workspace:)/.test(specifier),
        `published dependency ${name} uses unsafe specifier ${specifier}`,
      );
    }
  }

  writeFileSync(
    join(consumerDirectory, 'package.json'),
    JSON.stringify({ private: true, type: 'module' }, null, 2),
  );
  run(
    'npm',
    ['install', '--ignore-scripts', '--package-lock=false', '--no-audit', '--no-fund', tarball],
    { cwd: consumerDirectory },
  );
  writeFileSync(
    join(consumerDirectory, 'smoke.mjs'),
    [
      `import { createCelldWorld, createWorld } from '${PACKAGE_NAME}';`,
      `import { FakeFleet, startDevFleet } from '${PACKAGE_NAME}/testing';`,
      "if (typeof createCelldWorld !== 'function' || createWorld !== createCelldWorld) throw new Error('root exports are invalid');",
      "if (typeof FakeFleet !== 'function' || typeof startDevFleet !== 'function') throw new Error('testing exports are invalid');",
    ].join('\n'),
  );
  run(process.execPath, ['smoke.mjs'], { cwd: consumerDirectory });

  console.log(
    `package check OK — ${pack.id}, ${pack.entryCount} files, ${pack.size} compressed bytes`,
  );
} finally {
  rmSync(tempRoot, { recursive: true, force: true });
}
