#!/usr/bin/env node

import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { basename, join, resolve } from 'node:path';

const packageName = '@ewhauser/world-celld';
const outputDirectory = resolve('release-artifacts');

rmSync(outputDirectory, { recursive: true, force: true });
mkdirSync(outputDirectory);

const output = execFileSync(
  'npm',
  ['pack', '--json', '--ignore-scripts', '--pack-destination', outputDirectory],
  { cwd: resolve('.'), encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'] },
);
const [pack] = JSON.parse(output);
assert(pack, 'npm pack did not return package metadata');
assert.equal(pack.name, packageName);

const tarball = join(outputDirectory, basename(pack.filename));
const digest = createHash('sha256').update(readFileSync(tarball)).digest('hex');
const checksum = `${tarball}.sha256`;
writeFileSync(checksum, `${digest}  ${basename(tarball)}\n`);

console.log(`release package: ${tarball}`);
console.log(`sha256: ${digest}`);
