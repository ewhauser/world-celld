# Changelog

All notable changes to this project will be documented in this file.

## 0.3.1 (2026-08-19)

## What's Changed
* perf: reduce runs.list RPC fanout by @ewhauser in https://github.com/ewhauser/world-celld/pull/21
* perf: remove unused correlation index by @ewhauser in https://github.com/ewhauser/world-celld/pull/23
* perf: bound QueueDO inflight deadline work by @ewhauser in https://github.com/ewhauser/world-celld/pull/24
* chore(deps-dev): bump @types/node from 22.20.1 to 26.1.2 by @dependabot[bot] in https://github.com/ewhauser/world-celld/pull/4
* chore(deps-dev): bump the development-dependencies group across 1 directory with 2 updates by @dependabot[bot] in https://github.com/ewhauser/world-celld/pull/25
* perf: batch and long-poll stream I/O by @ewhauser in https://github.com/ewhauser/world-celld/pull/26
* perf: bound cleanup and batch Durable Object storage by @ewhauser in https://github.com/ewhauser/world-celld/pull/27
* perf: shard workflow indexes by @ewhauser in https://github.com/ewhauser/world-celld/pull/28


**Full Changelog**: https://github.com/ewhauser/world-celld/compare/v0.3.0...v0.3.1

## 0.3.0 (2026-08-16)

## What's Changed
* fix: split Release Please release and PR phases by @ewhauser in https://github.com/ewhauser/world-celld/pull/16
* feat: target Workflow 5 beta by @ewhauser in https://github.com/ewhauser/world-celld/pull/17
* fix: enforce Workflow 5 storage contracts by @ewhauser in https://github.com/ewhauser/world-celld/pull/19


**Full Changelog**: https://github.com/ewhauser/world-celld/compare/v0.2.0...v0.3.0

## 0.2.0 (2026-08-16)

## What's Changed
* Patch vulnerable Hono test dependencies by @ewhauser in https://github.com/ewhauser/world-celld/pull/8
* chore(deps-dev): bump typescript from 5.9.3 to 7.0.2 by @dependabot[bot] in https://github.com/ewhauser/world-celld/pull/3
* Fix flaky harness teardown by @ewhauser in https://github.com/ewhauser/world-celld/pull/10
* Add terminal workflow retention by @ewhauser in https://github.com/ewhauser/world-celld/pull/11
* chore(deps): bump the production-dependencies group across 1 directory with 3 updates by @dependabot[bot] in https://github.com/ewhauser/world-celld/pull/1
* ci: automate releases with Release Please by @ewhauser in https://github.com/ewhauser/world-celld/pull/13
* chore(deps-dev): bump the development-dependencies group across 1 directory with 2 updates by @dependabot[bot] in https://github.com/ewhauser/world-celld/pull/12
* fix: exclude generated changelog from formatting by @ewhauser in https://github.com/ewhauser/world-celld/pull/15

## New Contributors
* @dependabot[bot] made their first contribution in https://github.com/ewhauser/world-celld/pull/3

**Full Changelog**: https://github.com/ewhauser/world-celld/compare/v0.1.1...v0.2.0

## 0.1.1 - 2026-08-15

- Publish through npm trusted publishing with OIDC provenance.
- Make npm registry readback the canonical, checksummed GitHub release artifact.

## 0.1.0 - 2026-08-15

- Initial `@workflow/world` implementation backed by celld Durable Objects.
- Durable workflow storage, streams, queues, retries, deduplication, and dead
  letters.
- Authenticated fleet RPC, deployable worker bundle, local development fleet,
  conformance tests, and the MinIO performance and loss harness.
