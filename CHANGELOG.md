# Changelog

All notable changes to this project will be documented in this file.

## 0.1.1 - 2026-08-15

- Publish through npm trusted publishing with OIDC provenance.
- Make npm registry readback the canonical, checksummed GitHub release artifact.

## 0.1.0 - 2026-08-15

- Initial `@workflow/world` implementation backed by celld Durable Objects.
- Durable workflow storage, streams, queues, retries, deduplication, and dead
  letters.
- Authenticated fleet RPC, deployable worker bundle, local development fleet,
  conformance tests, and the MinIO performance and loss harness.
