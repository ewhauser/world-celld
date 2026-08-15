# Releasing

Releases are built and published by GitHub Actions. Do not publish routine
releases from a maintainer workstation.

## One-time npm bootstrap

npm cannot configure trusted publishing until a package exists. The initial
`0.1.0` release is therefore the only interactive bootstrap:

1. Run `pnpm check`, `pnpm audit --audit-level high`, and `pnpm pack:release`.
2. Inspect the tarball and checksum in `release-artifacts/`.
3. Publish that exact tarball with an npm account protected by 2FA:
   `npm publish ./release-artifacts/ewhauser-world-celld-0.1.0.tgz --access public --ignore-scripts`.
4. Create `v0.1.0` as an immutable GitHub release at the exact source commit and
   attach the tarball and checksum.
5. Configure npm trusted publishing for repository `ewhauser/world-celld`,
   workflow `release.yml`, environment `release`, and `npm publish` permission:
   `npm trust github @ewhauser/world-celld --repo ewhauser/world-celld --file release.yml --env release --allow-publish`.
6. Require 2FA and disallow traditional tokens in the package publishing
   settings, then revoke any temporary npm CLI session.

## Subsequent releases

1. Open a pull request that updates `package.json` and `CHANGELOG.md` to the
   intended semantic version.
2. Merge only after CI and review succeed.
3. Run the **Release** workflow from `main`, entering the exact package version.
4. Review and approve the protected `release` environment deployment.
5. Verify the npm provenance, registry integrity, tag target, attached checksum,
   and immutable GitHub release.

The workflow is idempotent for recovery. It accepts an existing npm version
only when its complete uncompressed tar stream matches the freshly built
package, then uses the registry's recorded integrity. This permits harmless
gzip-encoder differences without accepting different package contents. It will
read the package back from npm so the registry tarball's exact bytes become the
GitHub release artifact, then finish a missing or draft GitHub release without
republishing different bytes.
