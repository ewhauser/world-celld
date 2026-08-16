# Releasing

Release Please prepares versions and release notes. GitHub Actions builds the
package, publishes it through npm trusted publishing, verifies the registry
artifact, and only then publishes the Git tag and GitHub release. Do not publish
routine releases from a maintainer workstation.

## Preparing a release

Pull request titles become squash commit messages, so use Conventional Commits:

- `fix: ...` proposes a patch release.
- `feat: ...` proposes a minor release.
- `feat!: ...`, `fix!: ...`, or a `BREAKING CHANGE:` footer proposes a breaking
  release. Before 1.0, breaking releases increment the minor version.

Other commit types can be used, but do not create a release by themselves.
GitHub-generated changelog notes still include all merged pull requests since
the previous tag.

After a releasable commit reaches `main`, the **Release** workflow creates or
updates one Release Please pull request. That pull request updates
`package.json`, `CHANGELOG.md`, and `.release-please-manifest.json`. CI is
explicitly dispatched on the generated branch so the normal required checks
must pass without a personal access token.

Review the proposed version and release notes like any other pull request. Do
not manually edit the version files outside the generated release pull request.

## Publishing a release

1. Merge the Release Please pull request after its required checks pass.
2. The **Release** workflow creates a draft GitHub release and rebuilds the exact
   merge commit on a GitHub-hosted runner.
3. Review and approve the protected `release` environment deployment.
4. The workflow publishes the checksummed tarball through npm trusted
   publishing, reads the canonical package back from npm, and verifies its
   integrity and complete uncompressed tar stream.
5. Only after those checks succeed does the workflow attach the canonical
   tarball and checksum, create the tag, and publish the immutable GitHub
   release.

## Recovering a failed release

If the automated run fails after the Release Please pull request is merged,
rerun the failed job first. If a fresh run is required, manually dispatch the
**Release** workflow from `main` with the exact package version.

Recovery is idempotent. When a draft or existing release is present, the
workflow rebuilds its recorded target commit rather than the latest commit on
`main`. It accepts an existing npm version only when its complete uncompressed
tar stream matches the rebuilt package, then uses the registry's recorded
integrity. This permits harmless gzip-encoder differences without accepting
different package contents.

## One-time npm bootstrap

npm cannot configure trusted publishing until a package exists. The initial
`0.1.0` release was the only interactive bootstrap. Trusted publishing is
configured for repository `ewhauser/world-celld`, workflow `release.yml`,
environment `release`, with no long-lived npm publication token.
