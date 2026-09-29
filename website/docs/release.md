# Release Guide

The package follows semantic versioning. Releases are coordinated with the sample
tree and the documentation site.

## Version Synchronization (Mandatory)

Version drift between `packages/kafka` and `sample/*` is a release blocker. When
bumping `packages/kafka/package.json`:

1. Update every `sample/*/package.json` entry for `@nest-native/kafka` to the new
   version in the same change.
2. Update the version literals in prose — the **Status** line in `README.md` and
   `packages/kafka/README.md`, the published release line in `CONTRIBUTING.md`,
   and this page.
3. Regenerate `package-lock.json`.
4. Run `npm run release:check`.
5. Run `npm run ci`.

`release:check` validates README links, README/CONTRIBUTING version literals,
sample-version sync, and the package tarball, so a drifted version fails the gate
before it can be published. Version badges must stay dynamic
(`img.shields.io/npm/v/...`) — `release:check:readme-version` rejects hardcoded
`img.shields.io/badge/version-…` and `badge/status-…` badges outright.

## Release Steps

1. Land all milestone work on `main` through reviewed pull requests.
2. Bump `packages/kafka/package.json` and the sample versions together.
3. Update `CHANGELOG.md`: move the `Unreleased` entries under the new version.
4. Run `npm run ci` on Node 22 and confirm CI is green, including the
   Node 24 build and typecheck leg.
5. Push a lightweight `vX.Y.Z` tag on `main`: `release.yml` builds the package
   and publishes it to npm with provenance. Nothing else publishes.

## The 0.x Release Line

The current published version is `0.6.0`. `release:check` fails when this line
disagrees with `packages/kafka/package.json`, as it does for the README status
lines and the `CONTRIBUTING.md` release line.

What each release added — and which `0.x` minors changed behaviour — lives in one
place, [`CHANGELOG.md`](https://github.com/nest-native/kafka/blob/main/CHANGELOG.md).
This page used to keep a second list; it stopped at `0.3.0` while three releases
shipped after it, so it is gone rather than kept in sync by hand.

Per semver, `0.x` minor releases can include breaking changes — pin a version. See
the [support policy](support-policy.md).

## Tarball Contract

The published package keeps `"dependencies": {}`. The Confluent client and the
NestJS packages are peers. `release:check` validates the tarball contents so only
the built `dist/` artifacts ship. See [Quality and CI](quality-and-ci.md).
