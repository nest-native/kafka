# Release Guide

The package follows semantic versioning. Releases are coordinated with the sample
tree and the documentation site.

## Version Synchronization (Mandatory)

Version drift between `packages/kafka` and `sample/*` is a release blocker. When
bumping `packages/kafka/package.json`:

1. Update every `sample/*/package.json` entry for `@nest-native/kafka` to the new
   version in the same change.
2. Regenerate `package-lock.json`.
3. Run `npm run release:check`.
4. Run `npm run ci`.

`release:check` validates README links, sample-version sync, and the package
tarball, so a drifted version fails the gate before it can be published.

## Release Steps

1. Land all milestone work on `main` through reviewed pull requests.
2. Bump `packages/kafka/package.json` and the sample versions together.
3. Update `CHANGELOG.md`: move the `Unreleased` entries under the new version.
4. Run `npm run ci` and confirm green on Node 20 and 22.
5. Tag the release (for `0.1.0`, a lightweight `v0.1.0` tag on `main`).

## The 0.1.0 Release

The first published version is `0.1.0`. The initial `0.x` release covers the module,
the producer service, consumer decorators with the full enhancer pipeline, the
parameter decorators, error mapping, batch consumption with per-topic concurrency,
the transactional producer, `KafkaTestModule`, the migration guide, and this
documentation site.

## Tarball Contract

The published package keeps `"dependencies": {}`. The Confluent client and the
NestJS packages are peers. `release:check` validates the tarball contents so only
the built `dist/` artifacts ship. See [Quality and CI](quality-and-ci.md).
