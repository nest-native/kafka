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
5. Tag the release (for `0.3.0`, a lightweight `v0.3.0` tag on `main`).

## The 0.x Release Line

The current published version is `0.3.0`. The `0.x` line covers the module, the
producer service, consumer decorators with the full enhancer pipeline, the
parameter decorators, error mapping, batch consumption with per-topic concurrency,
the transactional producer, the testing utilities, the migration guide, and this
documentation site.

What each release since `0.1.0` added:

- `0.1.1` — a real-broker CI integration suite gated on `KAFKA_BROKERS`, plus a
  documentation-truth pass. No public API changes.
- `0.2.0` — **breaking (testing entrypoint)**: `KafkaTestModule`,
  `InMemoryKafkaBroker`, `createMockKafkaProducer`/`createMockTransaction`,
  `InjectKafkaTestBroker`, and `KAFKA_TEST_BROKER` moved out of the package root
  into `@nest-native/kafka/testing`. Runtime exports are unchanged.
- `0.3.0` — `InMemoryKafkaBroker.idle()`, an awaitable settle point that replaces
  fixed sleeps in tests built on `KafkaTestModule`.

Per semver, `0.x` minor releases can include breaking changes — pin a version. See
the [support policy](support-policy.md). `CHANGELOG.md` is the authoritative
per-release record.

## Tarball Contract

The published package keeps `"dependencies": {}`. The Confluent client and the
NestJS packages are peers. `release:check` validates the tarball contents so only
the built `dist/` artifacts ship. See [Quality and CI](quality-and-ci.md).
