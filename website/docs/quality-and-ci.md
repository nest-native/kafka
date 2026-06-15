# Quality and CI

The package ships with the same quality bar as the rest of the nest-native
family. `npm run ci` runs the whole gate locally; CI runs it on Node 20 and 22.

## The Gate

`npm run ci` chains:

| Step | What it checks |
| --- | --- |
| `typecheck` | The package and every sample type-check. |
| `test:cov` | `node:test` + `c8` with **100% statements, branches, functions, and lines** enforced on the package source. |
| `complexity:check` | ESLint + SonarJS cognitive-complexity threshold of `15` per source function. |
| `complexity:report` | Generates the per-function complexity summary. |
| `release:check` | README link validation, sample-version sync, and package tarball validation. |
| `security:audit` | A high-severity supply-chain audit. |
| `ci:sample` | Runs every sample (typecheck + smoke) against the in-memory broker. |

## Coverage

Coverage is enforced at 100% on all four metrics. Every branch — every `??`, every
option path, every error path — has a test. The CI posts a sticky coverage comment
on each pull request.

## Cognitive Complexity

SonarJS enforces a cognitive-complexity threshold of `15` per source function.
Complexity is never reduced by weakening the Nest-native architecture, the public
API clarity, rebalance safety, or test coverage.

## Release Version Synchronization

Version drift between `packages/kafka` and `sample/*` is a release blocker. When
the package version bumps, every `sample/*/package.json` entry for
`@nest-native/kafka` updates in the same change, `package-lock.json` is
regenerated, and `release:check` validates the sync. See the [Release Guide](release.md).

## Driver-Backed Integration

A dedicated `integration` CI job stands up a single-node **KRaft** Kafka
(`apache/kafka`) and runs `npm run test:integration` against it with
`KAFKA_BROKERS=localhost:9092`. That suite (`packages/kafka/test/kafka.integration.spec.ts`)
opens a real connection through `createConfluentDriver` and the native
`@confluentinc/kafka-javascript` client to prove the behaviour the in-memory
broker cannot: a real produce → consume round-trip, a transactional commit via
`KafkaProducerService.transactional`, and per-topic concurrency with durable
offset commits (a fresh consumer in the same group is not redelivered
already-committed messages). Every topic and group name is unique per run.

The suite is **gated on `KAFKA_BROKERS`**: it is skipped when the variable is
unset, so it never runs during `npm run test:cov` and the 100% coverage gate is
unaffected. `npm run test:integration` is separate from the `ci` script and is
not part of the standard local gate — the in-memory broker covers the same
transport logic without native dependencies, so the rest of the suite runs
anywhere.

`@confluentinc/kafka-javascript` stays an **optional** peer (the published
package keeps `"dependencies": {}`), so the integration job installs it
on-demand with `npm i --no-save` and never persists it to `package.json` or the
lockfile.

## Supply Chain

The published package's `"dependencies"` block must stay empty; runtime
requirements are peers and build tools are devDependencies. Every dependency
change is reviewed for legitimacy, and install/lifecycle scripts are inspected.
Unpinned Git/URL dependencies are flagged. See [Contributing](contributing.md).
