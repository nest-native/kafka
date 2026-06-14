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

A driver-backed integration test runs against a real Kafka in CI through a
GitHub-Actions service container, and is skipped locally when `KAFKA_BROKERS` is
missing. The in-memory broker covers the same logic without native dependencies,
so the full suite runs anywhere.

## Supply Chain

The published package's `"dependencies"` block must stay empty; runtime
requirements are peers and build tools are devDependencies. Every dependency
change is reviewed for legitimacy, and install/lifecycle scripts are inspected.
Unpinned Git/URL dependencies are flagged. See [Contributing](contributing.md).
