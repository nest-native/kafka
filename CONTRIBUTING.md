# Contributing

Thanks for helping improve `@nest-native/kafka`.

## Project Status

This package is published and stable at `v0.1.x`. The full v1 transport surface
has shipped — the module (`KafkaModule.forRoot()` / `forRootAsync()` /
`forFeature()`), the `KafkaProducerService`, the consumer decorators
(`@KafkaConsumer`, `@KafkaHandler`) with the complete Nest enhancer pipeline, the
parameter decorators (`@KafkaMessage`, `@KafkaHeaders`, `@KafkaCtx`,
`@KafkaBatch`), error mapping, graceful shutdown, batch consumption with
per-topic concurrency, the transactional producer helper, the testing utilities
(`KafkaTestModule`, `InMemoryKafkaBroker`, `createMockKafkaProducer`), the sample
catalog, and the documentation site. Contributions now focus on bug fixes,
hardening, and incremental API additions.

`@confluentinc/kafka-javascript` is an **optional** peer dependency: the
published package keeps `"dependencies": {}` and never loads the native client
unless an application opens a real connection. It must be installed for real
(non-test) usage; the in-memory `KafkaTestModule` and `createMockKafkaProducer`
exercise the whole transport without it. The real-broker integration suite
(`packages/kafka/test/kafka.integration.spec.ts`) is gated on `KAFKA_BROKERS` and
runs in its own CI job against a single-node KRaft Kafka; it is skipped locally
unless `KAFKA_BROKERS` is set, so it never affects the 100% coverage gate. Run it
explicitly with `npm run test:integration` (which is separate from `npm run ci`).

## Sample Work Must Stay Separate From Library Fixes

Sample PRs are allowed to change sample code, docs, CI wiring, and release checks
that are directly needed for samples. They must not include changes under
`packages/kafka/**`.

If a sample exposes a package bug, stop the sample PR and use this workflow:

1. Stash the sample and docs work, including untracked files:

   ```bash
   git stash push -u -m "sample work before library fix"
   ```

2. Create a separate library-fix branch from `main`.
3. Fix the package bug with focused regression tests.
4. Run the package validation commands for that fix.
5. Open and merge the library-fix PR first.
6. Return to the sample branch and re-apply the stash:

   ```bash
   git stash pop
   ```

7. Before committing the sample PR, verify the touched package files list is
   empty:

   ```bash
   git diff --name-only main...HEAD -- packages/kafka
   git diff --cached --name-only -- packages/kafka
   ```

If either command prints files, split those package changes into a dedicated
library-fix PR before continuing the sample PR.

## Local Validation

Run the full local gate before opening a PR:

```bash
npm run ci
```

This runs typecheck, coverage (enforced at 100%), cognitive complexity checks,
release checks (README links and package tarball), and the supply-chain audit.

## Library-Fix PR Checklist

- The PR includes a regression test under `packages/kafka/test`.
- The PR does not include sample implementation work.
- `npm run test:cov` passes at 100% coverage.
- `npm run complexity:check` and `npm run complexity:report` pass when package
  source files are touched.
- The PR body includes a short security pass, reviewing any dependency or
  `peerDependencies` changes (the published `dependencies` must stay `{}`, and
  install/lifecycle scripts on any new dependency must be inspected).
