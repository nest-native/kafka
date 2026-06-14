# Contributing

Thanks for helping improve `@nest-native/kafka`.

## Project Status

This package is at its bootstrap milestone (`v0.0.1-scaffold`). The public
transport primitives — the consumer decorators (`@KafkaConsumer`,
`@KafkaHandler`), the parameter decorators (`@KafkaMessage`, `@KafkaHeaders`,
`@KafkaContext`), the producer service, and the sample catalog — land in later
milestones. Until then, contributions focus on the workspace skeleton, the
`KafkaModule` shell, CI, and project documentation.

## Sample Work Must Stay Separate From Library Fixes

Once samples exist, sample PRs are allowed to change sample code, docs, CI
wiring, and release checks that are directly needed for samples. They must not
include changes under `packages/kafka/**`.

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
