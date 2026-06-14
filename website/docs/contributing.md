# Contributing

Thanks for helping improve `@nest-native/kafka`. The canonical guide is
[CONTRIBUTING.md](https://github.com/nest-native/kafka/blob/main/CONTRIBUTING.md)
in the repository; this page summarizes the rules that shape every pull request.

## Local Validation

Run the full local gate before opening a PR:

```bash
npm run ci
```

This runs typecheck, coverage (enforced at 100%), cognitive-complexity checks,
release checks (README links, sample-version sync, and the package tarball), the
supply-chain audit, and the samples. Local green is required before pushing. See
[Quality and CI](quality-and-ci.md).

## Sample Work Stays Separate From Library Fixes

Sample PRs may change sample code, docs, CI wiring, and the release checks samples
need. They must **not** include changes under `packages/kafka/**`. Before
committing a sample PR, confirm the touched package-file list is empty:

```bash
git diff --name-only main...HEAD -- packages/kafka
git diff --cached --name-only -- packages/kafka
```

If a sample exposes a package bug, split the fix into a dedicated library-fix PR
first, then return to the sample.

## Security Review (Mandatory)

Every PR includes an explicit security pass:

- Dependency additions/updates are reviewed for legitimacy, and install/lifecycle
  scripts are inspected.
- The published `packages/kafka/package.json` keeps `"dependencies": {}`. Runtime
  requirements are peers; build tools are devDependencies.
- Auth/authz in handlers, input-validation gaps in deserialization, and
  SSL/SASL credential handling are reviewed. Secrets never appear in samples,
  logs, or docs.

## Cognitive Complexity

When a change touches `packages/kafka/**/*.ts`, run `npm run complexity:check`
and `npm run complexity:report`. CI enforces the SonarJS threshold of `15` per
source function. Complexity is never reduced by weakening the architecture, API
clarity, rebalance safety, or coverage.

## Pull Requests

Use the repository's pull request template. Keep changes focused, write
imperative-mood commit messages, and check off the scope, validation, and
security sections.
