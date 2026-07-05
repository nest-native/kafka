// Mutation testing — LOCAL ONLY, on demand. Deliberately not wired into CI.
// See GUIDELINES_NEST_KAFKA.md, "Local Full-Mode Verification".
//
//   npm run test:mutation                          incremental (the pre-PR ritual)
//   npm run test:mutation:full                     every mutant from scratch
//   STRYKER_MUTATE='packages/kafka/kafka-dispatcher.ts'  scope to the files
//                                                  you changed (comma-separated)
//   STRYKER_WITH_INFRA=1                           run the real-broker
//                                                  integration suite per mutant
//                                                  too (`npm run infra:up`
//                                                  first; forces concurrency 1)
const withInfra = process.env.STRYKER_WITH_INFRA === '1';

/** @type {import('@stryker-mutator/api/core').PartialStrykerOptions} */
export default {
  mutate: process.env.STRYKER_MUTATE
    ? process.env.STRYKER_MUTATE.split(',')
    : ['packages/kafka/**/*.ts', '!packages/kafka/test/**'],
  testRunner: 'command',
  // `test:mutant` = the normal suite plus `--test-force-exit`: a mutant that
  // breaks teardown would otherwise leave open handles and turn every kill
  // into a slow timeout.
  commandRunner: {
    command: withInfra ? 'npm run test:mutant:full' : 'npm run test:mutant',
  },
  // Each command-runner mutant already runs the suite's test files in
  // parallel (node --test spawns ~one ts-node child per spec file — a single
  // run burns ~80s of CPU in ~7s of wall time on 16 cores), so high Stryker
  // concurrency oversubscribes the CPU and turns every kill into a timeout
  // (measured: concurrency 4 → 28/30 timeouts; concurrency 2 → 0). With
  // infra, concurrency must be 1 — the integration specs share the one
  // broker.
  concurrency: withInfra ? 1 : 2,
  timeoutMS: 15000,
  incremental: true,
  ignorePatterns: ['sample', 'website', 'docs', 'coverage', '**/dist'],
  reporters: ['clear-text', 'progress', 'html'],
  thresholds: { high: 90, low: 80, break: null },
  tempDirName: '.stryker-tmp',
};
