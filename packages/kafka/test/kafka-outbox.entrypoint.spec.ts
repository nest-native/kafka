import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import path from 'node:path';
import { describe, it } from 'node:test';

/**
 * `@nestjs/outbox` is an optional peer, imported only by the
 * `@nest-native/kafka/outbox` entrypoint, so an application that never installed
 * it can load everything else. Each case loads one entrypoint in a fresh Node
 * process in which `@nestjs/outbox` cannot be resolved, as in such an
 * application. A fresh process because this one has loaded the outbox already:
 * `test:cov` runs every spec in a single process.
 */
const packageDir = path.resolve(__dirname, '..');
const repoRoot = path.resolve(packageDir, '..', '..');

const LOAD_WITHOUT_OUTBOX = `
const Module = require('node:module');
const resolve = Module._resolveFilename;
Module._resolveFilename = function (request, ...rest) {
  if (request === '@nestjs/outbox' || request.startsWith('@nestjs/outbox/')) {
    const error = new Error("Cannot find module '" + request + "'");
    error.code = 'MODULE_NOT_FOUND';
    throw error;
  }
  return resolve.call(this, request, ...rest);
};
let result;
try {
  require(process.env.OUTBOX_ENTRYPOINT);
  result = { loaded: true };
} catch (error) {
  result = { loaded: false, error: String(error && error.message) };
}
process.stdout.write('\\n' + JSON.stringify(result) + '\\n');
`;

function loadWithoutOutbox(entrypoint: string): { loaded: boolean; error?: string } {
  const { NODE_TEST_CONTEXT: _, ...env } = process.env;
  const output = execFileSync(
    process.execPath,
    ['--require', 'ts-node/register', '--require', 'reflect-metadata', '-e', LOAD_WITHOUT_OUTBOX],
    {
      cwd: repoRoot,
      encoding: 'utf8',
      env: {
        ...env,
        // Not a coverage source: the child compiles the sources without type
        // information, so its V8 coverage would not line up with this process's
        // under `test:cov`. Node copies the variable into a child's environment
        // unless the child's sets it, and an empty one turns coverage off.
        NODE_V8_COVERAGE: '',
        OUTBOX_ENTRYPOINT: path.join(packageDir, entrypoint),
        TS_NODE_PROJECT: 'tsconfig.spec.json',
        // Loading, not type checking, is what this proves; TypeScript 6 wants the
        // root directory named when it compiles file by file.
        TS_NODE_TRANSPILE_ONLY: 'true',
        TS_NODE_COMPILER_OPTIONS: JSON.stringify({ rootDir: repoRoot }),
      },
    },
  );
  const lines = output.trim().split('\n');
  return JSON.parse(lines[lines.length - 1]);
}

describe('the @nestjs/outbox optional peer', () => {
  it('is not needed to load the package root', () => {
    assert.deepEqual(loadWithoutOutbox('index.ts'), { loaded: true });
  });

  it('is not needed to load the testing entrypoint', () => {
    assert.deepEqual(loadWithoutOutbox('testing/index.ts'), { loaded: true });
  });

  it('is required by the outbox entrypoint, which shows the cases above really ran without it', () => {
    const result = loadWithoutOutbox('outbox/index.ts');

    assert.equal(result.loaded, false);
    assert.match(String(result.error), /Cannot find module '@nestjs\/outbox'/);
  });
});
