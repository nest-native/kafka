import assert from 'node:assert/strict';
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import { describe, it } from 'node:test';

/**
 * Guard for every deep import into `@nestjs/*`.
 *
 * The package reaches into `@nestjs/core` internals (context creators, guard
 * and pipe consumers, `STATIC_CONTEXT`, ...) to run the real enhancer pipeline,
 * and there is no public entry point for most of them. NestJS 11 is CommonJS
 * without an exports map, so any deep path resolves — including a *directory*
 * such as `@nestjs/common/interfaces`, which Node's CJS resolver completes to
 * `interfaces/index.js`. NestJS 12 is ESM-only with an exports map of
 * `"./*": "./*.js"`: a deep import of a *file* still resolves
 * (`@nestjs/core/injector/constants` -> `injector/constants.js`), a deep import
 * of a directory does not — there is no `interfaces.js`, and ESM never
 * completes a directory to its index.
 *
 * That difference is invisible on the default (11) install, which is why the
 * unit suite alone could not catch it. This test scans the package's own `.ts`
 * sources for every `@nestjs/<pkg>/<subpath>` import and requires `<subpath>`
 * to name a real file (`.js`, `.ts`, or `.d.ts`) inside the installed package,
 * never a directory. It runs on whichever NestJS major is installed, and fails
 * on both for a directory import.
 */
const packageDir = path.resolve(__dirname, '..');
const packageRequire = createRequire(path.join(packageDir, 'index.ts'));
const IGNORED_DIRECTORIES = new Set(['node_modules', 'dist']);
const DEEP_IMPORT_PATTERN =
  /(?:\bfrom\s*|\brequire\(\s*|\bimport\(\s*)['"]@nestjs\/([^'"/]+)\/([^'"]+)['"]/g;

/** Every `.ts` source under `packages/kafka`, tests and testing utilities included. */
function collectSourceFiles(): string[] {
  return readdirSync(packageDir, { recursive: true, withFileTypes: true })
    .filter(entry => entry.isFile() && entry.name.endsWith('.ts'))
    .map(entry => path.join(entry.parentPath, entry.name))
    .filter(file => {
      const segments = path.relative(packageDir, file).split(path.sep);
      return !segments.some(segment => IGNORED_DIRECTORIES.has(segment));
    })
    .sort();
}

/** `@nestjs/<pkg>/<subpath>` specifier -> the source files importing it. */
function collectDeepImports(files: string[]): Map<string, string[]> {
  const imports = new Map<string, string[]>();
  for (const file of files) {
    const source = readFileSync(file, 'utf8');
    for (const match of source.matchAll(DEEP_IMPORT_PATTERN)) {
      const specifier = `@nestjs/${match[1]}/${match[2]}`;
      const importers = imports.get(specifier) ?? [];
      importers.push(path.relative(packageDir, file));
      imports.set(specifier, importers);
    }
  }
  return imports;
}

/** The installed root of `@nestjs/<name>`, resolved the way the package's own code resolves it. */
function resolvePackageRoot(name: string): string {
  let directory = path.dirname(packageRequire.resolve(name));
  while (true) {
    const manifest = path.join(directory, 'package.json');
    if (existsSync(manifest)) {
      const { name: manifestName } = JSON.parse(readFileSync(manifest, 'utf8'));
      if (manifestName === name) {
        return directory;
      }
    }
    const parent = path.dirname(directory);
    assert.notEqual(parent, directory, `could not locate the root of ${name}`);
    directory = parent;
  }
}

function describeTarget(root: string, subpath: string): string {
  const target = path.join(root, subpath);
  if (!existsSync(target)) {
    return 'nothing at that path';
  }
  return statSync(target).isDirectory()
    ? 'a DIRECTORY — NestJS 12 (ESM-only, exports map "./*": "./*.js") does not resolve a directory index'
    : 'a file without a .js/.ts/.d.ts sibling';
}

describe('@nestjs/* deep imports', () => {
  const deepImports = collectDeepImports(collectSourceFiles());

  it('finds the deep imports the enhancer pipeline depends on', () => {
    // Guard against the scan silently matching nothing and making the check
    // below vacuous: the package cannot run enhancers without reaching into
    // @nestjs/core, so an empty result means the scanner broke, not the code.
    assert.ok(
      deepImports.size > 0,
      'expected at least one @nestjs/<pkg>/<subpath> import under packages/kafka',
    );
  });

  it('only imports paths that are files inside the installed @nestjs package', () => {
    const offenders: string[] = [];

    for (const [specifier, importers] of deepImports) {
      const [, packageName, ...rest] = specifier.split('/');
      const root = resolvePackageRoot(`@nestjs/${packageName}`);
      const subpath = rest.join('/').replace(/\.js$/, '');
      const resolvesToFile = ['.js', '.ts', '.d.ts'].some(extension =>
        existsSync(path.join(root, `${subpath}${extension}`)),
      );

      if (!resolvesToFile) {
        offenders.push(
          `${specifier} (imported by ${importers.join(', ')}) resolves to ${describeTarget(root, subpath)}`,
        );
      }
    }

    assert.deepEqual(
      offenders,
      [],
      'Every deep import into @nestjs/* must name a file, never a directory:\n' +
        offenders.join('\n') +
        '\nImport the file that declares the symbol, or declare a local alias ' +
        '(see `Controller` in kafka-params.resolver.ts).',
    );
  });
});
