import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const npmExecutable = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const repoRoot = process.cwd();
const tempCache = fs.mkdtempSync(path.join(os.tmpdir(), 'nest-native-kafka-pack-'));

try {
  const rawOutput = execFileSync(
    npmExecutable,
    ['pack', '--dry-run', '--json', '--workspace', '@nest-native/kafka'],
    {
      cwd: repoRoot,
      encoding: 'utf8',
      env: {
        ...process.env,
        npm_config_cache: tempCache,
      },
    },
  );

  const [packResult] = JSON.parse(rawOutput);
  if (!packResult || !Array.isArray(packResult.files)) {
    throw new Error('npm pack --dry-run did not return the expected JSON payload.');
  }

  const filePaths = packResult.files.map(file => file.path);
  // Every file the manifest points a consumer at — `main`, `types`, and each
  // target of each `exports` entry (`.`, `./testing`, `./outbox`, ...) — must be
  // in the tarball, so a new entrypoint cannot ship without its files.
  const manifest = JSON.parse(
    fs.readFileSync(path.join(repoRoot, 'packages', 'kafka', 'package.json'), 'utf8'),
  );
  const requiredFiles = [
    ...new Set([
      'README.md',
      'package.json',
      ...[manifest.main, manifest.types, ...collectExportTargets(manifest.exports)].map(
        target => target.replace(/^\.\//, ''),
      ),
    ]),
  ];
  const missingFiles = requiredFiles.filter(filePath => !filePaths.includes(filePath));

  if (missingFiles.length > 0) {
    throw new Error(`Packed tarball is missing required files: ${missingFiles.join(', ')}`);
  }

  const unexpectedFiles = filePaths.filter(filePath => {
    if (filePath === 'LICENSE' || filePath === 'README.md' || filePath === 'package.json') {
      return false;
    }

    return !/^dist\/.+\.(d\.ts|js|js\.map)$/.test(filePath);
  });

  if (unexpectedFiles.length > 0) {
    throw new Error(
      `Packed tarball contains unexpected files:\n${unexpectedFiles
        .map(filePath => `- ${filePath}`)
        .join('\n')}`,
    );
  }

  console.log(
    `Pack validation OK: ${packResult.filename} contains ${packResult.entryCount} expected files, ` +
      `including every file the manifest's entrypoints name.`,
  );
} finally {
  fs.rmSync(tempCache, { recursive: true, force: true });
}

/** Every file path in an `exports` map, through its condition objects. */
function collectExportTargets(exportsField) {
  if (typeof exportsField === 'string') {
    return [exportsField];
  }
  if (exportsField === null || typeof exportsField !== 'object') {
    return [];
  }
  return Object.values(exportsField).flatMap(collectExportTargets);
}
