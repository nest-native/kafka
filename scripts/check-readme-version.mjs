#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const repoRoot = process.cwd();
const statusPattern = /\*\*Status: `(\d+\.\d+\.\d+)`(?: \(`(\d+)\.x`\))?/g;
const contributingPattern = /published at `(\d+\.\d+)\.x`/g;
const hardcodedBadgePattern = /img\.shields\.io\/badge\/(?:version|status)-[^)\s"']*/g;
const publishedPackages = collectPublishedPackages();
const failures = [];
let checkedLiterals = 0;

if (publishedPackages.length === 0) {
  throw new Error(
    'README version check found no publishable package under packages/*; ' +
      'every non-private workspace package must declare a version.',
  );
}

const knownVersions = publishedPackages.map(publishedPackage => publishedPackage.version);
const scannedFiles = ['README.md', 'CONTRIBUTING.md'];

for (const publishedPackage of publishedPackages) {
  const readmePath = path.join('packages', publishedPackage.directoryName, 'README.md');
  if (fs.existsSync(path.join(repoRoot, readmePath))) {
    scannedFiles.push(readmePath);
    checkStatusLiterals(readmePath, [publishedPackage.version]);
  }
}

checkStatusLiterals('README.md', knownVersions);
checkReleaseLineLiterals('CONTRIBUTING.md', knownVersions);

for (const relativeFilePath of scannedFiles) {
  checkHardcodedBadges(relativeFilePath);
}

if (checkedLiterals === 0) {
  throw new Error(
    'README version check found no version literal to verify. The "**Status: `X.Y.Z`" ' +
      'convention changed — update scripts/check-readme-version.mjs so the gate keeps biting.',
  );
}

if (failures.length > 0) {
  throw new Error(`README version sync failed:\n${failures.join('\n')}`);
}

console.log(
  `README version sync OK: ${checkedLiterals} version literals across ${scannedFiles.length} ` +
    `files match ${knownVersions.join(', ')}.`,
);

function checkStatusLiterals(relativeFilePath, expectedVersions) {
  for (const match of matchFile(relativeFilePath, statusPattern)) {
    const [literal, version, releaseLineMajor] = match;
    checkedLiterals += 1;

    if (!expectedVersions.includes(version)) {
      failures.push(
        `${relativeFilePath}: "${literal}" declares ${version}; expected ${expectedVersions.join(
          ' or ',
        )}`,
      );
      continue;
    }

    const major = version.split('.')[0];
    if (releaseLineMajor !== undefined && releaseLineMajor !== major) {
      failures.push(
        `${relativeFilePath}: "${literal}" pairs ${version} with the \`${releaseLineMajor}.x\` release line; expected \`${major}.x\``,
      );
    }
  }
}

function checkReleaseLineLiterals(relativeFilePath, expectedVersions) {
  const expectedReleaseLines = expectedVersions.map(version =>
    version.split('.').slice(0, 2).join('.'),
  );

  for (const match of matchFile(relativeFilePath, contributingPattern)) {
    const [literal, releaseLine] = match;
    checkedLiterals += 1;

    if (!expectedReleaseLines.includes(releaseLine)) {
      failures.push(
        `${relativeFilePath}: "${literal}" declares the ${releaseLine}.x line; expected ${expectedReleaseLines
          .map(expected => `${expected}.x`)
          .join(' or ')}`,
      );
    }
  }
}

function checkHardcodedBadges(relativeFilePath) {
  for (const match of matchFile(relativeFilePath, hardcodedBadgePattern)) {
    failures.push(
      `${relativeFilePath}: hardcoded version badge "${match[0]}" — use a dynamic badge ` +
        '(https://img.shields.io/npm/v/<package>.svg) so it cannot drift',
    );
  }
}

function matchFile(relativeFilePath, pattern) {
  const absoluteFilePath = path.join(repoRoot, relativeFilePath);
  if (!fs.existsSync(absoluteFilePath)) {
    return [];
  }

  return [...fs.readFileSync(absoluteFilePath, 'utf8').matchAll(pattern)];
}

function collectPublishedPackages() {
  const packagesRoot = path.join(repoRoot, 'packages');
  if (!fs.existsSync(packagesRoot)) {
    return [];
  }

  return fs
    .readdirSync(packagesRoot, { withFileTypes: true })
    .filter(entry => entry.isDirectory())
    .map(entry => ({
      directoryName: entry.name,
      manifestPath: path.join('packages', entry.name, 'package.json'),
    }))
    .filter(candidate => fs.existsSync(path.join(repoRoot, candidate.manifestPath)))
    .map(candidate => ({ ...candidate, manifest: readJson(candidate.manifestPath) }))
    .filter(candidate => candidate.manifest.private !== true)
    .map(candidate => ({
      directoryName: candidate.directoryName,
      name: candidate.manifest.name,
      version: candidate.manifest.version,
    }))
    .sort((left, right) => left.directoryName.localeCompare(right.directoryName));
}

function readJson(relativePath) {
  return JSON.parse(fs.readFileSync(path.join(repoRoot, relativePath), 'utf8'));
}
