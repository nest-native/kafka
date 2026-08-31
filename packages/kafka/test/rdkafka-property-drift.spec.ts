import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { describe, it } from 'node:test';
import {
  KAFKAJS_SHARED_UNDOTTED_PROPERTIES,
  RDKAFKA_UNDOTTED_PROPERTIES,
} from '../driver';

/**
 * Drift guard for {@link RDKAFKA_UNDOTTED_PROPERTIES}.
 *
 * `splitDriverConfig` routes dotted names to `librdkafka` automatically, but the
 * dot rule is not total: the client declares undotted properties too, and one
 * left inside `kafkaJS` fails at `connect()` with "The '<name>' property is not
 * supported" — the failure the split exists to prevent. The list of undotted
 * names is therefore hand-maintained, which is exactly the kind of table that
 * rots silently when a dependency adds a property.
 *
 * This test removes that risk by reading the *installed* client's own type
 * definitions and requiring every undotted property it declares to be
 * classified: either routed to `librdkafka`, or explicitly recorded as one the
 * KafkaJS layer accepts. A new property in a future client release fails here
 * rather than in a user's application.
 *
 * The Confluent client is an optional peer, so this is skipped when it is not
 * installed — the same contract the rest of the suite honours.
 */
function readConfigTypes(): string | undefined {
  try {
    const require = createRequire(__filename);
    const entry = require.resolve('@confluentinc/kafka-javascript');
    const typesPath = entry.replace(
      /(\/|\\)(lib|index\.js).*$/,
      '/types/config.d.ts',
    );
    return readFileSync(typesPath, 'utf8');
  } catch {
    return undefined;
  }
}

const configTypes = readConfigTypes();

describe('librdkafka undotted property drift', { skip: !configTypes }, () => {
  /** Undotted, quoted property names declared by the installed client. */
  function declaredUndottedProperties(source: string): string[] {
    const matches = source.matchAll(/^\s+"([a-z_0-9]+)"\??:/gm);
    return [...new Set([...matches].map(match => match[1]))].sort();
  }

  it('classifies every undotted property the installed client declares', () => {
    const declared = declaredUndottedProperties(configTypes as string);

    // Guard against the extraction silently matching nothing and making the
    // assertion below vacuous.
    assert.ok(
      declared.length > 20,
      `expected the client to declare many undotted properties, found ${declared.length}`,
    );

    const unclassified = declared.filter(
      name =>
        !RDKAFKA_UNDOTTED_PROPERTIES.has(name) &&
        !KAFKAJS_SHARED_UNDOTTED_PROPERTIES.has(name),
    );

    assert.deepEqual(
      unclassified,
      [],
      `@confluentinc/kafka-javascript declares undotted properties this package does not classify: ${unclassified.join(', ')}. ` +
        'Add each to RDKAFKA_UNDOTTED_PROPERTIES (routed to librdkafka), or to ' +
        'KAFKAJS_SHARED_UNDOTTED_PROPERTIES if the KafkaJS layer accepts it too.',
    );
  });

  it('never routes a name the KafkaJS layer accepts', () => {
    const overlap = [...KAFKAJS_SHARED_UNDOTTED_PROPERTIES].filter(name =>
      RDKAFKA_UNDOTTED_PROPERTIES.has(name),
    );
    assert.deepEqual(
      overlap,
      [],
      `these names are in both sets, so routing would break working config: ${overlap.join(', ')}`,
    );
  });
});
