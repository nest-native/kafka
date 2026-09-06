# Support Policy

The supported runtime and peer lines, mirroring the project's constitution and
the package's published peer ranges.

## Supported Lines

| Item | Supported |
| --- | --- |
| Node.js | `>=22` |
| NestJS | `^11.0.0 \|\| ^12.0.0` |
| `@confluentinc/kafka-javascript` | `^1.9` (pin the major; it tracks librdkafka) |
| TypeScript | `^6` |
| Validation | `class-validator` and Zod, both app-owned |

Both ends of the NestJS range are tested, not assumed: the default lockfile
keeps the suite on 11.x, and a dedicated CI leg installs `@nestjs/*@^12` on
top of it and runs the unit suite and the sample matrix. NestJS 12 is
ESM-only; loading it from CommonJS code (this package, and every sample)
goes through Node's `require(esm)`, which is unflagged on Node.js `>=22.12`
(and `>=20.19`), so run NestJS 12 on a current Node 22 or 24.

## Peer Dependencies

The published package keeps `"dependencies": {}`. Everything the runtime needs is
a peer, so applications install only the ecosystems they use:

- `@confluentinc/kafka-javascript` is an **optional** peer — it is loaded only when
  you open a real broker connection. Unit tests on `KafkaTestModule` never load it.
- `class-validator` and Zod are **optional** peers — install whichever validator
  your app uses, or neither.
- `@nestjs/common`, `@nestjs/core`, `@nestjs/microservices`, `reflect-metadata`,
  and `rxjs` are required peers.

This keeps the supply chain lean and the install matrix under the host
application's control. See [Quality and CI](quality-and-ci.md) for how the empty
runtime-dependency contract is enforced.

## librdkafka Note

`@confluentinc/kafka-javascript` ships a native `librdkafka` binding. Alpine,
Windows, and ARM64 each have their own install considerations. Because the client
is an optional peer, you only take on that install when you actually connect to a
broker — local development and the test suite run entirely on the in-memory
broker.

## Upgrade Contract

The Confluent client tracks librdkafka, so the package pins to a major
(`^1.9`) and documents behavioral deltas on upgrade. Treat a Confluent major bump
as a coordinated change.
