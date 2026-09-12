# Support Policy

The supported runtime and peer lines, mirroring the project's constitution and
the package's published peer ranges.

## Supported Lines

| Item | Supported |
| --- | --- |
| Node.js | `>=22` (`>=22.12` with NestJS 12 — see the note below the table) |
| NestJS | `^11.0.0 \|\| ^12.0.0` |
| `@confluentinc/kafka-javascript` | `^1.9` (pin the major; it tracks librdkafka) |
| TypeScript | `^6` |
| Validation | `class-validator` and Zod, both app-owned |

Both ends of the NestJS range are tested, not assumed. The default lockfile
keeps the suite on an 11.x in the middle of the range; the `nestjs-compat` CI
matrix installs each end on top of it and runs the unit suite and the sample
matrix. The oldest installable 11 graph we run is `11.0.0`, pinned exactly,
because nothing this package uses was added by a later 11.x; the other leg
floats on `^12.0.0`. See [Quality & CI](quality-and-ci.md) for how each leg
proves it is testing the tree it claims to.

The Node.js floor depends on which end of that range you are on. NestJS 11
runs on any Node.js `>=22`. NestJS 12 is ESM-only; loading it from CommonJS
code (this package, and every sample) goes through Node's `require(esm)`,
which is behind a flag before Node.js 22.12.0, so NestJS 12 needs Node.js
`>=22.12`. `engines` stays `>=22` because the 11 end does not need more;
CI's NestJS 12 leg runs on a current 22.x.

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
