# Security Policy

Thank you for helping keep `@nest-native/kafka` safe for NestJS and Kafka
applications.

## Supported Versions

Security fixes target the current published package line.

| Package | Supported |
| --- | --- |
| `@nest-native/kafka` latest minor | Yes |
| Older unpublished branches | No |

## Reporting A Vulnerability

Please do not open a public issue for vulnerabilities or suspected secret
leakage.

Use GitHub's private vulnerability reporting for this repository when available:

<https://github.com/nest-native/kafka/security/advisories/new>

If private reporting is unavailable, contact the maintainer through the GitHub
profile and include only the minimum information needed to establish a private
channel. Do not send exploit details, credentials, SSL/SASL secrets, API keys,
broker tokens, or customer data in public comments.

## What To Include

Private reports are most useful when they include:

- Affected package version or commit.
- NestJS, `@confluentinc/kafka-javascript`, and Node.js versions.
- The smallest reproduction or vulnerable code path.
- Expected impact, such as authorization bypass through a consumer handler,
  input-validation gaps in message deserialization, secret leakage in payloads
  or headers, dependency confusion, or incorrect offset-commit / rebalance
  behavior.
- Whether the issue affects package code, samples, docs, CI, or release
  automation.

Please redact secrets, hostnames, broker credentials, tokens, API keys,
SSL/SASL material, and private customer data.

## Project Security Boundaries

This package is a Nest transport layer around the Confluent
`@confluentinc/kafka-javascript` client. Applications still own:

- Broker credentials and SSL/SASL configuration.
- Topic ACLs and authorization decisions.
- Message construction, serialization, and input handling.
- Authorization and tenant selection inside handlers.
- Rate limiting, backpressure tuning, and consumer-lag controls.
- Validation choices such as class-validator DTOs or app-owned Zod schemas.

Security fixes in this repository focus on package behavior — especially that
offsets commit only after a successful handler return, that rebalances do not
strand in-flight work, and that the transport never logs or leaks broker
credentials or message secrets — alongside samples, docs, release automation,
and patterns that could encourage unsafe usage.

## Supply-Chain Notes

The published package keeps `"dependencies": {}`. Runtime requirements live in
`peerDependencies`; build tooling lives in `devDependencies`. Every dependency
addition or update is reviewed for legitimacy and inspected for install or
lifecycle scripts. The native `@confluentinc/kafka-javascript` client (which
ships a librdkafka binary) is a peer dependency owned by the consuming
application, not bundled or auto-installed by this package.

## Disclosure

The maintainer will acknowledge valid private reports as soon as practical,
coordinate a fix when the issue is in scope, and publish release notes or an
advisory when public disclosure is appropriate.
