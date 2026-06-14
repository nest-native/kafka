import type {ReactNode} from 'react';
import clsx from 'clsx';
import Heading from '@theme/Heading';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  icon: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Decorator-First Transport',
    icon: 'Nest',
    description: (
      <>
        Mark a class with <code>@KafkaConsumer</code> and its methods with{' '}
        <code>@KafkaHandler</code>. The <code>@MessagePattern</code> /{' '}
        <code>@EventPattern</code> ergonomics carry over from{' '}
        <code>@nestjs/microservices</code>.
      </>
    ),
  },
  {
    title: 'Full Enhancer Pipeline',
    icon: 'Pipe',
    description: (
      <>
        <code>@UseGuards</code>, <code>@UseInterceptors</code>,{' '}
        <code>@UsePipes</code>, and <code>@UseFilters</code> work on handler
        methods exactly as they do on an HTTP controller, including
        request-scoped providers.
      </>
    ),
  },
  {
    title: 'Confluent, Not kafkajs',
    icon: 'Conf',
    description: (
      <>
        Built on Confluent’s officially supported{' '}
        <code>@confluentinc/kafka-javascript</code> client, the replacement the
        community asked for in <code>nestjs/nest#13223</code>.
      </>
    ),
  },
  {
    title: 'Correctness Gaps Closed',
    icon: 'Fix',
    description: (
      <>
        Per-topic concurrency (<code>#12703</code>), rebalance-safe batch
        offsets (<code>#12355</code>), and exception mapping instead of
        swallowing (<code>#9679</code>) each have a regression test.
      </>
    ),
  },
  {
    title: 'Zero Runtime Dependencies',
    icon: 'Zero',
    description: (
      <>
        The published package keeps <code>dependencies</code> empty. Nest, the
        Confluent client, and validators stay peers under the host
        application’s control.
      </>
    ),
  },
  {
    title: 'Test Without A Broker',
    icon: 'Test',
    description: (
      <>
        <code>KafkaTestModule</code> runs the whole transport — producer,
        consumers, batches, transactions, shutdown — against an in-memory
        broker. No <code>librdkafka</code>, no <code>KAFKA_BROKERS</code>.
      </>
    ),
  },
];

function Feature({title, icon, description}: FeatureItem) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md feature-card">
        <div className={styles.featureIcon}>{icon}</div>
        <Heading as="h3">{title}</Heading>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
