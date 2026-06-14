import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docsSidebar: [
    {
      type: 'category',
      label: 'Getting Started',
      items: ['introduction', 'why-native', 'quick-start'],
    },
    {
      type: 'category',
      label: 'Core API',
      items: ['module', 'producer', 'consumers', 'parameter-decorators'],
    },
    {
      type: 'category',
      label: 'Correctness',
      items: [
        'error-mapping',
        'batch-and-concurrency',
        'transactions',
        'graceful-shutdown',
      ],
    },
    'testing',
    'migration',
    {
      type: 'category',
      label: 'Samples',
      items: ['samples/index', 'samples/catalog'],
    },
    {
      type: 'category',
      label: 'Project Reference',
      items: [
        'api-reference',
        'support-policy',
        'quality-and-ci',
        'release',
        'contributing',
        'roadmap',
      ],
    },
  ],
};

export default sidebars;
