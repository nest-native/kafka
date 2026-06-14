import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: '@nest-native/kafka',
  tagline:
    'Decorator-first NestJS Kafka transport on Confluent’s officially supported client, with the full Nest enhancer pipeline intact',
  favicon: 'img/logo.svg',

  future: {
    v4: true,
  },

  url: 'https://nest-native.github.io',
  baseUrl: '/kafka/',

  organizationName: 'nest-native',
  projectName: 'kafka',

  onBrokenLinks: 'throw',

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/nest-native/kafka/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/social-card.png',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: '@nest-native/kafka',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://www.npmjs.com/package/@nest-native/kafka',
          label: 'npm',
          position: 'right',
        },
        {
          href: 'https://github.com/nest-native/kafka',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Introduction', to: '/docs/introduction'},
            {label: 'Quick Start', to: '/docs/quick-start'},
            {label: 'Consumers', to: '/docs/consumers'},
          ],
        },
        {
          title: 'More',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/nest-native/kafka',
            },
            {
              label: 'npm',
              href: 'https://www.npmjs.com/package/@nest-native/kafka',
            },
            {
              label: 'Confluent client',
              href: 'https://github.com/confluentinc/confluent-kafka-javascript',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} @nest-native/kafka contributors. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['bash', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
