/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config for all the possible
// site configuration options.

// List of projects/orgs using your project for the users page.
const users = [];

const siteConfig = {
  title: "Mindsdb" /* title for your website */,
  tagline: "Create AI based apps in a few lines of code",
  url: "https://docs.mindsdb.com" /* your website url */,
  baseUrl: "/mindsdb/" /* base url for your project */,
  projectName: "mindsdb",
  organizationName: "mindsdb",
  twitter: false,
  twitterUsername: "Mindsdb",
  cname: "mindsdb.github.io",
  //gaTrackingId: '',
  //ogImage: '',
  headerLinks: [
    {
      doc: "installing-mindsdb",
      label: "Docs"
    },
    {
      href: "https://github.com/mindsdb/mindsdb/releases",
      label: "Download"
    }
  ],

  // If you have users set above, you add it here:
  users,

  /* path to images for header/footer */
  headerIcon: 'img/mindsdb_logo_white.png',
  footerIcon: 'img/logo_bear.png',
  favicon: 'img/favicon.png',

  /* Colors for website */
  colors: {
    primaryColor: '#00b06d',
    secondaryColor: '#205C3B',
  },

  /* Custom fonts for website */
  /*
  fonts: {
    myFont: [
      "Times New Roman",
      "Serif"
    ],
    myOtherFont: [
      "-apple-system",
      "system-ui"
    ]
  },
  */

  // This copyright info is used in /core/Footer.js and blog RSS/Atom feeds.
  copyright: `Copyright © ${new Date().getFullYear()} Mindsdb`,

  highlight: {
    // Highlight.js theme to use for syntax highlighting in code blocks.
    theme: 'default',
  },

  // Add custom scripts here that would be placed in <script> tags.
  scripts: ['https://buttons.github.io/buttons.js'],

  // On page navigation for the current documentation page.
  onPageNav: 'separate',
  // No .html extensions for paths.
  cleanUrl: true,

  // Open Graph and Twitter card images.
  ogImage: 'img/docusaurus.png',
  twitterImage: 'img/docusaurus.png',

  // Show documentation's last contributor's name.
  // enableUpdateBy: true,

  // Show documentation's last update time.
  // enableUpdateTime: true,

  // You may provide arbitrary config keys to be used as needed by your
  // template. For example, if you need your repo's URL...
    repoUrl: 'https://github.com/mindsdb/mindsdb',
};

module.exports = siteConfig;
