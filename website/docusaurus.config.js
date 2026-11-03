const config = {
  title: "Vectis",
  tagline: "Self-hosted build/CI system",
  favicon: "img/favicon.svg",
  url: "https://vectis.dev",
  baseUrl: "/",
  organizationName: "garrett-s-wininger",
  projectName: "vectis",
  onBrokenLinks: "throw",
  onBrokenAnchors: "throw",
  i18n: {
    defaultLocale: "en",
    locales: ["en"]
  },
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: "throw",
      onBrokenMarkdownImages: "throw"
    },
    mermaid: true
  },
  themes: ["@docusaurus/theme-mermaid"],
  presets: [
    [
      require.resolve("./presets/classic.cjs"),
      {
        docs: {
          sidebarPath: "./sidebars.js",
          editUrl: undefined,
          showLastUpdateTime: true
        },
        blog: false,
        theme: {
          customCss: "./src/css/custom.css"
        }
      }
    ]
  ],
  themeConfig: {
    colorMode: { defaultMode: "light", respectPrefersColorScheme: true },
    navbar: {
      title: "",
      logo: {
        alt: "Vectis",
        src: "img/vectis.png",
        srcDark: "img/vectis-dark.png"
      },
      items: [
        {
          to: "/",
          label: "Home",
          position: "left",
          activeBaseRegex: "^/$"
        },
        {
          type: "docSidebar",
          sidebarId: "docs",
          position: "left",
          label: "Docs"
        },
        {
          to: "/api",
          label: "API",
          position: "left"
        },
        {
          href: "https://github.com/garrett-s-wininger/vectis",
          label: "GitHub",
          position: "right"
        }
      ]
    },
    footer: {
      style: "dark",
      copyright: `Copyright © ${new Date().getFullYear()} The Vectis Authors.`
    },
    prism: {
      additionalLanguages: ["go", "toml", "yaml", "bash", "json"]
    },
    mermaid: {
      theme: {
        light: "neutral",
        dark: "dark"
      }
    }
  }
};

module.exports = config;
