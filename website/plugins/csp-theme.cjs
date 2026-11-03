const classic = require("@docusaurus/theme-classic");
const { createHash } = require("node:crypto");
const fs = require("node:fs/promises");
const path = require("node:path");

module.exports = async function theme(context, options) {
  const plugin = await classic.default(context, options);
  const swaggerFiles = [
    "swagger-ui-bundle.js",
    "swagger-ui.css",
    "swagger-ui-bundle.js.LICENSE.txt",
    "LICENSE",
    "NOTICE"
  ];

  const swaggerAssets = await Promise.all(
    swaggerFiles.map(async (name) => [name, await fs.readFile(require.resolve(`swagger-ui-dist/${name}`))])
  );

  return {
    ...plugin,
    getThemePath: () => path.resolve(path.dirname(require.resolve("@docusaurus/theme-classic")), plugin.getThemePath()),
    getTypeScriptThemePath: () =>
      path.resolve(path.dirname(require.resolve("@docusaurus/theme-classic")), plugin.getTypeScriptThemePath()),
    getClientModules: () =>
      plugin
        .getClientModules()
        .map((entry) =>
          entry.startsWith(".")
            ? path.resolve(path.dirname(require.resolve("@docusaurus/theme-classic")), entry)
            : entry
        ),
    configureWebpack(config, isServer, helpers) {
      const result = plugin.configureWebpack(config, isServer, helpers);
      if (!isServer) {
        const webpack = helpers.currentBundler.instance;
        result.plugins.push({
          apply(compiler) {
            compiler.hooks.thisCompilation.tap("VectisSwaggerAssets", (compilation) => {
              compilation.hooks.processAssets.tap(
                { name: "VectisSwaggerAssets", stage: webpack.Compilation.PROCESS_ASSETS_STAGE_ADDITIONAL },
                () => {
                  for (const [name, bytes] of swaggerAssets)
                    compilation.emitAsset(`assets/swagger/${name}`, new webpack.sources.RawSource(bytes));
                }
              );
            });
          }
        });
      }
      return result;
    },
    injectHtmlTags(args) {
      const tags = plugin.injectHtmlTags(args);
      for (const tag of tags.preBodyTags || []) {
        if (tag.tagName === "svg" && tag.attributes?.style === "display: none;") {
          tag.attributes = { class: "vectis-svg-sprite", "aria-hidden": "true" };
        }
      }

      return tags;
    },
    async postBuild({ outDir, baseUrl }) {
      const assets = path.join(outDir, "assets", "csp");
      await fs.mkdir(assets, { recursive: true });

      async function visit(directory) {
        for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
          const filename = path.join(directory, entry.name);

          if (entry.isDirectory()) await visit(filename);
          else if (entry.name.endsWith(".html")) {
            const html = await fs.readFile(filename, "utf8");
            const scripts = new Map();

            // NOTE(garrett): Docusaurus emits trusted, complete script elements in generated HTML.
            const updated = html.replace(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi, (tag, attributes, code) => {
              if (
                /\bsrc\s*=/.test(attributes) ||
                !code.trim() ||
                /\btype\s*=\s*["']application\/(?:ld\+)?json["']/.test(attributes)
              )
                return tag;

              const name = `${createHash("sha256").update(code).digest("hex").slice(0, 20)}.js`;
              scripts.set(name, code);

              return `<script${attributes} src="${baseUrl}assets/csp/${name}"></script>`;
            });

            for (const [name, code] of scripts) await fs.writeFile(path.join(assets, name), code);
            if (updated !== html) await fs.writeFile(filename, updated);
          }
        }
      }

      await visit(outDir);
    }
  };
};

module.exports.validateOptions = classic.validateOptions;
module.exports.validateThemeConfig = classic.validateThemeConfig;
