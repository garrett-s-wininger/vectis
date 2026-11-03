const classic = require("@docusaurus/preset-classic").default;

module.exports = function preset(context, options) {
  const result = classic(context, options);
  result.themes = result.themes.map((entry) => {
    const [source, themeOptions] = Array.isArray(entry) ? entry : [entry, {}];
    return source === require.resolve("@docusaurus/theme-classic")
      ? [require.resolve("../plugins/csp-theme.cjs"), themeOptions]
      : entry;
  });

  return result;
};
