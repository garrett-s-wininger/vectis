export default {
  extends: ["stylelint-config-standard"],
  ignoreFiles: [".docusaurus/**", "build/**", "node_modules/**", "playwright-report/**", "test-results/**"],
  rules: {
    "alpha-value-notation": null,
    "color-function-alias-notation": null,
    "color-function-notation": null,
    "color-hex-length": null,
    "custom-property-pattern": null,
    "custom-property-empty-line-before": null,
    "declaration-block-no-redundant-longhand-properties": null,
    "import-notation": null,
    "media-feature-range-notation": null,
    "no-descending-specificity": null,
    "property-no-deprecated": null,
    "selector-class-pattern": null,
    "value-keyword-case": null
  }
};
