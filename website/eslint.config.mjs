import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";

export default [
  {
    ignores: [".docusaurus/**", "build/**", "node_modules/**", "playwright-report/**", "test-results/**"]
  },
  js.configs.recommended,
  {
    files: ["**/*.{js,jsx,mjs,cjs}"],
    languageOptions: {
      ecmaVersion: "latest",
      globals: {
        ...globals.browser,
        ...globals.node
      },
      parser: tseslint.parser,
      sourceType: "module"
    }
  },
  {
    files: ["*.cjs"],
    languageOptions: {
      sourceType: "commonjs"
    }
  }
];
