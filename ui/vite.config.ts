import { defineConfig } from "vitest/config";
import babel from "@rolldown/plugin-babel";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";

export default defineConfig({
  build: {
    chunkSizeWarningLimit: 1_500,
    rolldownOptions: {
      checks: {
        pluginTimings: false
      }
    }
  },
  plugins: [
    react(),
    babel({
      presets: [reactCompilerPreset()]
    })
  ],
  server: {
    proxy: {
      "/api": "http://localhost:8080",
      "/ui/api": "http://localhost:8089"
    }
  },
  test: {
    environment: "jsdom",
    exclude: ["smoke/**", "node_modules/**", "dist/**"],
    globals: true,
    setupFiles: "./src/test/setup.ts"
  }
});
