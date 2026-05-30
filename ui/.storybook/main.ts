import type { StorybookConfig } from "@storybook/react-vite";
import { mergeConfig } from "vite";

const config: StorybookConfig = {
  stories: ["../src/**/*.stories.@(ts|tsx)"],
  features: {
    sidebarOnboardingChecklist: false
  },
  framework: {
    name: "@storybook/react-vite",
    options: {}
  },
  viteFinal: (config) =>
    mergeConfig(config, {
      build: {
        chunkSizeWarningLimit: 1_500,
        rolldownOptions: {
          checks: {
            pluginTimings: false
          }
        }
      }
    })
};

export default config;
