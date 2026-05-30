import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
  stories: ["../src/**/*.stories.@(ts|tsx)"],
  features: {
    sidebarOnboardingChecklist: false
  },
  framework: {
    name: "@storybook/react-vite",
    options: {}
  }
};

export default config;
