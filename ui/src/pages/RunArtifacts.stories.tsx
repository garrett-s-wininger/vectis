import type { Meta, StoryObj } from "@storybook/react-vite";
import { PageStoryFrame } from "../mocks/pageHarnesses";
import { artifactRun, loadMockArtifacts, mockArtifacts } from "../mocks/runArtifacts";
import { RunArtifacts } from "./RunArtifacts";

const meta = {
  title: "Pages/RunArtifacts",
  component: RunArtifacts,
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ],
  args: {
    run: artifactRun,
    loadArtifacts: loadMockArtifacts
  }
} satisfies Meta<typeof RunArtifacts>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Published: Story = {};
export const Empty: Story = { args: { loadArtifacts: async () => ({ data: [] }) } };
export const Unavailable: Story = {
  args: {
    loadArtifacts: async () => {
      throw new Error("Artifact service is unavailable. Try again shortly.");
    }
  }
};

export const LongNames: Story = {
  args: {
    loadArtifacts: async () => ({
      data: [
        {
          ...mockArtifacts[0],
          name: "test-results-and-build-outputs-for-the-complete-linux-windows-and-macos-release-matrix",
          path: "reports/integration-tests/very-long-directory-name/complete-release-verification-report.txt"
        }
      ]
    })
  }
};
