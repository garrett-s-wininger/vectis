import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { NamespacesPageHarness, PageStoryFrame } from "../mocks/pageHarnesses";
import { NamespacesPage } from "./NamespacesPage";

const meta = {
  title: "Pages/Namespaces",
  component: NamespacesPage,
  args: {
    canDeleteNamespace: () => false,
    namespaces: createMockConsoleDataSnapshot().namespaces,
    onCreateNamespace: () => undefined,
    onDeleteNamespace: () => undefined
  },
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta<typeof NamespacesPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Hierarchy: Story = {
  render: () => <NamespacesPageHarness />
};

export const Empty: Story = {
  args: {
    canDeleteNamespace: () => false,
    namespaces: [],
    onCreateNamespace: () => undefined,
    onDeleteNamespace: () => undefined
  }
};
