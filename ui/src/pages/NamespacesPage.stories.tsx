import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { NamespacesPageHarness, PageStoryFrame } from "../mocks/pageHarnesses";
import { NamespacesPage } from "./NamespacesPage";

const meta = {
  title: "Pages/Namespaces",
  component: NamespacesPage,
  args: {
    canDeleteNamespace: () => false,
    jobs: createMockConsoleDataSnapshot().jobs,
    namespaces: createMockConsoleDataSnapshot().namespaces,
    onCreateNamespace: () => undefined,
    onDeleteNamespace: () => undefined,
    onOpenJobs: () => undefined,
    onOpenNamespace: () => undefined,
    onOpenNamespaces: () => undefined
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
    jobs: [],
    namespaces: [],
    onCreateNamespace: () => undefined,
    onDeleteNamespace: () => undefined,
    onOpenJobs: () => undefined,
    onOpenNamespace: () => undefined,
    onOpenNamespaces: () => undefined
  }
};

export const Detail: Story = {
  args: {
    selectedNamespaceID: 2
  }
};
