import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { NamespacesPageHarness, PageStoryFrame } from "../mocks/pageHarnesses";
import { NamespacesPage } from "./NamespacesPage";

const meta = {
  title: "Pages/Namespaces",
  component: NamespacesPage,
  args: {
    canDeleteNamespace: () => false,
    editorMode: null,
    jobs: createMockConsoleDataSnapshot().jobs,
    namespaces: createMockConsoleDataSnapshot().namespaces,
    onCloseEditor: () => undefined,
    onConfigureNamespace: () => undefined,
    onCreateNamespace: () => undefined,
    onDeleteNamespace: () => undefined,
    onOpenJobs: () => undefined,
    onOpenNamespace: () => undefined,
    onOpenNamespaces: () => undefined,
    onUpdateNamespace: () => undefined
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
    editorMode: null,
    jobs: [],
    namespaces: [],
    onCloseEditor: () => undefined,
    onConfigureNamespace: () => undefined,
    onCreateNamespace: () => undefined,
    onDeleteNamespace: () => undefined,
    onOpenJobs: () => undefined,
    onOpenNamespace: () => undefined,
    onOpenNamespaces: () => undefined,
    onUpdateNamespace: () => undefined
  }
};

export const Detail: Story = {
  args: {
    selectedNamespaceID: 2
  }
};

export const Configure: Story = {
  args: {
    editorMode: { kind: "edit", namespaceID: 2 },
    selectedNamespaceID: 2
  }
};
