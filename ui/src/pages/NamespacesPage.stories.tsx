import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import {
  canDeleteMockNamespace,
  createMockConsoleDataSnapshot,
  createMockNamespace,
  deleteMockNamespace,
  type MockConsoleData
} from "../mocks/consoleData";
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
      <main className="storybook-page-main">
        <Story />
      </main>
    )
  ]
} satisfies Meta<typeof NamespacesPage>;

export default meta;

type Story = StoryObj<typeof meta>;

function NamespacesPageMock() {
  const [data, setData] = useState<MockConsoleData>(() =>
    createMockConsoleDataSnapshot()
  );

  return (
    <NamespacesPage
      canDeleteNamespace={(namespaceID) =>
        canDeleteMockNamespace(data, namespaceID)
      }
      namespaces={data.namespaces}
      onCreateNamespace={(input) =>
        setData((current) => createMockNamespace(current, input))
      }
      onDeleteNamespace={(namespaceID) =>
        setData((current) => deleteMockNamespace(current, namespaceID))
      }
    />
  );
}

export const Hierarchy: Story = {
  render: () => <NamespacesPageMock />
};

export const Empty: Story = {
  args: {
    canDeleteNamespace: () => false,
    namespaces: [],
    onCreateNamespace: () => undefined,
    onDeleteNamespace: () => undefined
  }
};
