import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import {
  createMockConsoleDataSnapshot,
  createMockUser,
  deleteMockUser,
  updateMockUserStatus,
  type MockConsoleData
} from "../mocks/consoleData";
import { UsersPage } from "./UsersPage";

const meta = {
  title: "Pages/Users",
  component: UsersPage,
  args: {
    onCreateUser: () => undefined,
    onDeleteUser: () => undefined,
    onUpdateUserStatus: () => undefined,
    users: createMockConsoleDataSnapshot().users
  },
  decorators: [
    (Story) => (
      <main className="storybook-page-main">
        <Story />
      </main>
    )
  ]
} satisfies Meta<typeof UsersPage>;

export default meta;

type Story = StoryObj<typeof meta>;

function UsersPageMock() {
  const [data, setData] = useState<MockConsoleData>(() =>
    createMockConsoleDataSnapshot()
  );

  return (
    <UsersPage
      onCreateUser={(input) => setData((current) => createMockUser(current, input))}
      onDeleteUser={(userID) => setData((current) => deleteMockUser(current, userID))}
      onUpdateUserStatus={(userID, status) =>
        setData((current) => updateMockUserStatus(current, userID, status))
      }
      users={data.users}
    />
  );
}

export const UserManagement: Story = {
  render: () => <UsersPageMock />
};

export const Empty: Story = {
  args: {
    onCreateUser: () => undefined,
    onDeleteUser: () => undefined,
    onUpdateUserStatus: () => undefined,
    users: []
  }
};
