import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { PageStoryFrame, UsersPageHarness } from "../mocks/pageHarnesses";
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
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta<typeof UsersPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const UserManagement: Story = {
  render: () => <UsersPageHarness />
};

export const Empty: Story = {
  args: {
    onCreateUser: () => undefined,
    onDeleteUser: () => undefined,
    onUpdateUserStatus: () => undefined,
    users: []
  }
};
