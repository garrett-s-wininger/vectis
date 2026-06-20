import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { PageStoryFrame, UsersPageHarness } from "../mocks/pageHarnesses";
import { UsersPage } from "./UsersPage";
import { UserDetailPage } from "./users/UserDetailPage";

const meta = {
  title: "Pages/Users",
  component: UsersPage,
  args: {
    onCreateUser: () => undefined,
    onDeleteUser: () => undefined,
    onOpenUser: () => undefined,
    onOpenUsers: () => undefined,
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
    onOpenUser: () => undefined,
    onOpenUsers: () => undefined,
    onUpdateUserStatus: () => undefined,
    users: []
  }
};

export const Detail: Story = {
  render: () => {
    const user = createMockConsoleDataSnapshot().users[1];

    return (
      <UserDetailPage
        onBack={() => undefined}
        onRemoveUser={() => undefined}
        onUpdateUserStatus={() => undefined}
        user={user}
      />
    );
  }
};
