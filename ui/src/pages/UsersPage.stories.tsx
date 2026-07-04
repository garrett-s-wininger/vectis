import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { PageStoryFrame, UsersPageHarness } from "../mocks/pageHarnesses";
import { UsersPage } from "./UsersPage";
import { UserDetailPage } from "./users/UserDetailPage";

const meta = {
  title: "Pages/Users",
  component: UsersPage,
  args: {
    namespaces: createMockConsoleDataSnapshot().namespaces,
    onCreateUser: () => undefined,
    onDeleteUser: () => undefined,
    onGrantRoleBinding: () => undefined,
    onOpenUser: () => undefined,
    onOpenUsers: () => undefined,
    onRevokeRoleBinding: () => undefined,
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
    onGrantRoleBinding: () => undefined,
    onOpenUser: () => undefined,
    onOpenUsers: () => undefined,
    onRevokeRoleBinding: () => undefined,
    onUpdateUserStatus: () => undefined,
    users: []
  }
};

export const Detail: Story = {
  render: () => {
    const user = createMockConsoleDataSnapshot().users[1];

    return (
      <UserDetailPage
        namespaces={createMockConsoleDataSnapshot().namespaces}
        onBack={() => undefined}
        onGrantRoleBinding={() => undefined}
        onRemoveUser={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateUserStatus={() => undefined}
        user={user}
      />
    );
  }
};

export const DetailNoBindings: Story = {
  render: () => {
    const data = createMockConsoleDataSnapshot();
    const user = {
      ...data.users[1],
      role: "Unassigned" as const,
      roleBindings: []
    };

    return (
      <UserDetailPage
        namespaces={data.namespaces}
        onBack={() => undefined}
        onGrantRoleBinding={() => undefined}
        onRemoveUser={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateUserStatus={() => undefined}
        user={user}
      />
    );
  }
};
