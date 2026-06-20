import type { FormEvent } from "react";
import { useState } from "react";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { FormField } from "../components";
import { PageHeader } from "../components";
import { SelectField } from "../components";
import type { NewUser, User, UserRole, UserStatus } from "../domain/console";
import { userRoleOptions } from "../domain/consoleOptions";
import { ResourceStatus, ResourceTitle, TableActions } from "./shared";

type UsersPageProps = {
  onCreateUser: (input: NewUser) => void;
  onDeleteUser: (userID: string) => void;
  onUpdateUserStatus: (userID: string, status: UserStatus) => void;
  users: User[];
};

export function UsersPage({ onCreateUser, onDeleteUser, onUpdateUserStatus, users }: UsersPageProps) {
  const [values, setValues] = useState<NewUser>({
    username: "",
    role: "Viewer"
  });

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!values.username.trim()) {
      return;
    }

    onCreateUser(values);
    setValues({ username: "", role: "Viewer" });
  }

  const columns: DataTableColumn<User>[] = [
    {
      header: "User",
      cell: (user) => <ResourceTitle subtitle={user.lastSeen} title={user.username} />
    },
    {
      header: "Role",
      cell: (user) => user.role
    },
    {
      header: "Tokens",
      cell: (user) => user.tokens,
      align: "end"
    },
    {
      align: "end",
      header: "Status",
      cell: (user) => (
        <ResourceStatus tone={user.status}>{user.status === "active" ? "Active" : "Disabled"}</ResourceStatus>
      )
    },
    {
      align: "end",
      header: "Actions",
      cell: (user) => (
        <TableActions>
          <Button
            aria-label={user.status === "active" ? `Disable ${user.username}` : `Activate ${user.username}`}
            onClick={() => onUpdateUserStatus(user.id, user.status === "active" ? "disabled" : "active")}
          >
            {user.status === "active" ? "Disable" : "Activate"}
          </Button>
          <Button
            aria-label={`Remove ${user.username}`}
            disabled={user.username === "admin"}
            onClick={() => onDeleteUser(user.id)}
          >
            Remove
          </Button>
        </TableActions>
      )
    }
  ];

  return (
    <>
      <PageHeader description="Accounts with access to this Vectis console." eyebrow="Users" title="Users" />
      <form className="inline-form" onSubmit={handleSubmit}>
        <FormField
          autoComplete="username"
          label="Username"
          name="newUsername"
          onChange={(event) => setValues({ ...values, username: event.target.value })}
          required
          value={values.username}
        />
        <SelectField
          label="Role"
          name="newUserRole"
          onChange={(event) =>
            setValues({
              ...values,
              role: event.target.value as UserRole
            })
          }
          options={userRoleOptions}
          value={values.role}
        />
        <Button type="submit">Add user</Button>
      </form>
      <DataTable columns={columns} emptyMessage="No users yet." getRowKey={(user) => user.id} rows={users} />
    </>
  );
}
