import type { FormEvent } from "react";
import { useState } from "react";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { FormField } from "../components";
import { PageHeader } from "../components";
import { SelectField } from "../components";
import type { NewUser, User, UserRole, UserStatus } from "../domain/console";

const roleOptions: { label: string; value: UserRole }[] = [
  { label: "Admin", value: "Admin" },
  { label: "Operator", value: "Operator" },
  { label: "Viewer", value: "Viewer" }
];

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
      cell: (user) => (
        <div className="resource-title">
          <strong>{user.username}</strong>
          <small>{user.lastSeen}</small>
        </div>
      )
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
        <span className={`resource-status resource-status--${user.status}`}>
          {user.status === "active" ? "Active" : "Disabled"}
        </span>
      )
    },
    {
      align: "end",
      header: "Actions",
      cell: (user) => (
        <div className="table-actions">
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
        </div>
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
          options={roleOptions}
          value={values.role}
        />
        <Button type="submit">Add user</Button>
      </form>
      <DataTable columns={columns} emptyMessage="No users loaded." getRowKey={(user) => user.id} rows={users} />
    </>
  );
}
