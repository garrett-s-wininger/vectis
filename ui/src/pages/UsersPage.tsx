import type { FormEvent } from "react";
import { useRef, useState } from "react";
import { BreadcrumbTrail } from "../components";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { EmptyStatePanel } from "../components";
import { FormField } from "../components";
import { MetricCard } from "../components";
import { PageHeader } from "../components";
import { ResourceStatus } from "../components";
import { SelectField } from "../components";
import type { CreatedUserCredential } from "../data/consoleDataSource";
import type { AssignableUserRole, Namespace, NewUser, RoleBindingRole, User, UserStatus } from "../domain/console";
import { userRoleOptions } from "../domain/consoleOptions";
import { EmptyStateRail, PageMissingState, ResourceTitle } from "./shared";
import { UserDetailPage } from "./users/UserDetailPage";
import { InitialPasswordDialog } from "./users/InitialPasswordDialog";
import { roleTone } from "./users/UserPresentation";
import styles from "./UsersPage.module.css";

type UsersPageProps = {
  namespaces: Namespace[];
  onCreateUser: (input: NewUser) => Promise<CreatedUserCredential | undefined> | CreatedUserCredential | undefined;
  onDeleteUser: (userID: string) => Promise<void> | void;
  onGrantRoleBinding: (
    userID: string,
    namespaceID: number,
    role: RoleBindingRole,
    previousRole?: RoleBindingRole
  ) => Promise<void> | void;
  onOpenUser: (userID: string) => void;
  onOpenUsers: () => void;
  onRevokeRoleBinding: (userID: string, namespaceID: number, role: RoleBindingRole) => Promise<void> | void;
  onUpdateUserStatus: (userID: string, status: UserStatus) => Promise<void> | void;
  selectedUserID?: string;
  users: User[];
};

export function UsersPage({
  namespaces,
  onCreateUser,
  onDeleteUser,
  onGrantRoleBinding,
  onOpenUser,
  onOpenUsers,
  onRevokeRoleBinding,
  onUpdateUserStatus,
  selectedUserID,
  users
}: UsersPageProps) {
  const createButtonRef = useRef<HTMLButtonElement>(null);
  const [credential, setCredential] = useState<CreatedUserCredential | null>(null);
  const [isCreating, setIsCreating] = useState(false);
  const [values, setValues] = useState<NewUser>({
    username: "",
    namespaceID: namespaces[0]?.id,
    role: "Viewer"
  });

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!values.username.trim()) {
      return;
    }

    try {
      const createdCredential = await onCreateUser(values);
      setCredential(createdCredential ?? null);
      setValues({ username: "", namespaceID: namespaces[0]?.id, role: "Viewer" });
      setIsCreating(false);
    } catch {
      // The app-level action alert owns the visible error; keep the form open for correction or retry.
    }
  }

  async function handleRemoveUser(user: User) {
    await onDeleteUser(user.id);
    if (user.id === selectedUserID) onOpenUsers();
  }

  const userStats = summarizeUsers(users);
  const selectedUser = selectedUserID ? users.find((user) => user.id === selectedUserID) : null;

  const columns: DataTableColumn<User>[] = [
    {
      header: "User",
      cell: (user) => <ResourceTitle subtitle={user.lastSeen} title={user.username} />
    },
    {
      header: "Role",
      cell: (user) => (
        <ResourceStatus className={styles.userChip} tone={roleTone(user.role)}>
          {user.role}
        </ResourceStatus>
      ),
      width: "120px"
    },
    {
      align: "end",
      header: "Status",
      cell: (user) => (
        <ResourceStatus className={styles.userChip} tone={user.status}>
          {user.status === "active" ? "Active" : "Disabled"}
        </ResourceStatus>
      )
    }
  ];

  if (selectedUserID && !selectedUser) {
    return (
      <PageMissingState
        actionLabel="View Users"
        breadcrumbs={userMissingBreadcrumbItems()}
        description="This user is no longer available, or the route points to an account that does not exist."
        label="User location"
        onAction={onOpenUsers}
        panelDescription="Return to the users index to choose an active account."
        panelEyebrow="Missing User"
        panelTitle="No User Found"
        title="User Not Found"
      />
    );
  }

  if (selectedUser) {
    return (
      <UserDetailPage
        key={selectedUser.id}
        onBack={onOpenUsers}
        namespaces={namespaces}
        onGrantRoleBinding={onGrantRoleBinding}
        onRemoveUser={handleRemoveUser}
        onRevokeRoleBinding={onRevokeRoleBinding}
        onUpdateUserStatus={onUpdateUserStatus}
        user={selectedUser}
      >
        <InitialPasswordDialog
          returnFocusRef={createButtonRef}
          credential={credential}
          onDismissCredential={() => setCredential(null)}
        />
      </UserDetailPage>
    );
  }

  return (
    <>
      <PageHeader
        actions={
          !isCreating ? (
            <Button ref={createButtonRef} onClick={() => setIsCreating(true)} type="button">
              Create
            </Button>
          ) : null
        }
        description="Local accounts that can sign in to the console."
        navigation={
          <BreadcrumbTrail items={[{ label: "Manage" }, { current: true, label: "Users" }]} label="Users location" />
        }
        title="Users"
      />
      <div className={styles.workspace}>
        {isCreating ? (
          <section
            className={`${styles.createPanel} polished-panel polished-panel--accent-top polished-panel--overlay-safe`}
            aria-labelledby="create-user-title"
          >
            <div className={styles.createCopy}>
              <p className="eyebrow">Create</p>
              <h2 id="create-user-title">New User</h2>
              <p>Create an account with a one-time password and an initial namespace role.</p>
              <p id="initial-role-description">
                {values.role === "Operator"
                  ? "Operators can trigger, cancel, and replay runs. Editing jobs and managing namespaces requires Admin access."
                  : values.role === "Viewer"
                    ? "Viewers can inspect jobs, runs, logs, and artifacts without starting or changing work."
                    : "Admins can edit jobs and manage access within their namespace. Root admins can also manage users."}
              </p>
            </div>
            <form className={styles.form} onSubmit={handleSubmit}>
              <FormField
                autoComplete="username"
                label="Username"
                name="newUsername"
                onChange={(event) => setValues({ ...values, username: event.target.value })}
                required
                value={values.username}
                wide
              />
              <SelectField
                label="Initial Namespace"
                name="newUserNamespace"
                options={namespaces.map((namespace) => ({
                  label: namespace.path === "/" ? "Root" : namespace.path,
                  value: String(namespace.id)
                }))}
                value={String(values.namespaceID ?? namespaces[0]?.id ?? "")}
                onChange={(event) => setValues({ ...values, namespaceID: Number(event.target.value) })}
                wide
              />
              <SelectField
                label="Role"
                name="newUserRole"
                aria-describedby="initial-role-description"
                onChange={(event) =>
                  setValues({
                    ...values,
                    role: event.target.value as AssignableUserRole
                  })
                }
                options={userRoleOptions}
                value={values.role}
                wide
              />
              <div className={styles.formActions}>
                <Button onClick={() => setIsCreating(false)} type="button" variant="quiet">
                  Cancel
                </Button>
                <Button disabled={!namespaces.length} type="submit">
                  Add User
                </Button>
              </div>
            </form>
          </section>
        ) : null}

        <section className={`${styles.summary} polished-panel polished-panel--accent-top`} aria-label="User summary">
          <div className={styles.summaryCopy}>
            <h2>Access</h2>
            <p>Account state for the local authentication provider.</p>
          </div>
          <div className={styles.summaryFacts} role="group" aria-label="User totals">
            <MetricCard detail="Local console accounts" label="Total" value={userStats.total} variant="plain" />
            <MetricCard detail="Can sign in" label="Active" value={userStats.active} variant="plain" />
            <MetricCard detail="Cannot sign in" label="Disabled" value={userStats.disabled} variant="plain" />
          </div>
        </section>

        {users.length > 0 ? (
          <DataTable
            columns={columns}
            emptyMessage="No users found."
            getRowActionLabel={(user) => `View ${user.username}`}
            getRowKey={(user) => user.id}
            onRowClick={(user) => onOpenUser(user.id)}
            rows={users}
          />
        ) : (
          <EmptyStateRail>
            <EmptyStatePanel
              actions={
                <Button onClick={() => setIsCreating(true)} variant="quiet">
                  Create
                </Button>
              }
              description="Create a local account so another operator can sign in and manage Vectis."
              eyebrow="No Users"
              title="No Accounts Found"
            />
          </EmptyStateRail>
        )}
      </div>
      <InitialPasswordDialog
        returnFocusRef={createButtonRef}
        credential={credential}
        onDismissCredential={() => setCredential(null)}
      />
    </>
  );
}

function userMissingBreadcrumbItems() {
  return [{ label: "Manage" }, { label: "Users" }, { current: true, label: "Missing" }];
}

function summarizeUsers(users: User[]) {
  return users.reduce(
    (summary, user) => ({
      active: summary.active + (user.status === "active" ? 1 : 0),
      disabled: summary.disabled + (user.status === "disabled" ? 1 : 0),
      total: summary.total + 1
    }),
    { active: 0, disabled: 0, total: 0 }
  );
}
