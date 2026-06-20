import type { FormEvent } from "react";
import { useState } from "react";
import { BreadcrumbTrail } from "../components";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { EmptyStatePanel } from "../components";
import { FormField } from "../components";
import { MetricCard } from "../components";
import { PageHeader } from "../components";
import { SelectField } from "../components";
import type { CreatedUserCredential } from "../data/consoleDataSource";
import type { AssignableUserRole, NewUser, User, UserStatus } from "../domain/console";
import { userRoleOptions } from "../domain/consoleOptions";
import { EmptyStateRail, PageMissingState, ResourceStatus, ResourceTitle, TableActions } from "./shared";
import { UserDetailPage } from "./users/UserDetailPage";
import { UserModals } from "./users/UserModals";
import { roleTone } from "./users/UserPresentation";
import styles from "./UsersPage.module.css";

type UsersPageProps = {
  onCreateUser: (input: NewUser) => Promise<CreatedUserCredential | undefined> | CreatedUserCredential | undefined;
  onDeleteUser: (userID: string) => Promise<void> | void;
  onOpenUser: (userID: string) => void;
  onOpenUsers: () => void;
  onUpdateUserStatus: (userID: string, status: UserStatus) => Promise<void> | void;
  selectedUserID?: string;
  users: User[];
};

export function UsersPage({
  onCreateUser,
  onDeleteUser,
  onOpenUser,
  onOpenUsers,
  onUpdateUserStatus,
  selectedUserID,
  users
}: UsersPageProps) {
  const [credential, setCredential] = useState<CreatedUserCredential | null>(null);
  const [credentialCopyState, setCredentialCopyState] = useState<"copied" | "idle">("idle");
  const [isCreating, setIsCreating] = useState(false);
  const [pendingDeleteUser, setPendingDeleteUser] = useState<User | null>(null);
  const [values, setValues] = useState<NewUser>({
    username: "",
    role: "Viewer"
  });

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!values.username.trim()) {
      return;
    }

    try {
      const createdCredential = await onCreateUser(values);
      setCredentialCopyState("idle");
      setCredential(createdCredential ?? null);
      setValues({ username: "", role: "Viewer" });
      setIsCreating(false);
    } catch {
      // The app-level action alert owns the visible error; keep the form open for correction or retry.
    }
  }

  async function handleCopyCredential() {
    if (!credential || !canCopyCredential) {
      return;
    }

    await navigator.clipboard.writeText(credential.password);
    setCredentialCopyState("copied");
  }

  async function handleConfirmDeleteUser() {
    if (!pendingDeleteUser) {
      return;
    }

    try {
      await onDeleteUser(pendingDeleteUser.id);
      setPendingDeleteUser(null);
      if (pendingDeleteUser.id === selectedUserID) {
        onOpenUsers();
      }
    } catch {
      // The app-level action alert owns the visible error; keep the dialog open for retry or cancel.
    }
  }

  const canCopyCredential = hasSecureClipboard();
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
    },
    {
      align: "end",
      header: "Actions",
      cell: (user) => (
        <TableActions className={styles.userActions}>
          <Button aria-label={`View ${user.username}`} className={styles.userActionButton} onClick={() => onOpenUser(user.id)} variant="quiet">
            View
          </Button>
        </TableActions>
      ),
      width: "92px"
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
        onBack={onOpenUsers}
        onRemoveUser={setPendingDeleteUser}
        onUpdateUserStatus={onUpdateUserStatus}
        user={selectedUser}
      >
        <UserModals
          canCopyCredential={canCopyCredential}
          credential={credential}
          credentialCopyState={credentialCopyState}
          onConfirmDeleteUser={handleConfirmDeleteUser}
          onCopyCredential={handleCopyCredential}
          onDismissCredential={() => setCredential(null)}
          onDismissDeleteUser={() => setPendingDeleteUser(null)}
          pendingDeleteUser={pendingDeleteUser}
        />
      </UserDetailPage>
    );
  }

  return (
    <>
      <PageHeader
        actions={
          !isCreating ? (
            <Button onClick={() => setIsCreating(true)} type="button">
              Create
            </Button>
          ) : null
        }
        description="Local accounts that can sign in to the console."
        navigation={
          <BreadcrumbTrail
            items={[
              { label: "Root" },
              { current: true, label: "Users" }
            ]}
            label="Users location"
          />
        }
        title="Users"
      />
      <div className={styles.workspace}>
        {isCreating ? (
          <section className={`${styles.createPanel} polished-panel polished-panel--accent-top`} aria-labelledby="create-user-title">
            <div className={styles.createCopy}>
              <p className="eyebrow">Create</p>
              <h2 id="create-user-title">New User</h2>
              <p>Add a local account. The API will generate the initial password until password entry is added here.</p>
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
                label="Role"
                name="newUserRole"
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
                <Button type="submit">Add User</Button>
              </div>
            </form>
          </section>
        ) : null}

        <section className={`${styles.summary} polished-panel polished-panel--accent-top`} aria-label="User summary">
          <div className={styles.summaryCopy}>
            <h2>Access</h2>
            <p>Account state for the local authentication provider.</p>
          </div>
          <div className={styles.summaryFacts} aria-label="User totals">
            <MetricCard detail="Local console accounts" label="Total" value={userStats.total} variant="plain" />
            <MetricCard detail="Can sign in" label="Active" value={userStats.active} variant="plain" />
            <MetricCard detail="Cannot sign in" label="Disabled" value={userStats.disabled} variant="plain" />
          </div>
        </section>

        {users.length > 0 ? (
          <DataTable columns={columns} emptyMessage="No users found." getRowKey={(user) => user.id} rows={users} />
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
      <UserModals
        canCopyCredential={canCopyCredential}
        credential={credential}
        credentialCopyState={credentialCopyState}
        onConfirmDeleteUser={handleConfirmDeleteUser}
        onCopyCredential={handleCopyCredential}
        onDismissCredential={() => setCredential(null)}
        onDismissDeleteUser={() => setPendingDeleteUser(null)}
        pendingDeleteUser={pendingDeleteUser}
      />
    </>
  );
}

function userMissingBreadcrumbItems() {
  return [{ label: "Root" }, { label: "Users" }, { current: true, label: "Missing" }];
}

function hasSecureClipboard() {
  return Boolean(window.isSecureContext && navigator.clipboard?.writeText);
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
