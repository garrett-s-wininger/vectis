import type { FormEvent, ReactNode } from "react";
import { useMemo, useState } from "react";
import { BreadcrumbTrail, Button, PageHeader } from "../../components";
import type { Namespace, RoleBindingRole, User, UserStatus } from "../../domain/console";
import { roleBindingRoleOptions } from "../../domain/consoleOptions";
import { ResourceStatus, RoleBindingPanel } from "../shared";
import styles from "../UsersPage.module.css";
import { roleTone } from "./UserPresentation";

type UserDetailPageProps = {
  children?: ReactNode;
  namespaces: Namespace[];
  onBack: () => void;
  onGrantRoleBinding: (userID: string, namespaceID: number, role: RoleBindingRole) => Promise<void> | void;
  onRemoveUser: (user: User) => void;
  onRevokeRoleBinding: (userID: string, namespaceID: number, role: RoleBindingRole) => Promise<void> | void;
  onUpdateUserStatus: (userID: string, status: UserStatus) => Promise<void> | void;
  user: User;
};

export function UserDetailPage({
  children,
  namespaces,
  onBack,
  onGrantRoleBinding,
  onRemoveUser,
  onRevokeRoleBinding,
  onUpdateUserStatus,
  user
}: UserDetailPageProps) {
  const nextStatus = user.status === "active" ? "disabled" : "active";
  const bindings = useMemo(() => user.roleBindings ?? [], [user.roleBindings]);
  const [bindingValues, setBindingValues] = useState(() => ({
    namespaceID: String(namespaces[0]?.id ?? ""),
    role: "Viewer" as RoleBindingRole
  }));
  const grantNamespaceOptions = useMemo(
    () =>
      namespaces.map((namespace) => ({
        label: namespace.path === "/" ? "Root" : namespace.path,
        value: String(namespace.id)
      })),
    [namespaces]
  );
  const selectedGrantNamespaceID = grantNamespaceOptions.some((option) => option.value === bindingValues.namespaceID)
    ? bindingValues.namespaceID
    : (grantNamespaceOptions[0]?.value ?? "");
  const selectedGrantBinding = bindings.find((binding) => binding.namespaceID === Number(selectedGrantNamespaceID));
  const grantRoleOptions = selectedGrantBinding
    ? [{ label: selectedGrantBinding.role, value: selectedGrantBinding.role }]
    : roleBindingRoleOptions;
  const selectedGrantRole = grantRoleOptions.some((option) => option.value === bindingValues.role)
    ? bindingValues.role
    : grantRoleOptions[0]?.value;
  const canGrantBinding =
    Boolean(selectedGrantNamespaceID) &&
    Boolean(selectedGrantRole) &&
    !selectedGrantBinding;

  async function handleGrantBinding(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canGrantBinding) {
      return;
    }

    await onGrantRoleBinding(user.id, Number(selectedGrantNamespaceID), selectedGrantRole);
  }

  return (
    <>
      <PageHeader
        description="Review account state, access, and sign-in readiness."
        navigation={
          <BreadcrumbTrail
            items={[
              { label: "Root" },
              { label: "Users", onClick: onBack },
              { current: true, label: user.username }
            ]}
            label="User location"
          />
        }
        title={user.username}
      />
      <div className={styles.detailWorkspace}>
        <section className={`${styles.identityPanel} polished-panel polished-panel--accent-top`} aria-label="User identity">
          <div className={styles.detailHeader}>
            <div>
              <p className="eyebrow">Account</p>
              <h2>Summary</h2>
              <p>{user.lastSeen}</p>
            </div>
            <ResourceStatus tone={user.status}>{user.status === "active" ? "Active" : "Disabled"}</ResourceStatus>
          </div>
          <div className={styles.detailFacts}>
            <DetailFact label="Role" value={user.role} tone={roleTone(user.role)} />
            <DetailFact label="Provider" value="Local" />
            <DetailFact label="Account ID" value={user.id} />
          </div>
        </section>

        <section className={`${styles.detailPanel} polished-panel polished-panel--accent-top`} aria-labelledby="user-access-title">
          <div className={styles.detailCopy}>
            <p className="eyebrow">Access</p>
            <h2 id="user-access-title">Role Bindings</h2>
            <p>Grant namespace-scoped access for this account. Bindings inherit through child namespaces unless inheritance is stopped.</p>
          </div>
          <RoleBindingPanel
            ariaLabel="Role bindings"
            canGrant={canGrantBinding}
            emptyMessage="No namespace role bindings."
            onGrant={handleGrantBinding}
            onPrimaryChange={(namespaceID) => setBindingValues({ ...bindingValues, namespaceID })}
            onRoleChange={(role) => setBindingValues({ ...bindingValues, role })}
            primaryLabel="Namespace"
            primaryName="roleBindingNamespace"
            primaryOptions={grantNamespaceOptions}
            primaryValue={selectedGrantNamespaceID}
            roleOptions={grantRoleOptions}
            roleTone={roleTone}
            roleValue={selectedGrantRole}
            rows={bindings.map((binding) => ({
              caption: "Namespace",
              id: binding.id,
              label: binding.namespacePath === "/" ? "Root" : binding.namespacePath,
              onRevoke: () => onRevokeRoleBinding(user.id, binding.namespaceID, binding.role),
              role: binding.role
            }))}
          />
          <div className={styles.detailActions}>
            <Button onClick={() => onUpdateUserStatus(user.id, nextStatus)} type="button" variant="quiet">
              {user.status === "active" ? "Disable" : "Activate"}
            </Button>
            <Button disabled={user.username === "admin"} onClick={() => onRemoveUser(user)} type="button" variant="danger">
              Remove User
            </Button>
          </div>
        </section>
      </div>
      {children}
    </>
  );
}

function DetailFact({ label, tone, value }: { label: string; tone?: ReturnType<typeof roleTone>; value: string }) {
  return (
    <div className={styles.detailFact}>
      <span>{label}</span>
      {tone ? <ResourceStatus tone={tone}>{value}</ResourceStatus> : <strong>{value}</strong>}
    </div>
  );
}
