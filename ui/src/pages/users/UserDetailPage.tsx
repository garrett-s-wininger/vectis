import type { FormEvent, ReactNode } from "react";
import { useMemo, useRef, useState } from "react";
import { Fingerprint, KeyRound, ShieldCheck } from "lucide-react";
import { BreadcrumbTrail, Button, OperationalFact, PageHeader, ResourceStatus } from "../../components";
import type { Namespace, RoleBindingRole, User, UserStatus } from "../../domain/console";
import { roleBindingRoleOptions } from "../../domain/consoleOptions";
import { RoleBindingPanel } from "../shared";
import styles from "../UsersPage.module.css";
import { UserRemoveButton } from "./UserRemoveButton";
import { roleTone } from "./UserPresentation";

type UserDetailPageProps = {
  children?: ReactNode;
  namespaces: Namespace[];
  onBack: () => void;
  onGrantRoleBinding: (
    userID: string,
    namespaceID: number,
    role: RoleBindingRole,
    previousRole?: RoleBindingRole
  ) => Promise<void> | void;
  onRemoveUser: (user: User) => Promise<void> | void;
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
  const [statusAction, setStatusAction] = useState<UserStatus | null>(null);
  const [removing, setRemoving] = useState(false);
  const [bindingPending, setBindingPending] = useState(false);
  const statusInFlight = useRef(false);

  async function updateStatus() {
    if (statusInFlight.current || removing || bindingPending) return;
    statusInFlight.current = true;
    setStatusAction(nextStatus);

    try {
      await onUpdateUserStatus(user.id, nextStatus);
    } catch {
      // NOTE(garrett): The app-level alert owns status-change failures.
    } finally {
      statusInFlight.current = false;
      setStatusAction(null);
    }
  }

  async function removeUser(target: User) {
    setRemoving(true);

    try {
      await onRemoveUser(target);
    } finally {
      setRemoving(false);
    }
  }

  const nextStatus = user.status === "active" ? "disabled" : "active";
  const bindings = useMemo(() => user.roleBindings ?? [], [user.roleBindings]);
  const [bindingValues, setBindingValues] = useState(() => ({
    namespaceID: String(namespaces[0]?.id ?? ""),
    role: undefined as RoleBindingRole | undefined
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
  const grantRoleOptions = roleBindingRoleOptions;
  const selectedGrantRole = bindingValues.role ?? selectedGrantBinding?.role ?? "Viewer";
  const canGrantBinding =
    Boolean(selectedGrantNamespaceID) &&
    !bindings.some(
      (binding) => binding.namespaceID === Number(selectedGrantNamespaceID) && binding.role === selectedGrantRole
    );

  async function handleGrantBinding(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canGrantBinding) {
      return;
    }

    await onGrantRoleBinding(user.id, Number(selectedGrantNamespaceID), selectedGrantRole, selectedGrantBinding?.role);
  }

  return (
    <>
      <PageHeader
        description="Review account state, access, and sign-in readiness."
        navigation={
          <BreadcrumbTrail
            items={[{ label: "Manage" }, { label: "Users", onClick: onBack }, { current: true, label: user.username }]}
            label="User location"
          />
        }
        title={user.username}
      />
      <div className={styles.detailWorkspace}>
        <section
          className={`${styles.identityPanel} polished-panel polished-panel--accent-top`}
          aria-label="User identity"
        >
          <div className={styles.detailHeader}>
            <div>
              <p className="eyebrow">Account</p>
              <h2>Summary</h2>
              <p>{user.lastSeen}</p>
            </div>
            <ResourceStatus tone={user.status}>{user.status === "active" ? "Active" : "Disabled"}</ResourceStatus>
          </div>
          <dl className={styles.detailFacts}>
            <OperationalFact
              icon={ShieldCheck}
              label="Role"
              value={<ResourceStatus tone={roleTone(user.role)}>{user.role}</ResourceStatus>}
            />
            <OperationalFact icon={KeyRound} label="Provider" value="Local" />
            <OperationalFact icon={Fingerprint} label="Account ID" value={user.id} wrap />
          </dl>
        </section>

        <section
          className={`${styles.detailPanel} polished-panel polished-panel--accent-top polished-panel--overlay-safe`}
          aria-labelledby="user-access-title"
        >
          <div className={styles.detailCopy}>
            <p className="eyebrow">Access</p>
            <h2 id="user-access-title">Role Bindings</h2>
            <p>
              Grant namespace-scoped access for this account. Bindings inherit through child namespaces unless
              inheritance is stopped.
            </p>
          </div>
          <RoleBindingPanel
            ariaLabel="Role bindings"
            disabled={statusAction !== null || removing}
            onPendingChange={setBindingPending}
            submitLabel={selectedGrantBinding ? "Update Role" : "Grant"}
            canGrant={canGrantBinding}
            emptyMessage="No namespace role bindings."
            onGrant={handleGrantBinding}
            onPrimaryChange={(namespaceID) => setBindingValues({ namespaceID, role: undefined })}
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
            <Button
              aria-describedby={`account-status-help-${user.id}`}
              onClick={() => void updateStatus()}
              disabled={removing || bindingPending}
              pending={statusAction !== null}
              pendingLabel={(statusAction ?? nextStatus) === "disabled" ? "Disabling…" : "Enabling…"}
              type="button"
              variant="quiet"
            >
              {user.status === "active" ? "Disable" : "Enable"}
            </Button>
            <UserRemoveButton
              key={user.id}
              user={user}
              onRemove={removeUser}
              disabled={statusAction !== null || bindingPending}
            />
          </div>
          <p id={`account-status-help-${user.id}`}>
            Disabling preserves namespace roles and revokes sessions and API tokens. Enabling restores role access.
          </p>
        </section>
      </div>
      {children}
    </>
  );
}
