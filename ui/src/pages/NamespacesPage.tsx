import { useConsoleAccess } from "../domain/access";
import type { FormEvent } from "react";
import { useMemo, useState } from "react";
import { BreadcrumbTrail } from "../components";
import { AppState, Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { EmptyStatePanel } from "../components";
import { FormField } from "../components";
import { MetricCard } from "../components";
import { PageHeader } from "../components";
import { ResourceStatus } from "../components";
import { SelectField } from "../components";
import type { Job, Namespace, NewNamespace, RoleBindingRole, UpdateNamespace, User } from "../domain/console";
import { roleBindingRoleOptions } from "../domain/consoleOptions";
import { EmptyStateRail, PageMissingState, ResourceTitle, RoleBindingPanel } from "./shared";
import styles from "./NamespacesPage.module.css";

type NamespacesPageProps = {
  unavailableBindings?: string[];
  usersUnavailable?: boolean;
  canDeleteNamespace: (namespaceID: number) => boolean;
  editorMode: { kind: "edit"; namespaceID: number } | null;
  jobs: Job[];
  namespaces: Namespace[];
  onCloseEditor: () => void;
  onConfigureNamespace: (namespaceID: number) => void;
  onCreateNamespace: (input: NewNamespace) => Promise<void> | void;
  onDeleteNamespace: (namespaceID: number) => Promise<void> | void;
  onGrantRoleBinding: (
    userID: string,
    namespaceID: number,
    role: RoleBindingRole,
    previousRole?: RoleBindingRole
  ) => Promise<void> | void;
  onOpenJobs: (namespacePath: string) => void;
  onOpenNamespace: (namespaceID: number) => void;
  onOpenNamespaces: () => void;
  onRevokeRoleBinding: (userID: string, namespaceID: number, role: RoleBindingRole) => Promise<void> | void;
  onUpdateNamespace: (namespaceID: number, input: UpdateNamespace) => Promise<void> | void;
  selectedNamespaceMissing?: boolean;
  selectedNamespaceID?: number;
  users: User[];
};

export function NamespacesPage({
  unavailableBindings = [],
  usersUnavailable = false,
  canDeleteNamespace,
  editorMode,
  jobs,
  namespaces,
  onCloseEditor,
  onConfigureNamespace,
  onCreateNamespace,
  onDeleteNamespace,
  onGrantRoleBinding,
  onOpenJobs,
  onOpenNamespace,
  onOpenNamespaces,
  onRevokeRoleBinding,
  onUpdateNamespace,
  selectedNamespaceMissing = false,
  selectedNamespaceID,
  users
}: NamespacesPageProps) {
  const { can } = useConsoleAccess();
  const [isCreating, setIsCreating] = useState(false);
  const [values, setValues] = useState<NewNamespace>({
    description: "",
    name: "",
    parentID: namespaces.find((namespace) => can("admin:*", namespace.path))?.id ?? 1
  });

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!values.name.trim()) {
      return;
    }

    if (!selectedParentID) return;
    await onCreateNamespace({ ...values, parentID: selectedParentID });
    setValues({ description: "", name: "", parentID: values.parentID });
    setIsCreating(false);
  }

  const selectedNamespace = selectedNamespaceID
    ? namespaces.find((namespace) => namespace.id === selectedNamespaceID)
    : null;

  const namespaceOptions = namespaces
    .filter((namespace) => can("admin:*", namespace.path))
    .map((namespace) => ({
      label: namespace.path === "/" ? "Root" : namespace.path,
      value: String(namespace.id)
    }));

  const selectedParentID = namespaceOptions.some((option) => option.value === String(values.parentID))
    ? values.parentID
    : Number(namespaceOptions[0]?.value);

  if (selectedNamespaceMissing) {
    return <NamespaceNotFound onOpenNamespaces={onOpenNamespaces} />;
  }

  if (selectedNamespaceID) {
    if (!selectedNamespace) {
      return <NamespaceNotFound onOpenNamespaces={onOpenNamespaces} />;
    }

    if (editorMode?.kind === "edit" && !can("admin:*", selectedNamespace.path))
      return (
        <AppState
          title="Namespace Administration Access Required"
          description="Your account can inspect this namespace but cannot change its settings."
          tone="error"
        />
      );

    if (editorMode?.kind === "edit") {
      return (
        <NamespaceEditor
          namespace={selectedNamespace}
          namespaces={namespaces}
          onCancel={onCloseEditor}
          onOpenNamespace={onOpenNamespace}
          onOpenNamespaces={onOpenNamespaces}
          onUpdateNamespace={onUpdateNamespace}
        />
      );
    }

    return (
      <NamespaceDetail
        bindingsAvailable={!usersUnavailable && !unavailableBindings.includes(selectedNamespace.path)}
        canDeleteNamespace={canDeleteNamespace(selectedNamespace.id)}
        jobs={jobs}
        namespace={selectedNamespace}
        namespaces={namespaces}
        onDeleteNamespace={onDeleteNamespace}
        onConfigureNamespace={onConfigureNamespace}
        onGrantRoleBinding={onGrantRoleBinding}
        onOpenJobs={onOpenJobs}
        onOpenNamespace={onOpenNamespace}
        onOpenNamespaces={onOpenNamespaces}
        onRevokeRoleBinding={onRevokeRoleBinding}
        users={users}
      />
    );
  }

  const namespaceStats = summarizeNamespaces(namespaces);

  const columns: DataTableColumn<Namespace>[] = [
    {
      header: "Namespace",
      cell: (namespace) => (
        <ResourceTitle
          subtitle={namespace.description ?? namespaceFallbackDescription(namespaces, namespace)}
          title={namespace.path === "/" ? "Root" : namespace.path}
        />
      )
    },
    {
      header: "Access",
      cell: (namespace) => <NamespaceAccess namespace={namespace} />,
      width: "124px"
    },
    {
      align: "end",
      header: "Inheritance",
      cell: (namespace) => (
        <ResourceStatus tone={namespace.breakInheritance ? "paused" : "enabled"}>
          {namespace.breakInheritance ? "Stopped" : "Inherited"}
        </ResourceStatus>
      ),
      width: "132px"
    }
  ];

  return (
    <>
      <PageHeader
        description="Namespace boundaries, inherited access, and where stored definitions are organized."
        actions={
          !isCreating && namespaceOptions.length > 0 ? (
            <Button onClick={() => setIsCreating(true)} type="button">
              Create
            </Button>
          ) : null
        }
        navigation={
          <BreadcrumbTrail
            items={[{ label: "Manage" }, { current: true, label: "Namespaces" }]}
            label="Namespaces location"
          />
        }
        title="Namespaces"
      />
      <div className={styles.workspace}>
        {isCreating && namespaceOptions.length > 0 ? (
          <section
            className={`${styles.createPanel} polished-panel polished-panel--accent-top polished-panel--overlay-safe`}
            aria-labelledby="create-namespace-title"
          >
            <div className={styles.createCopy}>
              <p className="eyebrow">Create</p>
              <h2 id="create-namespace-title">New Namespace</h2>
              <p>Choose a short path segment, parent boundary, and description for the namespace.</p>
            </div>
            <form className={styles.form} onSubmit={handleSubmit}>
              <FormField
                label="Name"
                name="namespaceName"
                onChange={(event) => setValues({ ...values, name: event.target.value })}
                pattern="[A-Za-z0-9_-]+"
                placeholder="team-a"
                required
                value={values.name}
                wide
              />
              <SelectField
                label="Parent"
                name="parentNamespace"
                onChange={(event) => setValues({ ...values, parentID: Number(event.target.value) })}
                options={namespaceOptions}
                value={String(selectedParentID)}
                wide
              />
              <FormField
                label="Description"
                name="namespaceDescription"
                onChange={(event) => setValues({ ...values, description: event.target.value })}
                placeholder="Jobs and access for a product area"
                value={values.description ?? ""}
                wide
              />
              <div className={styles.formActions}>
                <Button onClick={() => setIsCreating(false)} type="button" variant="quiet">
                  Cancel
                </Button>
                <Button type="submit">Create Namespace</Button>
              </div>
            </form>
          </section>
        ) : null}

        <section
          className={`${styles.summary} polished-panel polished-panel--accent-top`}
          aria-label="Namespace summary"
        >
          <div className={styles.summaryCopy}>
            <h2>Hierarchy</h2>
            <p>Boundaries and inheritance at a glance.</p>
          </div>
          <div className={styles.summaryFacts} role="group" aria-label="Namespace totals">
            <MetricCard detail="Available boundaries" label="Total" value={namespaceStats.total} variant="plain" />
            <MetricCard
              detail="Directly under Root"
              label="Root Children"
              value={namespaceStats.rootChildren}
              variant="plain"
            />
            <MetricCard
              detail="Custom access boundary"
              label="Stopped Inheritance"
              value={namespaceStats.stoppedInheritance}
              variant="plain"
            />
          </div>
        </section>

        {namespaces.length > 0 ? (
          <DataTable
            columns={columns}
            emptyMessage="No namespaces found."
            getRowActionLabel={(namespace) => `View ${namespace.path}`}
            getRowKey={(namespace) => String(namespace.id)}
            onRowClick={(namespace) => onOpenNamespace(namespace.id)}
            rows={namespaces}
          />
        ) : (
          <EmptyStateRail>
            <EmptyStatePanel
              actions={
                namespaceOptions.length > 0 ? (
                  <Button
                    onClick={() => {
                      setValues({
                        description: "Definitions for a team or product area.",
                        name: "team-a",
                        parentID: 1
                      });
                      setIsCreating(true);
                    }}
                    variant="quiet"
                  >
                    Fill Example
                  </Button>
                ) : undefined
              }
              description={
                namespaceOptions.length > 0
                  ? "Create a namespace to give jobs and access policies a first boundary."
                  : "Your account has no namespaces available. An administrator can grant access."
              }
              eyebrow="No Namespaces"
              title="Start With a Boundary"
            />
          </EmptyStateRail>
        )}
      </div>
    </>
  );
}

function NamespaceNotFound({ onOpenNamespaces }: { onOpenNamespaces: () => void }) {
  return (
    <PageMissingState
      actionLabel="View Namespaces"
      breadcrumbs={[
        { label: "Manage" },
        { label: "Namespaces", onClick: onOpenNamespaces },
        { current: true, label: "Missing" }
      ]}
      description="This namespace is no longer available, or the route points to an ID that does not exist."
      label="Namespace location"
      onAction={onOpenNamespaces}
      panelDescription="Return to the namespace index to choose an active boundary."
      panelEyebrow="Missing Namespace"
      panelTitle="No Namespace Found"
      title="Namespace Not Found"
    />
  );
}

function NamespaceDetail({
  bindingsAvailable,
  canDeleteNamespace,
  jobs,
  namespace,
  namespaces,
  onDeleteNamespace,
  onConfigureNamespace,
  onGrantRoleBinding,
  onOpenJobs,
  onOpenNamespace,
  onOpenNamespaces,
  onRevokeRoleBinding,
  users
}: {
  bindingsAvailable: boolean;
  canDeleteNamespace: boolean;
  jobs: Job[];
  namespace: Namespace;
  namespaces: Namespace[];
  onDeleteNamespace: (namespaceID: number) => Promise<void> | void;
  onConfigureNamespace: (namespaceID: number) => void;
  onGrantRoleBinding: (
    userID: string,
    namespaceID: number,
    role: RoleBindingRole,
    previousRole?: RoleBindingRole
  ) => Promise<void> | void;
  onOpenJobs: (namespacePath: string) => void;
  onOpenNamespace: (namespaceID: number) => void;
  onOpenNamespaces: () => void;
  onRevokeRoleBinding: (userID: string, namespaceID: number, role: RoleBindingRole) => Promise<void> | void;
  users: User[];
}) {
  const { can } = useConsoleAccess();
  const canManage = can("admin:*", namespace.path);
  const childNamespaces = namespaces.filter((candidate) => candidate.parentID === namespace.id);
  const namespaceJobs = jobs.filter((job) => job.namespacePath === namespace.path);
  const displayPath = namespace.path === "/" ? "Root" : namespace.path;
  const description = namespace.description ?? namespaceFallbackDescription(namespaces, namespace);
  const namespaceBindings = users.flatMap((user) =>
    (user.roleBindings ?? [])
      .filter((binding) => binding.namespaceID === namespace.id)
      .map((binding) => ({
        ...binding,
        username: binding.username ?? user.username
      }))
  );

  const [bindingValues, setBindingValues] = useState(() => ({
    role: undefined as RoleBindingRole | undefined,
    userID: users[0]?.id ?? ""
  }));

  const userOptions = useMemo(
    () =>
      users.map((user) => ({
        label: user.username,
        value: user.id
      })),
    [users]
  );

  const selectedBinding = namespaceBindings.find((binding) => binding.userID === bindingValues.userID);
  const roleOptions = roleBindingRoleOptions;
  const selectedRole = bindingValues.role ?? selectedBinding?.role ?? "Viewer";
  const canGrantBinding =
    Boolean(bindingValues.userID) &&
    !namespaceBindings.some((binding) => binding.userID === bindingValues.userID && binding.role === selectedRole);

  async function handleGrantBinding(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canGrantBinding) {
      return;
    }

    await onGrantRoleBinding(bindingValues.userID, namespace.id, selectedRole, selectedBinding?.role);
  }

  const childColumns: DataTableColumn<Namespace>[] = [
    {
      header: "Namespace",
      cell: (child) => (
        <ResourceTitle
          subtitle={child.description ?? namespaceFallbackDescription(namespaces, child)}
          title={child.path === "/" ? "Root" : child.path}
        />
      )
    },
    {
      align: "end",
      header: "Access",
      cell: (child) => <NamespaceAccess namespace={child} />,
      width: "124px"
    }
  ];

  const jobColumns: DataTableColumn<Job>[] = [
    {
      header: "Job",
      cell: (job) => <ResourceTitle subtitle={job.description ?? job.sourceDetail} title={job.name} />
    },
    {
      align: "end",
      header: "State",
      cell: (job) => (
        <ResourceStatus tone={job.status}>{job.status === "enabled" ? "Enabled" : "Paused"}</ResourceStatus>
      ),
      width: "120px"
    }
  ];

  return (
    <>
      <PageHeader
        actions={
          canManage ? (
            <>
              <Button
                disabled={!canDeleteNamespace}
                onClick={() => onDeleteNamespace(namespace.id)}
                type="button"
                variant="quiet"
              >
                Delete
              </Button>
              <Button onClick={() => onConfigureNamespace(namespace.id)} type="button">
                Configure
              </Button>
            </>
          ) : undefined
        }
        description={description}
        navigation={
          <BreadcrumbTrail
            items={[
              { label: "Manage" },
              { label: "Namespaces", onClick: onOpenNamespaces },
              { current: true, label: displayPath }
            ]}
            label="Namespace location"
          />
        }
        title={displayPath}
      />

      <div className={styles.workspace}>
        <section
          className={`${styles.detailPanel} ${styles.overviewPanel} polished-panel polished-panel--accent-top`}
          aria-labelledby="namespace-overview-title"
        >
          <div className={styles.detailHeader}>
            <div>
              <h2 id="namespace-overview-title">Overview</h2>
              <p>Counts and access posture for this boundary.</p>
            </div>
            <ResourceStatus tone={namespace.breakInheritance ? "paused" : "enabled"}>
              {namespace.breakInheritance ? "Stopped Inheritance" : "Inherited Access"}
            </ResourceStatus>
          </div>
          <div className={styles.summaryFacts} role="group" aria-label="Namespace summary">
            <MetricCard
              detail="Immediate descendants"
              label="Child Namespaces"
              value={childNamespaces.length}
              variant="plain"
            />
            <MetricCard
              detail="Directly organized here"
              label="Stored Jobs"
              value={namespaceJobs.length}
              variant="plain"
            />
            <MetricCard
              detail={namespace.breakInheritance ? "Local boundary" : "Inherited boundary"}
              label="Access"
              value={namespace.role === "Not reported" ? namespaceAccessLabel(can, namespace.path) : namespace.role}
              variant="plain"
            />
          </div>
        </section>

        <section
          className={`${styles.detailPanel} polished-panel polished-panel--overlay-safe`}
          aria-labelledby="namespace-access-title"
        >
          <div className={styles.detailHeader}>
            <div>
              <h2 id="namespace-access-title">Access</h2>
              <p>Users with direct role bindings on this namespace.</p>
            </div>
          </div>
          {canManage && bindingsAvailable ? (
            <RoleBindingPanel
              ariaLabel="Namespace role bindings"
              submitLabel={selectedBinding ? "Update Role" : "Grant"}
              canGrant={canGrantBinding}
              emptyMessage="No direct role bindings."
              onGrant={handleGrantBinding}
              onPrimaryChange={(userID) => setBindingValues({ userID, role: undefined })}
              onRoleChange={(role) => setBindingValues({ ...bindingValues, role })}
              primaryLabel="User"
              primaryName="namespaceBindingUser"
              primaryOptions={userOptions}
              primaryValue={bindingValues.userID}
              roleOptions={roleOptions}
              roleTone={roleTone}
              roleValue={selectedRole}
              rows={namespaceBindings.map((binding) => ({
                caption: "User",
                id: binding.id,
                label: binding.username ?? binding.userID,
                onRevoke: () => onRevokeRoleBinding(binding.userID, namespace.id, binding.role),
                role: binding.role
              }))}
            />
          ) : (
            <p>Your account cannot view the users or role bindings for this namespace.</p>
          )}
        </section>

        <section className={`${styles.detailPanel} polished-panel`} aria-labelledby="namespace-jobs-title">
          <div className={styles.detailHeader}>
            <div>
              <h2 id="namespace-jobs-title">Jobs</h2>
              <p>Stored definitions directly organized in this namespace.</p>
            </div>
            <Button onClick={() => onOpenJobs(namespace.path)} type="button" variant="quiet">
              View Jobs
            </Button>
          </div>
          <DataTable
            columns={jobColumns}
            emptyMessage="No stored jobs in this namespace."
            getRowKey={(job) => job.id}
            rows={namespaceJobs}
          />
        </section>

        <section className={`${styles.detailPanel} polished-panel`} aria-labelledby="namespace-children-title">
          <div className={styles.detailHeader}>
            <div>
              <h2 id="namespace-children-title">Children</h2>
              <p>Immediate boundaries nested under this namespace.</p>
            </div>
          </div>
          <DataTable
            columns={childColumns}
            emptyMessage="No child namespaces."
            getRowActionLabel={(child) => `View ${child.path}`}
            getRowKey={(child) => String(child.id)}
            onRowClick={(child) => onOpenNamespace(child.id)}
            rows={childNamespaces}
          />
        </section>
      </div>
    </>
  );
}

function NamespaceEditor({
  namespace,
  namespaces,
  onCancel,
  onOpenNamespace,
  onOpenNamespaces,
  onUpdateNamespace
}: {
  namespace: Namespace;
  namespaces: Namespace[];
  onCancel: () => void;
  onOpenNamespace: (namespaceID: number) => void;
  onOpenNamespaces: () => void;
  onUpdateNamespace: (namespaceID: number, input: UpdateNamespace) => Promise<void> | void;
}) {
  const [description, setDescription] = useState(namespace.description ?? "");

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await onUpdateNamespace(namespace.id, { description });
    onOpenNamespace(namespace.id);
  }

  return (
    <>
      <PageHeader
        description="Update the descriptive metadata operators see across namespace views."
        navigation={
          <BreadcrumbTrail
            items={[
              { label: "Manage" },
              { label: "Namespaces", onClick: onOpenNamespaces },
              { label: namespace.path === "/" ? "Root" : namespace.path, onClick: () => onOpenNamespace(namespace.id) },
              { current: true, label: "Configure" }
            ]}
            label="Namespace configure location"
          />
        }
        title="Configure Namespace"
      />

      <div className={styles.workspace}>
        <section
          className={`${styles.editorPanel} polished-panel polished-panel--accent-top`}
          aria-labelledby="namespace-config-title"
        >
          <div className={styles.createCopy}>
            <p className="eyebrow">Namespace</p>
            <h2 id="namespace-config-title">Metadata</h2>
            <p>Name, parent, and path are fixed for this first configuration pass.</p>
          </div>
          <form className={styles.configForm} onSubmit={handleSubmit}>
            <FormField disabled label="Name" name="namespaceName" value={namespace.name} wide />
            <FormField
              disabled
              label="Path"
              name="namespacePath"
              value={namespace.path === "/" ? "Root" : namespace.path}
              wide
            />
            <FormField
              disabled
              label="Parent"
              name="namespaceParent"
              value={parentPathFor(namespaces, namespace)}
              wide
            />
            <FormField
              label="Description"
              name="namespaceDescription"
              onChange={(event) => setDescription(event.target.value)}
              placeholder="Jobs and access for a product area"
              value={description}
              wide
            />
            <div className={styles.formActions}>
              <Button onClick={onCancel} type="button" variant="quiet">
                Cancel
              </Button>
              <Button type="submit">Save</Button>
            </div>
          </form>
        </section>
      </div>
    </>
  );
}

function parentPathFor(namespaces: Namespace[], namespace: Namespace) {
  const parent = namespaces.find((candidate) => candidate.id === namespace.parentID);

  return parent?.path === "/" || !parent?.path ? "Root" : parent.path;
}

function namespaceAccessLabel(can: ReturnType<typeof useConsoleAccess>["can"], path: string) {
  if (can("admin:*", path)) return "Administer";
  if (can("job:write", path)) return "Edit jobs";
  if (can("run:operator", path)) return "Operate runs";
  if (can("run:trigger", path)) return "Trigger runs";
  return "Read only";
}

function NamespaceAccess({ namespace }: { namespace: Namespace }) {
  const { can } = useConsoleAccess();
  const label = namespace.role === "Not reported" ? namespaceAccessLabel(can, namespace.path) : namespace.role;
  const tone =
    namespace.role === "Not reported"
      ? can("admin:*", namespace.path)
        ? "active"
        : can("run:operator", namespace.path)
          ? "enabled"
          : "disabled"
      : roleTone(namespace.role);

  return <ResourceStatus tone={tone}>{label}</ResourceStatus>;
}

function roleTone(role: Namespace["role"] | RoleBindingRole) {
  if (role === "Admin") {
    return "active";
  }

  if (role === "Operator") {
    return "enabled";
  }

  return "disabled";
}

function depthLabel(namespace: Namespace) {
  const depth = namespace.path.split("/").filter(Boolean).length;

  return depth === 1 ? "Top-level boundary" : `Level ${depth} boundary`;
}

function namespaceFallbackDescription(namespaces: Namespace[], namespace: Namespace) {
  if (namespace.path === "/") {
    return "Root boundary";
  }

  return `${depthLabel(namespace)} under ${parentPathFor(namespaces, namespace)}`;
}

function summarizeNamespaces(namespaces: Namespace[]) {
  return {
    rootChildren: namespaces.filter((namespace) => namespace.parentID === 1).length,
    stoppedInheritance: namespaces.filter((namespace) => namespace.breakInheritance).length,
    total: namespaces.length
  };
}
