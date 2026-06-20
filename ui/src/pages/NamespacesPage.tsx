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
import type { Job, Namespace, NewNamespace, UpdateNamespace } from "../domain/console";
import { EmptyStateRail, PageMissingState, ResourceStatus, ResourceTitle } from "./shared";
import styles from "./NamespacesPage.module.css";

type NamespacesPageProps = {
  canDeleteNamespace: (namespaceID: number) => boolean;
  editorMode: { kind: "edit"; namespaceID: number } | null;
  jobs: Job[];
  namespaces: Namespace[];
  onCloseEditor: () => void;
  onConfigureNamespace: (namespaceID: number) => void;
  onCreateNamespace: (input: NewNamespace) => Promise<void> | void;
  onDeleteNamespace: (namespaceID: number) => Promise<void> | void;
  onOpenJobs: (namespacePath: string) => void;
  onOpenNamespace: (namespaceID: number) => void;
  onOpenNamespaces: () => void;
  onUpdateNamespace: (namespaceID: number, input: UpdateNamespace) => Promise<void> | void;
  selectedNamespaceMissing?: boolean;
  selectedNamespaceID?: number;
};

export function NamespacesPage({
  canDeleteNamespace,
  editorMode,
  jobs,
  namespaces,
  onCloseEditor,
  onConfigureNamespace,
  onCreateNamespace,
  onDeleteNamespace,
  onOpenJobs,
  onOpenNamespace,
  onOpenNamespaces,
  onUpdateNamespace,
  selectedNamespaceMissing = false,
  selectedNamespaceID
}: NamespacesPageProps) {
  const [isCreating, setIsCreating] = useState(false);
  const [values, setValues] = useState<NewNamespace>({
    description: "",
    name: "",
    parentID: 1
  });

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!values.name.trim()) {
      return;
    }

    await onCreateNamespace(values);
    setValues({ description: "", name: "", parentID: values.parentID });
    setIsCreating(false);
  }

  const selectedNamespace = selectedNamespaceID
    ? namespaces.find((namespace) => namespace.id === selectedNamespaceID)
    : null;

  const namespaceOptions = namespaces.map((namespace) => ({
    label: namespace.path === "/" ? "Root" : namespace.path,
    value: String(namespace.id)
  }));

  if (selectedNamespaceMissing) {
    return <NamespaceNotFound onOpenNamespaces={onOpenNamespaces} />;
  }

  if (selectedNamespaceID) {
    if (!selectedNamespace) {
      return <NamespaceNotFound onOpenNamespaces={onOpenNamespaces} />;
    }

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
        canDeleteNamespace={canDeleteNamespace(selectedNamespace.id)}
        jobs={jobs}
        namespace={selectedNamespace}
        namespaces={namespaces}
        onDeleteNamespace={onDeleteNamespace}
        onConfigureNamespace={onConfigureNamespace}
        onOpenJobs={onOpenJobs}
        onOpenNamespace={onOpenNamespace}
        onOpenNamespaces={onOpenNamespaces}
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
      cell: (namespace) => <ResourceStatus tone={roleTone(namespace.role)}>{namespace.role}</ResourceStatus>,
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
          !isCreating ? (
            <Button onClick={() => setIsCreating(true)} type="button">
              Create
            </Button>
          ) : null
        }
        navigation={
          <BreadcrumbTrail
            items={[
              { label: "Root" },
              { current: true, label: "Namespaces" }
            ]}
            label="Namespaces location"
          />
        }
        title="Namespaces"
      />
      <div className={styles.workspace}>
        {isCreating ? (
          <section className={`${styles.createPanel} polished-panel polished-panel--accent-top`} aria-labelledby="create-namespace-title">
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
                value={String(values.parentID)}
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

        <section className={`${styles.summary} polished-panel polished-panel--accent-top`} aria-label="Namespace summary">
          <div className={styles.summaryCopy}>
            <h2>Hierarchy</h2>
            <p>Boundaries and inheritance at a glance.</p>
          </div>
          <div className={styles.summaryFacts} aria-label="Namespace totals">
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
                <Button
                  onClick={() => {
                    setValues({ description: "Definitions for a team or product area.", name: "team-a", parentID: 1 });
                    setIsCreating(true);
                  }}
                  variant="quiet"
                >
                  Fill Example
                </Button>
              }
              description="Create a namespace to give jobs and access policies a first boundary."
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
        { label: "Root" },
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
  canDeleteNamespace,
  jobs,
  namespace,
  namespaces,
  onDeleteNamespace,
  onConfigureNamespace,
  onOpenJobs,
  onOpenNamespace,
  onOpenNamespaces
}: {
  canDeleteNamespace: boolean;
  jobs: Job[];
  namespace: Namespace;
  namespaces: Namespace[];
  onDeleteNamespace: (namespaceID: number) => Promise<void> | void;
  onConfigureNamespace: (namespaceID: number) => void;
  onOpenJobs: (namespacePath: string) => void;
  onOpenNamespace: (namespaceID: number) => void;
  onOpenNamespaces: () => void;
}) {
  const childNamespaces = namespaces.filter((candidate) => candidate.parentID === namespace.id);
  const namespaceJobs = jobs.filter((job) => job.namespacePath === namespace.path);
  const displayPath = namespace.path === "/" ? "Root" : namespace.path;
  const description = namespace.description ?? namespaceFallbackDescription(namespaces, namespace);

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
      cell: (child) => <ResourceStatus tone={roleTone(child.role)}>{child.role}</ResourceStatus>,
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
      cell: (job) => <ResourceStatus tone={job.status}>{job.status === "enabled" ? "Enabled" : "Paused"}</ResourceStatus>,
      width: "120px"
    }
  ];

  return (
    <>
      <PageHeader
        actions={
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
        }
        description={description}
        navigation={
          <BreadcrumbTrail
            items={[
              { label: "Root" },
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
          <div className={styles.summaryFacts} aria-label="Namespace summary">
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
              value={namespace.role}
              variant="plain"
            />
          </div>
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
              { label: "Root" },
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
        <section className={`${styles.editorPanel} polished-panel polished-panel--accent-top`} aria-labelledby="namespace-config-title">
          <div className={styles.createCopy}>
            <p className="eyebrow">Namespace</p>
            <h2 id="namespace-config-title">Metadata</h2>
            <p>Name, parent, and path are fixed for this first configuration pass.</p>
          </div>
          <form className={styles.configForm} onSubmit={handleSubmit}>
            <FormField disabled label="Name" name="namespaceName" value={namespace.name} wide />
            <FormField disabled label="Path" name="namespacePath" value={namespace.path === "/" ? "Root" : namespace.path} wide />
            <FormField disabled label="Parent" name="namespaceParent" value={parentPathFor(namespaces, namespace)} wide />
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

function roleTone(role: Namespace["role"]) {
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
