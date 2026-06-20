import type { FormEvent } from "react";
import { useState } from "react";
import { BreadcrumbTrail } from "../components";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { EmptyStatePanel } from "../components";
import { FormField } from "../components";
import { PageHeader } from "../components";
import { SelectField } from "../components";
import type { Namespace, NewNamespace } from "../domain/console";
import { ResourceStatus, ResourceTitle, TableActions } from "./shared";
import styles from "./NamespacesPage.module.css";

type NamespacesPageProps = {
  canDeleteNamespace: (namespaceID: number) => boolean;
  namespaces: Namespace[];
  onCreateNamespace: (input: NewNamespace) => Promise<void> | void;
  onDeleteNamespace: (namespaceID: number) => Promise<void> | void;
};

export function NamespacesPage({
  canDeleteNamespace,
  namespaces,
  onCreateNamespace,
  onDeleteNamespace
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

  const namespaceOptions = namespaces.map((namespace) => ({
    label: namespace.path === "/" ? "Root" : namespace.path,
    value: String(namespace.id)
  }));

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
    },
    {
      align: "end",
      header: "Actions",
      cell: (namespace) => (
        <TableActions>
          <Button
            aria-label={`Delete ${namespace.path}`}
            disabled={!canDeleteNamespace(namespace.id)}
            onClick={() => onDeleteNamespace(namespace.id)}
            variant="quiet"
          >
            Delete
          </Button>
        </TableActions>
      ),
      width: "116px"
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
            <p>Use namespaces to separate definitions while letting access inherit from broader boundaries.</p>
          </div>
          <div className={styles.summaryFacts} aria-label="Namespace totals">
            <NamespaceFact label="Total" value={String(namespaceStats.total)} />
            <NamespaceFact label="Root Children" value={String(namespaceStats.rootChildren)} />
            <NamespaceFact label="Stopped Inheritance" value={String(namespaceStats.stoppedInheritance)} />
          </div>
        </section>

        {namespaces.length > 0 ? (
          <DataTable
            columns={columns}
            emptyMessage="No namespaces loaded."
            getRowKey={(namespace) => String(namespace.id)}
            rows={namespaces}
          />
        ) : (
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
        )}
      </div>
    </>
  );
}

function NamespaceFact({ label, value }: { label: string; value: string }) {
  return (
    <div className={styles.fact}>
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
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
