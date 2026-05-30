import type { FormEvent } from "react";
import { useState } from "react";
import { Button } from "../components/Button";
import { DataTable, type DataTableColumn } from "../components/DataTable";
import { FormField } from "../components/FormField";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import type { MockNamespace, NewMockNamespace } from "../mocks/consoleData";

type NamespacesPageProps = {
  canDeleteNamespace: (namespaceID: number) => boolean;
  namespaces: MockNamespace[];
  onCreateNamespace: (input: NewMockNamespace) => void;
  onDeleteNamespace: (namespaceID: number) => void;
};

export function NamespacesPage({
  canDeleteNamespace,
  namespaces,
  onCreateNamespace,
  onDeleteNamespace
}: NamespacesPageProps) {
  const [values, setValues] = useState<NewMockNamespace>({
    name: "",
    parentID: 1
  });

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!values.name.trim()) {
      return;
    }

    onCreateNamespace(values);
    setValues({ ...values, name: "" });
  }

  const namespaceOptions = namespaces.map((namespace) => ({
    label: namespace.path === "/" ? "/ root" : namespace.path,
    value: String(namespace.id)
  }));

  const columns: DataTableColumn<MockNamespace>[] = [
    {
      header: "Namespace",
      cell: (namespace) => (
        <div className="resource-title">
          <strong>{namespace.path}</strong>
          <small>{namespace.path === "/" ? "Root namespace" : `Parent ${parentPathFor(namespaces, namespace)}`}</small>
        </div>
      )
    },
    {
      header: "Role",
      cell: (namespace) => namespace.role
    },
    {
      align: "end",
      header: "Inheritance",
      cell: (namespace) => (
        <span className={`resource-status resource-status--${namespace.breakInheritance ? "paused" : "enabled"}`}>
          {namespace.breakInheritance ? "Stopped" : "Inherited"}
        </span>
      )
    },
    {
      align: "end",
      header: "Actions",
      cell: (namespace) => (
        <Button
          aria-label={`Delete ${namespace.path}`}
          disabled={!canDeleteNamespace(namespace.id)}
          onClick={() => onDeleteNamespace(namespace.id)}
        >
          Delete
        </Button>
      )
    }
  ];

  return (
    <>
      <PageHeader
        description="Namespace hierarchy and inherited access boundaries."
        eyebrow="Access"
        title="Namespaces"
      />
      <form className="inline-form" onSubmit={handleSubmit}>
        <FormField
          label="Name"
          name="namespaceName"
          onChange={(event) => setValues({ ...values, name: event.target.value })}
          pattern="[A-Za-z0-9_-]+"
          required
          value={values.name}
        />
        <SelectField
          label="Parent"
          name="parentNamespace"
          onChange={(event) => setValues({ ...values, parentID: Number(event.target.value) })}
          options={namespaceOptions}
          value={String(values.parentID)}
        />
        <Button type="submit">Create namespace</Button>
      </form>
      <DataTable
        columns={columns}
        emptyMessage="No namespaces loaded."
        getRowKey={(namespace) => String(namespace.id)}
        rows={namespaces}
      />
    </>
  );
}

function parentPathFor(namespaces: MockNamespace[], namespace: MockNamespace) {
  const parent = namespaces.find((candidate) => candidate.id === namespace.parentID);

  return parent?.path ?? "/";
}
