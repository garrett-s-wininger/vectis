import type { ReactNode } from "react";
import { useState } from "react";
import type { Cell } from "../domain/console";
import {
  canDeleteMockNamespace,
  createMockConsoleDataSnapshot,
  createMockJob,
  createMockNamespace,
  createMockUser,
  deleteMockNamespace,
  deleteMockUser,
  scopeMockConsoleData,
  submitMockEphemeralRun,
  triggerMockRun,
  updateMockJob,
  updateMockUserStatus,
  type MockConsoleData
} from "./consoleData";
import { HealthPage } from "../pages/HealthPage";
import { JobsPage } from "../pages/JobsPage";
import type { JobEditorMode } from "../pages/jobs/JobEditor";
import { NamespacesPage } from "../pages/NamespacesPage";
import { RunsPage } from "../pages/RunsPage";
import { UsersPage } from "../pages/UsersPage";

export function PageStoryFrame({ children }: { children: ReactNode }) {
  return <main className="storybook-page-main">{children}</main>;
}

export function HealthPageHarness({ cells }: { cells: Cell[] }) {
  const [selectedCellID, setSelectedCellID] = useState<string | undefined>();

  return <HealthPage cells={cells} onSelectCell={setSelectedCellID} selectedCellID={selectedCellID} />;
}

export function JobsPageHarness({ namespacePath = "/" }: { namespacePath?: string }) {
  const [data, setData] = useState<MockConsoleData>(() => createMockConsoleDataSnapshot());
  const [detailJobID, setDetailJobID] = useState<string | undefined>();
  const [editorMode, setEditorMode] = useState<JobEditorMode | null>(null);
  const [selectedNamespacePath, setSelectedNamespacePath] = useState(namespacePath);
  const scopedData = scopeMockConsoleData(data, selectedNamespacePath);

  return (
    <JobsPage
      detailJobID={detailJobID}
      editorMode={editorMode}
      jobs={scopedData.jobs}
      namespaces={data.namespaces}
      namespacePath={selectedNamespacePath}
      onCloseEditor={() => setEditorMode(null)}
      onCreateJob={(input) => setData((current) => createMockJob(current, input))}
      onOpenCreate={() => setEditorMode({ kind: "create" })}
      onOpenEditor={(jobID) => setEditorMode({ kind: "edit", jobID })}
      onOpenJob={(jobID) => setDetailJobID(jobID || undefined)}
      onSelectRun={() => undefined}
      onSelectNamespace={setSelectedNamespacePath}
      onTriggerRun={(jobID) => setData((current) => triggerMockRun(current, jobID))}
      onUpdateJob={(jobID, input) => setData((current) => updateMockJob(current, jobID, input))}
      runs={scopedData.runs}
    />
  );
}

export function NamespacesPageHarness() {
  const [data, setData] = useState<MockConsoleData>(() => createMockConsoleDataSnapshot());

  return (
    <NamespacesPage
      canDeleteNamespace={(namespaceID) => canDeleteMockNamespace(data, namespaceID)}
      namespaces={data.namespaces}
      onCreateNamespace={(input) => setData((current) => createMockNamespace(current, input))}
      onDeleteNamespace={(namespaceID) => setData((current) => deleteMockNamespace(current, namespaceID))}
    />
  );
}

export function RunsPageHarness({ namespacePath = "/" }: { namespacePath?: string }) {
  const [data, setData] = useState<MockConsoleData>(() => createMockConsoleDataSnapshot());
  const [selectedNamespacePath, setSelectedNamespacePath] = useState(namespacePath);
  const scopedData = scopeMockConsoleData(data, selectedNamespacePath);

  return (
    <RunsPage
      namespaces={data.namespaces}
      namespacePath={selectedNamespacePath}
      onSelectNamespace={setSelectedNamespacePath}
      onSelectRun={() => undefined}
      onSubmitEphemeralRun={(definition) =>
        setData((current) =>
          submitMockEphemeralRun(current, {
            definition,
            namespacePath: selectedNamespacePath,
            submittedBy: "admin"
          })
        )
      }
      runs={scopedData.runs}
    />
  );
}

export function MixedSourceRunsPageHarness() {
  const namespacePath = "/team-a";
  const [data, setData] = useState<MockConsoleData>(() =>
    submitMockEphemeralRun(createMockConsoleDataSnapshot(), {
      definition: JSON.stringify({ id: "database-backfill", root: {} }, null, 2),
      namespacePath,
      submittedBy: "admin"
    })
  );
  const scopedData = scopeMockConsoleData(data, namespacePath);

  return (
    <RunsPage
      namespaces={data.namespaces}
      namespacePath={namespacePath}
      onSelectNamespace={() => undefined}
      onSelectRun={() => undefined}
      onSubmitEphemeralRun={(definition) =>
        setData((current) =>
          submitMockEphemeralRun(current, {
            definition,
            namespacePath,
            submittedBy: "admin"
          })
        )
      }
      runs={scopedData.runs}
    />
  );
}

export function UsersPageHarness() {
  const [data, setData] = useState<MockConsoleData>(() => createMockConsoleDataSnapshot());

  return (
    <UsersPage
      onCreateUser={(input) => setData((current) => createMockUser(current, input))}
      onDeleteUser={(userID) => setData((current) => deleteMockUser(current, userID))}
      onUpdateUserStatus={(userID, status) => setData((current) => updateMockUserStatus(current, userID, status))}
      users={data.users}
    />
  );
}
