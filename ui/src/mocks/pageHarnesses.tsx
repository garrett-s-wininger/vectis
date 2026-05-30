import type { ReactNode } from "react";
import { useState } from "react";
import type { Cell } from "../domain/console";
import {
  canDeleteMockNamespace,
  createMockConsoleDataSnapshot,
  createMockJob,
  createMockNamespace,
  createMockUser,
  deleteMockJob,
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
  const scopedData = scopeMockConsoleData(data, namespacePath);

  return (
    <JobsPage
      jobs={scopedData.jobs}
      namespacePath={namespacePath}
      onCreateJob={(input) => setData((current) => createMockJob(current, input))}
      onDeleteJob={(jobID) => setData((current) => deleteMockJob(current, jobID))}
      onTriggerRun={(jobID) => setData((current) => triggerMockRun(current, jobID))}
      onUpdateJob={(jobID, input) => setData((current) => updateMockJob(current, jobID, input))}
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
  const scopedData = scopeMockConsoleData(data, namespacePath);

  return (
    <RunsPage
      namespacePath={namespacePath}
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
      namespacePath={namespacePath}
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
