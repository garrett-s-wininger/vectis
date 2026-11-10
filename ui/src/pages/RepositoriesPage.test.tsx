import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { ConsoleAccessProvider, unrestrictedAccess } from "../domain/access";
import { VectisAPIError } from "../api/client";
import type { Repository, RepositorySource } from "../data/repositories";
import { RepositoriesPage } from "./RepositoriesPage";

const repository: Repository = {
  repository_id: "builds",
  namespace: "/team",
  source_kind: "local_checkout",
  checkout_mode: "managed",
  enabled: true,
  declared: true,
  default_ref: "main",
  sync: { status: "failed", error: "Remote rejected credentials", last_finished_at_unix: 1793664000 },
  authoring: { write_definitions: true }
};

const page = {
  jobs: [{ job_id: "build", name: "build", path: ".vectis/build.json", blob_sha: "blob" }],
  invalid: [{ path: ".vectis/bad.json", error: "Unknown action" }],
  requested_ref: "main",
  resolved_commit: "commit-one",
  truncated: false
};

function source(): RepositorySource {
  return {
    credentials: vi.fn().mockResolvedValue([]),
    validate: vi.fn().mockResolvedValue({ requested_ref: "main", resolved_commit: "commit-one" }),
    create: vi.fn().mockResolvedValue({ ...repository, declared: false }),
    update: vi.fn().mockResolvedValue({ ...repository, declared: false }),
    remove: vi.fn().mockResolvedValue(undefined),
    impact: vi
      .fn()
      .mockResolvedValue({ jobs: [], automations: [], namespaces: [], unregistered_jobs: [], revision: "preview" }),
    access: vi.fn().mockResolvedValue([]),
    grant: vi.fn().mockResolvedValue([]),
    revoke: vi.fn().mockResolvedValue([]),
    namespaces: vi
      .fn()
      .mockResolvedValue([{ id: 2, name: "team", path: "/team", breakInheritance: false, role: "Viewer" }]),
    list: vi.fn().mockResolvedValue([repository]),
    get: vi.fn().mockResolvedValue(repository),
    status: vi.fn().mockResolvedValue({
      status: "ok",
      default_ref_resolved: true,
      git_repository: true,
      resolved_commit: "commit-one"
    }),
    jobs: vi.fn().mockResolvedValue(page),
    sync: vi.fn().mockResolvedValue({ ...repository, sync: { status: "succeeded" } })
  };
}

it("lists repositories in an accessible namespace and opens a short repository URL", async () => {
  const api = source();
  render(<RepositoriesPage source={api} />);

  fireEvent.click(await screen.findByRole("button", { name: "Inspect builds" }));
  expect(api.list).toHaveBeenCalledWith("/team");
  expect(window.location.pathname).toBe("/repositories/builds");
});

it("keeps repository diagnostics visible when job discovery fails", async () => {
  const api = source();
  vi.mocked(api.jobs).mockRejectedValue(new Error("Ref does not exist"));
  render(<RepositoriesPage source={api} repositoryID="builds" />);

  expect(await screen.findByText("Ref does not exist")).toBeInTheDocument();
  expect(screen.getByText("Remote rejected credentials")).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "Source" })).toBeInTheDocument();
  expect(screen.getByText("Available")).toBeInTheDocument();
  expect(screen.queryByText("No valid job definitions discovered.")).not.toBeInTheDocument();
});

it("shows invalid definitions alongside valid jobs and preserves source context when opening a job", async () => {
  render(<RepositoriesPage source={source()} repositoryID="builds" />);

  fireEvent.click(await screen.findByRole("button", { name: "View build" }));
  expect(window.location.pathname).toBe("/jobs/build");
  expect(new URLSearchParams(window.location.search).get("repository")).toBe("builds");
  expect(new URLSearchParams(window.location.search).get("namespace")).toBe("/team");
  expect(screen.getByText("Unknown action")).toBeInTheDocument();
});

it("lets read-only users inspect repositories without sync controls", async () => {
  const api = source();
  render(
    <ConsoleAccessProvider access={{ actions: ["job:read", "api:any"], namespaces: { "/team": ["job:read"] } }}>
      <RepositoriesPage source={api} repositoryID="builds" />
    </ConsoleAccessProvider>
  );

  await screen.findByRole("button", { name: "View build" });
  expect(screen.queryByRole("button", { name: "Sync now" })).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(api.get).toHaveBeenCalledTimes(2));
  await waitFor(() => expect(api.status).toHaveBeenCalledTimes(2));
  await waitFor(() => expect(api.jobs).toHaveBeenCalledTimes(2));
  expect(api.sync).not.toHaveBeenCalled();
});

it("locks duplicate sync requests, preserves known jobs, and refreshes diagnostics afterwards", async () => {
  const api = source();

  let finish!: (row: Repository) => void;
  vi.mocked(api.sync).mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      })
  );

  render(<RepositoriesPage source={api} repositoryID="builds" />);

  const job = await screen.findByRole("button", { name: "View build" });
  fireEvent.click(screen.getByRole("button", { name: "Sync now" }));

  const pending = screen.getByRole("button", { name: "Syncing…" });
  expect(pending).toBeDisabled();

  fireEvent.click(pending);
  expect(api.sync).toHaveBeenCalledExactlyOnceWith("builds");
  expect(screen.getByRole("button", { name: "View build" })).toBe(job);

  const done = { ...repository, sync: { status: "succeeded" } };
  vi.mocked(api.get).mockResolvedValue(done);
  await act(async () => finish(done));
  expect(await screen.findByText("Sync completed.")).toBeInTheDocument();

  await waitFor(() => expect(api.jobs).toHaveBeenCalledTimes(2));
});

it("allows an explicit check of a disabled external checkout without enabling it", async () => {
  const api = source();
  const disabled = { ...repository, enabled: false, checkout_mode: "external" as const, sync: { status: "succeeded" } };
  vi.mocked(api.get).mockResolvedValue(disabled);
  vi.mocked(api.sync).mockResolvedValue(disabled);
  render(<RepositoriesPage source={api} repositoryID="builds" />);

  fireEvent.click(await screen.findByRole("button", { name: "Check checkout" }));
  expect(await screen.findByText("Checkout check completed.")).toBeInTheDocument();
  expect(api.sync).toHaveBeenCalledExactlyOnceWith("builds");
  expect(screen.getByText("Local checkout", { exact: true })).toBeInTheDocument();
  expect(screen.getByText("Disabled", { exact: true })).toBeInTheDocument();
  expect(screen.getByText(/leaves this connection disabled and preserves active hotfixes/)).toBeInTheDocument();
  expect(api.update).not.toHaveBeenCalled();
});

it("reports a failed sync and unlocks the button for retry", async () => {
  const api = source();
  vi.mocked(api.sync).mockRejectedValue(new Error("Permission to fetch denied"));
  render(<RepositoriesPage source={api} repositoryID="builds" />);

  fireEvent.click(await screen.findByRole("button", { name: "Sync now" }));
  expect(await screen.findByText("Permission to fetch denied")).toBeInTheDocument();

  await waitFor(() => expect(screen.getByRole("button", { name: "Sync now" })).toBeEnabled());
});

it("pins additional definition pages to the scanned commit", async () => {
  const api = source();
  vi.mocked(api.jobs)
    .mockResolvedValueOnce({ ...page, truncated: true, next_cursor: "cursor" })
    .mockResolvedValueOnce({
      ...page,
      jobs: [{ job_id: "test", name: "test", path: ".vectis/test.json", blob_sha: "blob" }],
      invalid: []
    });

  render(<RepositoriesPage source={api} repositoryID="builds" />);

  fireEvent.click(await screen.findByRole("button", { name: "Load more definitions" }));
  await screen.findByRole("button", { name: "View test" });
  expect(api.jobs).toHaveBeenLastCalledWith("builds", "cursor", "commit-one");
  expect(screen.getByRole("button", { name: "View build" })).toBeInTheDocument();
});

it("does not discover jobs in a disabled repository", async () => {
  const api = source();
  vi.mocked(api.get).mockResolvedValue({ ...repository, enabled: false });
  render(<RepositoriesPage source={api} repositoryID="builds" />);

  expect(await screen.findByText(/Job discovery is unavailable/)).toBeInTheDocument();
  expect(api.jobs).not.toHaveBeenCalled();
});

it("shows a missing repository without inventing an empty checkout", async () => {
  const api = source();
  vi.mocked(api.get).mockRejectedValue(new VectisAPIError(404, "source_repository_not_found", "Repository not found."));
  render(<RepositoriesPage source={api} repositoryID="missing" />);

  expect(await screen.findByText("Repository Not Found")).toBeInTheDocument();
  expect(screen.queryByText("No valid job definitions discovered.")).not.toBeInTheDocument();
  expect(api.status).not.toHaveBeenCalled();
});

it("waits for permission discovery before requesting repositories", () => {
  const api = source();
  render(
    <ConsoleAccessProvider access={null}>
      <RepositoriesPage source={api} />
    </ConsoleAccessProvider>
  );

  expect(api.namespaces).not.toHaveBeenCalled();
  expect(api.list).not.toHaveBeenCalled();
});

it("does not list an explicitly inaccessible namespace", async () => {
  const api = source();
  render(
    <ConsoleAccessProvider access={{ ...unrestrictedAccess, namespaces: { "/team": ["job:read"] } }}>
      <RepositoriesPage source={api} namespacePath="/other" />
    </ConsoleAccessProvider>
  );

  expect(await screen.findByText("Choose a namespace where you can read jobs.")).toBeInTheDocument();
  expect(api.list).not.toHaveBeenCalled();
});

it("ignores an old repository response after navigating to a different repository", async () => {
  const api = source();
  let finish!: (row: Repository) => void;

  vi.mocked(api.get)
    .mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        })
    )
    .mockResolvedValue({ ...repository, repository_id: "second" });

  const { rerender } = render(<RepositoriesPage source={api} repositoryID="builds" />);
  rerender(<RepositoriesPage source={api} repositoryID="second" />);

  await screen.findByRole("button", { name: "View build" });
  await act(async () => finish(repository));
  expect(screen.getByRole("heading", { name: "second" })).toBeInTheDocument();
  expect(screen.getByLabelText("Repository location")).not.toHaveTextContent("builds");
});

it("keeps loaded repositories visible after a failed refresh and allows retry", async () => {
  const api = source();
  render(<RepositoriesPage source={api} />);

  const row = await screen.findByRole("button", { name: "Inspect builds" });
  vi.mocked(api.list).mockRejectedValueOnce(new Error("Temporary outage"));
  fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  expect(screen.getByRole("button", { name: "Refreshing…" })).toBeDisabled();
  expect(await screen.findByText("Temporary outage")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Inspect builds" })).toBe(row);
  expect(screen.getByRole("button", { name: "Refresh" })).toBeEnabled();
});

it("rejects pagination from a different revision without mixing definitions", async () => {
  const api = source();
  vi.mocked(api.jobs)
    .mockResolvedValueOnce({ ...page, truncated: true, next_cursor: "next" })
    .mockResolvedValueOnce({
      ...page,
      resolved_commit: "commit-two",
      jobs: [{ job_id: "other", name: "other", path: ".vectis/other.json", blob_sha: "two" }]
    });

  render(<RepositoriesPage source={api} repositoryID="builds" />);

  fireEvent.click(await screen.findByRole("button", { name: "Load more definitions" }));
  expect(
    await screen.findByText("Repository revision changed. Refresh to reload its definitions.")
  ).toBeInTheDocument();

  expect(screen.queryByRole("button", { name: "View other" })).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "View build" })).toBeInTheDocument();
});

it("polls only the lightweight record while a sync is running and stops on completion", async () => {
  const api = source();
  vi.mocked(api.get)
    .mockResolvedValueOnce({ ...repository, sync: { status: "running" } })
    .mockResolvedValue({ ...repository, sync: { status: "succeeded" } });

  vi.useFakeTimers();
  let unmount = () => {};

  try {
    await act(async () => {
      unmount = render(<RepositoriesPage source={api} repositoryID="builds" />).unmount;
    });

    expect(api.get).toHaveBeenCalledTimes(1);

    await act(async () => vi.advanceTimersByTimeAsync(3000));
    expect(api.get).toHaveBeenCalledTimes(3);
    expect(api.status).toHaveBeenCalledTimes(2);
    expect(api.jobs).toHaveBeenCalledTimes(2);

    await act(async () => vi.advanceTimersByTimeAsync(9000));
    expect(api.get).toHaveBeenCalledTimes(3);
  } finally {
    unmount();
    vi.useRealTimers();
  }
});

it("consolidates source diagnostics and renders a matching revision once", async () => {
  const api = source();
  vi.mocked(api.get).mockResolvedValue({ ...repository, sync: { status: "succeeded", commit: "commit-one" } });
  render(<RepositoriesPage source={api} repositoryID="builds" />);

  await screen.findByRole("button", { name: "View build" });
  expect(screen.getAllByText("commit-one", { exact: true })).toHaveLength(1);
  expect(screen.getByRole("button", { name: "Copy Revision" })).toBeInTheDocument();
  expect(screen.queryByRole("heading", { name: "Synchronization" })).not.toBeInTheDocument();
  expect(screen.queryByRole("heading", { name: "Checkout health" })).not.toBeInTheDocument();
  expect(screen.getByLabelText("Repository location")).toHaveTextContent("/teamRepositoriesbuilds");
});

it("explains a changed checkout without hiding the snapshot used by discovered jobs", async () => {
  const api = source();
  vi.mocked(api.status).mockResolvedValue({
    status: "ok",
    default_ref_resolved: true,
    path_exists: true,
    path_is_directory: true,
    git_repository: true,
    resolved_commit: "commit-two"
  });

  render(<RepositoriesPage source={api} repositoryID="builds" />);

  await screen.findByRole("button", { name: "View build" });
  expect(screen.getAllByText("commit-one", { exact: true })).toHaveLength(1);
  expect(screen.getAllByText("commit-two", { exact: true })).toHaveLength(1);
  expect(screen.getByText("Checkout changed")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Copy Current checkout revision" })).toBeInTheDocument();
});

it("updates the shared namespace from a repository's actual scope", async () => {
  const onNamespaceChange = vi.fn();
  render(<RepositoriesPage source={source()} repositoryID="builds" onNamespaceChange={onNamespaceChange} />);

  await screen.findByRole("button", { name: "View build" });
  expect(onNamespaceChange).toHaveBeenCalledWith("/team");
});

it("keeps shared administration separate from discovery and grants namespace use", async () => {
  const api = source();
  vi.mocked(api.get).mockResolvedValue({ ...repository, namespace: "" });
  vi.mocked(api.grant).mockResolvedValue([{ namespace_id: 2, namespace: "/team" }]);
  render(<RepositoriesPage source={api} repositoryID="builds" management sharedWorkspace />);

  await screen.findByRole("heading", { name: "Namespace access" });
  fireEvent.click(await screen.findByRole("button", { name: "Grant access" }));

  await screen.findByRole("button", { name: "Revoke access" });
  expect(api.grant).toHaveBeenCalledExactlyOnceWith("builds", 2);
  expect(api.jobs).not.toHaveBeenCalled();
  expect(screen.queryByRole("heading", { name: "Discovered jobs" })).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Revoke access" }));
  expect(api.revoke).not.toHaveBeenCalled();

  fireEvent.click(screen.getByRole("button", { name: "Confirm revoke" }));
  await screen.findByText("No namespaces can use this connection.");
  expect(api.revoke).toHaveBeenCalledExactlyOnceWith("builds", 2);
});

it("retains grants and offers retry when revocation fails", async () => {
  const api = source();
  vi.mocked(api.access).mockResolvedValue([{ namespace_id: 2, namespace: "/team" }]);
  vi.mocked(api.revoke).mockRejectedValue(new Error("Access update failed"));
  render(<RepositoriesPage source={api} repositoryID="builds" management />);

  fireEvent.click(await screen.findByRole("button", { name: "Revoke access" }));
  fireEvent.click(screen.getByRole("button", { name: "Confirm revoke" }));
  await screen.findByText("Access update failed");
  expect(screen.getByRole("button", { name: "Revoke access" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Revoke access" })).toBeInTheDocument();
});

it("denies shared administration to namespace administrators before fetching data", () => {
  const api = source();
  render(
    <ConsoleAccessProvider
      access={{ actions: ["job:read"], namespaces: { "/team": ["admin:*", "job:read", "job:write"] } }}
    >
      <RepositoriesPage source={api} repositoryID="builds" management />
    </ConsoleAccessProvider>
  );

  expect(api.get).not.toHaveBeenCalled();
  expect(api.access).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: "Grant access" })).not.toBeInTheDocument();
});

it.each([
  ["viewer", ["job:read", "run:read"]],
  ["operator", ["job:read", "run:read", "run:trigger", "run:operator"]],
  ["namespace admin", ["job:read", "job:write", "run:read", "run:trigger", "run:operator"]]
] as const)("rejects a %s's direct connection editor URL", async (_role, actions) => {
  const api = source();
  render(
    <ConsoleAccessProvider access={{ actions: [...actions], namespaces: { "/team": [...actions, "admin:*"] } }}>
      <RepositoriesPage management editor="create" source={api} />
    </ConsoleAccessProvider>
  );

  expect(screen.getByRole("heading", { name: "Connection Administration Required" })).toBeInTheDocument();
  expect(screen.queryByLabelText("Repository URL")).not.toBeInTheDocument();
  expect(api.create).not.toHaveBeenCalled();
  expect(api.get).not.toHaveBeenCalled();
});

it("confirms connection deletion, supports Escape and blur, and returns to connections", async () => {
  const api = source();
  vi.mocked(api.get).mockResolvedValue({ ...repository, declared: false });
  render(<RepositoriesPage source={api} repositoryID="builds" management />);

  const button = await screen.findByRole("button", { name: "Delete connection" });
  fireEvent.click(button);
  expect(api.remove).not.toHaveBeenCalled();

  fireEvent.keyDown(screen.getByRole("button", { name: "Confirm deletion" }), { key: "Escape" });
  expect(button).toHaveTextContent("Delete connection");

  fireEvent.click(button);
  fireEvent.blur(button);
  expect(button).toHaveTextContent("Delete connection");

  fireEvent.click(button);
  fireEvent.click(screen.getByRole("button", { name: "Confirm deletion" }));
  await waitFor(() => expect(api.remove).toHaveBeenCalledExactlyOnceWith("builds"));
  await waitFor(() => expect(window.location.pathname).toBe("/manage/repositories"));
});

it("keeps a referenced connection visible and requires fresh confirmation after deletion fails", async () => {
  const api = source();
  vi.mocked(api.get).mockResolvedValue({ ...repository, declared: false });
  vi.mocked(api.remove).mockRejectedValue(new VectisAPIError(409, "source_repository_in_use", "Recorded references"));
  render(<RepositoriesPage source={api} repositoryID="builds" management />);

  fireEvent.click(await screen.findByRole("button", { name: "Delete connection" }));
  fireEvent.click(screen.getByRole("button", { name: "Confirm deletion" }));
  expect(await screen.findByText(/Disable it in Edit connection/)).toBeInTheDocument();
  expect(screen.getByRole("heading", { name: "Source" })).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Delete connection" }));
  expect(api.remove).toHaveBeenCalledTimes(1);
  expect(screen.getByRole("button", { name: "Confirm deletion" })).toBeEnabled();
});

it("locks duplicate deletion and other source actions while deleting", async () => {
  const api = source();
  vi.mocked(api.get).mockResolvedValue({ ...repository, declared: false });
  let finish!: () => void;
  vi.mocked(api.remove).mockImplementation(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      })
  );

  render(<RepositoriesPage source={api} repositoryID="builds" management />);

  fireEvent.click(await screen.findByRole("button", { name: "Delete connection" }));
  fireEvent.click(screen.getByRole("button", { name: "Confirm deletion" }));
  const pending = screen.getByRole("button", { name: "Deleting…" });
  expect(pending).toBeDisabled();

  fireEvent.click(pending);
  expect(api.remove).toHaveBeenCalledTimes(1);
  expect(screen.getByRole("button", { name: "Sync now" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Edit connection" })).toBeDisabled();

  await act(async () => finish());
});

it("explains configuration-owned deletion and never offers it outside instance management", async () => {
  const api = source();
  const { unmount } = render(<RepositoriesPage source={api} repositoryID="builds" management />);
  expect(await screen.findByText(/Remove this connection from server configuration/)).toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Delete connection" })).not.toBeInTheDocument();

  unmount();
  vi.mocked(api.get).mockResolvedValue({ ...repository, declared: false });
  render(<RepositoriesPage source={api} repositoryID="builds" />);

  await screen.findByRole("button", { name: "View build" });
  expect(screen.queryByRole("button", { name: "Delete connection" })).not.toBeInTheDocument();
});
