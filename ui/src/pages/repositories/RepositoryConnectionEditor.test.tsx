import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { createMockRepositorySource, type Repository } from "../../data/repositories";
import { RepositoryConnectionEditor } from "./RepositoryConnectionEditor";

const row: Repository = {
  repository_id: "public",
  namespace: "/",
  source_kind: "local_checkout",
  checkout_mode: "managed",
  canonical_url: "https://example.test/public.git",
  default_ref: "HEAD",
  enabled: true,
  declared: false,
  sync: { status: "never" }
};

const listing = {
  jobs: [{ job_id: "ci", name: "ci.json", path: ".vectis/ci.json", blob_sha: "blob" }],
  resolved_commit: "commit",
  requested_ref: "HEAD",
  truncated: false
};

function source() {
  const api = createMockRepositorySource(undefined, true);
  return {
    ...api,
    validate: vi.fn().mockResolvedValue({ requested_ref: "HEAD", resolved_commit: "validated-commit" }),
    create: vi.fn().mockResolvedValue(row),
    update: vi.fn().mockResolvedValue(row),
    get: vi.fn().mockResolvedValue(row),
    access: vi.fn().mockResolvedValue([{ namespace_id: 1, namespace: "/" }]),
    sync: vi.fn().mockResolvedValue({ ...row, sync: { status: "succeeded" } }),
    status: vi.fn().mockResolvedValue({ status: "ok", default_ref_resolved: true }),
    jobs: vi.fn().mockResolvedValue(listing)
  };
}

async function fill() {
  await screen.findByLabelText("Repository ID");
  fireEvent.change(screen.getByLabelText("Repository ID"), { target: { value: "public" } });
  fireEvent.change(screen.getByLabelText("Repository URL"), { target: { value: row.canonical_url } });
}

it("saves a public connection without credentials, validates it, and opens jobs in its granted namespace", async () => {
  const api = source();
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.create).toHaveBeenCalledExactlyOnceWith({
    repository_id: "public",
    validate_connection: true,
    canonical_url: row.canonical_url,
    default_ref: "HEAD",
    credential_ref: "",
    namespace: "/",
    source_kind: "local_checkout",
    checkout_mode: "managed",
    authoring_mode: "read_only",
    enabled: true
  });

  expect(api.sync).toHaveBeenCalledExactlyOnceWith("public");
  expect(api.jobs).toHaveBeenCalledExactlyOnceWith("public");

  fireEvent.click(screen.getByRole("button", { name: "View jobs" }));
  await waitFor(() => expect(window.location.pathname).toBe("/repositories/public"));
  expect(new URLSearchParams(window.location.search).get("namespace")).toBe("/");
});

it("offers saved credentials with None selected for public repositories", async () => {
  const api = source();
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  expect(screen.getByLabelText("Credential", { exact: true })).toHaveValue("");
  expect(screen.getByLabelText(/^Credential:/)).toHaveTextContent("None");
  expect(screen.getByLabelText("Credential", { exact: true })).toBeEnabled();

  fireEvent.change(screen.getByLabelText("Credential", { exact: true }), {
    target: { value: "encryptedfs://git/example" }
  });

  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.create).toHaveBeenCalledWith(expect.objectContaining({ credential_ref: "encryptedfs://git/example" }));
});

it("previews errors and the resolved revision without saving, then reuses successful validation", async () => {
  const api = source();
  api.validate.mockRejectedValueOnce(new Error("Branch not found"));
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  fireEvent.click(screen.getByRole("button", { name: "Validate connection" }));
  await screen.findByText("Branch not found");
  expect(api.create).not.toHaveBeenCalled();
  expect(api.sync).not.toHaveBeenCalled();

  fireEvent.click(screen.getByRole("button", { name: "Validate connection" }));
  await screen.findByText("validated-commit");
  expect(screen.getByRole("button", { name: "Copy Validated revision" })).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.create).toHaveBeenCalledWith(expect.not.objectContaining({ validate_connection: true }));
  expect(api.validate).toHaveBeenCalledTimes(2);
});

it("keeps the last revision visible during revalidation without reusing a failed result", async () => {
  const api = source();
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  const panel = screen.getByRole("region", { name: "Connection validation" });
  fireEvent.click(screen.getByRole("button", { name: "Validate connection" }));

  await screen.findByText("validated-commit");
  let fail!: (cause: Error) => void;
  api.validate.mockImplementationOnce(
    () =>
      new Promise((_, reject) => {
        fail = reject;
      })
  );

  fireEvent.click(screen.getByRole("button", { name: "Revalidate" }));
  expect(panel).toHaveAttribute("aria-busy", "true");
  expect(screen.getByText("validated-commit")).toBeInTheDocument();
  expect(screen.getByText("Validating connection…")).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();

  await act(async () => fail(new Error("Remote unavailable")));
  expect(panel).toHaveAttribute("aria-busy", "false");
  expect(screen.getByText("validated-commit")).toBeInTheDocument();
  expect(screen.getByText("Validation failed")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.create).toHaveBeenCalledWith(expect.objectContaining({ validate_connection: true }));
});

it("invalidates a successful preview when settings change", async () => {
  const api = source();
  render(<RepositoryConnectionEditor id="public" namespacePath="/" source={api} />);

  fireEvent.click(await screen.findByRole("button", { name: "Validate connection" }));
  await screen.findByText("validated-commit");
  expect(api.validate).toHaveBeenCalledWith(expect.objectContaining({ default_ref: "HEAD" }), "public");

  fireEvent.change(screen.getByLabelText("Default ref"), { target: { value: "release" } });
  expect(screen.getByText("validated-commit")).toBeInTheDocument();
  expect(screen.getByText("Validation needed")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.update).toHaveBeenCalledWith(
    "public",
    expect.objectContaining({ validate_connection: true, default_ref: "release" })
  );
});

it("keeps an invalid connection unsaved and editable, then saves corrected settings", async () => {
  const api = source();
  api.create.mockRejectedValueOnce(new Error("Remote unavailable"));
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Remote unavailable");
  expect(screen.queryByRole("heading", { name: "Connection saved" })).not.toBeInTheDocument();
  expect(screen.getByLabelText("Repository URL")).toBeEnabled();
  expect(api.sync).not.toHaveBeenCalled();

  fireEvent.change(screen.getByLabelText("Default ref"), { target: { value: "main" } });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.create).toHaveBeenLastCalledWith(
    expect.objectContaining({ default_ref: "main", validate_connection: true })
  );
});

it("keeps a saved connection if checkout readiness fails after successful pre-save validation", async () => {
  const api = source();
  api.sync.mockResolvedValueOnce({ ...row, sync: { status: "failed", error: "Remote unavailable" } });
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Remote unavailable");
  expect(screen.getByRole("heading", { name: "Connection saved" })).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Retry validation" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.create).toHaveBeenCalledTimes(1);
  expect(api.sync).toHaveBeenCalledTimes(2);
});

it("locks repeated submissions while registration is pending", async () => {
  const api = source();
  let finish!: (value: Repository) => void;
  api.create.mockImplementation(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      })
  );

  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  const form = screen.getByRole("button", { name: "Save" }).closest("form")!;
  fireEvent.submit(form);
  fireEvent.submit(form);
  expect(api.create).toHaveBeenCalledTimes(1);
  expect(screen.getByLabelText(/^Credential:/)).toHaveAttribute("aria-disabled", "true");

  await act(async () => finish(row));
  await screen.findByText("Validated", { exact: true });
});

it("edits existing settings without replacing the repository ID or other server-managed fields", async () => {
  const api = source();
  render(<RepositoryConnectionEditor id="public" namespacePath="/" source={api} />);

  const id = await screen.findByLabelText("Repository ID");
  expect(id).toHaveAttribute("readonly");

  fireEvent.change(screen.getByLabelText("Default ref"), { target: { value: "release" } });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.update).toHaveBeenCalledExactlyOnceWith("public", {
    validate_connection: true,
    canonical_url: row.canonical_url,
    default_ref: "release",
    credential_ref: "",
    enabled: true
  });

  expect(api.create).not.toHaveBeenCalled();
});

it("shows configuration-managed connections as read-only", async () => {
  const api = source();
  api.get.mockResolvedValue({ ...row, declared: true });
  render(<RepositoryConnectionEditor id="public" namespacePath="/" source={api} />);

  await screen.findByRole("heading", { name: "Managed by configuration" });
  expect(screen.queryByLabelText("Repository URL")).not.toBeInTheDocument();
  expect(api.update).not.toHaveBeenCalled();
});

it("rejects embedded HTTP credentials before sending a registration request", async () => {
  const api = source();
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  fireEvent.change(screen.getByLabelText("Repository URL"), {
    target: { value: "https://user:password@example.test/repo.git" }
  });

  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Use a credential reference instead of credentials in the URL.");
  expect(screen.getByLabelText("Repository URL")).toHaveFocus();
  expect(api.create).not.toHaveBeenCalled();
});

it("keeps connection setup generic without an example shortcut", async () => {
  const api = source();
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await screen.findByLabelText("Repository ID");
  expect(screen.getByLabelText("Repository ID")).toHaveValue("");
  expect(screen.getByLabelText("Repository URL")).toHaveValue("");
  expect(screen.queryByRole("button", { name: "Use Vectis example" })).not.toBeInTheDocument();
});

it("uses a remaining grant if namespace access changed before viewing jobs", async () => {
  const api = source();
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });

  api.access.mockResolvedValue([{ namespace_id: 2, namespace: "/team-a" }]);
  fireEvent.click(screen.getByRole("button", { name: "View jobs" }));
  await waitFor(() => expect(new URLSearchParams(window.location.search).get("namespace")).toBe("/team-a"));
});

it("preserves external checkout settings and immutable legacy IDs while editing a ref", async () => {
  const api = source();
  api.get.mockResolvedValue({
    ...row,
    repository_id: "team/repo",
    checkout_mode: "external",
    canonical_url: "/srv/remote",
    checkout_path: "/srv/worktree"
  });

  render(<RepositoryConnectionEditor id="team/repo" namespacePath="/" source={api} />);

  await screen.findByLabelText("Repository ID");
  expect(screen.getByLabelText("Repository URL")).toHaveAttribute("readonly");

  fireEvent.change(screen.getByLabelText("Default ref"), { target: { value: "release" } });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.update).toHaveBeenCalledWith(
    "team/repo",
    expect.objectContaining({ canonical_url: "/srv/remote", default_ref: "release" })
  );
});

it("saves a disabled connection without requesting validation", async () => {
  const api = source();
  api.create.mockResolvedValue({ ...row, enabled: false });
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  fireEvent.click(screen.getByRole("checkbox", { name: "Connection enabled" }));
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByRole("heading", { name: "Connection saved" });
  expect(api.create).toHaveBeenCalledWith(expect.objectContaining({ enabled: false }));
  expect(api.sync).not.toHaveBeenCalled();
  expect(api.jobs).not.toHaveBeenCalled();
});

it("accepts the common SSH shorthand without embedding an HTTP credential", async () => {
  const api = source();
  render(<RepositoryConnectionEditor namespacePath="/" source={api} />);

  await fill();
  fireEvent.change(screen.getByLabelText("Repository URL"), { target: { value: "git@github.com:owner/project.git" } });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.create).toHaveBeenCalledWith(
    expect.objectContaining({ canonical_url: "git@github.com:owner/project.git" })
  );
});

it("can reopen an existing connection's settings after validation fails without relying on same-URL navigation", async () => {
  const api = source();
  api.sync.mockResolvedValueOnce({ ...row, sync: { status: "failed", error: "Unknown branch" } });
  render(<RepositoryConnectionEditor id="public" namespacePath="/" source={api} />);

  await screen.findByLabelText("Default ref");
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Unknown branch");
  fireEvent.click(screen.getByRole("button", { name: "Edit settings" }));
  expect(screen.getByLabelText("Default ref")).toBeEnabled();

  fireEvent.change(screen.getByLabelText("Default ref"), { target: { value: "main" } });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("Validated", { exact: true });
  expect(api.update).toHaveBeenCalledTimes(2);
  expect(api.update).toHaveBeenLastCalledWith("public", expect.objectContaining({ default_ref: "main" }));
  expect(api.create).not.toHaveBeenCalled();
});

it("requires reviewing the affected inventory before disabling and sends its revision", async () => {
  const api = source();
  api.update.mockResolvedValue({ ...row, enabled: false });
  api.impact = vi.fn().mockResolvedValue({
    jobs: [
      {
        repository_id: "public",
        job_id: "ci",
        namespace_id: 1,
        namespace: "/",
        path: ".vectis.json",
        ref: "",
        revision: 2,
        hotfix: { reason: "incident repair" }
      }
    ],
    automations: [{ job_id: "ci", namespace: "/", kind: "cron", enabled: true }],
    namespaces: ["/"],
    unregistered_jobs: [],
    revision: "reviewed-impact"
  });

  render(<RepositoryConnectionEditor id="public" namespacePath="/" source={api} />);

  await screen.findByLabelText("Repository ID");
  fireEvent.click(screen.getByRole("checkbox", { name: "Connection enabled" }));
  expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();

  await screen.findByText("Hotfix active");
  expect(screen.getByText("cron (enabled)")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("checkbox", { name: "I reviewed the affected jobs and automation" }));
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByRole("heading", { name: "Connection saved" });
  expect(api.update).toHaveBeenCalledWith(
    "public",
    expect.objectContaining({ enabled: false, impact_revision: "reviewed-impact" })
  );

  expect(api.sync).not.toHaveBeenCalled();
});

it("blocks disable if the affected-job inventory cannot be loaded", async () => {
  const api = source();
  api.impact = vi.fn().mockRejectedValue(new Error("Inventory unavailable"));
  render(<RepositoryConnectionEditor id="public" namespacePath="/" source={api} />);

  await screen.findByLabelText("Repository ID");
  fireEvent.click(screen.getByRole("checkbox", { name: "Connection enabled" }));
  await screen.findByText("Inventory unavailable");
  expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
  expect(api.update).not.toHaveBeenCalled();
});
