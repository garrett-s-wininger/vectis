import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { SourceJobDefinition } from "./SourceJobDefinition";
import type { SourceJobClient, SourceJobLifecycle } from "../../data/sourceJobs";
import type { Job } from "../../domain/console";
import { ConsoleAccessProvider } from "../../domain/access";

const job: Job = {
  id: "build",
  name: "build",
  repository: "mono",
  sourceDetail: ".vectis/build.json",
  sourceKind: "repo",
  namespacePath: "/team",
  branch: "main",
  schedule: "Manual",
  nextRun: "On demand",
  triggers: [],
  status: "enabled",
  canAuthor: false
};

const original = { root: { id: "root", uses: "builtins/script", with: { script: "exit 7" } } };
const fixed = { root: { id: "root", uses: "builtins/script", with: { script: "exit 0" } } };
function fixture(active = false) {
  let state: SourceJobLifecycle = {
    registration: {
      repository_id: "mono",
      job_id: "build",
      namespace_id: 2,
      namespace: "/team",
      path: ".vectis/build.json",
      ref: "",
      revision: 3,
      ...(active
        ? {
            hotfix: {
              commit: "hotfix",
              reason: "incident fix",
              actor: "owner",
              base_commit: "old-source",
              base_ref: "main",
              base_path: ".vectis/build.json",
              created_at: 1
            }
          }
        : {})
    },
    source_commit: "current-source",
    source_definition: original,
    source_json: JSON.stringify(original, null, 2),
    effective_definition: active ? fixed : original
  };

  const client: SourceJobClient = {
    load: vi.fn(async () => structuredClone(state)),
    namespaces: vi.fn().mockResolvedValue([{ path: "/team", label: "/team" }]),
    assign: vi.fn().mockResolvedValue(state.registration),
    activate: vi.fn(async (input) => {
      state = {
        ...state,
        effective_definition: input.definition,
        registration: {
          ...state.registration!,
          revision: state.registration!.revision + 1,
          hotfix: {
            commit: "hotfix",
            reason: input.reason!,
            actor: "owner",
            base_commit: state.source_commit!,
            base_ref: "main",
            base_path: job.sourceDetail,
            created_at: 1
          }
        }
      };

      return state.registration!;
    }),
    clear: vi.fn(async () => {
      state = {
        ...state,
        effective_definition: original,
        registration: { ...state.registration!, revision: state.registration!.revision + 1, hotfix: undefined }
      };

      return state.registration!;
    })
  };

  return {
    client,
    snapshot: () => structuredClone(state),
    replace: (next: SourceJobLifecycle) => {
      state = structuredClone(next);
    },
    unassign: () => {
      state = { ...state, registration: undefined };
    }
  };
}

async function edit() {
  fireEvent.click(await screen.findByRole("button", { name: /^(Hotfix|Edit|Run with changes)$/ }));
  expect(screen.getByLabelText("Reason for changes")).toHaveFocus();

  fireEvent.change(screen.getByRole("textbox", { name: "Definition" }), { target: { value: JSON.stringify(fixed) } });
  fireEvent.change(screen.getByLabelText("Reason for changes"), { target: { value: "restore production" } });
}

it("requires review before activation and keeps source distinct from the effective hotfix", async () => {
  const { client } = fixture();
  render(<SourceJobDefinition job={job} client={client} />);
  expect(await screen.findByRole("checkbox", { name: "Definition comparison" })).toBeDisabled();

  await edit();
  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  expect(client.activate).not.toHaveBeenCalled();
  expect(screen.getByLabelText("Effective to Proposed diff")).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Activate" }));
  await screen.findByRole("heading", { name: "Active hotfix" });
  expect(screen.getByRole("checkbox", { name: "Definition comparison" })).toBeEnabled();
  expect(screen.getByRole("checkbox", { name: "Definition comparison" })).not.toBeChecked();
  expect(screen.getByRole("button", { name: "Edit" })).toBeInTheDocument();
  expect(client.activate).toHaveBeenCalledWith({
    definition: fixed,
    reason: "restore production",
    expected_revision: 3,
    expected_source_commit: "current-source"
  });

  fireEvent.click(screen.getByRole("checkbox", { name: "Definition comparison" }));
  expect(screen.getByText(/"script": "exit 7"/)).toBeInTheDocument();
});

it("offers both outcomes after one review and preserves the draft when returning to edit", async () => {
  const { client } = fixture();
  const onRun = vi.fn();
  render(<SourceJobDefinition job={job} client={client} onRun={onRun} />);
  await edit();
  expect(screen.getAllByRole("button", { name: "Review" })).toHaveLength(1);

  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  expect(screen.getByRole("button", { name: "Activate" })).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Run once" })).toBeInTheDocument();
  expect(client.activate).not.toHaveBeenCalled();
  expect(onRun).not.toHaveBeenCalled();

  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(screen.getByRole("textbox", { name: "Definition" })).toHaveValue(JSON.stringify(fixed, null, 2));
  expect(screen.getByLabelText("Reason for changes")).toHaveValue("restore production");

  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  fireEvent.click(screen.getByRole("button", { name: "Run once" }));
  await waitFor(() => expect(onRun).toHaveBeenCalledTimes(1));
  expect(onRun).toHaveBeenCalledWith(expect.objectContaining({ definition: fixed, reason: "restore production" }));
  expect(client.activate).not.toHaveBeenCalled();
});

it("previews the current upstream before clearing a hotfix", async () => {
  const { client } = fixture(true);
  render(<SourceJobDefinition job={job} client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: "Revert" }));
  expect(client.clear).not.toHaveBeenCalled();
  expect(screen.getByLabelText("Effective to Source diff")).toBeInTheDocument();

  const review = within(screen.getByRole("form", { name: "Revert hotfix" }));
  fireEvent.click(review.getByRole("button", { name: "Revert" }));
  expect(client.clear).not.toHaveBeenCalled();

  fireEvent.keyDown(review.getByRole("button", { name: "Confirm revert" }), { key: "Escape" });
  expect(review.queryByRole("button", { name: "Confirm revert" })).not.toBeInTheDocument();

  fireEvent.click(review.getByRole("button", { name: "Revert" }));
  fireEvent.blur(review.getByRole("button", { name: "Confirm revert" }));
  expect(review.queryByRole("button", { name: "Confirm revert" })).not.toBeInTheDocument();

  fireEvent.click(review.getByRole("button", { name: "Revert" }));
  expect(client.clear).not.toHaveBeenCalled();

  fireEvent.click(review.getByRole("button", { name: "Confirm revert" }));
  await screen.findByText("Hotfix reverted. Future runs use the source definition.");
  expect(screen.getByRole("checkbox", { name: "Definition comparison" })).toBeDisabled();
  expect(screen.getByRole("checkbox", { name: "Definition comparison" })).not.toBeChecked();
  expect(client.clear).toHaveBeenCalledWith({ expected_revision: 3, expected_source_commit: "current-source" });
});

it("lets an operator review and run changes once without persistent hotfix controls", async () => {
  const { client } = fixture();
  const onRun = vi.fn();
  render(
    <ConsoleAccessProvider
      access={{
        actions: ["job:read"],
        namespaces: { "/team": ["job:read", "run:trigger", "run:read", "run:operator"] }
      }}
    >
      <SourceJobDefinition job={job} client={client} onRun={onRun} />
    </ConsoleAccessProvider>
  );

  await edit();
  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  expect(onRun).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: "Activate" })).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Run once" }));
  await waitFor(() =>
    expect(onRun).toHaveBeenCalledWith(
      expect.objectContaining({
        definition: fixed,
        reason: "restore production",
        expected_revision: 3,
        expected_source_commit: "current-source",
        idempotency_key: expect.any(String)
      })
    )
  );

  expect(client.activate).not.toHaveBeenCalled();
});

it("keeps all mutation controls hidden for viewers", async () => {
  const { client } = fixture(true);
  render(
    <ConsoleAccessProvider access={{ actions: ["job:read"], namespaces: { "/team": ["job:read", "run:read"] } }}>
      <SourceJobDefinition job={job} client={client} />
    </ConsoleAccessProvider>
  );

  await screen.findByText("incident fix");
  expect(screen.queryByRole("button", { name: /^(Hotfix|Edit|Run with changes)$/ })).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "Revert" })).not.toBeInTheDocument();
  expect(client.namespaces).not.toHaveBeenCalled();
});

it("requires a reason and valid JSON before submitting any definition", async () => {
  const { client } = fixture();
  render(<SourceJobDefinition job={job} client={client} />);
  fireEvent.click(await screen.findByRole("button", { name: /^(Hotfix|Edit|Run with changes)$/ }));
  fireEvent.change(screen.getByRole("textbox", { name: "Definition" }), { target: { value: "{bad" } });
  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  expect(client.activate).not.toHaveBeenCalled();

  fireEvent.change(screen.getByRole("textbox", { name: "Definition" }), { target: { value: JSON.stringify(fixed) } });
  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  await screen.findByText("Enter a reason for these changes.");
  expect(screen.getByRole("alert")).toHaveTextContent("Check your changes");
  expect(screen.queryByRole("button", { name: "Reload definition" })).not.toBeInTheDocument();
  expect(client.activate).not.toHaveBeenCalled();

  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(screen.getByLabelText("Effective definition")).toHaveTextContent("exit 7");
});

it("keeps a failed activation draft until Reset reloads the latest definition and revision", async () => {
  const { client, snapshot, replace } = fixture();
  vi.mocked(client.activate).mockRejectedValueOnce(new Error("Source changed; reset"));
  render(<SourceJobDefinition job={job} client={client} />);
  await edit();
  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  fireEvent.click(screen.getByRole("button", { name: "Activate" }));
  await screen.findByText("Source changed; reset");
  expect(screen.getByRole("alert")).toHaveTextContent("Could not activate hotfix");
  expect(screen.getByLabelText("Reason for changes")).toHaveValue("restore production");
  expect(screen.getByRole("textbox", { name: "Definition" })).toHaveValue(JSON.stringify(fixed, null, 2));

  const current = snapshot();
  const upstream = { root: { id: "root", uses: "builtins/script", with: { script: "exit 3" } } };
  replace({
    ...current,
    registration: { ...current.registration!, revision: 5 },
    source_commit: "new-source",
    source_definition: upstream,
    source_json: JSON.stringify(upstream),
    effective_definition: upstream
  });

  fireEvent.click(screen.getByRole("button", { name: "Reset" }));
  await waitFor(() => expect(screen.getByLabelText("Reason for changes")).toHaveValue(""));
  expect(client.load).toHaveBeenCalledTimes(2);
  expect(screen.getByRole("textbox", { name: "Definition" })).toHaveValue(JSON.stringify(upstream, null, 2));
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(screen.getByLabelText("Reason for changes")).toHaveFocus();

  fireEvent.change(screen.getByLabelText("Reason for changes"), { target: { value: "review refreshed source" } });
  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  fireEvent.click(screen.getByRole("button", { name: "Activate" }));
  await screen.findByRole("heading", { name: "Active hotfix" });
  expect(client.activate).toHaveBeenLastCalledWith({
    definition: upstream,
    reason: "review refreshed source",
    expected_revision: 5,
    expected_source_commit: "new-source"
  });
});

it.each(["network", "unavailable"])("preserves the local draft when Reset fails (%s)", async (failure) => {
  const { client } = fixture();
  render(<SourceJobDefinition job={job} client={client} />);
  await edit();

  if (failure === "network") vi.mocked(client.load).mockRejectedValueOnce(new Error("Network unavailable"));
  else vi.mocked(client.load).mockResolvedValueOnce({ source_error: "Repository unavailable" });

  fireEvent.click(screen.getByRole("button", { name: "Reset" }));
  await screen.findByText("Could not reset changes");
  expect(screen.getByRole("textbox", { name: "Definition" })).toHaveValue(JSON.stringify(fixed));
  expect(screen.getByLabelText("Reason for changes")).toHaveValue("restore production");
  expect(client.activate).not.toHaveBeenCalled();
});

it("offers Retry only when the initial definition cannot be loaded", async () => {
  const { client } = fixture();
  vi.mocked(client.load).mockRejectedValueOnce(new Error("Service unavailable"));
  render(<SourceJobDefinition job={job} client={client} />);
  await screen.findByText("Could not load definition");
  expect(screen.queryByRole("button", { name: "Reload definition" })).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Retry" }));
  await screen.findByRole("button", { name: "Hotfix" });
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
});

it("renders live provenance in the supplied layout without duplicating it in the definition", async () => {
  const { client } = fixture();
  render(
    <SourceJobDefinition
      job={job}
      client={client}
      sourceDetails={(ownership) => <dl>{ownership}</dl>}
      renderLayout={(definition, provenance) => (
        <div>
          {definition}
          <aside aria-label="Job context">{provenance}</aside>
        </div>
      )}
    />
  );

  const context = within(screen.getByRole("complementary", { name: "Job context" }));
  await context.findByText("/team");
  expect(within(screen.getByRole("region", { name: "Definition" })).queryByText("/team")).not.toBeInTheDocument();

  await edit();
  fireEvent.click(screen.getByRole("button", { name: "Review" }));
  fireEvent.click(screen.getByRole("button", { name: "Activate" }));
  await screen.findByRole("heading", { name: "Active hotfix" });
  expect(context.getByText("/team")).toBeInTheDocument();
  expect(client.load).toHaveBeenCalledTimes(2);
});

it("opens an existing hotfix in Effective mode until the user chooses Diff", async () => {
  const { client } = fixture(true);
  render(<SourceJobDefinition job={job} client={client} />);

  await screen.findByRole("button", { name: "Edit" });
  expect(screen.getByRole("checkbox", { name: "Definition comparison" })).not.toBeChecked();
  expect(screen.getByLabelText("Effective definition")).toHaveTextContent("exit 0");
  expect(screen.queryByLabelText("Source to Effective diff")).not.toBeInTheDocument();
});
