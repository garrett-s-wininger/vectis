import type { Meta, StoryObj } from "@storybook/react-vite";
import { ConsoleAccessProvider } from "../domain/access";
import { createMockRepositorySource } from "../data/repositories";
import { PageStoryFrame } from "../mocks/pageHarnesses";
import { RepositoriesPage } from "./RepositoriesPage";

const meta = {
  title: "Pages/Repositories",
  component: RepositoriesPage,
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta<typeof RepositoriesPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Index: Story = { args: { source: createMockRepositorySource() } };
export const Detail: Story = { args: { repositoryID: "vectis-local", source: createMockRepositorySource() } };
export const FailedDiscovery: Story = {
  args: {
    repositoryID: "vectis-local",
    source: {
      ...createMockRepositorySource(),
      status: async () => ({
        status: "unavailable",
        default_ref_resolved: false,
        path_exists: false,
        path_is_directory: false,
        git_repository: false,
        error: { code: "checkout_missing", message: "The configured checkout is not available." }
      }),
      jobs: async () => {
        throw new Error("Cannot scan definitions: checkout is missing.");
      }
    }
  }
};

export const InvalidDefinitions: Story = {
  args: {
    repositoryID: "vectis-local",
    source: {
      ...createMockRepositorySource(),
      get: async (id) => ({
        ...(await createMockRepositorySource().get(id)),
        checkout_mode: "managed",
        canonical_url: "https://git.example.test/engineering/platform/build-pipelines.git",
        default_ref: "refs/heads/feature/repository-discovery-improvements"
      }),
      jobs: async () => ({
        jobs: [{ job_id: "build", name: "build", path: ".vectis/build.json", blob_sha: "abc" }],
        invalid: [{ path: ".vectis/broken.json", error: "actions[0]: unknown action type" }],
        requested_ref: "main",
        resolved_commit: "b28a74c41946e6952910ddab7e5bd791d5bd0194",
        truncated: false
      })
    }
  }
};

export const Viewer: Story = {
  ...Detail,
  decorators: [
    (Story) => (
      <ConsoleAccessProvider access={{ actions: ["job:read", "api:any"], namespaces: { "/": ["job:read"] } }}>
        <Story />
      </ConsoleAccessProvider>
    )
  ]
};

export const Connections: Story = { args: { management: true, source: createMockRepositorySource(undefined, true) } };
export const NamespaceAccess: Story = {
  args: { management: true, repositoryID: "vectis-local", source: createMockRepositorySource(undefined, true) }
};
