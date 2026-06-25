import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppState, Button, EmptyStatePanel } from "../../components";
import { PageStoryFrame } from "../../mocks/pageHarnesses";
import { EmptyStateRail } from "./EmptyStateRail";
import { InlineEmptyState } from "./InlineEmptyState";
import { PageMissingState } from "./PageMissingState";

const meta = {
  title: "Pages/Shared/Page States",
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

export const EmptyPage: Story = {
  render: () => (
    <EmptyStateRail>
      <EmptyStatePanel
        actions={<Button variant="quiet">Create</Button>}
        description="Create a saved definition so this namespace has work to run."
        eyebrow="No Stored Jobs"
        title="Create One Today"
      />
    </EmptyStateRail>
  )
};

export const MissingResource: Story = {
  render: () => (
    <PageMissingState
      actionLabel="View Jobs"
      breadcrumbs={[
        { label: "Root" },
        { label: "Jobs" },
        { current: true, label: "Missing" }
      ]}
      description="This job is no longer available, or the route points to a definition that does not exist."
      label="Job location"
      onAction={() => undefined}
      panelDescription="Return to the jobs index to choose an active definition."
      panelEyebrow="Missing Job"
      panelTitle="No Job Found"
      title="Job Not Found"
    />
  )
};

export const InlineEmpty: Story = {
  render: () => <InlineEmptyState>No direct role bindings.</InlineEmptyState>
};

export const Loading: Story = {
  render: () => <AppState title="Loading console" tone="loading" />
};

export const Error: Story = {
  render: () => <AppState description="The API did not return console data." title="Unable to load console" tone="error" />
};
