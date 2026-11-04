import { useState, type ComponentProps } from "react";
import { WorkspacePicker } from "../navigation/WorkspacePicker";
import { workspacePath } from "../../routing/workspace";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { primaryNavItems, managementNavItems } from "../../routing/routes";
import { AppShell } from "./AppShell";
import { Button } from "../primitives/Button";
import { MetricCard } from "../data/MetricCard";
import { PageHeader } from "./PageHeader";

const meta = {
  title: "Components/Layout/AppShell",
  component: AppShell,
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ height: "100dvh" }}>
        <Story />
      </div>
    )
  ],
  args: {
    brand: "Vectis",
    activeHref: "/runs",
    navItems: primaryNavItems,
    utilityNavItems: managementNavItems,
    accountName: "admin",
    actions: <Button>Refresh</Button>,
    children: (
      <>
        <PageHeader
          eyebrow="Operator Console"
          title="Cluster dashboard"
          description="Current workload, queue pressure, and service health."
        />
        <div className="metric-card-grid">
          <MetricCard label="Services" value="Healthy" detail="all systems reporting" tone="success" />
          <MetricCard label="Queue pressure" value="3 queued" detail="0 idle workers" tone="attention" />
          <MetricCard label="Active capacity" value="6 executing" detail="1 worker drained" />
        </div>
      </>
    )
  }
} satisfies Meta<typeof AppShell>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Dashboard: Story = {};

function HierarchyShell(args: ComponentProps<typeof AppShell>) {
  const [namespace, setNamespace] = useState("/teams/platform/build/east/nightly");

  return (
    <AppShell
      {...args}
      homeHref={workspacePath("/jobs", namespace)}
      contextLabel={namespace}
      workspaceControl={
        <WorkspacePicker
          namespaces={[
            "/",
            "/teams",
            "/teams/platform",
            "/teams/platform/build/east/nightly",
            "/teams/edge",
            "/teams/edge/production"
          ]}
          value={namespace}
          onChange={setNamespace}
          accessLabel="Operator"
          namespaceManagementHref="/namespaces"
          namespaceHref={workspacePath("/namespaces", namespace)}
        />
      }
    />
  );
}

export const NestedNamespaces: Story = {
  render: (args) => <HierarchyShell {...args} />
};
