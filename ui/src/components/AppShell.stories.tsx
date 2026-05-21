import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppShell } from "./AppShell";
import { Button } from "./Button";
import { MetricCard } from "./MetricCard";
import { PageHeader } from "./PageHeader";

const meta = {
  title: "Components/AppShell",
  component: AppShell,
  args: {
    brand: "Vectis",
    activeHref: "/dashboard",
    navItems: [
      { href: "/dashboard", label: "Dashboard" },
      { href: "/jobs", label: "Jobs" },
      { href: "/runs", label: "Runs" }
    ],
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
