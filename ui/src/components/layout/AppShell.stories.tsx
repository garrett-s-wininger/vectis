import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppShell } from "./AppShell";
import { Button } from "../primitives/Button";
import { MetricCard } from "../data/MetricCard";
import { PageHeader } from "./PageHeader";

const meta = {
  title: "Components/Layout/AppShell",
  component: AppShell,
  args: {
    brand: "Vectis",
    activeHref: "/runs",
    navItems: [
      { href: "/runs", label: "Runs" },
      { href: "/jobs", label: "Jobs" }
    ],
    utilityNavItems: [
      {
        label: "Admin",
        items: [
          { href: "/health", label: "Health" },
          { href: "/users", label: "Users" },
          { href: "/namespaces", label: "Namespaces" }
        ]
      }
    ],
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
