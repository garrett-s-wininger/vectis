import type { Meta, StoryObj } from "@storybook/react-vite";
import { Activity, Clock, Network, Server } from "lucide-react";
import { OperationalFact } from "../primitives/OperationalFact";
import { StatusBadge } from "../status/StatusBadge";
import { RecordList, RecordListIdentity, RecordListMeta, RecordListSummary } from "./RecordList";

const meta = {
  title: "Components/Data/RecordList",
  component: RecordList
} satisfies Meta<typeof RecordList>;

export default meta;

type Story = StoryObj<typeof meta>;

export const ExecutionRecords: Story = {
  args: {
    countLabel: "2 runs",
    description: "Latest execution records",
    emptyMessage: "No records.",
    items: [
      {
        actions: <StatusBadge status="succeeded" />,
        ariaLabel: "Open api-test-suite",
        content: (
          <RecordListSummary>
            <RecordListIdentity subtitle="Run #1240" title="api-test-suite" />
            <RecordListMeta>
              <OperationalFact emphasis icon={Clock} label="Duration" value="42s" />
              <OperationalFact icon={Server} label="Cell" value="local" />
              <OperationalFact icon={Activity} label="Trigger" value="Manual" />
            </RecordListMeta>
          </RecordListSummary>
        ),
        key: "api-test-suite",
        railTone: "success"
      },
      {
        actions: <StatusBadge status="running" />,
        content: (
          <RecordListSummary>
            <RecordListIdentity subtitle="Run #1241" title="docs-publish" />
            <RecordListMeta>
              <OperationalFact emphasis icon={Clock} label="Elapsed" value="18s" />
              <OperationalFact icon={Server} label="Cell" value="edge" />
              <OperationalFact icon={Activity} label="Trigger" value="Schedule" />
            </RecordListMeta>
          </RecordListSummary>
        ),
        key: "docs-publish",
        railTone: "info"
      }
    ],
    title: "Runs"
  }
};

export const FeaturedMetadata: Story = {
  args: {
    countLabel: "1 cell",
    description: "Gateway-visible execution topology",
    emptyMessage: "No cells.",
    items: [
      {
        content: (
          <RecordListSummary>
            <RecordListIdentity subtitle="iad" title="edge" />
            <RecordListMeta featuredFirst>
              <OperationalFact emphasis icon={Network} label="Endpoint" value="https://edge.vectis.internal" />
              <OperationalFact icon={Activity} label="Runs" value="6" />
              <OperationalFact icon={Server} label="Workers" value="5/6" />
            </RecordListMeta>
          </RecordListSummary>
        ),
        key: "edge",
        railTone: "warning"
      }
    ],
    title: "Cells"
  }
};

export const Empty: Story = {
  args: {
    countLabel: "0 items",
    description: "Latest records",
    emptyMessage: "No records.",
    items: [],
    title: "Records"
  }
};
