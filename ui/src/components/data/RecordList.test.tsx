import { fireEvent, render, screen } from "@testing-library/react";
import { Clock } from "lucide-react";
import { OperationalFact } from "../primitives/OperationalFact";
import { RecordList, RecordListIdentity, RecordListMeta, RecordListSummary } from "./RecordList";

describe("RecordList", () => {
  it("renders header, count, row content, and actions", () => {
    render(
      <RecordList
        countLabel="1 item"
        description="Latest records"
        emptyMessage="No records."
        items={[
          {
            actions: <span>Healthy</span>,
            content: (
              <RecordListSummary>
                <RecordListIdentity subtitle="local" title="api" />
                <RecordListMeta>
                  <OperationalFact icon={Clock} label="Duration" value="42s" />
                </RecordListMeta>
              </RecordListSummary>
            ),
            key: "api",
            railTone: "success"
          }
        ]}
        title="Records"
      />
    );

    expect(screen.getByRole("heading", { name: "Records" })).toBeInTheDocument();
    expect(screen.getByText("1 item")).toBeInTheDocument();
    expect(screen.getByText("api")).toBeInTheDocument();
    expect(screen.getByText("Healthy")).toBeInTheDocument();
  });

  it("supports clickable rows", () => {
    const onSelect = vi.fn();

    render(
      <RecordList
        countLabel="1 item"
        description="Latest records"
        emptyMessage="No records."
        items={[
          {
            ariaLabel: "Open api",
            content: <RecordListIdentity title="api" />,
            key: "api",
            onSelect
          }
        ]}
        title="Records"
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Open api" }));

    expect(onSelect).toHaveBeenCalledOnce();
  });

  it("renders an empty message", () => {
    render(
      <RecordList
        countLabel="0 items"
        description="Latest records"
        emptyMessage="No records."
        items={[]}
        title="Records"
      />
    );

    expect(screen.getByText("No records.")).toBeInTheDocument();
  });
});
