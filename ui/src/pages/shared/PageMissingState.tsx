import { BreadcrumbTrail, Button, EmptyStatePanel, PageHeader, type BreadcrumbItem } from "../../components";
import { EmptyStateRail } from "./EmptyStateRail";

type PageMissingStateProps = {
  actionLabel: string;
  breadcrumbs: BreadcrumbItem[];
  description: string;
  label: string;
  onAction: () => void;
  panelDescription: string;
  panelEyebrow: string;
  panelTitle: string;
  title: string;
};

export function PageMissingState({
  actionLabel,
  breadcrumbs,
  description,
  label,
  onAction,
  panelDescription,
  panelEyebrow,
  panelTitle,
  title
}: PageMissingStateProps) {
  return (
    <>
      <PageHeader
        description={description}
        navigation={<BreadcrumbTrail items={breadcrumbs} label={label} />}
        title={title}
      />
      <EmptyStateRail>
        <EmptyStatePanel
          actions={
            <Button onClick={onAction} type="button" variant="quiet">
              {actionLabel}
            </Button>
          }
          description={panelDescription}
          eyebrow={panelEyebrow}
          title={panelTitle}
        />
      </EmptyStateRail>
    </>
  );
}
