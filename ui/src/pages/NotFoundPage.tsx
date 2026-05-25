import { AppState } from "../components/AppState";
import { PageHeader } from "../components/PageHeader";

export function NotFoundPage() {
  return (
    <>
      <PageHeader eyebrow="Console" title="Page not found" />
      <AppState
        description="Choose another destination from the primary navigation."
        title="No route matched"
        tone="error"
      />
    </>
  );
}
