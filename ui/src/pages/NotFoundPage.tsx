import { AppState } from "../components";
import { PageHeader } from "../components";

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
