import { AppState } from "../components";
import { Button } from "../components";
import { navigateTo } from "../routing/routes";

export function NotFoundPage() {
  return (
    <AppState
      actions={
        <Button onClick={() => navigateTo("/jobs")} type="button" variant="quiet">
          Go to Jobs
        </Button>
      }
      description="Choose another destination from the primary navigation."
      title="Page Not Found"
      tone="error"
    />
  );
}
