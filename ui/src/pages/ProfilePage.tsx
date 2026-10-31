import { PageHeader } from "../components";
import { ThemePreference } from "../components/ThemePreference";

export function ProfilePage() {
  return (
    <>
      <PageHeader title="Profile" description="Your console preferences." />
      <section
        className="polished-panel polished-panel--overlay-safe app-preferences"
        aria-labelledby="preferences-title"
      >
        <h2 id="preferences-title">Preferences</h2>
        <p>Saved in this browser for future visits, including sign-in. System follows your browser’s preference.</p>
        <ThemePreference />
      </section>
    </>
  );
}
