import "./index.css";
import { Button } from "./components/Button";

export function App() {
  return (
    <main className="app-shell">
      <section className="welcome-panel" aria-labelledby="welcome-title">
        <p className="eyebrow">Vectis Console</p>
        <h1 id="welcome-title">Hello from Vectis UI</h1>
        <p>
          The product console is ready for real components, routes, and API
          integration.
        </p>
        <Button>Refresh</Button>
      </section>
    </main>
  );
}
