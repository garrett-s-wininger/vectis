import { useRef, useState } from "react";
import { RotateCcw, Square } from "lucide-react";
import { Button, ErrorAlert, type RunListItem } from "../components";
import styles from "./RunControls.module.css";

export type RunControlActions = {
  onCancel?: (runID: string) => Promise<void>;
  onReplay?: (runID: string, idempotencyKey: string) => Promise<void>;
};

export function RunControls({ run, onCancel, onReplay }: RunControlActions & { run: RunListItem }) {
  const [confirmCancel, setConfirmCancel] = useState(false);
  const [busy, setBusy] = useState(false);
  const [cancelRequested, setCancelRequested] = useState(false);
  const [error, setError] = useState("");
  const inFlight = useRef(false);
  const replayKey = useRef<string | null>(null);
  const active = run.status === "queued" || run.status === "running";
  const requested = cancelRequested || run.cancelRequested;

  async function perform(action: "cancel" | "replay") {
    if (inFlight.current) return;
    inFlight.current = true;

    setBusy(true);
    setError("");

    try {
      if (action === "cancel") {
        await onCancel?.(run.id);
        setCancelRequested(true);
        setConfirmCancel(false);
      } else {
        replayKey.current ??= crypto.randomUUID();
        await onReplay?.(run.id, replayKey.current);
      }
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unable to update run.");
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }

  if (!onCancel && !onReplay) return null;
  return (
    <div className={styles.root} role="group" aria-label="Run actions">
      <div className={styles.actions}>
        {active && onCancel ? (
          <Button
            className={`${styles.actionButton} ${styles.cancelButton}`}
            variant="danger"
            disabled={busy || requested}
            onClick={() => {
              if (confirmCancel) void perform("cancel");
              else {
                setError("");
                setConfirmCancel(true);
              }
            }}
            onBlur={() => {
              if (!inFlight.current) setConfirmCancel(false);
            }}
            onKeyDown={(event) => {
              if (event.key === "Escape") setConfirmCancel(false);
            }}
            title={confirmCancel ? "Click again to cancel this run. Escape keeps it running." : undefined}
          >
            <Square aria-hidden="true" />
            {busy ? "Requesting…" : requested ? "Cancel requested" : confirmCancel ? "Confirm cancel" : "Cancel run"}
          </Button>
        ) : null}
        {!active && onReplay ? (
          <Button
            className={styles.actionButton}
            disabled={busy}
            onClick={() => void perform("replay")}
            title="Create a new run using the same saved definition"
          >
            <RotateCcw aria-hidden="true" />
            {busy ? "Starting run…" : "Run again"}
          </Button>
        ) : null}
      </div>
      {error ? <ErrorAlert message={error} /> : null}
    </div>
  );
}
