import { useEffect, useRef, useState, type ReactNode } from "react";
import { CircleAlert, Check, Copy } from "lucide-react";
import styles from "./CopyValue.module.css";

export function CopyValue({ value, label, children }: { value: string; label: string; children?: ReactNode }) {
  const [feedback, setFeedback] = useState({ value: "", message: "", copied: false });
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const busy = useRef(false);

  useEffect(() => () => clearTimeout(timer.current), []);
  const current = feedback.value === value ? feedback : undefined;

  async function copy() {
    if (busy.current) return;
    busy.current = true;
    clearTimeout(timer.current);

    try {
      await copyText(value);
      setFeedback({ value, message: `${label} copied.`, copied: true });
    } catch {
      setFeedback({
        value,
        message: `Could not copy ${label.toLowerCase()}. Select and copy the value.`,
        copied: false
      });
    } finally {
      busy.current = false;
      timer.current = setTimeout(() => setFeedback({ value: "", message: "", copied: false }), 3000);
    }
  }

  return (
    <span className={styles.root}>
      <span className={styles.value}>{children ?? value}</span>
      <button
        className={styles.button}
        type="button"
        aria-label={`Copy ${label}`}
        title={current?.message || `Copy ${label}`}
        onClick={() => void copy()}
      >
        {current?.copied ? (
          <Check aria-hidden="true" />
        ) : current?.message ? (
          <CircleAlert aria-hidden="true" />
        ) : (
          <Copy aria-hidden="true" />
        )}
      </button>
      <span className={styles.feedback} aria-live="polite" aria-atomic="true">
        {current?.message}
      </span>
    </span>
  );
}

async function copyText(value: string) {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value);
      return;
    } catch {
      // NOTE(garrett): HTTP demos and denied clipboard permissions can still permit a user-initiated copy.
    }
  }
  const active = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const selection = document.getSelection();
  const ranges = selection ? Array.from({ length: selection.rangeCount }, (_, i) => selection.getRangeAt(i)) : [];
  const input = document.createElement("textarea");
  input.value = value;
  input.readOnly = true;
  input.className = styles.fallback;
  document.body.append(input);

  try {
    input.select();
    if (!document.execCommand("copy")) throw new Error("Clipboard unavailable");
  } finally {
    input.remove();
    active?.focus({ preventScroll: true });
    selection?.removeAllRanges();
    for (const range of ranges) selection?.addRange(range);
  }
}
