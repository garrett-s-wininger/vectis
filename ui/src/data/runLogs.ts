export type RunLogEntry = {
  data: string;
  sequence: number;
  stream: "stdout" | "stderr" | "control";
  timestamp?: string;
  executionID?: string;
  completed?: boolean;
};

type APILogEntry = {
  data?: string;
  sequence?: number;
  stream?: number;
  timestamp?: string;
  execution_id?: string;
  completed?: number;
};

export function streamRunLogs(
  runID: string,
  onEntry: (entry: RunLogEntry) => void,
  onError: (message: string, streamOpened?: boolean) => void,
  onComplete?: () => void
) {
  if (typeof EventSource === "undefined" || shouldSkipLogStream()) {
    return () => {};
  }

  return openRunLogStream(runID, onEntry, onError, onComplete);
}

export function openRunLogStream(
  runID: string,
  onEntry: (entry: RunLogEntry) => void,
  onError: (message: string, streamOpened?: boolean) => void,
  onComplete?: () => void
) {
  const source = new EventSource(`/api/v1/runs/${encodeURIComponent(runID)}/logs?tail=500`);
  let closed = false;
  let opened = false;

  const close = () => {
    if (closed) return;
    closed = true;
    source.close();
  };

  source.onopen = () => {
    opened = true;
  };

  source.onmessage = (event) => {
    if (closed) return;
    opened = true;

    const entry = parseRunLogEvent(event.data);
    if (!entry) {
      return;
    }

    if (!entry.executionID && (entry.completed || (entry.stream === "control" && isCompletionEvent(entry.data)))) {
      close();
      onComplete?.();
      return;
    }

    if (entry.stream === "control") return;

    onEntry(entry);
  };

  source.onerror = () => {
    if (closed) return;
    close();
    onError("Log stream is unavailable.", opened);
  };

  return close;
}

function shouldSkipLogStream() {
  return import.meta.env.VITE_CONSOLE_DATA_SOURCE === "mock" || import.meta.env.STORYBOOK === "true";
}

export function parseRunLogEvent(data: string): RunLogEntry | null {
  let entry: APILogEntry;
  try {
    entry = JSON.parse(data) as APILogEntry;
  } catch {
    return null;
  }

  if (!entry || typeof entry !== "object" || typeof entry.data !== "string") {
    return null;
  }

  return {
    data: entry.data,
    sequence: typeof entry.sequence === "number" ? entry.sequence : -1,
    stream: streamName(entry.stream),
    timestamp: typeof entry.timestamp === "string" ? entry.timestamp : undefined,
    ...(typeof entry.execution_id === "string" && entry.execution_id ? { executionID: entry.execution_id } : {}),
    ...(typeof entry.completed === "number" && entry.completed !== 0 ? { completed: true } : {})
  };
}

function streamName(stream: number | undefined): RunLogEntry["stream"] {
  switch (stream) {
    case 1:
      return "stderr";
    case 2:
      return "control";
    default:
      return "stdout";
  }
}

function isCompletionEvent(data: string) {
  try {
    const event = JSON.parse(data) as { event?: string };
    return event.event === "completed";
  } catch {
    return false;
  }
}
