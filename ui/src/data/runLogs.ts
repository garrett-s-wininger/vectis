export type RunLogEntry = {
  data: string;
  sequence: number;
  stream: "stdout" | "stderr" | "control";
  timestamp?: string;
  executionID?: string;
};

type APILogEntry = {
  data?: string;
  sequence?: number;
  stream?: number;
  timestamp?: string;
  execution_id?: string;
};

export function streamRunLogs(
  runID: string,
  onEntry: (entry: RunLogEntry) => void,
  onError: (message: string) => void
) {
  if (typeof EventSource === "undefined" || shouldSkipLogStream()) {
    return () => {};
  }

  const source = new EventSource(`/api/v1/runs/${encodeURIComponent(runID)}/logs?tail=500`);

  source.onmessage = (event) => {
    const entry = parseRunLogEvent(event.data);
    if (!entry) {
      return;
    }

    if (entry.stream === "control") {
      if (!entry.executionID && isCompletionEvent(entry.data)) {
        source.close();
      }

      return;
    }

    onEntry(entry);
  };

  source.onerror = () => {
    source.close();
    onError("Log stream is unavailable.");
  };

  return () => source.close();
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
    ...(typeof entry.execution_id === "string" && entry.execution_id ? { executionID: entry.execution_id } : {})
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
