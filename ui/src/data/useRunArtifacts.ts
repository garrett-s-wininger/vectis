import { useEffect, useState } from "react";
import type { RunListItem } from "../components";
import { isTerminalRunStatus } from "../components/data/RunPresentation";
import type { ArtifactPage, LoadRunArtifacts } from "./runArtifacts";

type State = ArtifactPage & { key: string; loading: boolean; refreshing: boolean; error: string };

export function useRunArtifacts(run: RunListItem, load: LoadRunArtifacts, taskID?: string, attempt?: number) {
  const key = JSON.stringify([run.id, taskID, attempt]);
  const [paging, setPaging] = useState({ key, pages: 1 });
  const pages = paging.key === key ? paging.pages : 1;
  const [revision, setRevision] = useState(0);
  const [state, setState] = useState<State>({ key, data: [], loading: true, refreshing: false, error: "" });
  const terminal = isTerminalRunStatus(run.status);

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let finalReads = 0;
    let delay = 5000;

    async function refresh(foreground = false) {
      if (foreground)
        setState((previous) =>
          previous.key === key
            ? { ...previous, refreshing: true }
            : { key, data: [], loading: true, refreshing: true, error: "" }
        );

      try {
        const artifacts = new Map<string, ArtifactPage["data"][number]>();
        let cursor: number | undefined;

        for (let page = 0; page < pages; page++) {
          const result = await load(run.id, { taskID, attempt, cursor, signal: controller.signal });
          if (controller.signal.aborted) return;

          for (const artifact of result.data) artifacts.set(artifact.name, artifact);
          if (result.nextCursor !== undefined && result.nextCursor === cursor)
            throw new Error("Artifact pagination did not advance.");

          cursor = result.nextCursor;
          if (cursor === undefined) break;
        }

        setState({
          key,
          data: [...artifacts.values()],
          nextCursor: cursor,
          loading: false,
          refreshing: false,
          error: ""
        });

        delay = 5000;
      } catch (error) {
        if (controller.signal.aborted) return;

        setState((previous) => ({
          ...previous,
          loading: false,
          refreshing: false,
          error: error instanceof Error ? error.message : "Unable to load artifacts."
        }));

        delay = Math.min(delay * 2, 30000);
      }

      if (!controller.signal.aborted && (!terminal || finalReads++ < 2)) {
        timer = setTimeout(() => void refresh(), terminal ? 2000 : delay);
      }
    }

    void refresh(true);

    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [key, load, pages, revision, run.id, taskID, terminal, attempt]);

  const visible: State = state.key === key ? state : { key, data: [], loading: true, refreshing: true, error: "" };
  return {
    ...visible,
    refresh: () => setRevision((value) => value + 1),
    loadMore: () => setPaging({ key, pages: pages + 1 })
  };
}
