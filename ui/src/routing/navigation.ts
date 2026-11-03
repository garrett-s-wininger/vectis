import { useEffect, useLayoutEffect } from "react";
import type { AppRoute } from "./routes";

type Position = { top: number; left: number };
type RunListLocation = { href: string; scroll: Position; namespace?: string };
type NavigationState = { key: string; scroll?: Position; runList?: RunListLocation; namespace?: string };
type NavigationOptions = { replace?: boolean; scroll?: Position; namespace?: string };
const stateKey = "vectisNavigation";
const positions = new Map<string, Position>();
let sequence = 0;
let activeKey: string | undefined;

function stateRoot(): Record<string, unknown> {
  const state: unknown = window.history.state;
  return state && typeof state === "object" && !Array.isArray(state) ? (state as Record<string, unknown>) : {};
}

function navigationState(): NavigationState | undefined {
  const value = stateRoot()[stateKey] as NavigationState | undefined;
  return value && typeof value.key === "string" ? value : undefined;
}

function writeState(state: NavigationState) {
  window.history.replaceState({ ...stateRoot(), [stateKey]: state }, "");
}

function ensureState(): NavigationState {
  const existing = navigationState();
  if (existing) return existing;

  const state = { key: `${Date.now()}-${++sequence}` };
  writeState(state);

  return state;
}

function mainPanel() {
  return document.querySelector<HTMLElement>("main");
}

function currentPosition(): Position {
  const main = mainPanel();
  return { top: main?.scrollTop ?? 0, left: main?.scrollLeft ?? 0 };
}

function remember(key: string, position: Position) {
  positions.delete(key);
  positions.set(key, position);

  if (positions.size > 100) positions.delete(positions.keys().next().value!);
}

function savePosition() {
  const state = ensureState();
  const scroll = currentPosition();

  remember(state.key, scroll);
  writeState({ ...state, scroll });

  return scroll;
}

function isRunList(pathname: string) {
  return pathname === "/runs" || pathname === "/runs/";
}

export function navigateTo(path: string, options: NavigationOptions = {}) {
  const current = ensureState();
  const scroll = currentPosition();
  remember(current.key, scroll);

  const namespace = options.namespace ?? current.namespace;
  if (!options.replace) writeState({ ...current, scroll, namespace });

  const target = new URL(path, window.location.href);
  const samePage = target.pathname === window.location.pathname;
  const origin = isRunList(window.location.pathname)
    ? { href: `${window.location.pathname}${window.location.search}${window.location.hash}`, scroll, namespace }
    : current.runList;

  const next: NavigationState = {
    key: options.replace ? current.key : `${Date.now()}-${++sequence}`,
    scroll: options.scroll ?? (samePage ? scroll : target.hash ? undefined : { top: 0, left: 0 }),
    runList: target.pathname.startsWith("/runs/") && !isRunList(target.pathname) ? origin : undefined,
    namespace: isRunList(target.pathname) ? namespace : undefined
  };

  if (next.scroll) remember(next.key, next.scroll);
  else positions.delete(next.key);

  const state = { ...(options.replace ? stateRoot() : {}), [stateKey]: next };

  if (options.replace) window.history.replaceState(state, "", path);
  else window.history.pushState(state, "", path);

  window.dispatchEvent(new PopStateEvent("popstate"));
}

export function runListNamespace() {
  const value = navigationState()?.namespace;
  return typeof value === "string" ? value : undefined;
}

export function returnToRunList() {
  const origin = navigationState()?.runList;

  if (origin && typeof origin.href === "string") {
    try {
      const url = new URL(origin.href, window.location.origin);
      if (url.origin === window.location.origin && isRunList(url.pathname)) {
        navigateTo(`${url.pathname}${url.search}${url.hash}`, { scroll: origin.scroll, namespace: origin.namespace });
        return;
      }
    } catch {
      /* NOTE(garrett): Older or invalid history state falls back to the runs index. */
    }
  }

  navigateTo("/runs");
}

export function useNavigationScroll(route: AppRoute, ready: boolean) {
  useEffect(() => {
    const previous = window.history.scrollRestoration;
    window.history.scrollRestoration = "manual";
    window.addEventListener("pagehide", savePosition);

    return () => {
      window.history.scrollRestoration = previous;
      window.removeEventListener("pagehide", savePosition);
      activeKey = undefined;
    };
  }, []);

  useLayoutEffect(() => {
    if (!ready) return;
    const main = mainPanel();

    if (!main) return;
    const state = ensureState();
    const position = positions.get(state.key) ?? state.scroll;

    activeKey = undefined;
    main.scrollTop = position?.top ?? 0;
    main.scrollLeft = position?.left ?? 0;

    if (!position && window.location.hash) {
      try {
        document.getElementById(decodeURIComponent(window.location.hash.slice(1)))?.scrollIntoView?.();
      } catch {
        /* NOTE(garrett): An invalid fragment should not prevent the page from rendering. */
      }
    }

    activeKey = state.key;
    remember(state.key, currentPosition());
  }, [route, ready]);

  useLayoutEffect(() => {
    if (!ready) return;
    const main = mainPanel();
    const onScroll = () => {
      const state = navigationState();
      if (state && activeKey === state.key) remember(state.key, currentPosition());
    };

    main?.addEventListener("scroll", onScroll, { passive: true });
    return () => main?.removeEventListener("scroll", onScroll);
  }, [route, ready]);
}
