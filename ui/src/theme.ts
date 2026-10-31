import { useSyncExternalStore } from "react";

export type ThemePreference = "system" | "light" | "dark";

function subscribe(listener: () => void) {
  window.addEventListener("vectis:theme-change", listener);
  return () => window.removeEventListener("vectis:theme-change", listener);
}

function preferenceSnapshot(): ThemePreference {
  const value = document.documentElement.dataset.themePreference;
  return value === "light" || value === "dark" ? value : "system";
}

export function setThemePreference(preference: ThemePreference) {
  window.dispatchEvent(new CustomEvent("vectis:theme-preference", { detail: preference }));
}

export function useThemePreference() {
  return useSyncExternalStore(subscribe, preferenceSnapshot, () => "system" as const);
}

export function useResolvedTheme() {
  return useSyncExternalStore(
    subscribe,
    () => (document.documentElement.dataset.theme === "dark" ? "dark" : "light"),
    () => "light" as const
  );
}
