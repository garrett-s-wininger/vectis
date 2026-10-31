// Compiled as a dependency-free classic script to apply the theme before first paint.
(() => {
  type Preference = "system" | "light" | "dark";
  const storageKey = "vectis.ui.theme";
  const cookieName = "vectis_ui_theme";
  const root = document.documentElement;
  const media = window.matchMedia?.("(prefers-color-scheme: dark)");
  const normalize = (value: unknown): Preference => (value === "light" || value === "dark" ? value : "system");

  function readPreference() {
    try {
      const saved = localStorage.getItem(storageKey);
      if (saved !== null) return normalize(saved);
    } catch {
      // NOTE(garrett): Browser storage policies must not prevent the console from loading.
    }

    try {
      const cookie = document.cookie
        .split(";")
        .map((part) => part.trim())
        .find((part) => part.startsWith(`${cookieName}=`));
      return normalize(cookie?.slice(cookieName.length + 1));
    } catch {
      return "system";
    }
  }

  let preference = readPreference();

  function apply() {
    const theme = preference === "system" ? (media?.matches ? "dark" : "light") : preference;
    root.dataset.themePreference = preference;
    root.dataset.theme = theme;
    const meta = document.querySelector<HTMLMetaElement>('meta[name="color-scheme"]');

    if (meta) meta.content = theme;
    window.dispatchEvent(new Event("vectis:theme-change"));
  }

  window.addEventListener("vectis:theme-preference", (event) => {
    preference = normalize((event as CustomEvent<unknown>).detail);

    try {
      const maxAge = preference === "system" ? 0 : 31536000;
      document.cookie = `${cookieName}=${preference}; Path=/; Max-Age=${maxAge}; SameSite=Lax${location.protocol === "https:" ? "; Secure" : ""}`;
    } catch {
      // NOTE(garrett): The preference still applies to the current page when cookies are blocked.
    }

    try {
      if (preference === "system") localStorage.removeItem(storageKey);
      else localStorage.setItem(storageKey, preference);
    } catch {
      // NOTE(garrett): The cookie can retain the preference when localStorage is unavailable.
    }

    apply();
  });

  media?.addEventListener("change", () => {
    if (preference === "system") apply();
  });

  const refresh = () => {
    preference = readPreference();
    apply();
  };

  window.addEventListener("storage", (event) => {
    if (event.key === storageKey || event.key === null) refresh();
  });

  window.addEventListener("focus", refresh);
  apply();
})();
