import { BookOpen, Bug, CircleHelp, Code2, GitBranch } from "lucide-react";
import type { NavGroup } from "../layout/AppShell";

export const defaultDocsURL = "https://vectis.dev/";

export function helpLinks(docsURL = defaultDocsURL): NavGroup {
  let base: URL;

  try {
    base = new URL(docsURL, window.location.origin);
    if (!["http:", "https:"].includes(base.protocol) || base.username || base.password)
      throw new Error("Invalid docs URL");
  } catch {
    base = new URL(defaultDocsURL);
  }

  base.search = "";
  base.hash = "";
  base.pathname = `${base.pathname.replace(/\/$/, "")}/`;

  return {
    label: "Help",
    icon: CircleHelp,
    items: [
      { label: "Documentation", href: new URL("docs/getting-started", base).href, icon: BookOpen, external: true },
      { label: "API reference", href: new URL("api", base).href, icon: Code2, external: true },
      { label: "Source code", href: "https://github.com/garrett-s-wininger/vectis", icon: GitBranch, external: true },
      {
        label: "Report an issue",
        href: "https://github.com/garrett-s-wininger/vectis/issues",
        icon: Bug,
        external: true
      }
    ]
  };
}
