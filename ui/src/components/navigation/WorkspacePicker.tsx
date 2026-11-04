import { useId, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent, type MouseEvent } from "react";
import {
  ArrowRight,
  ChevronDown,
  ChevronRight,
  ChevronsUpDown,
  FolderTree,
  Info,
  Search,
  Settings2,
  Shield
} from "lucide-react";
import { closeOtherDropdowns, closeOtherDropdownsFromTrigger, coordinatedDropdownProps } from "./dropdownCoordination";
import { namespaceAncestors, namespaceTree } from "./namespaceTree";
import styles from "./WorkspacePicker.module.css";

export function WorkspacePicker({
  namespaces,
  accessLabel,
  namespaceHref,
  namespaceManagementHref,
  onNavigate,
  value,
  onChange,
  disabled = false
}: {
  namespaces: string[];
  accessLabel?: string;
  namespaceHref?: string;
  namespaceManagementHref?: string;
  onNavigate?: (href: string, event: MouseEvent<HTMLAnchorElement>) => void;
  value: string;
  onChange: (namespace: string) => void;
  disabled?: boolean;
}) {
  const id = useId();
  const details = useRef<HTMLDetailsElement>(null);
  const trigger = useRef<HTMLElement>(null);
  const search = useRef<HTMLInputElement>(null);
  const items = useRef(new Map<string, HTMLDivElement>());
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [expanded, setExpanded] = useState(() => new Set(namespaceAncestors(value)));
  const [focused, setFocused] = useState(value);
  const tree = useMemo(() => namespaceTree(namespaces), [namespaces]);
  const term = query.trim().toLocaleLowerCase();
  const matched = new Set(
    tree.filter((node) => node.selectable && node.path.toLocaleLowerCase().includes(term)).map((node) => node.path)
  );

  const searchPaths = new Set<string>();

  if (term)
    for (const path of matched) {
      searchPaths.add(path);
      for (const parent of namespaceAncestors(path)) searchPaths.add(parent);
    }

  const visible = tree.filter((node) =>
    term
      ? searchPaths.has(node.path)
      : namespaceAncestors(node.path).every((parent) => parent === node.path || expanded.has(parent))
  );

  const siblings = new Map<string | undefined, string[]>();
  for (const node of visible) {
    const group = siblings.get(node.parent) ?? [];
    group.push(node.path);
    siblings.set(node.parent, group);
  }

  const active = visible.some((node) => node.path === focused)
    ? focused
    : (visible.find((node) => node.path === value)?.path ?? visible[0]?.path);

  useLayoutEffect(() => {
    if (!open) return;
    search.current?.focus();
    items.current.get(value)?.scrollIntoView?.({ block: "nearest" });
  }, [open, value]);

  const parts = value.split("/").filter(Boolean);
  const leaf = parts.at(-1) ?? "Root";
  const parent = parts.length > 1 ? `/${parts.slice(0, -1).join("/")}` : "Namespace";
  function close(restore = false) {
    if (details.current) details.current.open = false;

    setOpen(false);
    setQuery("");

    if (restore) trigger.current?.focus();
  }

  function expand(path: string) {
    if (term) return;
    setExpanded((current) => {
      const next = new Set(current);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  }

  function select(path: string) {
    onChange(path);
    close(true);
  }

  function focus(path?: string) {
    if (!path) return;

    setFocused(path);
    items.current.get(path)?.focus();
  }

  function key(event: KeyboardEvent<HTMLDivElement>, index: number) {
    if (event.target !== event.currentTarget) return;
    const node = visible[index];

    if (["ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight", "Home", "End", "Enter", " "].includes(event.key))
      event.preventDefault();

    if (event.key === "ArrowDown") focus(visible[Math.min(index + 1, visible.length - 1)]?.path);
    if (event.key === "ArrowUp") focus(visible[Math.max(index - 1, 0)]?.path);
    if (event.key === "Home") focus(visible[0]?.path);
    if (event.key === "End") focus(visible.at(-1)?.path);
    if (event.key === "ArrowRight" && node.children) {
      if (!expanded.has(node.path) && !term) expand(node.path);
      else focus(visible[index + 1]?.path);
    }

    if (event.key === "ArrowLeft") {
      if (node.children && expanded.has(node.path) && !term) expand(node.path);
      else focus(node.parent);
    }

    if (event.key === "Enter" || event.key === " ") {
      if (node.selectable) select(node.path);
      else expand(node.path);
    }
  }

  return (
    <details
      {...coordinatedDropdownProps}
      className={styles.root}
      ref={details}
      onToggle={(event) => {
        setOpen(event.currentTarget.open);
        closeOtherDropdowns(event.currentTarget);
        if (!event.currentTarget.open) setQuery("");
      }}
      onKeyDown={(event) => {
        if (event.key === "Escape" && open) {
          event.preventDefault();
          event.stopPropagation();
          close(true);
        }
      }}
    >
      <summary
        ref={trigger}
        className={styles.trigger}
        aria-label={
          namespaces.length
            ? `Namespace: ${value === "/" ? "Root" : value}`
            : disabled
              ? "Loading namespaces"
              : "No accessible namespaces"
        }
        role="button"
        aria-haspopup="tree"
        aria-controls={open ? id : undefined}
        aria-expanded={open}
        aria-disabled={disabled || namespaces.length === 0}
        title={value === "/" ? "Root namespace" : value}
        onClick={(event) => {
          event.preventDefault();
          if (disabled || !namespaces.length) return;

          if (open) close();
          else {
            closeOtherDropdownsFromTrigger(event.currentTarget);
            setExpanded((current) => new Set([...current, ...namespaceAncestors(value)]));
            setFocused(value);

            if (details.current) details.current.open = true;
            setOpen(true);
          }
        }}
      >
        <FolderTree aria-hidden="true" />
        <span className={styles.identity}>
          <strong>{namespaces.length ? leaf : disabled ? "Loading…" : "No namespaces"}</strong>
          <small>{parent}</small>
        </span>
        <ChevronsUpDown className={styles.chevron} aria-hidden="true" />
      </summary>
      <div className={styles.panel} hidden={!open}>
        <div className={styles.search}>
          <Search aria-hidden="true" />
          <input
            ref={search}
            aria-label="Search namespaces"
            placeholder="Search namespaces…"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                event.preventDefault();
                focus(
                  event.key === "ArrowDown"
                    ? term
                      ? visible.find((node) => matched.has(node.path))?.path
                      : active
                    : visible.at(-1)?.path
                );
              }
            }}
          />
        </div>
        <div
          className={styles.tree}
          role={visible.length ? "tree" : undefined}
          aria-label={visible.length ? "Namespaces" : undefined}
          id={id}
        >
          {visible.map((node, index) => (
            <div
              key={node.path}
              ref={(element) => {
                if (element) items.current.set(node.path, element);
                else items.current.delete(node.path);
              }}
              className={styles.item}
              role="treeitem"
              tabIndex={node.path === active ? 0 : -1}
              aria-label={node.path === "/" ? "Root" : node.path}
              aria-level={node.depth}
              aria-posinset={siblings.get(node.parent)!.indexOf(node.path) + 1}
              aria-setsize={siblings.get(node.parent)!.length}
              aria-selected={node.selectable ? node.path === value : undefined}
              aria-expanded={node.children ? Boolean(term) || expanded.has(node.path) : undefined}
              title={node.selectable ? node.path : `${node.path} — parent namespace`}
              style={{ paddingLeft: `${8 + Math.min(node.depth - 1, 5) * 12}px` }}
              onFocus={() => setFocused(node.path)}
              onKeyDown={(event) => key(event, index)}
              onClick={() => (node.selectable ? select(node.path) : expand(node.path))}
            >
              {node.children ? (
                <button
                  type="button"
                  className={styles.branch}
                  tabIndex={-1}
                  disabled={Boolean(term)}
                  aria-label={`${term ? "Expanded" : expanded.has(node.path) ? "Collapse" : "Expand"} ${node.path}`}
                  onClick={(event) => {
                    event.stopPropagation();
                    expand(node.path);
                  }}
                >
                  {expanded.has(node.path) || term ? (
                    <ChevronDown aria-hidden="true" />
                  ) : (
                    <ChevronRight aria-hidden="true" />
                  )}
                </button>
              ) : (
                <span className={styles.branchSpace} />
              )}
              <span className={styles.nodeName}>{node.name}</span>
            </div>
          ))}
          {!visible.length ? (
            <p className={styles.empty} role="status">
              No matching namespaces.
            </p>
          ) : null}
        </div>
        {accessLabel || namespaceHref || namespaceManagementHref ? (
          <div className={styles.footer}>
            {accessLabel ? (
              <span className={styles.access} aria-label={`Effective access: ${accessLabel}`}>
                <Shield aria-hidden="true" />
                Your access: <strong>{accessLabel}</strong>
              </span>
            ) : null}
            {(
              [
                [namespaceHref, "View namespace", Info],
                [namespaceManagementHref, "Manage namespaces", Settings2]
              ] as const
            ).map(([href, label, Icon]) =>
              href ? (
                <a
                  className={styles.details}
                  href={href}
                  key={label}
                  onClick={(event) => {
                    onNavigate?.(href, event);
                    close();
                  }}
                >
                  <Icon aria-hidden="true" />
                  <span>{label}</span>
                  <ArrowRight className={styles.linkArrow} aria-hidden="true" />
                </a>
              ) : null
            )}
          </div>
        ) : null}
      </div>
    </details>
  );
}
