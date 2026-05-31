import { useRef } from "react";
import type { Namespace } from "../../domain/console";
import styles from "./NamespacePicker.module.css";

type NamespacePickerProps = {
  compact?: boolean;
  namespaces: Namespace[];
  onChange: (namespacePath: string) => void;
  value: string;
};

export function NamespacePicker({ compact = false, namespaces, onChange, value }: NamespacePickerProps) {
  const menuRef = useRef<HTMLDetailsElement>(null);
  const selectedNamespace = namespaces.find((namespace) => namespace.path === value);
  const selectedLabel = formatNamespaceLabel(selectedNamespace?.path ?? value);

  if (compact) {
    return (
      <details className={`${styles.root} ${styles.compact}`} ref={menuRef}>
        <summary className={styles.summary} aria-label="Namespace">
          <span>Namespace</span>
          <strong>{selectedLabel}</strong>
        </summary>
        <div className={styles.menu}>
          {namespaces.map((namespace) => (
            <button
              aria-current={namespace.path === value ? "page" : undefined}
              className={styles.menuItem}
              key={namespace.id}
              onClick={() => {
                onChange(namespace.path);
                if (menuRef.current) {
                  menuRef.current.open = false;
                }
              }}
              type="button"
            >
              {formatNamespaceLabel(namespace.path)}
            </button>
          ))}
        </div>
      </details>
    );
  }

  return (
    <label className={styles.root}>
      <span>Namespace</span>
      <select aria-label="Namespace" onChange={(event) => onChange(event.target.value)} value={value}>
        {namespaces.map((namespace) => (
          <option key={namespace.id} value={namespace.path}>
            {namespace.path === "/" ? "/ root" : namespace.path}
          </option>
        ))}
      </select>
    </label>
  );
}

function formatNamespaceLabel(path: string) {
  return path === "/" ? "/ root" : path;
}
