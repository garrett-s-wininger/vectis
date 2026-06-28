import { useId } from "react";
import type { Namespace } from "../../domain/console";
import { SelectControl } from "../primitives/SelectControl";
import {
  closeOtherDropdowns,
  closeOtherDropdownsFromTrigger,
  closeParentDropdown,
  coordinatedDropdownProps
} from "./dropdownCoordination";
import styles from "./NamespacePicker.module.css";

type NamespacePickerProps = {
  compact?: boolean;
  namespaces: Namespace[];
  onChange: (namespacePath: string) => void;
  value: string;
};

export function NamespacePicker({ compact = false, namespaces, onChange, value }: NamespacePickerProps) {
  const generatedID = useId();
  const selectedNamespace = namespaces.find((namespace) => namespace.path === value);
  const selectedLabel = formatNamespaceLabel(selectedNamespace?.path ?? value);

  if (compact) {
    return (
      <details
        {...coordinatedDropdownProps}
        className={`${styles.root} ${styles.compact}`}
        onToggle={(event) => closeOtherDropdowns(event.currentTarget)}
      >
        <summary
          className={styles.summary}
          aria-label="Namespace"
          onClick={(event) => closeOtherDropdownsFromTrigger(event.currentTarget)}
        >
          <span>Namespace</span>
          <strong>{selectedLabel}</strong>
        </summary>
        <div className={styles.menu}>
          {namespaces.map((namespace) => (
            <button
              aria-current={namespace.path === value ? "page" : undefined}
              className={styles.menuItem}
              key={namespace.id}
              onClick={(event) => {
                onChange(namespace.path);
                closeParentDropdown(event.currentTarget);
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
    <label className={styles.root} htmlFor={generatedID}>
      <span>Namespace</span>
      <span className={styles.selectWrap}>
        <SelectControl
          aria-label="Namespace"
          id={generatedID}
          onChange={(event) => onChange(event.target.value)}
          options={namespaces.map((namespace) => ({
            label: formatNamespaceLabel(namespace.path),
            value: namespace.path
          }))}
          summaryLabel="Namespace"
          value={value}
        />
      </span>
    </label>
  );
}

function formatNamespaceLabel(path: string) {
  return path === "/" ? "/ root" : path;
}
