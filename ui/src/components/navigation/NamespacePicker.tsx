import type { MockNamespace } from "../../mocks/consoleData";
import styles from "./NamespacePicker.module.css";

type NamespacePickerProps = {
  namespaces: MockNamespace[];
  onChange: (namespacePath: string) => void;
  value: string;
};

export function NamespacePicker({ namespaces, onChange, value }: NamespacePickerProps) {
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
