import type { MockNamespace } from "../mocks/consoleData";

type NamespacePickerProps = {
  namespaces: MockNamespace[];
  onChange: (namespacePath: string) => void;
  value: string;
};

export function NamespacePicker({
  namespaces,
  onChange,
  value
}: NamespacePickerProps) {
  return (
    <label className="namespace-picker">
      <span>Namespace</span>
      <select
        aria-label="Namespace"
        onChange={(event) => onChange(event.target.value)}
        value={value}
      >
        {namespaces.map((namespace) => (
          <option key={namespace.id} value={namespace.path}>
            {namespace.path === "/" ? "/ root" : namespace.path}
          </option>
        ))}
      </select>
    </label>
  );
}
