import type { SelectHTMLAttributes } from "react";
import styles from "./Field.module.css";

export type SelectOption = {
  label: string;
  value: string;
};

type SelectFieldProps = Omit<SelectHTMLAttributes<HTMLSelectElement>, "children"> & {
  label: string;
  options: SelectOption[];
};

export function SelectField({ label, options, ...props }: SelectFieldProps) {
  return (
    <label className={styles.root}>
      <span>{label}</span>
      <select {...props}>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}
