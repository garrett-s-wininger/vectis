import type { SelectHTMLAttributes } from "react";
import styles from "./Field.module.css";

export type SelectOption = {
  label: string;
  value: string;
};

type SelectFieldProps = Omit<SelectHTMLAttributes<HTMLSelectElement>, "children"> & {
  label: string;
  options: SelectOption[];
  wide?: boolean;
};

export function SelectField({ label, options, wide, ...props }: SelectFieldProps) {
  return (
    <label className={`${styles.root} ${wide ? styles.wide : ""}`}>
      <span>{label}</span>
      <span className={styles.selectWrap}>
        <select {...props}>
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </span>
    </label>
  );
}
