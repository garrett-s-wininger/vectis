import type { SelectHTMLAttributes } from "react";
import { useId } from "react";
import styles from "./Field.module.css";

export type SelectOption = {
  label: string;
  value: string;
};

type SelectFieldProps = Omit<SelectHTMLAttributes<HTMLSelectElement>, "children"> & {
  error?: string;
  label: string;
  options: SelectOption[];
  wide?: boolean;
};

export function SelectField({ error, id, label, options, wide, ...props }: SelectFieldProps) {
  const generatedID = useId();
  const selectID = id ?? generatedID;
  const errorID = `${selectID}-error`;

  return (
    <div className={`${styles.root} ${wide ? styles.wide : ""}`}>
      <label htmlFor={selectID}>
        <span>{label}</span>
      </label>
      <span className={styles.selectWrap}>
        <select
          {...props}
          aria-describedby={error ? errorID : props["aria-describedby"]}
          aria-invalid={error ? true : props["aria-invalid"]}
          id={selectID}
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </span>
      {error ? (
        <small className={styles.error} id={errorID}>
          {error}
        </small>
      ) : null}
    </div>
  );
}
