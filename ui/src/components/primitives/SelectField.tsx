import type { SelectHTMLAttributes } from "react";
import { useId } from "react";
import styles from "./Field.module.css";
import { SelectControl } from "./SelectControl";

export type SelectOption = {
  disabled?: boolean;
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
        <SelectControl
          {...props}
          aria-describedby={error ? errorID : props["aria-describedby"]}
          id={selectID}
          invalid={Boolean(error)}
          options={options}
          summaryLabel={label}
        />
      </span>
      {error ? (
        <small className={styles.error} id={errorID}>
          {error}
        </small>
      ) : null}
    </div>
  );
}
