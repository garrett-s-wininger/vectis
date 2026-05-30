import type { InputHTMLAttributes } from "react";
import styles from "./Field.module.css";

type FormFieldProps = InputHTMLAttributes<HTMLInputElement> & {
  label: string;
};

export function FormField({ label, type = "text", ...props }: FormFieldProps) {
  return (
    <label className={styles.root}>
      <span>{label}</span>
      <input type={type} {...props} />
    </label>
  );
}
