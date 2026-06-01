import type { InputHTMLAttributes } from "react";
import styles from "./Field.module.css";

type FormFieldProps = InputHTMLAttributes<HTMLInputElement> & {
  label: string;
  wide?: boolean;
};

export function FormField({ label, type = "text", wide, ...props }: FormFieldProps) {
  return (
    <label className={`${styles.root} ${wide ? styles.wide : ""}`}>
      <span>{label}</span>
      <input type={type} {...props} />
    </label>
  );
}
