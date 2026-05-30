import type { TextareaHTMLAttributes } from "react";
import styles from "./Field.module.css";

type TextAreaFieldProps = TextareaHTMLAttributes<HTMLTextAreaElement> & {
  label: string;
  wide?: boolean;
};

export function TextAreaField({ label, wide, ...props }: TextAreaFieldProps) {
  return (
    <label className={`${styles.root} ${wide ? styles.wide : ""}`}>
      <span>{label}</span>
      <textarea {...props} />
    </label>
  );
}
