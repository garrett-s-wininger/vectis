import type { TextareaHTMLAttributes } from "react";
import styles from "./Field.module.css";

type TextAreaFieldProps = TextareaHTMLAttributes<HTMLTextAreaElement> & {
  code?: boolean;
  label: string;
  wide?: boolean;
};

export function TextAreaField({ code, label, wide, ...props }: TextAreaFieldProps) {
  return (
    <label className={`${styles.root} ${wide ? styles.wide : ""}`}>
      <span>{label}</span>
      <textarea className={code ? styles.codeArea : undefined} {...props} />
    </label>
  );
}
