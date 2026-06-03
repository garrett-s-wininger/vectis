import type { TextareaHTMLAttributes } from "react";
import { useId } from "react";
import styles from "./Field.module.css";

type TextAreaFieldProps = TextareaHTMLAttributes<HTMLTextAreaElement> & {
  code?: boolean;
  error?: string;
  label: string;
  wide?: boolean;
};

export function TextAreaField({ code, error, id, label, wide, ...props }: TextAreaFieldProps) {
  const generatedID = useId();
  const textareaID = id ?? generatedID;
  const errorID = `${textareaID}-error`;

  return (
    <div className={`${styles.root} ${wide ? styles.wide : ""}`}>
      <label htmlFor={textareaID}>
        <span>{label}</span>
      </label>
      <textarea
        {...props}
        aria-describedby={error ? errorID : props["aria-describedby"]}
        aria-invalid={error ? true : props["aria-invalid"]}
        className={code ? styles.codeArea : undefined}
        id={textareaID}
      />
      {error ? (
        <small className={styles.error} id={errorID}>
          {error}
        </small>
      ) : null}
    </div>
  );
}
