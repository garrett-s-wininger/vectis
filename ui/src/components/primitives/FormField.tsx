import type { InputHTMLAttributes } from "react";
import { useId } from "react";
import styles from "./Field.module.css";

type FormFieldProps = InputHTMLAttributes<HTMLInputElement> & {
  error?: string;
  label: string;
  reserveErrorSpace?: boolean;
  wide?: boolean;
};

export function FormField({ error, id, label, reserveErrorSpace, type = "text", wide, ...props }: FormFieldProps) {
  const generatedID = useId();
  const inputID = id ?? generatedID;
  const errorID = `${inputID}-error`;

  return (
    <div className={`${styles.root} ${wide ? styles.wide : ""}`}>
      <label htmlFor={inputID}>
        <span>{label}</span>
      </label>
      <input
        {...props}
        aria-describedby={error ? errorID : props["aria-describedby"]}
        aria-invalid={error ? true : props["aria-invalid"]}
        id={inputID}
        type={type}
      />
      {error || reserveErrorSpace ? (
        <small
          aria-hidden={error ? undefined : true}
          className={`${styles.error} ${reserveErrorSpace ? styles.errorReserved : ""}`}
          id={error ? errorID : undefined}
        >
          {error}
        </small>
      ) : null}
    </div>
  );
}
