import type { InputHTMLAttributes } from "react";

type FormFieldProps = InputHTMLAttributes<HTMLInputElement> & {
  label: string;
};

export function FormField({ label, type = "text", ...props }: FormFieldProps) {
  return (
    <label className="field">
      <span>{label}</span>
      <input type={type} {...props} />
    </label>
  );
}
