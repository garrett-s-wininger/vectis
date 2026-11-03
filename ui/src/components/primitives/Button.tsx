import type { ButtonHTMLAttributes, ReactNode } from "react";
import styles from "./Button.module.css";

type ButtonVariant = "primary" | "quiet" | "danger";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  children: ReactNode;
  variant?: ButtonVariant;
  pending?: boolean;
  pendingLabel?: ReactNode;
  ref?: import("react").Ref<HTMLButtonElement>;
};

export function Button({
  children,
  className,
  type = "button",
  variant = "primary",
  pending,
  pendingLabel,
  disabled,
  "aria-busy": ariaBusy,
  ...props
}: ButtonProps) {
  const variantClassName = styles[variant];
  const buttonClassName = [styles.root, variantClassName, className].filter(Boolean).join(" ");

  return (
    <button
      className={buttonClassName}
      type={type}
      disabled={disabled || pending}
      aria-busy={pending || ariaBusy}
      {...props}
    >
      {pendingLabel ? (
        <span className={styles.labels}>
          <span className={pending ? styles.hidden : undefined} aria-hidden={pending || undefined}>
            {children}
          </span>
          <span className={!pending ? styles.hidden : undefined} aria-hidden={!pending || undefined}>
            {pendingLabel}
          </span>
        </span>
      ) : (
        children
      )}
    </button>
  );
}
