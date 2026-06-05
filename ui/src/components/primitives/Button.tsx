import type { ButtonHTMLAttributes, ReactNode } from "react";
import styles from "./Button.module.css";

type ButtonVariant = "primary" | "quiet" | "danger";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  children: ReactNode;
  variant?: ButtonVariant;
};

export function Button({ children, className, type = "button", variant = "primary", ...props }: ButtonProps) {
  const variantClassName = styles[variant];
  const buttonClassName = [styles.root, variantClassName, className].filter(Boolean).join(" ");

  return (
    <button className={buttonClassName} type={type} {...props}>
      {children}
    </button>
  );
}
