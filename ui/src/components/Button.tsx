import type { ButtonHTMLAttributes, ReactNode } from "react";
import styles from "./Button.module.css";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  children: ReactNode;
};

export function Button({
  children,
  className,
  type = "button",
  ...props
}: ButtonProps) {
  const buttonClassName = className ? `${styles.root} ${className}` : styles.root;

  return (
    <button className={buttonClassName} type={type} {...props}>
      {children}
    </button>
  );
}
