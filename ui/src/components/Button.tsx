import type { ButtonHTMLAttributes, ReactNode } from "react";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  children: ReactNode;
};

export function Button({ children, type = "button", ...props }: ButtonProps) {
  return (
    <button className="button" type={type} {...props}>
      {children}
    </button>
  );
}
