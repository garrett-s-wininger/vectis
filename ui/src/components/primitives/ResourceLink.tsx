import type { AnchorHTMLAttributes } from "react";
import { ArrowRight } from "lucide-react";
import { navigateTo } from "../../routing/navigation";
import styles from "./ResourceLink.module.css";

type ResourceLinkProps = Omit<AnchorHTMLAttributes<HTMLAnchorElement>, "onClick"> & {
  href: string;
  onNavigate?: () => void;
};

export function ResourceLink({ href, onNavigate, children, className, ...props }: ResourceLinkProps) {
  return (
    <a
      {...props}
      href={href}
      className={[styles.root, className].filter(Boolean).join(" ")}
      onClick={(event) => {
        if (
          event.defaultPrevented ||
          event.button !== 0 ||
          event.altKey ||
          event.ctrlKey ||
          event.metaKey ||
          event.shiftKey ||
          (props.target && props.target !== "_self")
        )
          return;

        if (href.startsWith("#")) {
          onNavigate?.();
          return;
        }

        if (new URL(href, window.location.href).origin !== window.location.origin) return;

        event.preventDefault();
        if (onNavigate) onNavigate();
        else navigateTo(href);
      }}
    >
      <span>{children}</span>
      <ArrowRight aria-hidden="true" />
    </a>
  );
}
