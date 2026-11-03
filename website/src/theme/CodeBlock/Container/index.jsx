import clsx from "clsx";
import { ThemeClassNames } from "@docusaurus/theme-common";
import styles from "@docusaurus/theme-classic/lib/theme/CodeBlock/Container/styles.module.css";

export default function CodeBlockContainer({ as: Element, ...props }) {
  return (
    <Element
      {...props}
      className={clsx(props.className, styles.codeBlockContainer, ThemeClassNames.common.codeBlock)}
    />
  );
}
