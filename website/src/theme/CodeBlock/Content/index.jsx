import clsx from "clsx";
import { useCodeBlockContext } from "@docusaurus/theme-common/internal";
import { Highlight } from "prism-react-renderer";
import Line from "@theme/CodeBlock/Line";
import styles from "@docusaurus/theme-classic/lib/theme/CodeBlock/Content/styles.module.css";

function withoutStyle(props) {
  const result = { ...props };
  delete result.style;
  return result;
}

export default function CodeBlockContent({ className }) {
  const { metadata, wordWrap } = useCodeBlockContext();
  return (
    <Highlight theme={{ plain: {}, styles: [] }} code={metadata.code} language={metadata.language}>
      {({ tokens, getLineProps, getTokenProps }) => (
        <pre
          ref={wordWrap.codeBlockRef}
          tabIndex={0}
          className={clsx(className, "prism-code", `language-${metadata.language}`, styles.codeBlock, "thin-scrollbar")}
        >
          <code
            className={clsx(styles.codeBlockLines, metadata.lineNumbersStart !== undefined && "vectis-code-numbered")}
          >
            {tokens.map((line, index) => (
              <Line
                key={index}
                line={line}
                classNames={metadata.lineClassNames[index]}
                getLineProps={(props) => ({
                  ...withoutStyle(getLineProps(props)),
                  "data-line-number":
                    metadata.lineNumbersStart === undefined ? undefined : metadata.lineNumbersStart + index
                })}
                getTokenProps={(props) => withoutStyle(getTokenProps(props))}
              />
            ))}
          </code>
        </pre>
      )}
    </Highlight>
  );
}
