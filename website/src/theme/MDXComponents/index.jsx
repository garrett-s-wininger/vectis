import MDXComponents from "@theme-original/MDXComponents";
import clsx from "clsx";

function tableCell(Element) {
  return function Cell({ style, className, ...props }) {
    const align = style?.textAlign;
    return (
      <Element
        {...props}
        className={clsx(className, ["left", "right", "center"].includes(align) && `vectis-table-align-${align}`)}
      />
    );
  };
}

export default { ...MDXComponents, th: tableCell("th"), td: tableCell("td") };
