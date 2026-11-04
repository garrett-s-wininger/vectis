export type NamespaceNode = {
  path: string;
  name: string;
  parent?: string;
  depth: number;
  selectable: boolean;
  children: boolean;
};

export function namespaceTree(paths: string[]): NamespaceNode[] {
  const allowed = new Set(paths);
  const nodes = new Map<string, NamespaceNode>();

  nodes.set("/", { path: "/", name: "Root", depth: 1, selectable: allowed.has("/"), children: false });
  for (const path of paths) {
    if (!path.startsWith("/")) continue;

    const segments = path.split("/").filter(Boolean);
    let parent = "/";
    for (let i = 0; i < segments.length; i++) {
      const current = `/${segments.slice(0, i + 1).join("/")}`;
      nodes.set(current, {
        path: current,
        name: segments[i],
        parent,
        depth: i + 2,
        selectable: allowed.has(current),
        children: false
      });

      parent = current;
    }
  }

  const children = new Map<string, NamespaceNode[]>();
  for (const node of nodes.values())
    if (node.parent) {
      const siblings = children.get(node.parent) ?? [];
      siblings.push(node);
      children.set(node.parent, siblings);
      nodes.get(node.parent)!.children = true;
    }

  const result: NamespaceNode[] = [];
  function visit(path: string) {
    result.push(nodes.get(path)!);
    for (const child of (children.get(path) ?? []).sort((a, b) =>
      a.name.localeCompare(b.name, undefined, { numeric: true })
    ))
      visit(child.path);
  }

  visit("/");
  return result;
}

export function namespaceAncestors(path: string) {
  const parts = path.split("/").filter(Boolean);
  return ["/", ...parts.slice(0, -1).map((_, i) => `/${parts.slice(0, i + 1).join("/")}`)];
}
