export type DiffLine = { kind: "same" | "added" | "removed"; text: string; before?: number; after?: number };

export function definitionDiff(before: string, after: string): DiffLine[] {
  const left = before ? before.split("\n") : [];
  const right = after ? after.split("\n") : [];
  let prefix = 0;
  while (prefix < left.length && prefix < right.length && left[prefix] === right[prefix]) prefix++;

  let suffix = 0;
  while (
    suffix < left.length - prefix &&
    suffix < right.length - prefix &&
    left[left.length - 1 - suffix] === right[right.length - 1 - suffix]
  )
    suffix++;

  const a = left.slice(prefix, left.length - suffix);
  const b = right.slice(prefix, right.length - suffix);
  const changes: { kind: DiffLine["kind"]; text: string }[] = [];

  if ((a.length + 1) * (b.length + 1) > 250_000) {
    for (const text of a) changes.push({ kind: "removed", text });
    for (const text of b) changes.push({ kind: "added", text });
  } else {
    const width = b.length + 1;
    const lengths = new Uint32Array((a.length + 1) * width);
    for (let i = a.length - 1; i >= 0; i--)
      for (let j = b.length - 1; j >= 0; j--)
        lengths[i * width + j] =
          a[i] === b[j]
            ? 1 + lengths[(i + 1) * width + j + 1]!
            : Math.max(lengths[(i + 1) * width + j]!, lengths[i * width + j + 1]!);

    let i = 0,
      j = 0;

    while (i < a.length || j < b.length) {
      if (i < a.length && j < b.length && a[i] === b[j]) {
        changes.push({ kind: "same", text: a[i++]! });
        j++;
      } else if (i < a.length && (j === b.length || lengths[(i + 1) * width + j]! >= lengths[i * width + j + 1]!))
        changes.push({ kind: "removed", text: a[i++]! });
      else changes.push({ kind: "added", text: b[j++]! });
    }
  }

  const lines = [
    ...left.slice(0, prefix).map((text) => ({ kind: "same" as const, text })),
    ...changes,
    ...left.slice(left.length - suffix).map((text) => ({ kind: "same" as const, text }))
  ];

  let oldLine = 1,
    newLine = 1;

  return lines.map((line) => ({
    ...line,
    before: line.kind === "added" ? undefined : oldLine++,
    after: line.kind === "removed" ? undefined : newLine++
  }));
}
