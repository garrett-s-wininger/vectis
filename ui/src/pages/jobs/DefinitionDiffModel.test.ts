import { definitionDiff } from "./DefinitionDiffModel";

function check(before: string, after: string) {
  const lines = definitionDiff(before, after);
  expect(
    lines
      .filter((line) => line.kind !== "added")
      .map((line) => line.text)
      .join("\n")
  ).toBe(before);

  expect(
    lines
      .filter((line) => line.kind !== "removed")
      .map((line) => line.text)
      .join("\n")
  ).toBe(after);

  expect(lines.filter((line) => line.before !== undefined).map((line) => line.before)).toEqual(
    Array.from({ length: before ? before.split("\n").length : 0 }, (_, i) => i + 1)
  );

  expect(lines.filter((line) => line.after !== undefined).map((line) => line.after)).toEqual(
    Array.from({ length: after ? after.split("\n").length : 0 }, (_, i) => i + 1)
  );
}

it("represents replacements as removed and added lines with shared context", () => {
  expect(definitionDiff("root\nexit 7\nend", "root\nexit 0\nend")).toEqual([
    { kind: "same", text: "root", before: 1, after: 1 },
    { kind: "removed", text: "exit 7", before: 2, after: undefined },
    { kind: "added", text: "exit 0", before: undefined, after: 2 },
    { kind: "same", text: "end", before: 3, after: 3 }
  ]);
});

it("preserves repeated lines, blanks, insertions and deletions", () => {
  for (const [before, after] of [
    ["", "new"],
    ["old", ""],
    ["a\n", "a"],
    ["a\na\nb", "a\nb\na"],
    ["x\n\ny", "\nx\ny\n"],
    ["same", "same"]
  ])
    check(before!, after!);

  let seed = 37;
  const random = () => {
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return seed;
  };

  for (let i = 0; i < 100; i++) {
    const text = () => Array.from({ length: random() % 15 }, () => ["alpha", "beta", "", "}"][random() % 4]).join("\n");
    check(text(), text());
  }
});

it("keeps large unrelated inputs exact through the bounded fallback", () => {
  check(
    Array.from({ length: 2000 }, (_, i) => `old-${i}`).join("\n"),
    Array.from({ length: 2000 }, (_, i) => `new-${i}`).join("\n")
  );
});
