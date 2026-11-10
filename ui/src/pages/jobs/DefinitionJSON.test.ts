import { formatDefinitionJSON } from "./DefinitionJSON";

it("normalizes nested object order between parsed effective and raw source definitions", () => {
  const source = '{"root":{"with":{"z":1,"a":2},"uses":"builtins/script","id":"root"}}';
  const effective = { root: { id: "root", uses: "builtins/script", with: { a: 2, z: 1 } } };
  expect(formatDefinitionJSON(source)).toBe(formatDefinitionJSON(effective));
});

it("preserves execution order in arrays and leaves the original definition untouched", () => {
  const definition = { children: [{ id: "second" }, { id: "first" }] };
  expect(JSON.parse(formatDefinitionJSON(definition))).toEqual(definition);
  expect(definition.children.map((child) => child.id)).toEqual(["second", "first"]);
});

it("keeps malformed upstream JSON available for repair", () => {
  expect(formatDefinitionJSON('{"root":')).toBe('{"root":');
  expect(formatDefinitionJSON(undefined)).toBe("");
});
