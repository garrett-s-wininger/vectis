export function formatDefinitionJSON(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "string") {
    const source = value;
    try {
      value = JSON.parse(source) as unknown;
    } catch {
      return source;
    }
  }

  return JSON.stringify(sortFields(value), null, 2) ?? "";
}

function sortFields(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortFields);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
        .map(([key, child]) => [key, sortFields(child)])
    );
  }

  return value;
}
