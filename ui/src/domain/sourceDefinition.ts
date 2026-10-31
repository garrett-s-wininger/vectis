export function sourceDefinition(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Definition must be a JSON object.");
  }

  const definition = { ...(value as Record<string, unknown>) };
  delete definition.id;
  return definition;
}
