import type { RoleBinding, UserRole } from "./console";

export function summarizeRoleBindings(bindings: RoleBinding[]): UserRole {
  if (bindings.some((binding) => binding.role === "Admin")) {
    return "Admin";
  }

  if (bindings.some((binding) => binding.role === "Operator")) {
    return "Operator";
  }

  if (bindings.some((binding) => binding.role === "Viewer")) {
    return "Viewer";
  }

  if (bindings.some((binding) => binding.role === "Trigger")) {
    return "Trigger";
  }

  return "Unassigned";
}
