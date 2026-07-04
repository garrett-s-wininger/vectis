import type { UserRole } from "../../domain/console";

export function roleTone(role: UserRole) {
  if (role === "Admin") {
    return "active";
  }

  if (role === "Operator") {
    return "enabled";
  }

  if (role === "Unassigned") {
    return "disabled";
  }

  return "paused";
}
