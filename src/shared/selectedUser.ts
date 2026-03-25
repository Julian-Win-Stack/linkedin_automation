export type SelectedUser = "raihan" | "cherry" | "julian";

const SELECTED_USER_SET: Record<SelectedUser, true> = {
  raihan: true,
  cherry: true,
  julian: true,
};

export function isSelectedUser(value: unknown): value is SelectedUser {
  if (typeof value !== "string") {
    return false;
  }
  const normalized = value.trim().toLowerCase();
  return normalized in SELECTED_USER_SET;
}
