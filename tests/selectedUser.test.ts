import { describe, expect, it } from "vitest";
import { isSelectedUser } from "../src/shared/selectedUser";

describe("isSelectedUser", () => {
  it("accepts 'raihan'", () => {
    expect(isSelectedUser("raihan")).toBe(true);
  });

  it("accepts 'cherry'", () => {
    expect(isSelectedUser("cherry")).toBe(true);
  });

  it("accepts 'julian'", () => {
    expect(isSelectedUser("julian")).toBe(true);
  });

  it("accepts uppercase by normalizing to lowercase", () => {
    expect(isSelectedUser("RAIHAN")).toBe(true);
    expect(isSelectedUser("Cherry")).toBe(true);
    expect(isSelectedUser("JULIAN")).toBe(true);
  });

  it("accepts with leading/trailing whitespace", () => {
    expect(isSelectedUser("  julian  ")).toBe(true);
  });

  it("rejects unknown user names", () => {
    expect(isSelectedUser("unknown")).toBe(false);
    expect(isSelectedUser("admin")).toBe(false);
  });

  it("rejects empty string", () => {
    expect(isSelectedUser("")).toBe(false);
  });

  it("rejects non-string types", () => {
    expect(isSelectedUser(123)).toBe(false);
    expect(isSelectedUser(null)).toBe(false);
    expect(isSelectedUser(undefined)).toBe(false);
    expect(isSelectedUser({})).toBe(false);
  });
});
