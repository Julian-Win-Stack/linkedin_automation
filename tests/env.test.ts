import { afterEach, describe, expect, it, vi } from "vitest";
import { getRequiredEnv, getEnvBoolean } from "../src/config/env";

describe("getRequiredEnv", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("returns trimmed value when env var exists", () => {
    vi.stubEnv("TEST_KEY", "  hello  ");
    expect(getRequiredEnv("TEST_KEY")).toBe("hello");
  });

  it("throws when env var is missing", () => {
    delete process.env.TEST_MISSING_KEY;
    expect(() => getRequiredEnv("TEST_MISSING_KEY")).toThrow("Missing TEST_MISSING_KEY");
  });

  it("throws when env var is empty string", () => {
    vi.stubEnv("TEST_EMPTY", "");
    expect(() => getRequiredEnv("TEST_EMPTY")).toThrow("Missing TEST_EMPTY");
  });

  it("throws when env var is whitespace only", () => {
    vi.stubEnv("TEST_SPACES", "   ");
    expect(() => getRequiredEnv("TEST_SPACES")).toThrow("Missing TEST_SPACES");
  });
});

describe("getEnvBoolean", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("returns true when env var is 'true'", () => {
    vi.stubEnv("TEST_BOOL", "true");
    expect(getEnvBoolean("TEST_BOOL", false)).toBe(true);
  });

  it("returns false when env var is 'false'", () => {
    vi.stubEnv("TEST_BOOL", "false");
    expect(getEnvBoolean("TEST_BOOL", true)).toBe(false);
  });

  it("returns default when env var is not set", () => {
    delete process.env.TEST_BOOL_MISSING;
    expect(getEnvBoolean("TEST_BOOL_MISSING", true)).toBe(true);
    expect(getEnvBoolean("TEST_BOOL_MISSING", false)).toBe(false);
  });

  it("returns default when env var is empty", () => {
    vi.stubEnv("TEST_BOOL_EMPTY", "");
    expect(getEnvBoolean("TEST_BOOL_EMPTY", true)).toBe(true);
  });

  it("returns default when env var is invalid value", () => {
    vi.stubEnv("TEST_BOOL_INVALID", "yes");
    expect(getEnvBoolean("TEST_BOOL_INVALID", false)).toBe(false);
  });

  it("handles case-insensitive 'TRUE'", () => {
    vi.stubEnv("TEST_BOOL_UPPER", "TRUE");
    expect(getEnvBoolean("TEST_BOOL_UPPER", false)).toBe(true);
  });

  it("handles whitespace around 'true'", () => {
    vi.stubEnv("TEST_BOOL_SPACE", "  true  ");
    expect(getEnvBoolean("TEST_BOOL_SPACE", false)).toBe(true);
  });
});
