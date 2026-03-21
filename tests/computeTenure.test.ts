import { describe, expect, it } from "vitest";
import { computeTenure } from "../src/services/computeTenure";

describe("computeTenure", () => {
  it("returns human-readable tenure for current role at target company", () => {
    const now = new Date("2026-03-20T00:00:00.000Z");
    const history = [
      {
        organization_name: "Acme",
        start_date: "2025-01-15",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", now);
    expect(result).toBe("1 year 2 months");
  });

  it("returns null when employment history is missing", () => {
    const result = computeTenure(undefined, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBeNull();
  });

  it("returns null when start date is missing", () => {
    const history = [
      {
        organization_name: "Acme",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBeNull();
  });
});
