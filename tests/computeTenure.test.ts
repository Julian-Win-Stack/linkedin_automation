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

  it("returns null when company not found in history", () => {
    const history = [
      {
        organization_name: "Other Corp",
        start_date: "2024-01-01",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBeNull();
  });

  it("returns null for empty history array", () => {
    const result = computeTenure([], "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBeNull();
  });

  it("matches company name case-insensitively", () => {
    const history = [
      {
        organization_name: "ACME",
        start_date: "2026-01-01",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBe("0 years 2 months");
  });

  it("matches using company_name field when organization_name is absent", () => {
    const history = [
      {
        company_name: "Acme",
        start_date: "2025-06-01",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBe("0 years 9 months");
  });

  it("picks the current role when multiple entries exist for the same company", () => {
    const history = [
      {
        organization_name: "Acme",
        start_date: "2020-01-01",
        end_date: "2022-01-01",
        current: false,
      },
      {
        organization_name: "Acme",
        start_date: "2023-01-01",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBe("3 years 2 months");
  });

  it("returns null for invalid start_date string", () => {
    const history = [
      {
        organization_name: "Acme",
        start_date: "not-a-date",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBeNull();
  });

  it("returns 0 years 0 months when started in same month", () => {
    const history = [
      {
        organization_name: "Acme",
        start_date: "2026-03-01",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBe("0 years 0 months");
  });

  it("handles exactly 1 month correctly", () => {
    const history = [
      {
        organization_name: "Acme",
        start_date: "2026-02-01",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBe("0 years 1 month");
  });

  it("handles exactly 1 year correctly", () => {
    const history = [
      {
        organization_name: "Acme",
        start_date: "2025-03-01",
        end_date: null,
        current: true,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBe("1 year 0 months");
  });

  it("treats role with end_date null as current", () => {
    const history = [
      {
        organization_name: "Acme",
        start_date: "2025-01-01",
        end_date: null,
        current: false,
      },
    ];

    const result = computeTenure(history, "Acme", new Date("2026-03-20T00:00:00.000Z"));
    expect(result).toBe("1 year 2 months");
  });
});
