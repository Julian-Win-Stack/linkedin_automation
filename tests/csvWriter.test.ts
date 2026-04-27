import { describe, expect, it } from "vitest";
import { rowsToCsvString, OutputRow } from "../src/services/observability/csvWriter";

describe("rowsToCsvString", () => {
  it("produces CSV with correct headers", async () => {
    const rows: OutputRow[] = [];
    const csv = await rowsToCsvString(rows);

    expect(csv).toContain("Company Name");
    expect(csv).toContain("Website");
    expect(csv).toContain("Company Linkedin Url");
    expect(csv).toContain("Apollo Account Id");
    expect(csv).toContain("Stage");
    expect(csv).toContain("Outreach Date");
    expect(csv).not.toContain("observability_tool");
    expect(csv).not.toContain("Number of SREs");
    expect(csv).not.toContain("Notes");
  });

  it("serializes a single row correctly", async () => {
    const rows: OutputRow[] = [
      {
        company_name: "Acme Corp",
        company_domain: "acme.com",
        company_linkedin_url: "https://linkedin.com/company/acme",
        apollo_account_id: "acc_123",
        stage: "ChasingPOC",
        outreach_date: "Week of 2026-04-28",
      },
    ];

    const csv = await rowsToCsvString(rows);
    const lines = csv.trim().split("\n");

    expect(lines).toHaveLength(2);
    expect(lines[1]).toContain("Acme Corp");
    expect(lines[1]).toContain("acme.com");
    expect(lines[1]).toContain("acc_123");
    expect(lines[1]).toContain("ChasingPOC");
  });

  it("handles empty stage", async () => {
    const rows: OutputRow[] = [
      {
        company_name: "Test",
        company_domain: "test.com",
        company_linkedin_url: "",
        stage: "",
      },
    ];

    const csv = await rowsToCsvString(rows);
    expect(csv).toBeDefined();
    expect(csv.trim().split("\n")).toHaveLength(2);
  });

  it("serializes multiple rows", async () => {
    const rows: OutputRow[] = [
      {
        company_name: "Alpha",
        company_domain: "alpha.com",
        company_linkedin_url: "",
        stage: "ChasingPOC",
      },
      {
        company_name: "Beta",
        company_domain: "beta.com",
        company_linkedin_url: "",
        stage: "ChasingPOC",
      },
    ];

    const csv = await rowsToCsvString(rows);
    const lines = csv.trim().split("\n");

    expect(lines).toHaveLength(3);
  });
});
