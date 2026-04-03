import { describe, expect, it } from "vitest";
import { rowsToCsvString, rejectedRowsToCsvString, OutputRow, RejectedOutputRow } from "../src/services/observability/csvWriter";

describe("rowsToCsvString", () => {
  it("produces CSV with correct headers", async () => {
    const rows: OutputRow[] = [];
    const csv = await rowsToCsvString(rows);

    expect(csv).toContain("Company Name");
    expect(csv).toContain("Website");
    expect(csv).toContain("Company Linkedin Url");
    expect(csv).toContain("observability_tool");
    expect(csv).toContain("Stage");
    expect(csv).toContain("Number of SREs");
    expect(csv).toContain("Number of Engineers");
    expect(csv).toContain("Notes");
  });

  it("serializes a single row correctly", async () => {
    const rows: OutputRow[] = [
      {
        company_name: "Acme Corp",
        company_domain: "acme.com",
        company_linkedin_url: "https://linkedin.com/company/acme",
        observability_tool_research: "Datadog",
        stage: "ChasingPOC",
        sre_count: 5,
        engineer_count: 100,
        notes: "Good prospect",
      },
    ];

    const csv = await rowsToCsvString(rows);
    const lines = csv.trim().split("\n");

    expect(lines).toHaveLength(2);
    expect(lines[1]).toContain("Acme Corp");
    expect(lines[1]).toContain("acme.com");
    expect(lines[1]).toContain("ChasingPOC");
  });

  it("handles empty sre_count and engineer_count", async () => {
    const rows: OutputRow[] = [
      {
        company_name: "Test",
        company_domain: "test.com",
        company_linkedin_url: "",
        observability_tool_research: "",
        stage: "NotActionableNow",
        sre_count: "",
        engineer_count: "",
        notes: "",
      },
    ];

    const csv = await rowsToCsvString(rows);
    expect(csv).toBeDefined();
    expect(csv.trim().split("\n")).toHaveLength(2);
  });

  it("handles > 1000 engineer count display value", async () => {
    const rows: OutputRow[] = [
      {
        company_name: "Big Corp",
        company_domain: "big.com",
        company_linkedin_url: "",
        observability_tool_research: "",
        stage: "NotActionableNow",
        sre_count: 0,
        engineer_count: "> 1000",
        notes: "Too many engineers",
      },
    ];

    const csv = await rowsToCsvString(rows);
    expect(csv).toContain("> 1000");
  });

  it("serializes multiple rows", async () => {
    const rows: OutputRow[] = [
      {
        company_name: "Alpha",
        company_domain: "alpha.com",
        company_linkedin_url: "",
        observability_tool_research: "Grafana",
        stage: "ChasingPOC",
        sre_count: 3,
        engineer_count: 50,
        notes: "",
      },
      {
        company_name: "Beta",
        company_domain: "beta.com",
        company_linkedin_url: "",
        observability_tool_research: "Prometheus",
        stage: "ChasingPOC",
        sre_count: 7,
        engineer_count: 200,
        notes: "",
      },
    ];

    const csv = await rowsToCsvString(rows);
    const lines = csv.trim().split("\n");

    expect(lines).toHaveLength(3);
  });
});

describe("rejectedRowsToCsvString", () => {
  it("produces CSV with correct headers for rejected rows", async () => {
    const rows: RejectedOutputRow[] = [];
    const csv = await rejectedRowsToCsvString(rows);

    expect(csv).toContain("Company Name");
    expect(csv).toContain("Website");
    expect(csv).toContain("Stage");
    expect(csv).toContain("Notes");
  });

  it("serializes a rejected row correctly", async () => {
    const rows: RejectedOutputRow[] = [
      {
        company_name: "Rejected Corp",
        company_domain: "rejected.com",
        company_linkedin_url: "",
        observability_tool_research: "New Relic",
        sre_count: 2,
        engineer_count: 10,
        status: "NotActionableNow",
        notes: "Engineer count too low",
      },
    ];

    const csv = await rejectedRowsToCsvString(rows);
    const lines = csv.trim().split("\n");

    expect(lines).toHaveLength(2);
    expect(lines[1]).toContain("Rejected Corp");
    expect(lines[1]).toContain("NotActionableNow");
  });
});
