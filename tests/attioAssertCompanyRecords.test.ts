import { beforeEach, describe, expect, it, vi } from "vitest";
import { syncAttioCompaniesFromOutputRows } from "../src/services/attioAssertCompanyRecords";
import { OutputRow } from "../src/services/observability/csvWriter";

const attioGetMock = vi.fn();
const attioPutMock = vi.fn();

vi.mock("../src/services/attioClient", () => ({
  attioGet: (...args: unknown[]) => attioGetMock(...args),
  attioPut: (...args: unknown[]) => attioPutMock(...args),
}));

function makeRow(overrides: Partial<OutputRow> = {}): OutputRow {
  return {
    company_name: "Acme",
    company_domain: "https://www.acme.com/path",
    company_linkedin_url: "https://linkedin.com/company/acme",
    apollo_account_id: "apollo_1",
    observability_tool_research: "Datadog",
    stage: "ChasingPOC",
    sre_count: 3,
    notes: "important",
    ...overrides,
  };
}

describe("syncAttioCompaniesFromOutputRows", () => {
  beforeEach(() => {
    attioGetMock.mockReset();
    attioPutMock.mockReset();
    attioGetMock.mockResolvedValue({
      data: [
        { api_slug: "name" },
        { api_slug: "domains" },
        { api_slug: "observability_tool_research" },
        { api_slug: "stage" },
        { api_slug: "sre_count" },
        { api_slug: "notes" },
        { api_slug: "company_linkedin_url" },
      ],
    });
    attioPutMock.mockResolvedValue({ data: {} });
  });

  it("asserts companies by domains and excludes Apollo Account Id", async () => {
    await syncAttioCompaniesFromOutputRows([makeRow()]);

    expect(attioPutMock).toHaveBeenCalledTimes(1);
    expect(attioPutMock.mock.calls[0]?.[0]).toContain(
      "/v2/objects/companies/records?matching_attribute=domains"
    );

    const body = attioPutMock.mock.calls[0]?.[1] as { data: { values: Record<string, unknown> } };
    expect(body.data.values).toMatchObject({
      name: "Acme",
      domains: ["acme.com"],
      company_linkedin_url: "https://linkedin.com/company/acme",
      observability_tool_research: "Datadog",
      stage: "ChasingPOC",
      sre_count: 3,
      notes: "important",
    });
    expect(body.data.values.apollo_account_id).toBeUndefined();
  });

  it("dedupes by normalized domain with deterministic last-row-wins", async () => {
    await syncAttioCompaniesFromOutputRows([
      makeRow({ company_domain: "acme.com", notes: "old" }),
      makeRow({ company_domain: "https://acme.com/about", notes: "new" }),
    ]);

    expect(attioPutMock).toHaveBeenCalledTimes(1);
    const body = attioPutMock.mock.calls[0]?.[1] as { data: { values: Record<string, unknown> } };
    expect(body.data.values.notes).toBe("new");
  });

  it("skips rows with missing domain", async () => {
    const result = await syncAttioCompaniesFromOutputRows([
      makeRow({ company_domain: "" }),
      makeRow({ company_domain: "   " }),
    ]);

    expect(attioPutMock).not.toHaveBeenCalled();
    expect(result.skippedMissingDomainCount).toBe(2);
  });

  it("logs colored errors for unmapped slugs and still syncs mapped values", async () => {
    attioGetMock.mockResolvedValueOnce({
      data: [{ api_slug: "domains" }, { api_slug: "name" }],
    });
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    await syncAttioCompaniesFromOutputRows([makeRow()]);

    expect(consoleErrorSpy).toHaveBeenCalled();
    expect(String(consoleErrorSpy.mock.calls[0]?.[0] ?? "")).toContain("\x1b[31m");
    const body = attioPutMock.mock.calls[0]?.[1] as { data: { values: Record<string, unknown> } };
    expect(body.data.values).toMatchObject({
      name: "Acme",
      domains: ["acme.com"],
    });

    consoleErrorSpy.mockRestore();
  });

  it("captures per-domain warnings when assert fails for some rows", async () => {
    attioPutMock
      .mockResolvedValueOnce({ data: {} })
      .mockRejectedValueOnce(new Error("rate-limited"));
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const result = await syncAttioCompaniesFromOutputRows([
      makeRow({ company_name: "One Co", company_domain: "one.com" }),
      makeRow({ company_name: "Two Co", company_domain: "two.com" }),
    ]);

    expect(result.assertedCount).toBe(1);
    expect(result.failedCount).toBe(1);
    expect(result.warnings).toContain("Uploading Two Co to Attio failed. Please contact Julian");
    expect(consoleErrorSpy).toHaveBeenCalled();
    expect(String(consoleErrorSpy.mock.calls[0]?.[0] ?? "")).toContain("[Attio][Assert][ERROR]");
    expect(String(consoleErrorSpy.mock.calls[0]?.[0] ?? "")).toContain("\x1b[31m");
    consoleErrorSpy.mockRestore();
  });
});
