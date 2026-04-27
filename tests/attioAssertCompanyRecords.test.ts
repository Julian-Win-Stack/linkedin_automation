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
    stage: "ChasingPOC",
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
        { api_slug: "status_5" },
        { api_slug: "stage" },
        { api_slug: "company_linkedin_url" },
        { api_slug: "outreach_date" },
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
      status_5: "ChasingPOC",
    });
    expect(body.data.values.stage).toBeUndefined();
    expect(body.data.values.apollo_account_id).toBeUndefined();
  });

  it("dedupes by normalized domain with deterministic last-row-wins", async () => {
    await syncAttioCompaniesFromOutputRows([
      makeRow({ company_domain: "acme.com", stage: "" }),
      makeRow({ company_domain: "https://acme.com/about", stage: "ChasingPOC" }),
    ]);

    expect(attioPutMock).toHaveBeenCalledTimes(1);
    const body = attioPutMock.mock.calls[0]?.[1] as { data: { values: Record<string, unknown> } };
    expect(body.data.values.status_5).toBe("ChasingPOC");
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

  it("maps Attio slugs case, underscore, and space insensitively", async () => {
    attioGetMock.mockResolvedValueOnce({
      data: [
        { api_slug: "NAME" },
        { api_slug: "Domains" },
        { api_slug: "Company Linkedin Url" },
        { api_slug: "Status 5" },
      ],
    });

    await syncAttioCompaniesFromOutputRows([makeRow()]);

    expect(attioPutMock).toHaveBeenCalledTimes(1);
    const body = attioPutMock.mock.calls[0]?.[1] as { data: { values: Record<string, unknown> } };
    expect(body.data.values).toMatchObject({
      NAME: "Acme",
      Domains: ["acme.com"],
      "Company Linkedin Url": "https://linkedin.com/company/acme",
      "Status 5": "ChasingPOC",
    });
    expect(body.data.values.Stage).toBeUndefined();
  });

  it("maps outreach_date onto the Attio outreach_date slug when the row carries it", async () => {
    await syncAttioCompaniesFromOutputRows([
      makeRow({ outreach_date: "Week of 2026-04-06" }),
    ]);

    expect(attioPutMock).toHaveBeenCalledTimes(1);
    const body = attioPutMock.mock.calls[0]?.[1] as { data: { values: Record<string, unknown> } };
    expect(body.data.values.outreach_date).toBe("Week of 2026-04-06");
  });

  it("omits outreach_date from the Attio payload when the row has no value", async () => {
    await syncAttioCompaniesFromOutputRows([makeRow()]);

    expect(attioPutMock).toHaveBeenCalledTimes(1);
    const body = attioPutMock.mock.calls[0]?.[1] as { data: { values: Record<string, unknown> } };
    expect(body.data.values.outreach_date).toBeUndefined();
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
