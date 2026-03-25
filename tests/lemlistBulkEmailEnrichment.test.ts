import { beforeEach, describe, expect, it, vi } from "vitest";
import { enrichMissingEmailsWithLemlist } from "../src/services/lemlistBulkEmailEnrichment";

const bulkEnrichDataMock = vi.fn();
const getEnrichmentResultMock = vi.fn();

vi.mock("../src/services/lemlistClient", () => ({
  bulkEnrichData: (...args: unknown[]) => bulkEnrichDataMock(...args),
  getEnrichmentResult: (...args: unknown[]) => getEnrichmentResultMock(...args),
}));

describe("enrichMissingEmailsWithLemlist", () => {
  beforeEach(() => {
    bulkEnrichDataMock.mockReset();
    getEnrichmentResultMock.mockReset();
  });

  it("sends find_email requests and hydrates employee emails", async () => {
    const employee = {
      id: "person-1",
      startDate: "2022-01-01",
      endDate: null,
      name: "John Doe",
      email: null,
      linkedinUrl: null,
      currentTitle: "Platform Engineer",
      tenure: 12,
    };
    bulkEnrichDataMock.mockResolvedValueOnce([
      {
        id: "enr_1",
        metadata: { metadataId: "person-1:0" },
      },
    ]);
    getEnrichmentResultMock.mockResolvedValueOnce({
      enrichmentStatus: "done",
      data: {
        email: {
          email: "john.doe@example.com",
          notFound: false,
        },
      },
    });

    const summary = await enrichMissingEmailsWithLemlist([
      { employee, companyName: "Acme", companyDomain: "acme.com" },
    ]);

    expect(bulkEnrichDataMock).toHaveBeenCalledWith([
      {
        input: {
          firstName: "John",
          lastName: "Doe",
          companyName: "Acme",
          companyDomain: "acme.com",
        },
        enrichmentRequests: ["find_email"],
        metadata: { metadataId: "person-1:0" },
      },
    ]);
    expect(employee.email).toBe("john.doe@example.com");
    expect(summary).toEqual({
      attempted: 1,
      accepted: 1,
      recovered: 1,
      notFound: 0,
    });
  });

  it("skips invalid candidates and reports not found results", async () => {
    const validEmployee = {
      id: "person-2",
      startDate: "2022-01-01",
      endDate: null,
      name: "Jane Doe",
      email: null,
      linkedinUrl: null,
      currentTitle: "Staff Engineer",
      tenure: 14,
    };
    const invalidEmployee = {
      id: "person-3",
      startDate: "2022-01-01",
      endDate: null,
      name: "SingleName",
      email: null,
      linkedinUrl: null,
      currentTitle: "Engineer",
      tenure: 10,
    };

    bulkEnrichDataMock.mockResolvedValueOnce([
      {
        id: "enr_2",
        metadata: { metadataId: "person-2:0" },
      },
    ]);
    getEnrichmentResultMock.mockResolvedValueOnce({
      enrichmentStatus: "done",
      data: {
        find_email: {
          status: "not_found",
        },
      },
    });

    const summary = await enrichMissingEmailsWithLemlist([
      { employee: validEmployee, companyName: "Acme", companyDomain: "acme.com" },
      { employee: invalidEmployee, companyName: "Acme", companyDomain: "acme.com" },
    ]);

    expect(summary).toEqual({
      attempted: 1,
      accepted: 1,
      recovered: 0,
      notFound: 1,
    });
    expect(validEmployee.email).toBeNull();
    expect(invalidEmployee.email).toBeNull();
  });
});
