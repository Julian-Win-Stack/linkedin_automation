import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  computeTenureFromExperience,
  filterPoolByStage,
  mapProfileToEnrichedEmployee,
  scrapeCompanyEmployees,
  scrapePastSreEmployees,
} from "../src/services/apifyCompanyEmployees";
import { ApifyOpenToWorkCache, EnrichedEmployee } from "../src/types/prospect";

const getRequiredEnvMock = vi.fn();

vi.mock("../src/config/env", () => ({
  getRequiredEnv: (...args: unknown[]) => getRequiredEnvMock(...args),
}));

function makeEmployee(id: string, currentTitle: string, linkedinUrl: string): EnrichedEmployee {
  return {
    id,
    startDate: "2022-01-01",
    endDate: null,
    name: id,
    email: null,
    linkedinUrl,
    currentTitle,
    headline: "",
    tenure: 10,
  };
}

describe("apifyCompanyEmployees", () => {
  beforeEach(() => {
    getRequiredEnvMock.mockReset();
    getRequiredEnvMock.mockReturnValue("test-apify-key");
    vi.stubGlobal("fetch", vi.fn());
  });

  it("computes tenure from matching company experience", () => {
    const tenure = computeTenureFromExperience(
      [
        {
          companyName: "Acme",
          position: "SRE",
          startDate: { month: "Jan", year: 2022 },
          endDate: { text: "Present", month: "Apr", year: 2024 },
        },
      ],
      "Acme",
      undefined,
      new Date(Date.UTC(2024, 3, 1))
    );

    expect(tenure).toBeGreaterThanOrEqual(26);
  });

  it("maps profile to enriched employee", () => {
    const employee = mapProfileToEnrichedEmployee(
      {
        id: "abc",
        firstName: "Alice",
        lastName: "Doe",
        linkedinUrl: "https://linkedin.com/in/alice",
        experience: [
          {
            companyName: "Acme",
            position: "Site Reliability Engineer",
            startDate: { month: "Jan", year: 2023 },
            endDate: { text: "Present", month: "Apr", year: 2024 },
          },
        ],
      },
      { companyName: "Acme", companyDomain: "acme.com" }
    );

    expect(employee).toMatchObject({
      id: "abc",
      name: "Alice Doe",
      currentTitle: "Site Reliability Engineer",
      linkedinUrl: "https://linkedin.com/in/alice",
    });
  });

  it("extracts headline from profile into enriched employee", () => {
    const employee = mapProfileToEnrichedEmployee(
      {
        id: "hl-1",
        firstName: "Bob",
        lastName: "Smith",
        headline: "Site Reliability Engineer | Platform | Kubernetes",
        linkedinUrl: "https://linkedin.com/in/bob",
        experience: [
          {
            companyName: "Acme",
            position: "Staff Engineer",
            startDate: { month: "Jan", year: 2023 },
            endDate: { text: "Present" },
          },
        ],
      },
      { companyName: "Acme", companyDomain: "acme.com" }
    );

    expect(employee?.headline).toBe("Site Reliability Engineer | Platform | Kubernetes");
    expect(employee?.currentTitle).toBe("Staff Engineer");
  });

  it("defaults headline to empty string when profile has no headline", () => {
    const employee = mapProfileToEnrichedEmployee(
      {
        id: "hl-2",
        firstName: "Carol",
        lastName: "Jones",
        linkedinUrl: "https://linkedin.com/in/carol",
        experience: [
          {
            companyName: "Acme",
            position: "Backend Engineer",
            startDate: { month: "Mar", year: 2022 },
            endDate: { text: "Present" },
          },
        ],
      },
      { companyName: "Acme", companyDomain: "acme.com" }
    );

    expect(employee?.headline).toBe("");
  });

  it("filters pool by current and past title conditions", () => {
    const pool = [
      makeEmployee("1", "SRE", "https://linkedin.com/in/1"),
      makeEmployee("2", "Platform Engineer", "https://linkedin.com/in/2"),
    ];
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/2", {
      openToWork: false,
      profileSkills: [],
      experience: [{ position: "Past SRE role" }],
    });

    const result = filterPoolByStage(pool, cache, {
      currentTitles: ["SRE"],
      pastTitles: ["SRE"],
      notTitles: ["intern"],
    });

    expect(result).toHaveLength(2);
  });

  it("scrapes company employees and builds cache", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => [
        {
          id: "emp-1",
          firstName: "A",
          lastName: "B",
          linkedinUrl: "https://linkedin.com/in/emp-1",
          openToWork: false,
          skills: [{ name: "Kubernetes" }],
          experience: [
            {
              companyName: "Acme",
              position: "SRE",
              startDate: { month: "Jan", year: 2022 },
              endDate: { text: "Present", month: "Apr", year: 2024 },
            },
          ],
        },
      ],
    });
    vi.stubGlobal("fetch", mockFetch);

    const result = await scrapeCompanyEmployees({
      companyName: "Acme",
      companyDomain: "acme.com",
      maxItemsPerCompany: 100,
    });

    expect(result.profileCount).toBe(1);
    expect(result.employees).toHaveLength(1);
    expect(result.apifyCache.get("linkedin.com/in/emp-1")).toBeDefined();
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body as string);
    expect(requestBody.jobTitles).toBeDefined();
    expect(requestBody.pastJobTitles).toBeUndefined();
    expect(requestBody.companyBatchMode).toBe("all_at_once");
    expect(requestBody.maxItems).toBe(100);
  });

  it("scrapes past SRE employees with dedicated payload", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => [
        {
          id: "past-1",
          firstName: "Past",
          lastName: "Sre",
          linkedinUrl: "https://linkedin.com/in/past-1",
          openToWork: false,
          experience: [
            {
              companyName: "Acme",
              position: "Platform Engineer",
              startDate: { month: "Jan", year: 2022 },
              endDate: { text: "Present", month: "Apr", year: 2024 },
            },
          ],
        },
      ],
    });
    vi.stubGlobal("fetch", mockFetch);

    const result = await scrapePastSreEmployees({
      companyName: "Acme",
      companyDomain: "acme.com",
      companyLinkedinUrl: "http://www.linkedin.com/company/acme",
      maxItemsPerCompany: 100,
    });

    expect(result.profileCount).toBe(1);
    expect(result.employees).toHaveLength(1);
    expect(result.apifyCache.get("linkedin.com/in/past-1")).toBeDefined();
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body as string);
    expect(requestBody.jobTitles).toBeUndefined();
    expect(requestBody.excludeCurrentJobTitles).toEqual([
      "SRE",
      "Site Reliability Engineer",
      "on-call",
      "incident",
    ]);
    expect(requestBody.pastJobTitles).toEqual(["SRE", "Site Reliability Engineer", "incident", "on-call"]);
    expect(requestBody.yearsAtCurrentCompanyIds).toEqual(["2", "3", "4", "5"]);
    expect(requestBody.recentlyChangedJobs).toBe(false);
    expect(requestBody.companyBatchMode).toBeUndefined();
    expect(requestBody.maxItems).toBeUndefined();
    expect(requestBody.companies).toEqual(["https://www.linkedin.com/company/acme"]);
    expect(requestBody.excludeFunctionIds?.slice(-3)).toEqual(["25", "24", "26"]);
  });
});
