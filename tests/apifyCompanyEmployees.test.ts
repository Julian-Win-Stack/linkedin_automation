import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  computeTenureFromExperience,
  filterByPastExperienceKeywords,
  filterPoolByStage,
  mapProfileToEnrichedEmployee,
  scrapeCompanyEmployees,
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
      maxItemsPerCompany: 30,
    });

    expect(result.profileCount).toBe(1);
    expect(result.employees).toHaveLength(1);
    expect(result.apifyCache.get("linkedin.com/in/emp-1")).toBeDefined();
    expect(mockFetch).toHaveBeenCalledTimes(1);
    const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body as string);
    expect(requestBody.jobTitles).toBeDefined();
    expect(requestBody.pastJobTitles).toEqual(["SRE", "Site Reliability"]);
    expect(requestBody.companyBatchMode).toBe("all_at_once");
    expect(requestBody.maxItems).toBe(30);
  });

  it("filterByPastExperienceKeywords returns employees with matching past experience positions", () => {
    const pool: EnrichedEmployee[] = [
      makeEmployee("1", "Platform Engineer", "https://linkedin.com/in/1"),
      makeEmployee("2", "DevOps", "https://linkedin.com/in/2"),
      makeEmployee("3", "Backend Engineer", "https://linkedin.com/in/3"),
    ];
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/1", {
      openToWork: false,
      profileSkills: [],
      experience: [{ position: "Site Reliability Engineer" }, { position: "Platform Engineer" }],
    });
    cache.set("linkedin.com/in/2", {
      openToWork: false,
      profileSkills: [],
      experience: [{ position: "DevOps Engineer" }],
    });
    cache.set("linkedin.com/in/3", {
      openToWork: false,
      profileSkills: [],
      experience: [{ position: "SRE Lead" }, { position: "Backend Engineer" }],
    });

    const result = filterByPastExperienceKeywords(pool, cache, ["SRE", "Site Reliability"]);

    expect(result).toHaveLength(2);
    expect(result.map((e) => e.id)).toContain("1");
    expect(result.map((e) => e.id)).toContain("3");
  });

  it("filterByPastExperienceKeywords excludes employees with no cache entry", () => {
    const pool: EnrichedEmployee[] = [
      makeEmployee("1", "Platform Engineer", "https://linkedin.com/in/1"),
    ];
    const cache: ApifyOpenToWorkCache = new Map();

    const result = filterByPastExperienceKeywords(pool, cache, ["SRE", "Site Reliability"]);

    expect(result).toHaveLength(0);
  });

  it("filterByPastExperienceKeywords matches case-insensitively", () => {
    const pool: EnrichedEmployee[] = [
      makeEmployee("1", "Platform Engineer", "https://linkedin.com/in/1"),
    ];
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/1", {
      openToWork: false,
      profileSkills: [],
      experience: [{ position: "senior sre | Infrastructure | On-call" }],
    });

    const result = filterByPastExperienceKeywords(pool, cache, ["SRE", "Site Reliability"]);

    expect(result).toHaveLength(1);
  });
});
