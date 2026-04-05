import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  computeTenureFromExperience,
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
  });
});
