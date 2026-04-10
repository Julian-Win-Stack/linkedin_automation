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
      experience: [{ position: "Past SRE role", endDate: { text: "Jan 2023" } }],
    });

    const result = filterPoolByStage(pool, cache, {
      currentTitles: ["SRE"],
      pastTitles: ["SRE"],
      notTitles: ["intern"],
    });

    expect(result).toHaveLength(2);
  });

  function makeProfile(id: string) {
    return {
      id,
      firstName: "A",
      lastName: "B",
      linkedinUrl: `https://linkedin.com/in/${id}`,
      openToWork: false,
      skills: [{ name: "Kubernetes" }],
      experience: [
        {
          companyName: "Acme",
          position: "SRE",
          startDate: { month: "Jan", year: 2022 },
          endDate: { text: "Present" },
        },
      ],
    };
  }

  it("makes only the SRE call when it returns 10 or more results", async () => {
    const profiles = Array.from({ length: 10 }, (_, i) => makeProfile(`emp-${i + 1}`));
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => profiles,
    });
    vi.stubGlobal("fetch", mockFetch);

    const result = await scrapeCompanyEmployees({
      companyName: "Acme",
      companyDomain: "acme.com",
      maxItemsPerCompany: 30,
    });

    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(result.employees).toHaveLength(10);
    const body = JSON.parse(mockFetch.mock.calls[0][1].body as string);
    expect(body.jobTitles).toEqual(["SRE", "Site Reliability", "platform engineer", "devops"]);
    expect(body.pastJobTitles).toEqual(["SRE", "Site Reliability"]);
    expect(body.seniorityLevelIds).toBeUndefined();
    expect(body.companyBatchMode).toBe("all_at_once");
    expect(body.maxItems).toBe(200);
    expect(body.recentlyChangedJobs).toBe(false);
  });

  it("makes the second call when SRE call returns fewer than 10 results", async () => {
    const call1Profiles = Array.from({ length: 5 }, (_, i) => makeProfile(`sre-${i + 1}`));
    const call2Profiles = Array.from({ length: 3 }, (_, i) => makeProfile(`devops-${i + 1}`));
    const mockFetch = vi.fn()
      .mockResolvedValueOnce({ ok: true, json: async () => call1Profiles })
      .mockResolvedValueOnce({ ok: true, json: async () => call2Profiles });
    vi.stubGlobal("fetch", mockFetch);

    const result = await scrapeCompanyEmployees({
      companyName: "Acme",
      companyDomain: "acme.com",
      maxItemsPerCompany: 30,
    });

    expect(mockFetch).toHaveBeenCalledTimes(2);
    expect(result.employees).toHaveLength(8);
    const body2 = JSON.parse(mockFetch.mock.calls[1][1].body as string);
    expect(body2.jobTitles).toEqual(["Infrastructure Engineer", "Staff engineer", "Principal engineer", "Software engineering lead"]);
    expect(body2.excludeCurrentJobTitles).toEqual(["SRE", "Site Reliability", "Platform engineer", "Devops"]);
    expect(body2.yearsAtCurrentCompanyIds).toEqual(["2", "3", "4", "5"]);
    expect(body2.recentlyChangedJobs).toBe(false);
  });

  it("does not make a second call when call 1 returns exactly 10 results (boundary)", async () => {
    const profiles = Array.from({ length: 10 }, (_, i) => makeProfile(`emp-${i + 1}`));
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => profiles,
    });
    vi.stubGlobal("fetch", mockFetch);

    await scrapeCompanyEmployees({ companyName: "Acme", companyDomain: "acme.com" });

    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it("deduplicates profiles that appear in both calls", async () => {
    const sharedProfile = makeProfile("shared-1");
    const uniqueProfile = makeProfile("unique-1");
    const mockFetch = vi.fn()
      .mockResolvedValueOnce({ ok: true, json: async () => [sharedProfile] })
      .mockResolvedValueOnce({ ok: true, json: async () => [sharedProfile, uniqueProfile] });
    vi.stubGlobal("fetch", mockFetch);

    const result = await scrapeCompanyEmployees({ companyName: "Acme", companyDomain: "acme.com" });

    expect(result.employees).toHaveLength(2);
    expect(result.employees.map((e) => e.id)).toContain("shared-1");
    expect(result.employees.map((e) => e.id)).toContain("unique-1");
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
      experience: [
        { position: "Site Reliability Engineer", endDate: { text: "Jan 2022" } },
        { position: "Platform Engineer", endDate: { text: "Present" } },
      ],
    });
    cache.set("linkedin.com/in/2", {
      openToWork: false,
      profileSkills: [],
      experience: [{ position: "DevOps Engineer", endDate: { text: "Mar 2023" } }],
    });
    cache.set("linkedin.com/in/3", {
      openToWork: false,
      profileSkills: [],
      experience: [
        { position: "SRE Lead", endDate: { text: "Dec 2021" } },
        { position: "Backend Engineer", endDate: { text: "Present" } },
      ],
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
      experience: [{ position: "senior sre | Infrastructure | On-call", endDate: { text: "Jun 2022" } }],
    });

    const result = filterByPastExperienceKeywords(pool, cache, ["SRE", "Site Reliability"]);

    expect(result).toHaveLength(1);
  });
});
