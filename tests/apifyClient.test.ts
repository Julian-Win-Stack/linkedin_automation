import { beforeEach, describe, expect, it, vi } from "vitest";
import { EnrichedEmployee, ApifyOpenToWorkCache, ApifyExperienceEntry } from "../src/types/prospect";

const getRequiredEnvMock = vi.fn();

vi.mock("../src/config/env", () => ({
  getRequiredEnv: (...args: unknown[]) => getRequiredEnvMock(...args),
}));

import {
  splitByTenure,
  filterFrontendEngineers,
  scrapeAndFilterOpenToWork,
} from "../src/services/apifyClient";

function makeEmployee(
  overrides: Partial<EnrichedEmployee> & { name: string }
): EnrichedEmployee {
  return {
    id: overrides.id ?? overrides.name,
    startDate: overrides.startDate ?? "2022-01-01",
    endDate: overrides.endDate ?? null,
    name: overrides.name,
    email: overrides.email ?? null,
    linkedinUrl: overrides.linkedinUrl ?? null,
    currentTitle: overrides.currentTitle ?? "SRE",
    tenure: overrides.tenure ?? null,
  };
}

describe("splitByTenure", () => {
  it("keeps employees with tenure above minimum", () => {
    const employees = [
      makeEmployee({ name: "Alice", tenure: 12 }),
      makeEmployee({ name: "Bob", tenure: 6 }),
    ];

    const result = splitByTenure(employees, 6);

    expect(result.eligible).toHaveLength(2);
    expect(result.droppedByTenure).toHaveLength(0);
  });

  it("drops employees with tenure below minimum", () => {
    const employees = [
      makeEmployee({ name: "Alice", tenure: 5 }),
      makeEmployee({ name: "Bob", tenure: 11 }),
    ];

    const result = splitByTenure(employees, 6);

    expect(result.eligible).toHaveLength(1);
    expect(result.eligible[0].name).toBe("Bob");
    expect(result.droppedByTenure).toHaveLength(1);
    expect(result.droppedByTenure[0].name).toBe("Alice");
  });

  it("keeps employees with null tenure (unknown is eligible)", () => {
    const employees = [
      makeEmployee({ name: "Alice", tenure: null }),
      makeEmployee({ name: "Bob", tenure: 3 }),
    ];

    const result = splitByTenure(employees, 6);

    expect(result.eligible).toHaveLength(1);
    expect(result.eligible[0].name).toBe("Alice");
    expect(result.droppedByTenure).toHaveLength(1);
  });

  it("returns all eligible when empty input", () => {
    const result = splitByTenure([], 6);

    expect(result.eligible).toHaveLength(0);
    expect(result.droppedByTenure).toHaveLength(0);
  });

  it("uses exact boundary: tenure equal to minimum is eligible", () => {
    const employees = [makeEmployee({ name: "Alice", tenure: 11 })];

    const result = splitByTenure(employees, 11);

    expect(result.eligible).toHaveLength(1);
  });

  it("uses exact boundary: tenure one below minimum is dropped", () => {
    const employees = [makeEmployee({ name: "Alice", tenure: 10 })];

    const result = splitByTenure(employees, 11);

    expect(result.droppedByTenure).toHaveLength(1);
  });

  it("handles zero tenure", () => {
    const employees = [makeEmployee({ name: "Alice", tenure: 0 })];

    const result = splitByTenure(employees, 1);

    expect(result.droppedByTenure).toHaveLength(1);
  });
});

describe("filterFrontendEngineers", () => {
  it("keeps employees with no cached data", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    const employees = [
      makeEmployee({ name: "Alice", linkedinUrl: "https://linkedin.com/in/alice" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.rejectedFrontend).toHaveLength(0);
  });

  it("keeps employees with no linkedin URL", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    const employees = [makeEmployee({ name: "Alice", linkedinUrl: null })];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
  });

  it("rejects employees with frontend keyword in matched company experience", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/alice", {
      openToWork: false,
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Building front-end components for the dashboard",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Alice", linkedinUrl: "https://linkedin.com/in/alice" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.rejectedFrontend).toHaveLength(1);
    expect(result.kept).toHaveLength(0);
  });

  it("keeps employees with frontend keyword but also backend override", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/bob", {
      openToWork: false,
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Full-stack engineer working on front-end and back-end systems",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Bob", linkedinUrl: "https://linkedin.com/in/bob" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.rejectedFrontend).toHaveLength(0);
  });

  it("keeps employee and adds warning when company cannot be matched", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/charlie", {
      openToWork: false,
      experience: [
        {
          companyName: "Other Corp",
          companyUniversalName: "other-corp",
          description: "frontend developer",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Charlie", linkedinUrl: "https://linkedin.com/in/charlie" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0]).toContain("Charlie");
  });

  it("keeps employee when description has no frontend keywords", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/dave", {
      openToWork: false,
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "Building infrastructure monitoring and alerting systems",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Dave", linkedinUrl: "https://linkedin.com/in/dave" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.rejectedFrontend).toHaveLength(0);
  });

  it("keeps employee with full-stack override keyword", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/eve", {
      openToWork: false,
      experience: [
        {
          companyName: "Acme",
          companyUniversalName: "acme",
          description: "end-to-end frontend and backend development",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Eve", linkedinUrl: "https://linkedin.com/in/eve" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
  });

  it("matches company by domain base when companyUniversalName matches", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/frank", {
      openToWork: false,
      experience: [
        {
          companyName: "Totally Different Name",
          companyUniversalName: "acme",
          description: "frontend developer",
        },
      ],
    });

    const employees = [
      makeEmployee({ name: "Frank", linkedinUrl: "https://linkedin.com/in/frank" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Totally Different Name",
      companyDomain: "acme.com",
    });

    expect(result.rejectedFrontend).toHaveLength(1);
  });

  it("handles empty experience array", () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/grace", {
      openToWork: false,
      experience: [],
    });

    const employees = [
      makeEmployee({ name: "Grace", linkedinUrl: "https://linkedin.com/in/grace" }),
    ];

    const result = filterFrontendEngineers(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
  });
});

describe("scrapeAndFilterOpenToWork", () => {
  beforeEach(() => {
    getRequiredEnvMock.mockReset();
    getRequiredEnvMock.mockReturnValue("test-apify-key");
    vi.stubGlobal("fetch", vi.fn());
  });

  it("returns empty kept list for empty input", async () => {
    const result = await scrapeAndFilterOpenToWork([], new Map(), {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(0);
    expect(result.warnings).toHaveLength(0);
  });

  it("skips employees without LinkedIn URL and keeps them with warning", async () => {
    const employees = [makeEmployee({ name: "NoUrl", linkedinUrl: null })];

    const result = await scrapeAndFilterOpenToWork(employees, new Map(), {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.kept[0].name).toBe("NoUrl");
    expect(result.warnings).toHaveLength(1);
    expect(result.warnings[0]).toContain("no LinkedIn URL");
  });

  it("uses cache to remove open-to-work candidates without API call", async () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/cached-otw", { openToWork: true, experience: [] });

    const employees = [
      makeEmployee({ name: "CachedOtw", linkedinUrl: "https://linkedin.com/in/cached-otw" }),
    ];

    const result = await scrapeAndFilterOpenToWork(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(0);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("uses cache to keep non-open-to-work candidates without API call", async () => {
    const cache: ApifyOpenToWorkCache = new Map();
    cache.set("linkedin.com/in/cached-not-otw", { openToWork: false, experience: [] });

    const employees = [
      makeEmployee({ name: "CachedNotOtw", linkedinUrl: "https://linkedin.com/in/cached-not-otw" }),
    ];

    const result = await scrapeAndFilterOpenToWork(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("calls Apify API for uncached employees and filters open-to-work", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => [
        {
          linkedinUrl: "https://linkedin.com/in/alice",
          openToWork: true,
          experience: [],
        },
        {
          linkedinUrl: "https://linkedin.com/in/bob",
          openToWork: false,
          experience: [],
        },
      ],
    });
    vi.stubGlobal("fetch", mockFetch);

    const employees = [
      makeEmployee({ name: "Alice", linkedinUrl: "https://linkedin.com/in/alice" }),
      makeEmployee({ name: "Bob", linkedinUrl: "https://linkedin.com/in/bob" }),
    ];

    const cache: ApifyOpenToWorkCache = new Map();
    const result = await scrapeAndFilterOpenToWork(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
    expect(result.kept[0].name).toBe("Bob");
    expect(cache.get("linkedin.com/in/alice")?.openToWork).toBe(true);
    expect(cache.get("linkedin.com/in/bob")?.openToWork).toBe(false);
  });

  it("keeps employees when Apify returns no matching profile (fail-open)", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => [],
    });
    vi.stubGlobal("fetch", mockFetch);

    const employees = [
      makeEmployee({ name: "Missing", linkedinUrl: "https://linkedin.com/in/missing" }),
    ];

    const result = await scrapeAndFilterOpenToWork(employees, new Map(), {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
  });

  it("keeps employees when Apify API throws (fail-open)", async () => {
    const mockFetch = vi.fn().mockRejectedValue(new Error("Network error"));
    vi.stubGlobal("fetch", mockFetch);

    const employees = [
      makeEmployee({ name: "ErrorCase", linkedinUrl: "https://linkedin.com/in/error" }),
    ];

    const result = await scrapeAndFilterOpenToWork(employees, new Map(), {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    expect(result.kept).toHaveLength(1);
  }, 15_000);

  it("populates cache from API response for future calls", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => [
        {
          linkedinUrl: "https://linkedin.com/in/alice",
          openToWork: false,
          experience: [{ companyName: "Acme", description: "SRE" }],
        },
      ],
    });
    vi.stubGlobal("fetch", mockFetch);

    const employees = [
      makeEmployee({ name: "Alice", linkedinUrl: "https://linkedin.com/in/alice" }),
    ];
    const cache: ApifyOpenToWorkCache = new Map();

    await scrapeAndFilterOpenToWork(employees, cache, {
      companyName: "Acme",
      companyDomain: "acme.com",
    });

    const cached = cache.get("linkedin.com/in/alice");
    expect(cached).toBeDefined();
    expect(cached!.openToWork).toBe(false);
    expect(cached!.experience).toHaveLength(1);
  });
});
