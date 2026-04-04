import { beforeEach, describe, expect, it, vi } from "vitest";
import { runEmailCandidateWaterfall } from "../src/services/emailCandidateWaterfall";
import { EnrichedEmployee, Prospect } from "../src/types/prospect";
import { EnrichmentCache } from "../src/services/bulkEnrichPeople";

const searchEmailCandidatePeopleMock = vi.fn();
const bulkEnrichPeopleMock = vi.fn();
const scrapeAndFilterOpenToWorkMock = vi.fn();
const splitByTenureMock = vi.fn();
const filterFrontendEngineersMock = vi.fn();

vi.mock("../src/services/searchPeople", () => ({
  searchEmailCandidatePeople: (...args: unknown[]) => searchEmailCandidatePeopleMock(...args),
}));

vi.mock("../src/services/bulkEnrichPeople", () => ({
  bulkEnrichPeople: (...args: unknown[]) => bulkEnrichPeopleMock(...args),
}));

vi.mock("../src/services/apifyClient", () => ({
  scrapeAndFilterOpenToWork: (...args: unknown[]) => scrapeAndFilterOpenToWorkMock(...args),
  splitByTenure: (...args: unknown[]) => splitByTenureMock(...args),
  filterFrontendEngineers: (...args: unknown[]) => filterFrontendEngineersMock(...args),
}));

const COMPANY = { companyName: "Acme", domain: "acme.com" };
const FILTERS = { apolloOrganizationId: "org_1" };
const APIFY_CACHE = new Map();

function makeProspect(id: string, title: string): Prospect {
  return { id, name: `Person ${id}`, title };
}

function makeEmployee(
  id: string,
  title: string,
  tenure: number | null,
  startDate: string | null = "2022-01-01"
): EnrichedEmployee {
  return {
    id,
    startDate,
    endDate: null,
    name: `Person ${id}`,
    email: null,
    linkedinUrl: null,
    currentTitle: title,
    tenure,
  };
}

describe("runEmailCandidateWaterfall", () => {
  beforeEach(() => {
    searchEmailCandidatePeopleMock.mockReset();
    bulkEnrichPeopleMock.mockReset();
    scrapeAndFilterOpenToWorkMock.mockReset();
    splitByTenureMock.mockReset();
    filterFrontendEngineersMock.mockReset();
    searchEmailCandidatePeopleMock.mockResolvedValue([]);
    bulkEnrichPeopleMock.mockResolvedValue([]);
    scrapeAndFilterOpenToWorkMock.mockImplementation(
      async (employees: EnrichedEmployee[]) => ({ kept: employees, warnings: [], filteredOut: [] })
    );
    splitByTenureMock.mockImplementation((employees: EnrichedEmployee[], minTenureMonths: number) => ({
      eligible: employees.filter((employee) => employee.tenure === null || employee.tenure >= minTenureMonths),
      droppedByTenure: employees.filter(
        (employee) => employee.tenure !== null && employee.tenure < minTenureMonths
      ),
    }));
    filterFrontendEngineersMock.mockImplementation((employees: EnrichedEmployee[]) => ({
      kept: employees,
      rejectedFrontend: [],
      warningCandidates: [],
    }));
  });

  it("returns empty when all stages produce no results", async () => {
    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toEqual([]);
    expect(searchEmailCandidatePeopleMock).toHaveBeenCalledTimes(7);
  });

  it("skips first two SRE stages when pre-filter SRE count is below 8", async () => {
    await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE,
      { rawSreCount: 7 }
    );

    expect(searchEmailCandidatePeopleMock).toHaveBeenCalledTimes(5);
    const firstCallSearchParams = searchEmailCandidatePeopleMock.mock.calls[0][2];
    expect(firstCallSearchParams).toMatchObject({
      currentTitles: ["Infrastructure"],
      pastTitles: undefined,
    });
  });

  it("selects SRE candidates from stage 1 with sre bucket", async () => {
    searchEmailCandidatePeopleMock.mockResolvedValueOnce([
      makeProspect("sre-1", "SRE"),
      makeProspect("sre-2", "Site Reliability Engineer"),
    ]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("sre-1", "SRE", 6),
      makeEmployee("sre-2", "Site Reliability Engineer", 3),
    ]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(2);
    expect(result.candidates[0].campaignBucket).toBe("sre");
    expect(result.candidates[1].campaignBucket).toBe("sre");
    expect(result.candidates[0].employee.id).toBe("sre-1");
    expect(result.candidates[1].employee.id).toBe("sre-2");
  });

  it("stops after reaching 7 candidates", async () => {
    const prospects = Array.from({ length: 8 }, (_, i) =>
      makeProspect(`sre-${i + 1}`, "SRE")
    );
    const employees = Array.from({ length: 8 }, (_, i) =>
      makeEmployee(`sre-${i + 1}`, "SRE", 20 - i)
    );

    searchEmailCandidatePeopleMock.mockResolvedValueOnce(prospects);
    bulkEnrichPeopleMock.mockResolvedValueOnce(employees);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(7);
    expect(searchEmailCandidatePeopleMock).toHaveBeenCalledTimes(1);
  });

  it("deduplicates against LinkedIn attempted keys", async () => {
    searchEmailCandidatePeopleMock.mockResolvedValueOnce([
      makeProspect("linkedin-1", "SRE"),
      makeProspect("sre-new", "SRE"),
    ]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("sre-new", "SRE", 5),
    ]);

    const linkedinKeys = new Set(["linkedin-1"]);
    const result = await runEmailCandidateWaterfall(
      COMPANY,
      linkedinKeys,
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(1);
    expect(result.candidates[0].employee.id).toBe("sre-new");
  });

  it("deduplicates across stages", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([makeProspect("shared-1", "SRE")])
      .mockResolvedValueOnce([makeProspect("shared-1", "SRE"), makeProspect("past-1", "SRE")]);
    bulkEnrichPeopleMock
      .mockResolvedValueOnce([makeEmployee("shared-1", "SRE", 5)])
      .mockResolvedValueOnce([makeEmployee("past-1", "SRE", 4)]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(2);
    expect(result.candidates[0].employee.id).toBe("shared-1");
    expect(result.candidates[1].employee.id).toBe("past-1");
  });

  it("drops people with tenure below minimum for SRE stages (2 months)", async () => {
    searchEmailCandidatePeopleMock.mockResolvedValueOnce([
      makeProspect("short-1", "SRE"),
      makeProspect("ok-1", "SRE"),
    ]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("short-1", "SRE", 1),
      makeEmployee("ok-1", "SRE", 2),
    ]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    const stage1Candidates = result.candidates.filter((c) => c.employee.id === "ok-1");
    expect(stage1Candidates).toHaveLength(1);
    expect(result.candidates.find((c) => c.employee.id === "short-1")).toBeUndefined();
  });

  it("uses null tenure people as fillers when not enough qualified", async () => {
    searchEmailCandidatePeopleMock.mockResolvedValueOnce([
      makeProspect("qualified-1", "SRE"),
      makeProspect("null-1", "SRE"),
      makeProspect("null-2", "SRE"),
    ]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("qualified-1", "SRE", 5),
      makeEmployee("null-1", "SRE", null, null),
      makeEmployee("null-2", "SRE", null, null),
    ]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(3);
    expect(result.candidates[0].employee.id).toBe("qualified-1");
    expect(result.candidates.map((c) => c.employee.id)).toContain("null-1");
    expect(result.candidates.map((c) => c.employee.id)).toContain("null-2");
  });

  it("prefers qualified over null tenure when capping at available slots", async () => {
    const prospects = Array.from({ length: 10 }, (_, i) =>
      makeProspect(`p-${i + 1}`, "SRE")
    );
    const employees = [
      ...Array.from({ length: 8 }, (_, i) =>
        makeEmployee(`p-${i + 1}`, "SRE", 20 - i)
      ),
      makeEmployee("p-9", "SRE", null, null),
      makeEmployee("p-10", "SRE", null, null),
    ];

    searchEmailCandidatePeopleMock.mockResolvedValueOnce(prospects);
    bulkEnrichPeopleMock.mockResolvedValueOnce(employees);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(7);
    const ids = result.candidates.map((c) => c.employee.id);
    expect(ids).not.toContain("p-9");
    expect(ids).not.toContain("p-10");
    expect(ids).not.toContain("p-8");
  });

  it("fills remaining slots from later stages with correct buckets", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([makeProspect("sre-1", "SRE")])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([makeProspect("infra-1", "Infrastructure")])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([makeProspect("devops-1", "DevOps")])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);
    bulkEnrichPeopleMock
      .mockResolvedValueOnce([makeEmployee("sre-1", "SRE", 5)])
      .mockResolvedValueOnce([makeEmployee("infra-1", "Infrastructure", 12)])
      .mockResolvedValueOnce([makeEmployee("devops-1", "DevOps", 15)]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(3);
    expect(result.candidates[0]).toMatchObject({
      campaignBucket: "sre",
      employee: expect.objectContaining({ id: "sre-1" }),
    });
    expect(result.candidates[1]).toMatchObject({
      campaignBucket: "eng",
      employee: expect.objectContaining({ id: "infra-1" }),
    });
    expect(result.candidates[2]).toMatchObject({
      campaignBucket: "eng",
      employee: expect.objectContaining({ id: "devops-1" }),
    });
  });

  it("assigns engLead bucket for infrastructure leadership candidates", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([makeProspect("lead-1", "Head of Infrastructure")]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("lead-1", "Head of Infrastructure", 24),
    ]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(1);
    expect(result.candidates[0].campaignBucket).toBe("engLead");
  });

  it("skips stage when all results are deduped against list A", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([makeProspect("sre-1", "SRE")])
      .mockResolvedValueOnce([makeProspect("sre-1", "SRE")]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("sre-1", "SRE", 5),
    ]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(1);
    expect(bulkEnrichPeopleMock).toHaveBeenCalledTimes(1);
  });

  it("ranks qualified people by tenure descending within a stage", async () => {
    const prospects = Array.from({ length: 5 }, (_, i) =>
      makeProspect(`sre-${i + 1}`, "SRE")
    );
    const employees = [
      makeEmployee("sre-1", "SRE", 3),
      makeEmployee("sre-2", "SRE", 24),
      makeEmployee("sre-3", "SRE", 12),
      makeEmployee("sre-4", "SRE", 6),
      makeEmployee("sre-5", "SRE", 18),
    ];

    searchEmailCandidatePeopleMock.mockResolvedValueOnce(prospects);
    bulkEnrichPeopleMock.mockResolvedValueOnce(employees);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    const tenures = result.candidates.map((c) => c.employee.tenure);
    expect(tenures).toEqual([24, 18, 12, 6, 3]);
  });

  it("passes correct search params for platform stage", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([makeProspect("plat-1", "Platform Engineer")]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("plat-1", "Platform Engineer", 15),
    ]);

    await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    const platformStageCall = searchEmailCandidatePeopleMock.mock.calls[3];
    expect(platformStageCall[2]).toEqual({
      currentTitles: [
        "Platform engineering",
        "Platform engineer",
        "Platforms Engineering Manager",
        "Director of Software Engineering, Platform",
        "Director, Engineering (Platform)",
        "Platform Engineering Manager",
        "VP of Engineering, Platform",
        "VP, Engineering - Platform",
        "VP, Product Platform & Engineering",
        "VP of Developer Platform",
        "VP of Engineering Systems",
        "Head of Platform",
        "Head of Developer Platform",
        "Head of Platform & Reliability",
        "Head of Cloud Platform",
        "Head of Engineering Productivity / Platform",
        "Chief Platform Officer",
        "backend platform",
        "cloud platform",
        "platform cloud",
      ],
      pastTitles: undefined,
      notTitles: [
        "data",
        "contract",
        "contractor",
        "freelance",
        "freelancer",
        "AI",
        "artificial intelligence",
        "machine learning",
        "ml",
        "frontend",
        "front-end",
        "front end",
        "solution",
      ],
      notPastTitles: [
        "client",
        "account",
        "sales",
        "customer",
        "insight",
        "research",
        "marketing",
        "consultant",
        "analyst",
        "partner",
        "commercial",
        "AI",
        "artificial intelligence",
        "machine learning",
        "ml",
      ],
    });
  });

  it("passes updated SRE keywords for stage 1 and stage 2", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([]);

    await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    const stage1Call = searchEmailCandidatePeopleMock.mock.calls[0];
    const stage2Call = searchEmailCandidatePeopleMock.mock.calls[1];

    expect(stage1Call[2]).toEqual({
      currentTitles: ["site reliability", "SRE", "Site Reliability Engineer", "Site Reliability Engineering", "Head of Reliability"],
      pastTitles: undefined,
      notTitles: ["contract", "contractor", "freelance", "freelancer"],
      notPastTitles: undefined,
    });
    expect(stage2Call[2]).toEqual({
      currentTitles: undefined,
      pastTitles: ["site reliability", "SRE", "Site Reliability Engineer", "Site Reliability Engineering", "Head of Reliability"],
      notTitles: ["contract", "contractor", "freelance", "freelancer"],
      notPastTitles: undefined,
    });
  });

  it("enforces 11-month tenure minimum for infrastructure stage", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        makeProspect("infra-short", "Infrastructure"),
        makeProspect("infra-ok", "Infrastructure"),
      ]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("infra-short", "Infrastructure", 10),
      makeEmployee("infra-ok", "Infrastructure", 11),
    ]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(1);
    expect(result.candidates[0].employee.id).toBe("infra-ok");
  });

  it("does not re-add leadership candidates in final Eng Leader stage", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([makeProspect("dup-leader", "Head of Infrastructure")])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        makeProspect("dup-leader", "Head of Infrastructure"),
        makeProspect("final-leader", "VP of Engineering"),
      ]);
    bulkEnrichPeopleMock
      .mockResolvedValueOnce([makeEmployee("dup-leader", "Head of Infrastructure", 20)])
      .mockResolvedValueOnce([makeEmployee("final-leader", "VP of Engineering", 24)]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    const ids = result.candidates.map((candidate) => candidate.employee.id);
    expect(ids.filter((id) => id === "dup-leader")).toHaveLength(1);
    expect(ids).toContain("final-leader");
    expect(result.candidates.find((candidate) => candidate.employee.id === "final-leader")?.campaignBucket).toBe(
      "engLead"
    );
  });

  it("collects normal engineer Apify warning candidates without filtering them out", async () => {
    searchEmailCandidatePeopleMock
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([makeProspect("normal-1", "Staff engineer")]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([
      makeEmployee("normal-1", "Staff engineer", 18),
    ]);
    filterFrontendEngineersMock.mockImplementationOnce((employees: EnrichedEmployee[]) => ({
      kept: employees,
      rejectedFrontend: [],
      warningCandidates: [
        {
          employee: employees[0],
          reason: "company_not_matched",
          problem: "Could not match this profile to Acme in Apify experience data.",
        },
      ],
    }));

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      new Map() as EnrichmentCache,
      FILTERS,
      APIFY_CACHE
    );

    expect(result.candidates.some((candidate) => candidate.employee.id === "normal-1")).toBe(true);
    expect(result.normalEngineerApifyWarnings).toHaveLength(1);
    expect(result.normalEngineerApifyWarnings[0]).toMatchObject({
      employee: expect.objectContaining({ id: "normal-1" }),
      problem: expect.stringContaining("Could not match"),
    });
    expect(result.warnings).toEqual([]);
  });
});
