import { beforeEach, describe, expect, it, vi } from "vitest";
import { runEmailCandidateWaterfall } from "../src/services/emailCandidateWaterfall";
import { EnrichedEmployee } from "../src/types/prospect";
const filterOpenToWorkFromCacheMock = vi.fn();
const filterFrontendEngineersMock = vi.fn();
const filterPoolByStageMock = vi.fn();

vi.mock("../src/services/apifyClient", () => ({
  filterOpenToWorkFromCache: (...args: unknown[]) => filterOpenToWorkFromCacheMock(...args),
  filterFrontendEngineers: (...args: unknown[]) => filterFrontendEngineersMock(...args),
}));

vi.mock("../src/services/apifyCompanyEmployees", () => ({
  filterPoolByStage: (...args: unknown[]) => filterPoolByStageMock(...args),
}));

const COMPANY = { companyName: "Acme", domain: "acme.com" };
const APIFY_CACHE = new Map();

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
    headline: "",
    tenure,
  };
}

describe("runEmailCandidateWaterfall", () => {
  beforeEach(() => {
    filterOpenToWorkFromCacheMock.mockReset();
    filterFrontendEngineersMock.mockReset();
    filterPoolByStageMock.mockReset();
    filterPoolByStageMock.mockImplementation((pool: EnrichedEmployee[]) => pool);
    filterOpenToWorkFromCacheMock.mockImplementation(
      (employees: EnrichedEmployee[]) => ({ kept: employees, warnings: [], filteredOut: [] })
    );
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
      [],
      APIFY_CACHE
    );

    expect(result.candidates).toEqual([]);
    expect(filterPoolByStageMock).toHaveBeenCalledTimes(6);
  });

  it("skips first two SRE stages when pre-filter SRE count is below 8", async () => {
    await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [],
      APIFY_CACHE,
      { rawSreCount: 7 }
    );

    expect(filterPoolByStageMock).toHaveBeenCalledTimes(4);
    const firstCallSearchParams = filterPoolByStageMock.mock.calls[0][2];
    expect(firstCallSearchParams).toMatchObject({
      currentTitles: ["Infrastructure"],
      pastTitles: undefined,
    });
    expect(firstCallSearchParams.notTitles).toContain("automation");
    expect(firstCallSearchParams.notTitles).toContain("business");
    expect(firstCallSearchParams.notTitles).toContain("sales");
    expect(firstCallSearchParams.notTitles).toContain("trainee");
  });

  it("selects SRE candidates from stage 1 with sre bucket", async () => {
    const pool = [
      makeEmployee("sre-1", "SRE", 6),
      makeEmployee("sre-2", "Site Reliability Engineer", 3),
    ];
    filterPoolByStageMock.mockImplementationOnce(() => pool).mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      pool,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(2);
    expect(result.candidates[0].campaignBucket).toBe("sre");
    expect(result.candidates[1].campaignBucket).toBe("sre");
    expect(result.candidates[0].employee.id).toBe("sre-1");
    expect(result.candidates[1].employee.id).toBe("sre-2");
  });

  it("splits SRE stage into sre and engLead buckets by leadership titles", async () => {
    const pool = [
      makeEmployee("sre-ic", "Site Reliability Engineer", 6),
      makeEmployee("sre-lead", "Director of Site Reliability", 8),
      makeEmployee("sre-chief", "Chief Reliability Officer", 10),
    ];
    filterPoolByStageMock.mockImplementationOnce(() => pool).mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      pool,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(3);
    const byId = new Map(result.candidates.map((candidate) => [candidate.employee.id, candidate.campaignBucket]));
    expect(byId.get("sre-ic")).toBe("sre");
    expect(byId.get("sre-lead")).toBe("engLead");
    expect(byId.get("sre-chief")).toBe("engLead");
  });

  it("stops after reaching 7 candidates", async () => {
    const employees = Array.from({ length: 8 }, (_, i) =>
      makeEmployee(`sre-${i + 1}`, "SRE", 20 - i)
    );
    filterPoolByStageMock.mockImplementationOnce(() => employees).mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      employees,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(7);
    expect(filterPoolByStageMock).toHaveBeenCalledTimes(1);
  });

  it("deduplicates against LinkedIn attempted keys", async () => {
    const pool = [
      makeEmployee("linkedin-1", "SRE", 4),
      makeEmployee("sre-new", "SRE", 5),
    ];
    filterPoolByStageMock.mockImplementationOnce(() => pool).mockImplementation(() => []);

    const linkedinKeys = new Set(["linkedin-1"]);
    const result = await runEmailCandidateWaterfall(
      COMPANY,
      linkedinKeys,
      pool,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(1);
    expect(result.candidates[0].employee.id).toBe("sre-new");
  });

  it("deduplicates across stages", async () => {
    const shared = makeEmployee("shared-1", "SRE", 5);
    const past = makeEmployee("past-1", "SRE", 4);
    filterPoolByStageMock
      .mockImplementationOnce(() => [shared])
      .mockImplementationOnce(() => [shared, past])
      .mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [shared, past],
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(2);
    expect(result.candidates[0].employee.id).toBe("shared-1");
    expect(result.candidates[1].employee.id).toBe("past-1");
  });

  it("uses null tenure people as fillers when not enough qualified", async () => {
    const pool = [
      makeEmployee("qualified-1", "SRE", 5),
      makeEmployee("null-1", "SRE", null, null),
      makeEmployee("null-2", "SRE", null, null),
    ];
    filterPoolByStageMock.mockImplementationOnce(() => pool).mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      pool,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(3);
    expect(result.candidates[0].employee.id).toBe("qualified-1");
    expect(result.candidates.map((c) => c.employee.id)).toContain("null-1");
    expect(result.candidates.map((c) => c.employee.id)).toContain("null-2");
  });

  it("prefers qualified over null tenure when capping at available slots", async () => {
    const employees = [
      ...Array.from({ length: 8 }, (_, i) =>
        makeEmployee(`p-${i + 1}`, "SRE", 20 - i)
      ),
      makeEmployee("p-9", "SRE", null, null),
      makeEmployee("p-10", "SRE", null, null),
    ];

    filterPoolByStageMock.mockImplementationOnce(() => employees).mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      employees,
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(7);
    const ids = result.candidates.map((c) => c.employee.id);
    expect(ids).not.toContain("p-9");
    expect(ids).not.toContain("p-10");
    expect(ids).not.toContain("p-8");
  });

  it("fills remaining slots from later stages with correct buckets", async () => {
    const sre = makeEmployee("sre-1", "SRE", 5);
    const infra = makeEmployee("infra-1", "Infrastructure", 12);
    const devops = makeEmployee("devops-1", "DevOps", 15);
    filterPoolByStageMock
      .mockImplementationOnce(() => [sre])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [infra])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [devops])
      .mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [sre, infra, devops],
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
    const lead = makeEmployee("lead-1", "Head of Infrastructure", 24);
    filterPoolByStageMock
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [lead])
      .mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [lead],
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(1);
    expect(result.candidates[0].campaignBucket).toBe("engLead");
  });

  it("skips stage when all results are deduped against list A", async () => {
    const shared = makeEmployee("sre-1", "SRE", 5);
    filterPoolByStageMock
      .mockImplementationOnce(() => [shared])
      .mockImplementationOnce(() => [shared])
      .mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [shared],
      APIFY_CACHE
    );

    expect(result.candidates).toHaveLength(1);
    expect(filterOpenToWorkFromCacheMock).toHaveBeenCalledTimes(1);
  });

  it("ranks qualified people by tenure descending within a stage", async () => {
    const employees = [
      makeEmployee("sre-1", "SRE", 3),
      makeEmployee("sre-2", "SRE", 24),
      makeEmployee("sre-3", "SRE", 12),
      makeEmployee("sre-4", "SRE", 6),
      makeEmployee("sre-5", "SRE", 18),
    ];

    filterPoolByStageMock.mockImplementationOnce(() => employees).mockImplementation(() => []);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      employees,
      APIFY_CACHE
    );

    const tenures = result.candidates.map((c) => c.employee.tenure);
    expect(tenures).toEqual([24, 18, 12, 6, 3]);
  });

  it("passes correct search params for platform stage", async () => {
    const platform = makeEmployee("plat-1", "Platform Engineer", 15);
    filterPoolByStageMock
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [platform])
      .mockImplementation(() => []);

    await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [platform],
      APIFY_CACHE
    );

    const platformStageCall = filterPoolByStageMock.mock.calls[3];
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
        "junior",
        "jr",
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
    filterPoolByStageMock.mockImplementation(() => []);

    await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [],
      APIFY_CACHE
    );

    const stage1Call = filterPoolByStageMock.mock.calls[0];
    const stage2Call = filterPoolByStageMock.mock.calls[1];
    const devopsCall = filterPoolByStageMock.mock.calls[4];

    expect(stage1Call[2]).toEqual({
      currentTitles: [
        "site reliability",
        "SRE",
        "Site Reliability Engineer",
        "Site Reliability Engineering",
        "Head of Reliability",
        "observability",
      ],
      pastTitles: undefined,
      notTitles: ["contract", "contractor", "freelance", "freelancer", "junior", "jr"],
      notPastTitles: undefined,
    });
    expect(stage2Call[2]).toEqual({
      currentTitles: undefined,
      pastTitles: [
        "site reliability",
        "SRE",
        "Site Reliability Engineer",
        "Site Reliability Engineering",
        "Head of Reliability",
        "observability",
      ],
      notTitles: ["contract", "contractor", "freelance", "freelancer", "junior", "jr"],
      notPastTitles: undefined,
    });
    expect(devopsCall[2].currentTitles).toEqual(["DevOps", "Dev Ops"]);
    expect(devopsCall[2].notTitles).toContain("business");
    expect(devopsCall[2].notTitles).toContain("sales");
    expect(devopsCall[2].notTitles).toContain("trainee");
  });

  it("does not re-add leadership candidates in final Eng Leader stage", async () => {
    const seed1 = makeEmployee("seed-1", "SRE", 20);
    const seed2 = makeEmployee("seed-2", "SRE", 19);
    const seed3 = makeEmployee("seed-3", "SRE", 18);
    const seed4 = makeEmployee("seed-4", "SRE", 17);
    const seed5 = makeEmployee("seed-5", "SRE", 16);
    const dup = makeEmployee("dup-leader", "Head of Infrastructure", 20);
    const final = makeEmployee("final-leader", "VP of Engineering", 24);
    filterPoolByStageMock
      .mockImplementationOnce(() => [seed1, seed2, seed3, seed4, seed5, dup])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [dup])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [dup, final]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [seed1, seed2, seed3, seed4, seed5, dup, final],
      APIFY_CACHE
    );

    const ids = result.candidates.map((candidate) => candidate.employee.id);
    expect(ids.filter((id) => id === "dup-leader")).toHaveLength(1);
    expect(ids).toContain("final-leader");
    expect(result.candidates.find((candidate) => candidate.employee.id === "final-leader")?.campaignBucket).toBe(
      "engLead"
    );
  });

  it("skips final Eng Leader stage when list has fewer than 5 candidates", async () => {
    const base1 = makeEmployee("base-1", "SRE", 20);
    const base2 = makeEmployee("base-2", "SRE", 19);
    const base3 = makeEmployee("base-3", "SRE", 18);
    const base4 = makeEmployee("base-4", "SRE", 17);
    const finalLeader = makeEmployee("final-leader", "VP of Engineering", 24);

    filterPoolByStageMock
      .mockImplementationOnce(() => [base1, base2, base3, base4])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [finalLeader]);

    const result = await runEmailCandidateWaterfall(
      COMPANY,
      new Set(),
      [base1, base2, base3, base4, finalLeader],
      APIFY_CACHE
    );

    const ids = result.candidates.map((candidate) => candidate.employee.id);
    expect(ids).toEqual(["base-1", "base-2", "base-3", "base-4"]);
    expect(ids).not.toContain("final-leader");
  });

  it("collects normal engineer Apify warning candidates without filtering them out", async () => {
    const normal = makeEmployee("normal-1", "Staff engineer", 18);
    filterPoolByStageMock
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [])
      .mockImplementationOnce(() => [normal])
      .mockImplementation(() => []);
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
      [normal],
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
