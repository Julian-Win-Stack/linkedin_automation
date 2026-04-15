import { beforeEach, describe, expect, it, vi } from "vitest";
import { createJob, getJob } from "../src/jobs/jobStore";
import { runResearchPipeline } from "../src/jobs/researchPipeline";
import { EnrichedEmployee } from "../src/types/prospect";

const readCompaniesMock = vi.fn();
const countProcessableCompaniesMock = vi.fn();
const researchCompanyMock = vi.fn();
const getCompanyMock = vi.fn();
const searchPeopleMock = vi.fn();
const scrapeCompanyEmployeesMock = vi.fn();
const filterPoolByStageMock = vi.fn();
const filterOpenToWorkFromCacheMock = vi.fn();
const splitByTenureMock = vi.fn();
const filterByKeywordsInApifyDataMock = vi.fn();
const filterOutHardwareHeavyPeopleMock = vi.fn();
const selectTopSreForLemlistMock = vi.fn();
const fillToMinimumWithBackfillMock = vi.fn();
const selectKeywordMatchedByTenureMock = vi.fn();
const runBackfillStagesMock = vi.fn();
const pushPeopleToLemlistCampaignMock = vi.fn();
const rowsToCsvStringMock = vi.fn();
const syncApolloAccountsFromOutputRowsMock = vi.fn();
const syncAttioCompaniesFromOutputRowsMock = vi.fn();
const saveWeeklySuccessForJobMock = vi.fn();
const getWeeklySuccessCountsMock = vi.fn();

vi.mock("../src/services/observability/csvReader", () => ({
  readCompanies: (...args: unknown[]) => readCompaniesMock(...args),
  countProcessableCompanies: (...args: unknown[]) => countProcessableCompaniesMock(...args),
}));

vi.mock("../src/services/observability/openaiClient", () => ({
  researchCompany: (...args: unknown[]) => researchCompanyMock(...args),
}));

vi.mock("../src/services/getCompany", () => ({
  getCompany: (...args: unknown[]) => getCompanyMock(...args),
}));

vi.mock("../src/services/searchPeople", () => ({
  searchPeople: (...args: unknown[]) => searchPeopleMock(...args),
}));

vi.mock("../src/services/apifyCompanyEmployees", () => ({
  scrapeCompanyEmployees: (...args: unknown[]) => scrapeCompanyEmployeesMock(...args),
  filterPoolByStage: (...args: unknown[]) => filterPoolByStageMock(...args),
  filterByPastExperienceKeywords: (pool: EnrichedEmployee[]) => pool,
}));

vi.mock("../src/services/apifyClient", () => ({
  filterOpenToWorkFromCache: (...args: unknown[]) => filterOpenToWorkFromCacheMock(...args),
  splitByTenure: (...args: unknown[]) => splitByTenureMock(...args),
  filterByKeywordsInApifyData: (...args: unknown[]) => filterByKeywordsInApifyDataMock(...args),
  filterOutHardwareHeavyPeople: (...args: unknown[]) => filterOutHardwareHeavyPeopleMock(...args),
}));

vi.mock("../src/services/sreSelection", () => ({
  selectTopSreForLemlist: (...args: unknown[]) => selectTopSreForLemlistMock(...args),
  fillToMinimumWithBackfill: (...args: unknown[]) => fillToMinimumWithBackfillMock(...args),
  selectKeywordMatchedByTenure: (...args: unknown[]) => selectKeywordMatchedByTenureMock(...args),
  runBackfillStages: (...args: unknown[]) => runBackfillStagesMock(...args),
  BACKFILL_STAGES: [],
}));

vi.mock("../src/services/lemlistPushQueue", () => ({
  pushPeopleToLemlistCampaign: (...args: unknown[]) => pushPeopleToLemlistCampaignMock(...args),
}));

vi.mock("../src/services/observability/csvWriter", () => ({
  rowsToCsvString: (...args: unknown[]) => rowsToCsvStringMock(...args),
}));

vi.mock("../src/services/apolloBulkUpdateAccounts", () => ({
  syncApolloAccountsFromOutputRows: (...args: unknown[]) => syncApolloAccountsFromOutputRowsMock(...args),
  formatCurrentWeekLabel: () => "Week of 2026-04-06",
}));

vi.mock("../src/services/attioAssertCompanyRecords", () => ({
  syncAttioCompaniesFromOutputRows: (...args: unknown[]) => syncAttioCompaniesFromOutputRowsMock(...args),
}));

vi.mock("../src/services/weeklySuccessStore", () => ({
  saveWeeklySuccessForJob: (...args: unknown[]) => saveWeeklySuccessForJobMock(...args),
  getWeeklySuccessCounts: (...args: unknown[]) => getWeeklySuccessCountsMock(...args),
}));

function asyncCompanyRows(
  rows: Array<{ companyName: string; companyDomain: string; companyLinkedinUrl?: string; apolloAccountId?: string; rowNumber: number }>
) {
  return (async function* () {
    for (const row of rows) {
      yield row;
    }
  })();
}

function makeEmployee(id: string, title = "SRE", tenure: number | null = 12, linkedinUrl?: string): EnrichedEmployee {
  return {
    id,
    startDate: "2022-01-01",
    endDate: null,
    name: `Person ${id}`,
    email: null,
    linkedinUrl: linkedinUrl ?? `https://linkedin.com/in/${id}`,
    currentTitle: title,
    headline: "",
    tenure,
  };
}

async function waitForCondition(predicate: () => void, attempts = 20): Promise<void> {
  let lastError: unknown;
  for (let index = 0; index < attempts; index += 1) {
    try {
      predicate();
      return;
    } catch (error) {
      lastError = error;
      await Promise.resolve();
    }
  }
  throw lastError;
}

describe("runResearchPipeline orchestration", () => {
  beforeEach(() => {
    readCompaniesMock.mockReset();
    countProcessableCompaniesMock.mockReset();
    researchCompanyMock.mockReset();
    getCompanyMock.mockReset();
    searchPeopleMock.mockReset();
    scrapeCompanyEmployeesMock.mockReset();
    filterPoolByStageMock.mockReset();
    filterOpenToWorkFromCacheMock.mockReset();
    splitByTenureMock.mockReset();
    filterByKeywordsInApifyDataMock.mockReset();
    filterOutHardwareHeavyPeopleMock.mockReset();
    filterOutHardwareHeavyPeopleMock.mockImplementation((employees: EnrichedEmployee[]) => ({ kept: employees, rejected: [] }));
    selectTopSreForLemlistMock.mockReset();
    fillToMinimumWithBackfillMock.mockReset();
    selectKeywordMatchedByTenureMock.mockReset();
    runBackfillStagesMock.mockReset();
    pushPeopleToLemlistCampaignMock.mockReset();
    rowsToCsvStringMock.mockReset();
    syncApolloAccountsFromOutputRowsMock.mockReset();
    syncAttioCompaniesFromOutputRowsMock.mockReset();
    saveWeeklySuccessForJobMock.mockReset();
    getWeeklySuccessCountsMock.mockReset();

    rowsToCsvStringMock.mockResolvedValue("company_name\nAcme\n");
    pushPeopleToLemlistCampaignMock.mockResolvedValue({
      attempted: 0,
      successful: 0,
      failed: 0,
      successItems: [],
      failedItems: [],
      outcomes: [],
    });
    searchPeopleMock.mockResolvedValue([]);
    researchCompanyMock.mockResolvedValue("Not found");
    getCompanyMock.mockResolvedValue({ companyName: "Acme", domain: "acme.com" });
    scrapeCompanyEmployeesMock.mockResolvedValue({ employees: [], apifyCache: new Map(), profileCount: 0 });
    filterPoolByStageMock.mockImplementation((pool: EnrichedEmployee[]) => pool);
    filterOpenToWorkFromCacheMock.mockImplementation((employees: EnrichedEmployee[]) => ({
      kept: employees,
      warnings: [],
      filteredOut: [],
    }));
    splitByTenureMock.mockImplementation((employees: EnrichedEmployee[]) => ({ eligible: employees, droppedByTenure: [] }));
    filterByKeywordsInApifyDataMock.mockReturnValue({ matched: [], unmatched: [] });
    selectTopSreForLemlistMock.mockImplementation((employees: EnrichedEmployee[]) => employees.slice(0, 7));
    fillToMinimumWithBackfillMock.mockImplementation((selected: EnrichedEmployee[]) => selected);
    selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [] });
    runBackfillStagesMock.mockReturnValue({ candidates: [], filteredOutReasons: [], warnings: [], normalEngineerApifyWarnings: [] });
    syncApolloAccountsFromOutputRowsMock.mockResolvedValue({
      attemptedRows: 0,
      dedupedAccounts: 0,
      updatedAccounts: 0,
      skippedMissingAccountIdCount: 0,
      skippedNoMappableFieldsCount: 0,
      duplicateAccountIdCount: 0,
      warnings: [],
    });
    syncAttioCompaniesFromOutputRowsMock.mockResolvedValue({
      attemptedRows: 0,
      dedupedDomains: 0,
      assertedCount: 0,
      failedCount: 0,
      skippedMissingDomainCount: 0,
      skippedNoMappableFieldsCount: 0,
      duplicateDomainCount: 0,
      warnings: [],
    });
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 0, companiesReachedOutToCount: 0 });
    countProcessableCompaniesMock.mockResolvedValue(500);
    process.env.LEMLIST_PUSH_ENABLED = "true";
  });

  it("uses company pool and pushes linkedin candidates", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce([{ id: "p1", name: "P1", title: "SRE" }]);
    const poolEmployees = [makeEmployee("sre-1"), makeEmployee("sre-2", "Director of Engineering")];
    scrapeCompanyEmployeesMock.mockResolvedValueOnce({
      employees: poolEmployees,
      apifyCache: new Map(),
      profileCount: 2,
    });

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    expect(scrapeCompanyEmployeesMock).toHaveBeenCalledTimes(1);
    expect(pushPeopleToLemlistCampaignMock).toHaveBeenCalledTimes(1);
    const tagged = pushPeopleToLemlistCampaignMock.mock.calls[0][0] as Array<{ linkedinBucket: string }>;
    expect(tagged.length).toBeGreaterThan(0);
  });

  it("logs returned SRE pre-filter people to the terminal stream", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce([
      { id: "sre-1", name: "Alice", title: "SRE" },
      { id: "sre-2", name: "Bob", title: "Site Reliability Engineer" },
      ...Array.from({ length: 28 }, (_, index) => ({
        id: `sre-extra-${index + 1}`,
        name: `Extra ${index + 1}`,
        title: "SRE",
      })),
    ]);
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    const loggedOutput = consoleErrorSpy.mock.calls.map(([message]) => String(message)).join("\n");
    expect(loggedOutput).toContain("SRE pre-filter results for Acme: 30 (returned cap of 30)");
    expect(loggedOutput).toContain("1. Alice | SRE | sre-1");
    expect(loggedOutput).toContain("2. Bob | Site Reliability Engineer | sre-2");

    consoleErrorSpy.mockRestore();
  });

  it("does not reject a domainless company from Apollo SRE pre-filter count", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    expect(searchPeopleMock).not.toHaveBeenCalled();
    expect(researchCompanyMock).toHaveBeenCalledTimes(1);
    const loggedOutput = consoleErrorSpy.mock.calls.map(([message]) => String(message)).join("\n");
    expect(loggedOutput).toContain(
      "SRE pre-filter skipped for Acme: missing company domain makes Apollo org-id-only search unreliable"
    );

    consoleErrorSpy.mockRestore();
  });

  it("rejects a domainless company when Apify SRE count exceeds maximum", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    const sreEmployees = Array.from({ length: 16 }, (_, index) =>
      makeEmployee(`sre-${index + 1}`, "SRE", 12)
    );
    scrapeCompanyEmployeesMock.mockResolvedValueOnce({
      employees: sreEmployees,
      apifyCache: new Map(),
      profileCount: 16,
    });

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    expect(searchPeopleMock).not.toHaveBeenCalled();
    expect(pushPeopleToLemlistCampaignMock).not.toHaveBeenCalled();
    const job = getJob(jobId);
    expect(job?.rejectedCompanies).toEqual(
      expect.arrayContaining([expect.stringContaining("Acme")])
    );
    const csvArg = rowsToCsvStringMock.mock.calls[0]?.[0] as Array<{ company_name: string; sre_count: number | "" }> | undefined;
    const acmeRow = csvArg?.find((row) => row.company_name === "Acme");
    expect(acmeRow?.sre_count).toBe(16);
  });

  it("writes Apify-derived SRE count to output row for domainless company that passes", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    const sreEmployees = Array.from({ length: 5 }, (_, index) =>
      makeEmployee(`sre-${index + 1}`, "SRE", 12)
    );
    scrapeCompanyEmployeesMock.mockResolvedValueOnce({
      employees: sreEmployees,
      apifyCache: new Map(),
      profileCount: 5,
    });

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    expect(searchPeopleMock).not.toHaveBeenCalled();
    const csvArg = rowsToCsvStringMock.mock.calls[0]?.[0] as Array<{ company_name: string; sre_count: number | "" }> | undefined;
    const acmeRow = csvArg?.find((row) => row.company_name === "Acme");
    expect(acmeRow?.sre_count).toBe(5);
  });

  it("uses the larger linkedin current SRE count for csv and sync outputs", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce([{ id: "apollo-1", name: "Apollo One", title: "SRE" }]);
    const linkedinSreEmployees = Array.from({ length: 4 }, (_, index) => makeEmployee(`linkedin-${index + 1}`, "SRE", 12));
    scrapeCompanyEmployeesMock.mockResolvedValueOnce({
      employees: linkedinSreEmployees,
      apifyCache: new Map(),
      profileCount: 4,
    });

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    const csvArg = rowsToCsvStringMock.mock.calls[0]?.[0] as Array<{ company_name: string; sre_count: number | "" }> | undefined;
    const apolloSyncArg =
      syncApolloAccountsFromOutputRowsMock.mock.calls[0]?.[0] as Array<{ company_name: string; sre_count: number | "" }> | undefined;
    const attioSyncArg =
      syncAttioCompaniesFromOutputRowsMock.mock.calls[0]?.[0] as Array<{ company_name: string; sre_count: number | "" }> | undefined;

    expect(csvArg?.find((row) => row.company_name === "Acme")?.sre_count).toBe(4);
    expect(apolloSyncArg?.find((row) => row.company_name === "Acme")?.sre_count).toBe(4);
    expect(attioSyncArg?.find((row) => row.company_name === "Acme")?.sre_count).toBe(4);
  });

  it("keeps the Apollo SRE count when it is larger than linkedin current SRE count", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce([
      { id: "apollo-1", name: "Apollo One", title: "SRE" },
      { id: "apollo-2", name: "Apollo Two", title: "SRE" },
      { id: "apollo-3", name: "Apollo Three", title: "SRE" },
      { id: "apollo-4", name: "Apollo Four", title: "SRE" },
      { id: "apollo-5", name: "Apollo Five", title: "SRE" },
    ]);
    scrapeCompanyEmployeesMock.mockResolvedValueOnce({
      employees: [makeEmployee("linkedin-1", "SRE", 12), makeEmployee("linkedin-2", "SRE", 12)],
      apifyCache: new Map(),
      profileCount: 2,
    });

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    const csvArg = rowsToCsvStringMock.mock.calls[0]?.[0] as Array<{ company_name: string; sre_count: number | "" }> | undefined;
    const apolloSyncArg =
      syncApolloAccountsFromOutputRowsMock.mock.calls[0]?.[0] as Array<{ company_name: string; sre_count: number | "" }> | undefined;
    const attioSyncArg =
      syncAttioCompaniesFromOutputRowsMock.mock.calls[0]?.[0] as Array<{ company_name: string; sre_count: number | "" }> | undefined;

    expect(csvArg?.find((row) => row.company_name === "Acme")?.sre_count).toBe(5);
    expect(apolloSyncArg?.find((row) => row.company_name === "Acme")?.sre_count).toBe(5);
    expect(attioSyncArg?.find((row) => row.company_name === "Acme")?.sre_count).toBe(5);
  });



  it("stores filtered candidate summaries as per-company counts", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    const currentSre = [makeEmployee("sre-1", "SRE", 12)];
    scrapeCompanyEmployeesMock.mockResolvedValueOnce({
      employees: currentSre,
      apifyCache: new Map(),
      profileCount: 1,
    });
    filterOpenToWorkFromCacheMock
      .mockReturnValueOnce({
        kept: currentSre,
        warnings: [],
        filteredOut: [
          { employee: makeEmployee("filtered-1", "SRE", 12), reason: "open_to_work" },
          { employee: makeEmployee("filtered-2", "SRE", 12), reason: "frontend_role" },
        ],
      })
      .mockReturnValue({
        kept: [],
        warnings: [],
        filteredOut: [],
      });
    runBackfillStagesMock.mockReturnValueOnce({
      candidates: [],
      filteredOutReasons: ["contract_employment", "open_to_work"],
      warnings: [],
      normalEngineerApifyWarnings: [],
    });

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    const job = getJob(jobId);
    expect(job?.campaignPushData?.filteredOutCandidates).toEqual([
      {
        companyName: "Acme",
        openToWorkCount: 2,
        frontendRoleCount: 1,
        contractEmploymentCount: 1,
        hardwareHeavyCount: 0,
        qaTitleCount: 0,
      },
    ]);
  });

  it("stores normal engineer warnings as per-company problem summaries", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    runBackfillStagesMock.mockReturnValueOnce({
      candidates: [],
      filteredOutReasons: [],
      warnings: [],
      normalEngineerApifyWarnings: [
        { problem: "Missing skills data" },
        { problem: "Missing skills data" },
        { problem: "Missing headline data" },
      ],
    });

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    const job = getJob(jobId);
    expect(job?.campaignPushData?.normalEngineerApifyWarnings).toEqual([
      {
        companyName: "Acme",
        totalCount: 3,
        problems: [
          { problem: "Missing skills data", count: 2 },
          { problem: "Missing headline data", count: 1 },
        ],
      },
    ]);
  });

  it("skips company when weekly linkedin limit reached", async () => {
    getWeeklySuccessCountsMock.mockReturnValueOnce({ linkedinCount: 100, companiesReachedOutToCount: 0 });
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    expect(scrapeCompanyEmployeesMock).not.toHaveBeenCalled();
    expect(searchPeopleMock).not.toHaveBeenCalled();
  });

  it("still marks done when account sync fails", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    syncApolloAccountsFromOutputRowsMock.mockRejectedValueOnce(new Error("sync fail"));

    const jobId = createJob();
    await runResearchPipeline(
      jobId,
      "csv",
      {
        azureOpenAiApiKey: "k",
        azureOpenAiBaseUrl: "u",
        searchApiKey: "s",
        model: "m",
        maxCompletionTokens: 1000,
        nameColumn: "Company Name",
        domainColumn: "Website",
        apolloAccountIdColumn: "Apollo Account Id",
      },
      "julian",
      Date.now()
    );

    const job = getJob(jobId);
    expect(job?.status).toBe("done");
    expect(job?.warnings.some((w) => w.includes("Apollo bulk account sync failed"))).toBe(true);
  });

  describe("LinkedIn keyword expansion (DevOps-only)", () => {
    const runWith = async (profile: EnrichedEmployee[]) => {
      readCompaniesMock.mockReturnValueOnce(
        asyncCompanyRows([
          { companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 },
        ])
      );
      scrapeCompanyEmployeesMock.mockResolvedValueOnce({
        employees: profile,
        apifyCache: new Map(),
        profileCount: profile.length,
      });
      const jobId = createJob();
      await runResearchPipeline(
        jobId,
        "csv",
        {
          azureOpenAiApiKey: "k",
          azureOpenAiBaseUrl: "u",
          searchApiKey: "s",
          model: "m",
          maxCompletionTokens: 1000,
          nameColumn: "Company Name",
          domainColumn: "Website",
          apolloAccountIdColumn: "Apollo Account Id",
        },
        "julian",
        Date.now()
      );
      return jobId;
    };

    it("includes a DevOps-titled employee whose Apify data matches an SRE keyword", async () => {
      const devopsEmployee = makeEmployee("devops-1", "DevOps Engineer");
      filterByKeywordsInApifyDataMock.mockReturnValueOnce({ matched: [devopsEmployee], unmatched: [] });
      selectKeywordMatchedByTenureMock.mockReturnValueOnce({ forLinkedin: [devopsEmployee] });

      await runWith([devopsEmployee]);

      expect(filterByKeywordsInApifyDataMock).toHaveBeenCalledTimes(1);
      const poolArg = filterByKeywordsInApifyDataMock.mock.calls[0][0] as EnrichedEmployee[];
      expect(poolArg.map((e) => e.id)).toContain("devops-1");

      const tagged = pushPeopleToLemlistCampaignMock.mock.calls[0][0] as Array<{ employee: EnrichedEmployee }>;
      expect(tagged.map((t) => t.employee.id)).toContain("devops-1");
    });

    it("excludes non-DevOps titles from the keyword expansion pool even if their profile contains keywords", async () => {
      const seniorEng = makeEmployee("sde-1", "Senior Software Engineer");
      const platformEng = makeEmployee("plat-1", "Platform Engineer");
      // Pretend the mock would match them if they reached it
      filterByKeywordsInApifyDataMock.mockReturnValueOnce({
        matched: [seniorEng, platformEng],
        unmatched: [],
      });
      selectKeywordMatchedByTenureMock.mockImplementation((matched: EnrichedEmployee[]) => ({ forLinkedin: matched }));

      await runWith([seniorEng, platformEng]);

      // filterByKeywordsInApifyData must be called, but with an empty pool
      expect(filterByKeywordsInApifyDataMock).toHaveBeenCalledTimes(1);
      const poolArg = filterByKeywordsInApifyDataMock.mock.calls[0][0] as EnrichedEmployee[];
      expect(poolArg).toEqual([]);
    });

    it("accepts DevOps, Dev Ops, and Dev-Ops title variants", async () => {
      const variants = [
        makeEmployee("v1", "DevOps Engineer"),
        makeEmployee("v2", "Senior Dev Ops Engineer"),
        makeEmployee("v3", "Dev-Ops Lead"),
      ];
      filterByKeywordsInApifyDataMock.mockReturnValueOnce({ matched: [], unmatched: variants });
      selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [] });

      await runWith(variants);

      expect(filterByKeywordsInApifyDataMock).toHaveBeenCalledTimes(1);
      const poolArg = filterByKeywordsInApifyDataMock.mock.calls[0][0] as EnrichedEmployee[];
      expect(poolArg.map((e) => e.id).sort()).toEqual(["v1", "v2", "v3"]);
    });

    it("excludes a DevOps-titled employee when their profile does not match any SRE keyword", async () => {
      const devops = makeEmployee("devops-2", "DevOps Engineer");
      filterByKeywordsInApifyDataMock.mockReturnValueOnce({ matched: [], unmatched: [devops] });
      selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [] });

      await runWith([devops]);

      const tagged = (pushPeopleToLemlistCampaignMock.mock.calls[0]?.[0] ?? []) as Array<{ employee: EnrichedEmployee }>;
      expect(tagged.map((t) => t.employee.id)).not.toContain("devops-2");
    });

    it("invokes filterByKeywordsInApifyData exactly once per company (no per-stage looping)", async () => {
      const mixed = [
        makeEmployee("d-1", "DevOps Engineer"),
        makeEmployee("i-1", "Infrastructure Engineer"),
        makeEmployee("p-1", "Platform Engineer"),
        makeEmployee("e-1", "Staff Engineer"),
      ];
      filterByKeywordsInApifyDataMock.mockReturnValueOnce({ matched: [], unmatched: [] });
      selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [] });

      await runWith(mixed);

      expect(filterByKeywordsInApifyDataMock).toHaveBeenCalledTimes(1);
    });

    it("rejects a DevOps-titled employee whose title also contains 'QA'", async () => {
      const devopsQa = makeEmployee("dqa-1", "DevOps QA Engineer");
      const devopsClean = makeEmployee("d-clean", "DevOps Engineer");
      filterByKeywordsInApifyDataMock.mockReturnValueOnce({ matched: [devopsClean], unmatched: [] });
      selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [devopsClean] });

      await runWith([devopsQa, devopsClean]);

      const poolArg = filterByKeywordsInApifyDataMock.mock.calls[0][0] as EnrichedEmployee[];
      expect(poolArg.map((e) => e.id)).not.toContain("dqa-1");
      expect(poolArg.map((e) => e.id)).toContain("d-clean");
    });
  });

  describe("Hardware filter scope", () => {
    it("does not apply the hardware filter to the current-SRE or past-SRE selection paths", async () => {
      const sre = makeEmployee("sre-1", "SRE");
      const devops = makeEmployee("devops-1", "DevOps Engineer");
      readCompaniesMock.mockReturnValueOnce(
        asyncCompanyRows([
          { companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 },
        ])
      );
      scrapeCompanyEmployeesMock.mockResolvedValueOnce({
        employees: [sre, devops],
        apifyCache: new Map(),
        profileCount: 2,
      });
      filterByKeywordsInApifyDataMock.mockReturnValueOnce({ matched: [], unmatched: [] });
      selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [] });

      const jobId = createJob();
      await runResearchPipeline(
        jobId,
        "csv",
        {
          azureOpenAiApiKey: "k",
          azureOpenAiBaseUrl: "u",
          searchApiKey: "s",
          model: "m",
          maxCompletionTokens: 1000,
          nameColumn: "Company Name",
          domainColumn: "Website",
          apolloAccountIdColumn: "Apollo Account Id",
        },
        "julian",
        Date.now()
      );

      // Hardware filter should be invoked only once per company (for the DevOps pool),
      // never for the current-SRE tier selection or past-SRE backfill.
      expect(filterOutHardwareHeavyPeopleMock).toHaveBeenCalledTimes(1);
      const hardwarePoolArg = filterOutHardwareHeavyPeopleMock.mock.calls[0][0] as EnrichedEmployee[];
      expect(hardwarePoolArg.map((e) => e.id)).not.toContain("sre-1");
    });
  });
});

const defaultPipelineConfig = {
  azureOpenAiApiKey: "k",
  azureOpenAiBaseUrl: "u",
  searchApiKey: "s",
  model: "m",
  maxCompletionTokens: 1000,
  nameColumn: "Company Name",
  domainColumn: "Website",
  apolloAccountIdColumn: "Apollo Account Id",
} as const;

function makeCompanyRows(count: number) {
  return Array.from({ length: count }, (_, i) => ({
    companyName: `Company${i}`,
    companyDomain: `company${i}.com`,
    companyLinkedinUrl: "",
    apolloAccountId: `org_${i}`,
    rowNumber: i + 2,
  }));
}

describe("50-company checkpoint flush", () => {
  beforeEach(() => {
    readCompaniesMock.mockReset();
    countProcessableCompaniesMock.mockReset();
    researchCompanyMock.mockReset();
    getCompanyMock.mockReset();
    searchPeopleMock.mockReset();
    scrapeCompanyEmployeesMock.mockReset();
    filterPoolByStageMock.mockReset();
    filterOpenToWorkFromCacheMock.mockReset();
    splitByTenureMock.mockReset();
    filterByKeywordsInApifyDataMock.mockReset();
    filterOutHardwareHeavyPeopleMock.mockReset();
    filterOutHardwareHeavyPeopleMock.mockImplementation((employees: EnrichedEmployee[]) => ({ kept: employees, rejected: [] }));
    selectTopSreForLemlistMock.mockReset();
    fillToMinimumWithBackfillMock.mockReset();
    selectKeywordMatchedByTenureMock.mockReset();
    runBackfillStagesMock.mockReset();
    pushPeopleToLemlistCampaignMock.mockReset();
    rowsToCsvStringMock.mockReset();
    syncApolloAccountsFromOutputRowsMock.mockReset();
    syncAttioCompaniesFromOutputRowsMock.mockReset();
    saveWeeklySuccessForJobMock.mockReset();
    getWeeklySuccessCountsMock.mockReset();

    rowsToCsvStringMock.mockResolvedValue("company_name\nAcme\n");
    pushPeopleToLemlistCampaignMock.mockResolvedValue({ attempted: 0, successful: 0, failed: 0, successItems: [], failedItems: [], outcomes: [] });
    searchPeopleMock.mockResolvedValue([]);
    researchCompanyMock.mockResolvedValue("Not found");
    getCompanyMock.mockResolvedValue({ companyName: "Acme", domain: "acme.com" });
    scrapeCompanyEmployeesMock.mockResolvedValue({ employees: [], apifyCache: new Map(), profileCount: 0 });
    filterPoolByStageMock.mockImplementation((pool: EnrichedEmployee[]) => pool);
    filterOpenToWorkFromCacheMock.mockImplementation((employees: EnrichedEmployee[]) => ({ kept: employees, warnings: [], filteredOut: [] }));
    splitByTenureMock.mockImplementation((employees: EnrichedEmployee[]) => ({ eligible: employees, droppedByTenure: [] }));
    filterByKeywordsInApifyDataMock.mockReturnValue({ matched: [], unmatched: [] });
    selectTopSreForLemlistMock.mockImplementation((employees: EnrichedEmployee[]) => employees.slice(0, 7));
    fillToMinimumWithBackfillMock.mockImplementation((selected: EnrichedEmployee[]) => selected);
    selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [] });
    runBackfillStagesMock.mockReturnValue({ candidates: [], filteredOutReasons: [], warnings: [], normalEngineerApifyWarnings: [] });
    syncApolloAccountsFromOutputRowsMock.mockResolvedValue({ attemptedRows: 0, dedupedAccounts: 0, updatedAccounts: 0, skippedMissingAccountIdCount: 0, skippedNoMappableFieldsCount: 0, duplicateAccountIdCount: 0, warnings: [] });
    syncAttioCompaniesFromOutputRowsMock.mockResolvedValue({ attemptedRows: 0, dedupedDomains: 0, assertedCount: 0, failedCount: 0, skippedMissingDomainCount: 0, skippedNoMappableFieldsCount: 0, duplicateDomainCount: 0, warnings: [] });
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 0, companiesReachedOutToCount: 0 });
    countProcessableCompaniesMock.mockResolvedValue(500);
    process.env.LEMLIST_PUSH_ENABLED = "true";
  });

  it("calls apollo/attio sync once per 50-company batch and once at end for 51 companies", async () => {
    readCompaniesMock.mockReturnValueOnce(asyncCompanyRows(makeCompanyRows(51)));
    countProcessableCompaniesMock.mockResolvedValue(51);

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", defaultPipelineConfig, "julian", Date.now());

    // Checkpoint fires at company 50, then again at end (1 remaining)
    expect(syncApolloAccountsFromOutputRowsMock).toHaveBeenCalledTimes(2);
    expect(syncAttioCompaniesFromOutputRowsMock).toHaveBeenCalledTimes(2);
    expect(getJob(jobId)?.status).toBe("done");
  });

  it("calls apollo/attio sync only once for fewer than 50 companies", async () => {
    readCompaniesMock.mockReturnValueOnce(asyncCompanyRows(makeCompanyRows(10)));
    countProcessableCompaniesMock.mockResolvedValue(10);

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", defaultPipelineConfig, "julian", Date.now());

    // Only the final checkpoint fires
    expect(syncApolloAccountsFromOutputRowsMock).toHaveBeenCalledTimes(1);
    expect(syncAttioCompaniesFromOutputRowsMock).toHaveBeenCalledTimes(1);
  });

  it("calls saveWeeklySuccessForJob at each checkpoint with cumulative linkedin counts", async () => {
    readCompaniesMock.mockReturnValueOnce(asyncCompanyRows(makeCompanyRows(51)));
    countProcessableCompaniesMock.mockResolvedValue(51);

    const employee = makeEmployee("sre-1");
    scrapeCompanyEmployeesMock.mockResolvedValue({ employees: [employee], apifyCache: new Map(), profileCount: 1 });
    selectTopSreForLemlistMock.mockImplementation((employees: EnrichedEmployee[]) => employees);
    pushPeopleToLemlistCampaignMock.mockResolvedValue({
      attempted: 1,
      successful: 1,
      failed: 0,
      successItems: [employee],
      failedItems: [],
      outcomes: [{ linkedinUrl: employee.linkedinUrl, status: "succeed" }],
    });

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", defaultPipelineConfig, "julian", Date.now());

    expect(saveWeeklySuccessForJobMock).toHaveBeenCalledTimes(2);
    // First checkpoint: 50 companies × 1 push each = 50
    const firstCallArgs = saveWeeklySuccessForJobMock.mock.calls[0][0] as { linkedinSuccessCount: number };
    expect(firstCallArgs.linkedinSuccessCount).toBe(50);
    // Final checkpoint: 51 companies × 1 push each = 51
    const secondCallArgs = saveWeeklySuccessForJobMock.mock.calls[1][0] as { linkedinSuccessCount: number };
    expect(secondCallArgs.linkedinSuccessCount).toBe(51);
  });

  it("sets partial csv and campaignPushData on job state after first checkpoint", async () => {
    readCompaniesMock.mockReturnValueOnce(asyncCompanyRows(makeCompanyRows(50)));
    countProcessableCompaniesMock.mockResolvedValue(50);

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", defaultPipelineConfig, "julian", Date.now());

    const job = getJob(jobId);
    expect(job?.partialCsvBase64).toBeDefined();
    expect(typeof job?.partialCsvBase64).toBe("string");
    expect(job?.partialCampaignPushData).toBeDefined();
  });

  it("weekly-limit skipped companies count toward the checkpoint threshold", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 100, companiesReachedOutToCount: 0 });
    readCompaniesMock.mockReturnValueOnce(asyncCompanyRows(makeCompanyRows(50)));
    countProcessableCompaniesMock.mockResolvedValue(50);

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", defaultPipelineConfig, "julian", Date.now());

    // All 50 companies are skipped (limit reached) but checkpoint still fires at 50 + final
    expect(syncApolloAccountsFromOutputRowsMock).toHaveBeenCalledTimes(2);
    expect(saveWeeklySuccessForJobMock).toHaveBeenCalledTimes(2);
    expect(searchPeopleMock).not.toHaveBeenCalled();
  });

  it("each checkpoint only syncs the new rows since the last checkpoint", async () => {
    readCompaniesMock.mockReturnValueOnce(asyncCompanyRows(makeCompanyRows(51)));
    countProcessableCompaniesMock.mockResolvedValue(51);

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", defaultPipelineConfig, "julian", Date.now());

    const firstSyncCall = syncApolloAccountsFromOutputRowsMock.mock.calls[0][0] as unknown[];
    const secondSyncCall = syncApolloAccountsFromOutputRowsMock.mock.calls[1][0] as unknown[];
    // First batch: companies 0–49 (50 rows), second batch: company 50 (1 row)
    expect(firstSyncCall.length).toBe(50);
    expect(secondSyncCall.length).toBe(1);
  });
});
