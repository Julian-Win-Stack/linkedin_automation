import { beforeEach, describe, expect, it, vi } from "vitest";
import { createJob, getJob } from "../src/jobs/jobStore";
import { runResearchPipeline } from "../src/jobs/researchPipeline";
import { EnrichedEmployee } from "../src/types/prospect";
import { TaggedEmailCandidate, EmailWaterfallResult } from "../src/services/emailCandidateWaterfall";

const readCompaniesMock = vi.fn();
const countProcessableCompaniesMock = vi.fn();
const researchCompanyMock = vi.fn();
const getCompanyMock = vi.fn();
const searchPeopleMock = vi.fn();
const scrapeCompanyEmployeesMock = vi.fn();
const scrapePastSreEmployeesMock = vi.fn();
const filterPoolByStageMock = vi.fn();
const filterOpenToWorkFromCacheMock = vi.fn();
const splitByTenureMock = vi.fn();
const filterByKeywordsInApifyDataMock = vi.fn();
const selectTopSreForLemlistMock = vi.fn();
const fillToMinimumWithBackfillMock = vi.fn();
const selectKeywordMatchedByTenureMock = vi.fn();
const pushPeopleToLemlistCampaignMock = vi.fn();
const pushPeopleToLemlistEmailCampaignMock = vi.fn();
const runEmailCandidateWaterfallMock = vi.fn();
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
  scrapePastSreEmployees: (...args: unknown[]) => scrapePastSreEmployeesMock(...args),
  filterPoolByStage: (...args: unknown[]) => filterPoolByStageMock(...args),
}));

vi.mock("../src/services/apifyClient", () => ({
  filterOpenToWorkFromCache: (...args: unknown[]) => filterOpenToWorkFromCacheMock(...args),
  splitByTenure: (...args: unknown[]) => splitByTenureMock(...args),
  filterByKeywordsInApifyData: (...args: unknown[]) => filterByKeywordsInApifyDataMock(...args),
}));

vi.mock("../src/services/sreSelection", () => ({
  selectTopSreForLemlist: (...args: unknown[]) => selectTopSreForLemlistMock(...args),
  fillToMinimumWithBackfill: (...args: unknown[]) => fillToMinimumWithBackfillMock(...args),
  selectKeywordMatchedByTenure: (...args: unknown[]) => selectKeywordMatchedByTenureMock(...args),
}));

vi.mock("../src/services/lemlistPushQueue", () => ({
  pushPeopleToLemlistCampaign: (...args: unknown[]) => pushPeopleToLemlistCampaignMock(...args),
}));

vi.mock("../src/services/lemlistEmailPushQueue", () => ({
  pushPeopleToLemlistEmailCampaign: (...args: unknown[]) => pushPeopleToLemlistEmailCampaignMock(...args),
}));

vi.mock("../src/services/emailCandidateWaterfall", () => ({
  runEmailCandidateWaterfall: (...args: unknown[]) => runEmailCandidateWaterfallMock(...args),
  LINKEDIN_KEYWORD_STAGE_INFRA: { currentTitles: ["Infrastructure"], minTenureMonths: 12, campaignBucket: "eng" },
  LINKEDIN_KEYWORD_STAGE_DEVOPS: { currentTitles: ["DevOps"], minTenureMonths: 12, campaignBucket: "eng" },
  LINKEDIN_KEYWORD_STAGE_NORMAL_ENG: { currentTitles: ["Engineer"], minTenureMonths: 12, campaignBucket: "eng" },
}));

vi.mock("../src/services/observability/csvWriter", () => ({
  rowsToCsvString: (...args: unknown[]) => rowsToCsvStringMock(...args),
}));

vi.mock("../src/services/apolloBulkUpdateAccounts", () => ({
  syncApolloAccountsFromOutputRows: (...args: unknown[]) => syncApolloAccountsFromOutputRowsMock(...args),
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

function emptyWaterfallResult(): EmailWaterfallResult {
  return { candidates: [], filteredOutCandidates: [], warnings: [], normalEngineerApifyWarnings: [] };
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
    scrapePastSreEmployeesMock.mockReset();
    filterPoolByStageMock.mockReset();
    filterOpenToWorkFromCacheMock.mockReset();
    splitByTenureMock.mockReset();
    filterByKeywordsInApifyDataMock.mockReset();
    selectTopSreForLemlistMock.mockReset();
    fillToMinimumWithBackfillMock.mockReset();
    selectKeywordMatchedByTenureMock.mockReset();
    pushPeopleToLemlistCampaignMock.mockReset();
    pushPeopleToLemlistEmailCampaignMock.mockReset();
    runEmailCandidateWaterfallMock.mockReset();
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
    pushPeopleToLemlistEmailCampaignMock.mockResolvedValue({
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
    scrapePastSreEmployeesMock.mockResolvedValue({ employees: [], apifyCache: new Map(), profileCount: 0 });
    filterPoolByStageMock.mockImplementation((pool: EnrichedEmployee[]) => pool);
    filterOpenToWorkFromCacheMock.mockImplementation((employees: EnrichedEmployee[]) => ({
      kept: employees,
      warnings: [],
      filteredOut: [],
    }));
    splitByTenureMock.mockImplementation((employees: EnrichedEmployee[]) => ({ eligible: employees, droppedByTenure: [] }));
    filterByKeywordsInApifyDataMock.mockReturnValue({ matched: [], unmatched: [] });
    runEmailCandidateWaterfallMock.mockResolvedValue(emptyWaterfallResult());
    selectTopSreForLemlistMock.mockImplementation((employees: EnrichedEmployee[]) => employees.slice(0, 7));
    fillToMinimumWithBackfillMock.mockImplementation((selected: EnrichedEmployee[]) => selected);
    selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [], forEmailRecycling: [] });
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
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 0, emailCount: 0 });
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
    expect(scrapePastSreEmployeesMock).toHaveBeenCalledTimes(1);
    expect(pushPeopleToLemlistCampaignMock).toHaveBeenCalledTimes(1);
    const tagged = pushPeopleToLemlistCampaignMock.mock.calls[0][0] as Array<{ linkedinBucket: string }>;
    expect(tagged.length).toBeGreaterThan(0);
  });

  it("starts past SRE scrape before the main Apify pool resolves", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    let resolvePool: ((value: { employees: EnrichedEmployee[]; apifyCache: Map<string, unknown>; profileCount: number }) => void) | null = null;
    const poolPromise = new Promise<{ employees: EnrichedEmployee[]; apifyCache: Map<string, unknown>; profileCount: number }>((resolve) => {
      resolvePool = resolve;
    });
    scrapeCompanyEmployeesMock.mockImplementationOnce(() => poolPromise);
    scrapePastSreEmployeesMock.mockResolvedValueOnce({
      employees: [makeEmployee("past-1", "Platform Engineer")],
      apifyCache: new Map(),
      profileCount: 1,
    });

    const jobId = createJob();
    const runPromise = runResearchPipeline(
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

    await waitForCondition(() => {
      expect(scrapeCompanyEmployeesMock).toHaveBeenCalledTimes(1);
      expect(scrapePastSreEmployeesMock).toHaveBeenCalledTimes(1);
    });

    resolvePool?.({ employees: [], apifyCache: new Map(), profileCount: 0 });
    await runPromise;
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

  it("runs email waterfall from local pool", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    scrapeCompanyEmployeesMock.mockResolvedValueOnce({
      employees: [makeEmployee("pool-1"), makeEmployee("pool-2")],
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

    expect(runEmailCandidateWaterfallMock).toHaveBeenCalledTimes(1);
    expect(runEmailCandidateWaterfallMock.mock.calls[0]?.[2]).toEqual(
      expect.arrayContaining([expect.objectContaining({ id: "pool-1" })])
    );
  });

  it("pushes email campaign from waterfall candidates without searching for missing emails", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    const candidates: TaggedEmailCandidate[] = [
      { employee: { ...makeEmployee("email-1", "SRE", 8), email: "has@example.com" }, campaignBucket: "sre" },
    ];
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates,
      filteredOutCandidates: [],
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

    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
    expect(pushPeopleToLemlistEmailCampaignMock.mock.calls[0][0]).toEqual(candidates);
  });

  it("pushes each company's email candidates before processing the next company", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([
        { companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 },
        { companyName: "Beta", companyDomain: "beta.com", companyLinkedinUrl: "", apolloAccountId: "org_2", rowNumber: 3 },
      ])
    );

    const acmeCandidates: TaggedEmailCandidate[] = [
      { employee: { ...makeEmployee("email-1", "SRE", 8), email: "acme@example.com" }, campaignBucket: "sre" },
    ];
    const betaCandidates: TaggedEmailCandidate[] = [
      { employee: { ...makeEmployee("email-2", "SRE", 8), email: "beta@example.com" }, campaignBucket: "sre" },
    ];
    getCompanyMock
      .mockResolvedValueOnce({ companyName: "Acme", domain: "acme.com" })
      .mockResolvedValueOnce({ companyName: "Beta", domain: "beta.com" });

    let allowFirstEmailPushToFinish: (() => void) | null = null;
    pushPeopleToLemlistEmailCampaignMock.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          allowFirstEmailPushToFinish = () =>
            resolve({
              attempted: acmeCandidates.length,
              successful: 1,
              failed: 0,
              successItems: ["Person email-1"],
              failedItems: [],
              outcomes: [
                {
                  key: "email-1",
                  name: "Person email-1",
                  title: "SRE",
                  linkedinUrl: "https://linkedin.com/in/email-1",
                  status: "succeed",
                },
              ],
            });
        })
    );
    pushPeopleToLemlistEmailCampaignMock.mockResolvedValueOnce({
      attempted: betaCandidates.length,
      successful: 1,
      failed: 0,
      successItems: ["Person email-2"],
      failedItems: [],
      outcomes: [
        {
          key: "email-2",
          name: "Person email-2",
          title: "SRE",
          linkedinUrl: "https://linkedin.com/in/email-2",
          status: "succeed",
        },
      ],
    });
    runEmailCandidateWaterfallMock
      .mockResolvedValueOnce({
        candidates: acmeCandidates,
        filteredOutCandidates: [],
        warnings: [],
        normalEngineerApifyWarnings: [],
      })
      .mockResolvedValueOnce({
        candidates: betaCandidates,
        filteredOutCandidates: [],
        warnings: [],
        normalEngineerApifyWarnings: [],
      });

    const jobId = createJob();
    const runPromise = runResearchPipeline(
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

    await waitForCondition(() => {
      expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
    });
    expect(researchCompanyMock).toHaveBeenCalledTimes(1);

    allowFirstEmailPushToFinish?.();
    await runPromise;

    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(2);
    expect(researchCompanyMock).toHaveBeenCalledTimes(2);
    expect(pushPeopleToLemlistEmailCampaignMock.mock.calls[0][1]).toBe("Acme");
    expect(pushPeopleToLemlistEmailCampaignMock.mock.calls[1][1]).toBe("Beta");
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
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates: [],
      filteredOutCandidates: [
        { employee: makeEmployee("filtered-3", "Engineer", 12), reason: "contract_employment" },
        { employee: makeEmployee("filtered-4", "Engineer", 12), reason: "open_to_work" },
      ],
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
      },
    ]);
  });

  it("stores normal engineer warnings as per-company problem summaries", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates: [],
      filteredOutCandidates: [],
      warnings: [],
      normalEngineerApifyWarnings: [
        { employee: makeEmployee("warn-1", "Engineer", 12), problem: "Missing skills data" },
        { employee: makeEmployee("warn-2", "Engineer", 12), problem: "Missing skills data" },
        { employee: makeEmployee("warn-3", "Engineer", 12), problem: "Missing headline data" },
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
    getWeeklySuccessCountsMock.mockReturnValueOnce({ linkedinCount: 100, emailCount: 0 });
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
    expect(job?.warnings).toContain("Apollo bulk account sync failed: sync fail");
  });
});
