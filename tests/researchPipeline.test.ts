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
const filterPoolByStageMock = vi.fn();
const filterOpenToWorkFromCacheMock = vi.fn();
const splitByTenureMock = vi.fn();
const filterByKeywordsInApifyDataMock = vi.fn();
const findEmailsInBulkMock = vi.fn();
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
  filterPoolByStage: (...args: unknown[]) => filterPoolByStageMock(...args),
  filterByPastExperienceKeywords: (pool: EnrichedEmployee[]) => pool,
}));

vi.mock("../src/services/apifyClient", () => ({
  filterOpenToWorkFromCache: (...args: unknown[]) => filterOpenToWorkFromCacheMock(...args),
  splitByTenure: (...args: unknown[]) => splitByTenureMock(...args),
  filterByKeywordsInApifyData: (...args: unknown[]) => filterByKeywordsInApifyDataMock(...args),
}));

vi.mock("../src/services/apolloBulkEmailEnrichment", () => ({
  findEmailsInBulk: (...args: unknown[]) => findEmailsInBulkMock(...args),
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
    filterPoolByStageMock.mockReset();
    filterOpenToWorkFromCacheMock.mockReset();
    splitByTenureMock.mockReset();
    filterByKeywordsInApifyDataMock.mockReset();
    findEmailsInBulkMock.mockReset();
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
    filterPoolByStageMock.mockImplementation((pool: EnrichedEmployee[]) => pool);
    filterOpenToWorkFromCacheMock.mockImplementation((employees: EnrichedEmployee[]) => ({
      kept: employees,
      warnings: [],
      filteredOut: [],
    }));
    splitByTenureMock.mockImplementation((employees: EnrichedEmployee[]) => ({ eligible: employees, droppedByTenure: [] }));
    filterByKeywordsInApifyDataMock.mockReturnValue({ matched: [], unmatched: [] });
    findEmailsInBulkMock.mockResolvedValue(new Map());
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
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 0, emailCount: 0, companiesReachedOutToCount: 0 });
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

  it("uses Apify bulk email enrichment before email campaign push", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    const candidates: TaggedEmailCandidate[] = [
      { employee: makeEmployee("email-1", "SRE", 8), campaignBucket: "sre" },
    ];
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates,
      filteredOutCandidates: [],
      warnings: [],
      normalEngineerApifyWarnings: [],
    });
    findEmailsInBulkMock.mockResolvedValueOnce(
      new Map([["linkedin.com/in/email-1", "has@example.com"]])
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

    expect(findEmailsInBulkMock).toHaveBeenCalledTimes(1);
    expect(candidates[0].employee.email).toBe("has@example.com");
    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
    expect(pushPeopleToLemlistEmailCampaignMock.mock.calls[0][0]).toEqual(candidates);
  });

  it("continues processing the next company while first email enrichment is still running", async () => {
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

    let allowFirstEmailEnrichmentToFinish: (() => void) | null = null;
    findEmailsInBulkMock.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          allowFirstEmailEnrichmentToFinish = () =>
            resolve(new Map([["linkedin.com/in/email-1", "acme@example.com"]]));
        })
    );
    findEmailsInBulkMock.mockResolvedValueOnce(
      new Map([["linkedin.com/in/email-2", "beta@example.com"]])
    );
    pushPeopleToLemlistEmailCampaignMock
      .mockResolvedValueOnce({
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
      })
      .mockResolvedValueOnce({
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
      expect(researchCompanyMock).toHaveBeenCalledTimes(2);
    });
    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(0);

    allowFirstEmailEnrichmentToFinish?.();

    await waitForCondition(() => {
      expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
    });
    expect(pushPeopleToLemlistEmailCampaignMock.mock.calls[0][1]).toBe("Acme");

    await runPromise;

    expect(findEmailsInBulkMock).toHaveBeenCalledTimes(2);
    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(2);
    expect(researchCompanyMock).toHaveBeenCalledTimes(2);
    expect(pushPeopleToLemlistEmailCampaignMock.mock.calls[1][1]).toBe("Beta");
  });

  it("waits for pending email task completion before finalizing after linkedin limit is reached", async () => {
    getWeeklySuccessCountsMock.mockReturnValueOnce({ linkedinCount: 99, emailCount: 0, companiesReachedOutToCount: 0 });
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([
        { companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 },
        { companyName: "Beta", companyDomain: "beta.com", companyLinkedinUrl: "", apolloAccountId: "org_2", rowNumber: 3 },
      ])
    );
    const linkedinSelection = [makeEmployee("linkedin-1", "SRE", 12)];
    const emailCandidates: TaggedEmailCandidate[] = [
      { employee: makeEmployee("email-1", "SRE", 8), campaignBucket: "sre" },
    ];
    scrapeCompanyEmployeesMock.mockResolvedValueOnce({
      employees: linkedinSelection,
      apifyCache: new Map(),
      profileCount: 1,
    });
    searchPeopleMock.mockResolvedValueOnce([{ id: "apollo-1", name: "Apollo One", title: "SRE" }]);
    selectTopSreForLemlistMock.mockReturnValueOnce(linkedinSelection);
    pushPeopleToLemlistCampaignMock.mockResolvedValueOnce({
      attempted: 1,
      successful: 1,
      failed: 0,
      successItems: ["Person linkedin-1"],
      failedItems: [],
      outcomes: [
        {
          key: "linkedin-1",
          name: "Person linkedin-1",
          title: "SRE",
          linkedinUrl: "https://linkedin.com/in/linkedin-1",
          status: "succeed",
        },
      ],
    });
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates: emailCandidates,
      filteredOutCandidates: [],
      warnings: [],
      normalEngineerApifyWarnings: [],
    });

    let releaseEnrichment: (() => void) | null = null;
    findEmailsInBulkMock.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          releaseEnrichment = () =>
            resolve(new Map([["linkedin.com/in/email-1", "acme@example.com"]]));
        })
    );
    pushPeopleToLemlistEmailCampaignMock.mockResolvedValueOnce({
      attempted: 1,
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
      expect(findEmailsInBulkMock).toHaveBeenCalledTimes(1);
    });
    expect(researchCompanyMock).toHaveBeenCalledTimes(1);
    expect(pushPeopleToLemlistEmailCampaignMock).not.toHaveBeenCalled();

    releaseEnrichment?.();
    await waitForCondition(() => {
      expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
    });
    await runPromise;

    expect(researchCompanyMock).toHaveBeenCalledTimes(1);
    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
    expect(emailCandidates[0].employee.email).toBe("acme@example.com");
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
    getWeeklySuccessCountsMock.mockReturnValueOnce({ linkedinCount: 100, emailCount: 0, companiesReachedOutToCount: 0 });
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
    findEmailsInBulkMock.mockReset();
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
    pushPeopleToLemlistCampaignMock.mockResolvedValue({ attempted: 0, successful: 0, failed: 0, successItems: [], failedItems: [], outcomes: [] });
    pushPeopleToLemlistEmailCampaignMock.mockResolvedValue({ attempted: 0, successful: 0, failed: 0, successItems: [], failedItems: [], outcomes: [] });
    searchPeopleMock.mockResolvedValue([]);
    researchCompanyMock.mockResolvedValue("Not found");
    getCompanyMock.mockResolvedValue({ companyName: "Acme", domain: "acme.com" });
    scrapeCompanyEmployeesMock.mockResolvedValue({ employees: [], apifyCache: new Map(), profileCount: 0 });
    filterPoolByStageMock.mockImplementation((pool: EnrichedEmployee[]) => pool);
    filterOpenToWorkFromCacheMock.mockImplementation((employees: EnrichedEmployee[]) => ({ kept: employees, warnings: [], filteredOut: [] }));
    splitByTenureMock.mockImplementation((employees: EnrichedEmployee[]) => ({ eligible: employees, droppedByTenure: [] }));
    filterByKeywordsInApifyDataMock.mockReturnValue({ matched: [], unmatched: [] });
    findEmailsInBulkMock.mockResolvedValue(new Map());
    runEmailCandidateWaterfallMock.mockResolvedValue(emptyWaterfallResult());
    selectTopSreForLemlistMock.mockImplementation((employees: EnrichedEmployee[]) => employees.slice(0, 7));
    fillToMinimumWithBackfillMock.mockImplementation((selected: EnrichedEmployee[]) => selected);
    selectKeywordMatchedByTenureMock.mockReturnValue({ forLinkedin: [], forEmailRecycling: [] });
    syncApolloAccountsFromOutputRowsMock.mockResolvedValue({ attemptedRows: 0, dedupedAccounts: 0, updatedAccounts: 0, skippedMissingAccountIdCount: 0, skippedNoMappableFieldsCount: 0, duplicateAccountIdCount: 0, warnings: [] });
    syncAttioCompaniesFromOutputRowsMock.mockResolvedValue({ attemptedRows: 0, dedupedDomains: 0, assertedCount: 0, failedCount: 0, skippedMissingDomainCount: 0, skippedNoMappableFieldsCount: 0, duplicateDomainCount: 0, warnings: [] });
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 0, emailCount: 0, companiesReachedOutToCount: 0 });
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
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 100, emailCount: 0, companiesReachedOutToCount: 0 });
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
