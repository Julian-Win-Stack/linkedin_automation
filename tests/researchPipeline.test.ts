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
const enrichMissingEmailsWithLemlistMock = vi.fn();
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
}));

vi.mock("../src/services/apifyClient", () => ({
  filterOpenToWorkFromCache: (...args: unknown[]) => filterOpenToWorkFromCacheMock(...args),
  splitByTenure: (...args: unknown[]) => splitByTenureMock(...args),
  filterByKeywordsInApifyData: (...args: unknown[]) => filterByKeywordsInApifyDataMock(...args),
}));

vi.mock("../src/services/apifyBulkEmailFinder", () => ({
  findEmailsInBulk: (...args: unknown[]) => findEmailsInBulkMock(...args),
}));

vi.mock("../src/services/lemlistBulkEmailEnrichment", () => ({
  enrichMissingEmailsWithLemlist: (...args: unknown[]) => enrichMissingEmailsWithLemlistMock(...args),
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
  LINKEDIN_KEYWORD_STAGE_INFRA: { currentTitles: ["Infrastructure"], minTenureMonths: 11, campaignBucket: "eng" },
  LINKEDIN_KEYWORD_STAGE_DEVOPS: { currentTitles: ["DevOps"], minTenureMonths: 11, campaignBucket: "eng" },
  LINKEDIN_KEYWORD_STAGE_NORMAL_ENG: { currentTitles: ["Engineer"], minTenureMonths: 11, campaignBucket: "eng" },
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
    tenure,
  };
}

function emptyWaterfallResult(): EmailWaterfallResult {
  return { candidates: [], filteredOutCandidates: [], warnings: [], normalEngineerApifyWarnings: [] };
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
    enrichMissingEmailsWithLemlistMock.mockReset();
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
    enrichMissingEmailsWithLemlistMock.mockResolvedValue({
      attempted: 0,
      accepted: 0,
      recovered: 0,
      notFound: 0,
    });
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
    process.env.LEMLIST_BULK_FIND_EMAIL_ENABLED = "true";
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

  it("uses apify bulk email finder before lemlist fallback", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    const candidates: TaggedEmailCandidate[] = [
      { employee: makeEmployee("missing-1", "SRE", 8, "https://linkedin.com/in/missing-1"), campaignBucket: "sre" },
    ];
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates,
      filteredOutCandidates: [],
      warnings: [],
      normalEngineerApifyWarnings: [],
    });
    findEmailsInBulkMock.mockResolvedValueOnce(
      new Map([["linkedin.com/in/missing-1", "found@example.com"]])
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
    expect(enrichMissingEmailsWithLemlistMock).not.toHaveBeenCalled();
    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
  });

  it("falls back to lemlist enrichment when apify finder misses", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", companyLinkedinUrl: "", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    const candidates: TaggedEmailCandidate[] = [
      { employee: makeEmployee("missing-2", "SRE", 8, "https://linkedin.com/in/missing-2"), campaignBucket: "sre" },
    ];
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates,
      filteredOutCandidates: [],
      warnings: [],
      normalEngineerApifyWarnings: [],
    });
    findEmailsInBulkMock.mockResolvedValueOnce(new Map());

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
    expect(enrichMissingEmailsWithLemlistMock).toHaveBeenCalledTimes(1);
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
