import { beforeEach, describe, expect, it, vi } from "vitest";
import { createJob, getJob } from "../src/jobs/jobStore";
import { runResearchPipeline } from "../src/jobs/researchPipeline";
import { EnrichedEmployee, Prospect } from "../src/types/prospect";
import { TaggedEmailCandidate, EmailWaterfallResult } from "../src/services/emailCandidateWaterfall";

const readCompaniesMock = vi.fn();
const researchCompanyMock = vi.fn();
const getCompanyMock = vi.fn();
const countEngineerPeopleMock = vi.fn();
const searchPeopleMock = vi.fn();
const searchPastSrePeopleMock = vi.fn();
const searchCurrentPlatformEngineerPeopleMock = vi.fn();
const bulkEnrichPeopleMock = vi.fn();
const runWaterfallEmailForPersonIdsMock = vi.fn();
const enrichMissingEmailsWithLemlistMock = vi.fn();
const selectTopSreForLemlistMock = vi.fn();
const fillToMinimumWithBackfillMock = vi.fn();
const pushPeopleToLemlistCampaignMock = vi.fn();
const pushPeopleToLemlistEmailCampaignMock = vi.fn();
const runEmailCandidateWaterfallMock = vi.fn();
const rowsToCsvStringMock = vi.fn();
const scrapeAndFilterOpenToWorkMock = vi.fn();
const splitByTenureMock = vi.fn();

vi.mock("../src/services/observability/csvReader", () => ({
  readCompanies: (...args: unknown[]) => readCompaniesMock(...args),
}));

vi.mock("../src/services/observability/openaiClient", () => ({
  researchCompany: (...args: unknown[]) => researchCompanyMock(...args),
}));

vi.mock("../src/services/getCompany", () => ({
  getCompany: (...args: unknown[]) => getCompanyMock(...args),
}));

vi.mock("../src/services/searchPeople", () => ({
  countEngineerPeople: (...args: unknown[]) => countEngineerPeopleMock(...args),
  searchPeople: (...args: unknown[]) => searchPeopleMock(...args),
  searchPastSrePeople: (...args: unknown[]) => searchPastSrePeopleMock(...args),
  searchCurrentPlatformEngineerPeople: (...args: unknown[]) => searchCurrentPlatformEngineerPeopleMock(...args),
}));

vi.mock("../src/services/bulkEnrichPeople", () => ({
  bulkEnrichPeople: (...args: unknown[]) => bulkEnrichPeopleMock(...args),
  runWaterfallEmailForPersonIds: (...args: unknown[]) => runWaterfallEmailForPersonIdsMock(...args),
}));

vi.mock("../src/services/lemlistBulkEmailEnrichment", () => ({
  enrichMissingEmailsWithLemlist: (...args: unknown[]) => enrichMissingEmailsWithLemlistMock(...args),
}));

vi.mock("../src/services/sreSelection", () => ({
  selectTopSreForLemlist: (...args: unknown[]) => selectTopSreForLemlistMock(...args),
  fillToMinimumWithBackfill: (...args: unknown[]) => fillToMinimumWithBackfillMock(...args),
}));

vi.mock("../src/services/lemlistPushQueue", () => ({
  pushPeopleToLemlistCampaign: (...args: unknown[]) => pushPeopleToLemlistCampaignMock(...args),
}));

vi.mock("../src/services/lemlistEmailPushQueue", () => ({
  pushPeopleToLemlistEmailCampaign: (...args: unknown[]) => pushPeopleToLemlistEmailCampaignMock(...args),
}));

vi.mock("../src/services/emailCandidateWaterfall", () => ({
  runEmailCandidateWaterfall: (...args: unknown[]) => runEmailCandidateWaterfallMock(...args),
}));

vi.mock("../src/services/observability/csvWriter", () => ({
  rowsToCsvString: (...args: unknown[]) => rowsToCsvStringMock(...args),
}));

vi.mock("../src/services/apifyClient", () => ({
  scrapeAndFilterOpenToWork: (...args: unknown[]) => scrapeAndFilterOpenToWorkMock(...args),
  splitByTenure: (...args: unknown[]) => splitByTenureMock(...args),
}));

function asyncCompanyRows(
  rows: Array<{ companyName: string; companyDomain: string; apolloAccountId?: string; rowNumber: number }>
) {
  return (async function* () {
    for (const row of rows) {
      yield row;
    }
  })();
}

function makeProspects(count: number, prefix: string): Prospect[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `${prefix}-${index + 1}`,
    name: `${prefix} ${index + 1}`,
    title: "SRE",
  }));
}

function makeEmployees(count: number, prefix: string): EnrichedEmployee[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `${prefix}-${index + 1}`,
    startDate: "2022-01-01",
    endDate: null,
    name: `${prefix} ${index + 1}`,
    linkedinUrl: null,
    currentTitle: "SRE",
    tenure: 24 - index,
  }));
}

function emptyWaterfallResult(): EmailWaterfallResult {
  return { candidates: [], filteredOutCandidates: [], warnings: [], normalEngineerApifyWarnings: [] };
}

describe("runResearchPipeline orchestration", () => {
  beforeEach(() => {
    readCompaniesMock.mockReset();
    researchCompanyMock.mockReset();
    getCompanyMock.mockReset();
    countEngineerPeopleMock.mockReset();
    searchPeopleMock.mockReset();
    searchPastSrePeopleMock.mockReset();
    searchCurrentPlatformEngineerPeopleMock.mockReset();
    bulkEnrichPeopleMock.mockReset();
    runWaterfallEmailForPersonIdsMock.mockReset();
    enrichMissingEmailsWithLemlistMock.mockReset();
    selectTopSreForLemlistMock.mockReset();
    fillToMinimumWithBackfillMock.mockReset();
    pushPeopleToLemlistCampaignMock.mockReset();
    pushPeopleToLemlistEmailCampaignMock.mockReset();
    runEmailCandidateWaterfallMock.mockReset();
    rowsToCsvStringMock.mockReset();
    scrapeAndFilterOpenToWorkMock.mockReset();
    splitByTenureMock.mockReset();

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
    searchCurrentPlatformEngineerPeopleMock.mockResolvedValue([]);
    searchPastSrePeopleMock.mockResolvedValue([]);
    bulkEnrichPeopleMock.mockResolvedValue([]);
    runWaterfallEmailForPersonIdsMock.mockResolvedValue(new Map());
    enrichMissingEmailsWithLemlistMock.mockResolvedValue({
      attempted: 0,
      accepted: 0,
      recovered: 0,
      notFound: 0,
    });
    runEmailCandidateWaterfallMock.mockResolvedValue(emptyWaterfallResult());
    researchCompanyMock.mockResolvedValue("Not found");
    getCompanyMock.mockResolvedValue({ companyName: "Acme", domain: "acme.com" });
    countEngineerPeopleMock.mockResolvedValue(120);
    scrapeAndFilterOpenToWorkMock.mockImplementation(async (employees: EnrichedEmployee[]) => ({
      kept: employees,
      warnings: [],
      filteredOut: [],
    }));
    splitByTenureMock.mockImplementation((employees: EnrichedEmployee[]) => ({ eligible: employees }));
    process.env.LEMLIST_PUSH_ENABLED = "true";
    process.env.LEMLIST_BULK_FIND_EMAIL_ENABLED = "true";
    delete process.env.APOLLO_WATERFALL_ENABLED;
  });

  it("uses backfill in two phases with max 7 then max 5", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce(makeProspects(3, "current"));
    searchPastSrePeopleMock.mockResolvedValueOnce(makeProspects(4, "past"));
    searchCurrentPlatformEngineerPeopleMock.mockResolvedValueOnce(makeProspects(4, "platform"));
    bulkEnrichPeopleMock
      .mockResolvedValueOnce(makeEmployees(3, "current"))
      .mockResolvedValueOnce(makeEmployees(4, "past"))
      .mockResolvedValueOnce(makeEmployees(4, "platform"));
    selectTopSreForLemlistMock.mockReturnValueOnce(makeEmployees(3, "selected-current"));
    fillToMinimumWithBackfillMock
      .mockReturnValueOnce(makeEmployees(4, "after-past-phase"))
      .mockReturnValueOnce(makeEmployees(5, "after-platform-phase"));

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", {
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "m",
      maxCompletionTokens: 1000,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    }, "julian");

    expect(fillToMinimumWithBackfillMock).toHaveBeenCalledTimes(2);
    expect(fillToMinimumWithBackfillMock).toHaveBeenNthCalledWith(
      1,
      expect.any(Array),
      expect.any(Array),
      [],
      { minimum: 5, max: 7 }
    );
    expect(fillToMinimumWithBackfillMock).toHaveBeenNthCalledWith(
      2,
      expect.any(Array),
      [],
      expect.any(Array),
      { minimum: 5, max: 5 }
    );
    expect(searchPeopleMock).toHaveBeenCalledWith(
      { companyName: "Acme", domain: "acme.com" },
      30,
      ["SRE", "Site Reliability", "Site Reliability Engineer", "Site Reliability Engineering", "Head of Reliability"],
      { apolloOrganizationId: "org_1", notTitles: ["contract"] }
    );
  });

  it("skips all backfill when raw current SRE count is zero", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce([]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([]);
    selectTopSreForLemlistMock.mockReturnValueOnce([]);

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", {
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "m",
      maxCompletionTokens: 1000,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    }, "julian");

    expect(searchPastSrePeopleMock).not.toHaveBeenCalled();
    expect(fillToMinimumWithBackfillMock).not.toHaveBeenCalled();
  });

  it("calls email waterfall and pushes candidates to email campaign", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce([]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([]);
    selectTopSreForLemlistMock.mockReturnValueOnce([]);

    const waterfallCandidates: TaggedEmailCandidate[] = [
      {
        employee: {
          id: "sre-email-1",
          startDate: "2022-01-01",
          endDate: null,
          name: "SRE Email",
          email: "sre.email@example.com",
          linkedinUrl: null,
          currentTitle: "SRE",
          tenure: 5,
        },
        campaignBucket: "sre",
      },
      {
        employee: {
          id: "eng-email-1",
          startDate: "2022-01-01",
          endDate: null,
          name: "Eng Email",
          email: "eng.email@example.com",
          linkedinUrl: null,
          currentTitle: "Infrastructure",
          tenure: 15,
        },
        campaignBucket: "eng",
      },
    ];
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates: waterfallCandidates,
      filteredOutCandidates: [],
      warnings: [],
      normalEngineerApifyWarnings: [],
    });
    pushPeopleToLemlistEmailCampaignMock.mockResolvedValueOnce({
      attempted: 2,
      successful: 2,
      failed: 0,
      successItems: ["SRE Email", "Eng Email"],
      failedItems: [],
      outcomes: [],
    });

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", {
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "m",
      maxCompletionTokens: 1000,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    }, "julian");

    expect(runEmailCandidateWaterfallMock).toHaveBeenCalledTimes(1);
    expect(runEmailCandidateWaterfallMock).toHaveBeenCalledWith(
      { companyName: "Acme", domain: "acme.com" },
      expect.any(Set),
      expect.any(Map),
      { apolloOrganizationId: "org_1" },
      expect.any(Map)
    );
    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledWith(
      waterfallCandidates,
      "Acme",
      "acme.com",
      "julian"
    );
    const job = getJob(jobId);
    expect(job?.summary?.totalLemlistSuccessful).toBe(2);
  });

  it("tracks missing email person ids from waterfall candidates", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce([]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([]);
    selectTopSreForLemlistMock.mockReturnValueOnce([]);

    const waterfallCandidates: TaggedEmailCandidate[] = [
      {
        employee: {
          id: "missing-email-1",
          startDate: "2022-01-01",
          endDate: null,
          name: "Missing Email",
          email: null,
          linkedinUrl: null,
          currentTitle: "SRE",
          tenure: 5,
        },
        campaignBucket: "sre",
      },
    ];
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates: waterfallCandidates,
      filteredOutCandidates: [],
      warnings: [],
      normalEngineerApifyWarnings: [],
    });
    enrichMissingEmailsWithLemlistMock.mockImplementationOnce(
      async (candidates: Array<{ employee: EnrichedEmployee }>) => {
        const target = candidates.find((c) => c.employee.id === "missing-email-1");
        if (target) {
          target.employee.email = "recovered@example.com";
        }
        return { attempted: 1, accepted: 1, recovered: 1, notFound: 0 };
      }
    );

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", {
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "m",
      maxCompletionTokens: 1000,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    }, "julian");

    expect(enrichMissingEmailsWithLemlistMock).toHaveBeenCalledTimes(1);
    expect(pushPeopleToLemlistEmailCampaignMock).toHaveBeenCalledTimes(1);
  });

  it("masks engineer count above 1000 in rejected outputs", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "BigCo", companyDomain: "big.co", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    countEngineerPeopleMock.mockResolvedValueOnce(1501);

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", {
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "m",
      maxCompletionTokens: 1000,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    }, "julian");

    const combinedRowsArg = rowsToCsvStringMock.mock.calls[0]?.[0] as Array<Record<string, unknown>>;
    const rejectedRow = combinedRowsArg.find((row) => row.company_name === "BigCo");
    expect(rejectedRow?.engineer_count).toBe("> 1000");

    const job = getJob(jobId);
    expect(job?.rejectedCompanies[0]).toContain("> 1000");
  });

  it("builds combined import csv with passed rows first and rejected rows last", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([
        { companyName: "PassCo", companyDomain: "pass.co", apolloAccountId: "org_1", rowNumber: 2 },
        { companyName: "RejectCo", companyDomain: "reject.co", apolloAccountId: "org_2", rowNumber: 3 },
      ])
    );
    researchCompanyMock.mockResolvedValueOnce("Datadog").mockResolvedValueOnce("Other observability tool");
    searchPeopleMock.mockResolvedValueOnce([]).mockResolvedValueOnce([]);
    selectTopSreForLemlistMock.mockReturnValueOnce([]);

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
      "julian"
    );

    const combinedRowsArg = rowsToCsvStringMock.mock.calls[0]?.[0] as Array<Record<string, unknown>>;
    expect(combinedRowsArg).toHaveLength(2);
    expect(combinedRowsArg[0]).toMatchObject({
      company_name: "PassCo",
      stage: "ChasingPOC",
      notes: "",
    });
    expect(combinedRowsArg[1]).toMatchObject({
      company_name: "RejectCo",
      stage: "NotActionableNow",
      notes: "Other observability tool",
    });
  });

  it("captures skipped companies and skip summary from csv reader callback", async () => {
    readCompaniesMock.mockImplementationOnce((options: { onSkipRow?: (info: { reason: string; companyName: string; rowNumber: number }) => void }) => {
      options.onSkipRow?.({
        reason: "missing_website_and_apollo_account_id",
        companyName: "Skipped Co",
        rowNumber: 2,
      });
      return asyncCompanyRows([]);
    });

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", {
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "m",
      maxCompletionTokens: 1000,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    }, "julian");

    const job = getJob(jobId);
    expect(job?.skippedCompanies).toEqual(["Skipped Co"]);
    expect(job?.summary?.skippedMissingWebsiteAndApolloAccountIdCount).toBe(1);
  });

  it("stores normal engineer Apify warning entries in campaign push data and not UI warnings", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce([]);
    bulkEnrichPeopleMock.mockResolvedValueOnce([]);
    selectTopSreForLemlistMock.mockReturnValueOnce([]);
    runEmailCandidateWaterfallMock.mockResolvedValueOnce({
      candidates: [],
      filteredOutCandidates: [],
      warnings: [],
      normalEngineerApifyWarnings: [
        {
          employee: {
            id: "warn-1",
            startDate: "2022-01-01",
            endDate: null,
            name: "Warn Person",
            email: null,
            linkedinUrl: "https://linkedin.com/in/warn-person",
            currentTitle: "Staff Engineer",
            tenure: 12,
          },
          problem: "Could not match this profile to Acme in Apify experience data.",
        },
      ],
    });

    const jobId = createJob();
    await runResearchPipeline(jobId, "csv", {
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "m",
      maxCompletionTokens: 1000,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    }, "julian");

    const job = getJob(jobId);
    expect(job?.warnings).toEqual([]);
    expect(job?.campaignPushData?.normalEngineerApifyWarnings).toEqual([
      {
        companyName: "Acme",
        name: "Warn Person",
        title: "Staff Engineer",
        linkedinUrl: "https://linkedin.com/in/warn-person",
        problem: "Could not match this profile to Acme in Apify experience data.",
      },
    ]);
  });
});
