import { beforeEach, describe, expect, it, vi } from "vitest";
import { createJob, getJob } from "../src/jobs/jobStore";
import { runResearchPipeline } from "../src/jobs/researchPipeline";
import { EnrichedEmployee, Prospect } from "../src/types/prospect";

const readCompaniesMock = vi.fn();
const researchCompanyMock = vi.fn();
const getCompanyMock = vi.fn();
const countEngineerPeopleMock = vi.fn();
const searchPeopleMock = vi.fn();
const searchPastSrePeopleMock = vi.fn();
const searchCurrentPlatformEngineerPeopleMock = vi.fn();
const bulkEnrichPeopleMock = vi.fn();
const selectTopSreForLemlistMock = vi.fn();
const fillToMinimumWithBackfillMock = vi.fn();
const pushPeopleToLemlistCampaignMock = vi.fn();
const rowsToCsvStringMock = vi.fn();
const rejectedRowsToCsvStringMock = vi.fn();

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
}));

vi.mock("../src/services/sreSelection", () => ({
  selectTopSreForLemlist: (...args: unknown[]) => selectTopSreForLemlistMock(...args),
  fillToMinimumWithBackfill: (...args: unknown[]) => fillToMinimumWithBackfillMock(...args),
}));

vi.mock("../src/services/lemlistPushQueue", () => ({
  pushPeopleToLemlistCampaign: (...args: unknown[]) => pushPeopleToLemlistCampaignMock(...args),
}));

vi.mock("../src/services/observability/csvWriter", () => ({
  rowsToCsvString: (...args: unknown[]) => rowsToCsvStringMock(...args),
  rejectedRowsToCsvString: (...args: unknown[]) => rejectedRowsToCsvStringMock(...args),
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
    selectTopSreForLemlistMock.mockReset();
    fillToMinimumWithBackfillMock.mockReset();
    pushPeopleToLemlistCampaignMock.mockReset();
    rowsToCsvStringMock.mockReset();
    rejectedRowsToCsvStringMock.mockReset();

    rowsToCsvStringMock.mockResolvedValue("company_name\nAcme\n");
    rejectedRowsToCsvStringMock.mockResolvedValue("company_name,engineer_count\n");
    pushPeopleToLemlistCampaignMock.mockResolvedValue({
      attempted: 0,
      successful: 0,
      failed: 0,
      successItems: [],
      failedItems: [],
    });
    researchCompanyMock.mockResolvedValue("Not found");
    getCompanyMock.mockResolvedValue({ companyName: "Acme", domain: "acme.com" });
    countEngineerPeopleMock.mockResolvedValue(120);
  });

  it("uses backfill in two phases with max 7 then max 5", async () => {
    readCompaniesMock.mockReturnValueOnce(
      asyncCompanyRows([{ companyName: "Acme", companyDomain: "acme.com", apolloAccountId: "org_1", rowNumber: 2 }])
    );
    searchPeopleMock.mockResolvedValueOnce(makeProspects(3, "current"));
    searchPastSrePeopleMock.mockResolvedValueOnce(makeProspects(4, "past"));
    searchCurrentPlatformEngineerPeopleMock.mockResolvedValueOnce(makeProspects(4, "platform"));
    bulkEnrichPeopleMock
      .mockResolvedValueOnce(makeEmployees(3, "current")) // current SRE enrich
      .mockResolvedValueOnce(makeEmployees(4, "past")) // past enrich
      .mockResolvedValueOnce(makeEmployees(4, "platform")); // platform enrich
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
    });

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
    });

    expect(searchPastSrePeopleMock).not.toHaveBeenCalled();
    expect(searchCurrentPlatformEngineerPeopleMock).not.toHaveBeenCalled();
    expect(fillToMinimumWithBackfillMock).not.toHaveBeenCalled();
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
    });

    const rejectedRowsArg = rejectedRowsToCsvStringMock.mock.calls[0]?.[0] as Array<Record<string, unknown>>;
    expect(rejectedRowsArg).toHaveLength(1);
    expect(rejectedRowsArg[0].engineer_count).toBe("> 1000");

    const job = getJob(jobId);
    expect(job?.rejectedCompanies[0]).toContain("> 1000");
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
    });

    const job = getJob(jobId);
    expect(job?.skippedCompanies).toEqual(["Skipped Co"]);
    expect(job?.summary?.skippedMissingWebsiteAndApolloAccountIdCount).toBe(1);
  });
});
