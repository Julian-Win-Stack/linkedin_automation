import { getEnvBoolean } from "../config/env";
import { PipelineConfig } from "../config/pipelineConfig";
import { getCompany, ResolvedCompany } from "../services/getCompany";
import { pushPeopleToLemlistEmailCampaign } from "../services/lemlistEmailPushQueue";
import { pushPeopleToLemlistCampaign, TaggedLinkedinCandidate } from "../services/lemlistPushQueue";
import {
  PeopleSearchFilters,
  searchPeople,
} from "../services/searchPeople";
import { countProcessableCompanies, readCompanies } from "../services/observability/csvReader";
import { researchCompany } from "../services/observability/openaiClient";
import { fillToMinimumWithBackfill, selectTopSreForLemlist, selectKeywordMatchedByTenure } from "../services/sreSelection";
import {
  OutputRow,
  RejectedOutputRow,
  rowsToCsvString,
} from "../services/observability/csvWriter";
import {
  addJobWarning,
  getJob,
  JobSummary,
  CampaignPushData,
  CampaignPushEntry,
  FilteredOutReason,
  markJobDone,
  markJobError,
  setJobMessage,
  setJobProgress,
  setSkippedCompanies,
  setJobStatus,
  setJobSummary,
  setCampaignPushData,
  setRejectedCompanies,
  setJobPartialResults,
} from "./jobStore";
import { EnrichedEmployee, ApifyOpenToWorkCache, LemlistPushOutcome, Prospect } from "../types/prospect";
import { SelectedUser } from "../shared/selectedUser";
import { runEmailCandidateWaterfall, TaggedEmailCandidate, LINKEDIN_KEYWORD_STAGE_INFRA, LINKEDIN_KEYWORD_STAGE_DEVOPS, LINKEDIN_KEYWORD_STAGE_NORMAL_ENG } from "../services/emailCandidateWaterfall";
import { filterOpenToWorkFromCache, filterByKeywordsInApifyData } from "../services/apifyClient";
import { syncApolloAccountsFromOutputRows } from "../services/apolloBulkUpdateAccounts";
import { syncAttioCompaniesFromOutputRows } from "../services/attioAssertCompanyRecords";
import { getWeeklySuccessCounts, saveWeeklySuccessForJob } from "../services/weeklySuccessStore";
import { scrapeCompanyEmployees, filterPoolByStage, filterByPastExperienceKeywords } from "../services/apifyCompanyEmployees";
import { findEmailsInBulk } from "../services/apolloBulkEmailEnrichment";

const MAX_ROWS = 500;
const CHECKPOINT_SIZE = 50;
const SRE_PERSON_TITLES = [
  "SRE",
  "Site Reliability",
  "Site Reliability Engineer",
  "Site Reliability Engineering",
  "Head of Reliability",
];
const MAX_RESULTS = 30;
/** Current-title exclusions for LinkedIn Apollo searches (SRE, past SRE, platform backfill). */
const LINKEDIN_APOLLO_NOT_TITLES: string[] = [];

function linkedinApolloPeopleFilters(filters: PeopleSearchFilters): PeopleSearchFilters {
  return { ...filters, notTitles: LINKEDIN_APOLLO_NOT_TITLES };
}
const REJECTED_REASON = "rejected because they were using other observability tools";
const MAX_SRE_COUNT = 15;
const PAST_SRE_EXPERIENCE_KEYWORDS = ["SRE", "Site Reliability"];
const COMPANY_LINKEDIN_URL_COLUMN = "Company Linkedin Url";
const WEEKLY_LINKEDIN_PUSH_LIMIT = 100;
const LINKEDIN_LEADERSHIP_TITLE_REGEX = /\b(director|svp|vp|head|chief)\b/i;
const EMAIL_TITLE_REJECT_REGEX = /\b(data|front[\s-]?end)\b/i;
const ENG_LEAD_EMAIL_TITLE_REGEX = /\b(vice\s+principal|vp|director|chief|head)\b/i;

const SRE_WORK_KEYWORDS: string[] = [
  "Oncall",
  "On-call",
  "incident response",
  "incident resolution",
  "incident management",
  "incident commander",
  "incident handling",
  "production incident",
  "production outage",
  "service outage",
  "live site incident",
  "triage",
  "availability improvement",
  "uptime improvement",
  "high availability systems",
  "resiliency",
  "fault tolerance",
  "SLO",
  "SLI",
  "error budget",
  "error budget policies",
  "postmortem",
  "post-mortem",
  "root cause analysis",
  "RCA",
  "incident review",
  "incident retrospective",
  "alert tuning",
  "alert optimization",
  "alert fatigue reduction",
  "alert noise reduction",
  "alert strategy",
  "alert design",
  "MTTR",
  "MTTD",
  "mean time to recovery",
  "mean time to detect",
  "incident metrics",
  "pagerduty",
  "pager duty",
  "opsgenie",
  "incident tooling",
  "reliability",
  "high traffic systems",
  "scalability issues",
  "capacity planning",
  "traffic spikes",
  "load handling",
  "automation",
  "AI adoption",
  "Terraform",
];

const LINKEDIN_KEYWORD_STAGES = [
  { label: "DevOps", config: LINKEDIN_KEYWORD_STAGE_DEVOPS },
  { label: "Infrastructure", config: LINKEDIN_KEYWORD_STAGE_INFRA },
  { label: "Normal Engineer", config: LINKEDIN_KEYWORD_STAGE_NORMAL_ENG },
];

interface PendingEmailPushBatch {
  companyName: string;
  companyDomain: string;
  candidates: TaggedEmailCandidate[];
}

type LinkedinPoolBucket = "sre" | "eng" | "engLead";

interface LinkedinPoolCandidate {
  employee: EnrichedEmployee;
  linkedinBucket: LinkedinPoolBucket;
}

interface WarningProblemCounter {
  count: number;
}

function getOrCreateFilteredOutSummary(
  campaignPushData: CampaignPushData,
  companyName: string
) {
  let existing = campaignPushData.filteredOutCandidates.find((entry) => entry.companyName === companyName);
  if (!existing) {
    existing = {
      companyName,
      openToWorkCount: 0,
      frontendRoleCount: 0,
      contractEmploymentCount: 0,
    };
    campaignPushData.filteredOutCandidates.push(existing);
  }
  return existing;
}

function addFilteredOutCounts(
  campaignPushData: CampaignPushData,
  companyName: string,
  reasons: FilteredOutReason[]
): void {
  if (reasons.length === 0) {
    return;
  }
  const summary = getOrCreateFilteredOutSummary(campaignPushData, companyName);
  for (const reason of reasons) {
    if (reason === "open_to_work") {
      summary.openToWorkCount += 1;
    } else if (reason === "frontend_role") {
      summary.frontendRoleCount += 1;
    } else if (reason === "contract_employment") {
      summary.contractEmploymentCount += 1;
    }
  }
}

function getOrCreateWarningSummary(
  campaignPushData: CampaignPushData,
  companyName: string
) {
  let existing = campaignPushData.normalEngineerApifyWarnings.find((entry) => entry.companyName === companyName);
  if (!existing) {
    existing = {
      companyName,
      totalCount: 0,
      problems: [],
    };
    campaignPushData.normalEngineerApifyWarnings.push(existing);
  }
  return existing;
}

function addWarningProblemCounts(
  campaignPushData: CampaignPushData,
  companyName: string,
  problems: string[]
): void {
  if (problems.length === 0) {
    return;
  }
  const summary = getOrCreateWarningSummary(campaignPushData, companyName);
  const problemMap = new Map<string, WarningProblemCounter>(
    summary.problems.map((entry) => [entry.problem, { count: entry.count }])
  );
  for (const problem of problems) {
    const existing = problemMap.get(problem);
    if (existing) {
      existing.count += 1;
    } else {
      problemMap.set(problem, { count: 1 });
    }
    summary.totalCount += 1;
  }
  summary.problems = [...problemMap.entries()]
    .map(([problem, entry]) => ({ problem, count: entry.count }))
    .sort((a, b) => b.count - a.count || a.problem.localeCompare(b.problem));
}

function isCancelled(jobId: string): boolean {
  const job = getJob(jobId);
  return !job || job.status === "cancelled";
}

function shouldProcessByObservability(observability: string): boolean {
  const normalized = observability.toLowerCase();
  if (normalized.trim() === "not found") {
    return true;
  }
  return normalized.includes("datadog") || normalized.includes("grafana") || normalized.includes("prometheus");
}

function dedupeProspectsById<T extends { id: string }>(items: T[]): T[] {
  const seen = new Set<string>();
  const deduped: T[] = [];

  for (const item of items) {
    if (seen.has(item.id)) {
      continue;
    }
    seen.add(item.id);
    deduped.push(item);
  }

  return deduped;
}

function toEmployeeKey(employee: EnrichedEmployee): string {
  return employee.id ?? `${employee.name}|${employee.currentTitle}|${employee.linkedinUrl ?? ""}`;
}

function toOutcomeMap(outcomes: LemlistPushOutcome[]): Map<string, LemlistPushOutcome> {
  return new Map(outcomes.map((outcome) => [outcome.key, outcome]));
}

function toCampaignPushEntry(
  employee: EnrichedEmployee,
  outcomeByKey: Map<string, LemlistPushOutcome>,
  companyName: string
): CampaignPushEntry {
  const outcome = outcomeByKey.get(toEmployeeKey(employee));
  if (!outcome) {
    return {
      companyName,
      name: employee.name,
      title: employee.currentTitle,
      linkedinUrl: employee.linkedinUrl ?? null,
      lemlistStatus: "failed",
      lemlistError: "Lemlist result missing for this candidate.",
    };
  }
  return {
    companyName,
    name: employee.name,
    title: employee.currentTitle,
    linkedinUrl: employee.linkedinUrl ?? null,
    lemlistStatus: outcome.status,
    ...(outcome.error ? { lemlistError: outcome.error } : {}),
  };
}

function isLinkedinLeadershipTitle(title: string | null | undefined): boolean {
  if (!title) {
    return false;
  }
  return LINKEDIN_LEADERSHIP_TITLE_REGEX.test(title);
}

function toLinkedinPoolBucket(employee: EnrichedEmployee): LinkedinPoolBucket {
  return isLinkedinLeadershipTitle(employee.currentTitle) ? "engLead" : "sre";
}

function normalizeLinkedinUrlForLookup(url: string): string {
  return url
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/+$/, "")
    .toLowerCase();
}

async function findEmailsForBatch(
  batch: PendingEmailPushBatch,
  jobId: string,
  companyContext: { index: number; total: number; companyName: string }
): Promise<void> {
  const enrichmentInput = batch.candidates
    .filter(({ employee }) => (employee.linkedinUrl?.trim() ?? "").length > 0)
    .map(({ employee }) => ({
      name: employee.name,
      domain: batch.companyDomain,
      linkedinUrl: employee.linkedinUrl!.trim(),
    }));

  if (enrichmentInput.length === 0) {
    return;
  }

  try {
    const emailsByLinkedin = await findEmailsInBulk(enrichmentInput);
    for (const candidate of batch.candidates) {
      const linkedinUrl = candidate.employee.linkedinUrl?.trim() ?? "";
      if (!linkedinUrl || (candidate.employee.email && candidate.employee.email.trim().length > 0)) {
        continue;
      }
      const foundEmail = emailsByLinkedin.get(normalizeLinkedinUrlForLookup(linkedinUrl));
      if (foundEmail) {
        candidate.employee.email = foundEmail;
      }
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown Apollo bulk email enrichment error";
    addJobWarning(jobId, `Apollo bulk email enrichment failed for ${batch.companyName}: ${message}`);
  }
}

async function enrichAndPushEmailBatch(
  batch: PendingEmailPushBatch,
  jobId: string,
  companyContext: { index: number; total: number; companyName: string },
  selectedUser: SelectedUser,
  campaignPushData: CampaignPushData,
  onTotals: (result: { successful: number; failed: number; skipped: number }) => void
): Promise<void> {
  await findEmailsForBatch(batch, jobId, companyContext);
  const emailPushMeta = await pushPeopleToLemlistEmailCampaign(
    batch.candidates,
    batch.companyName,
    batch.companyDomain,
    selectedUser
  );
  const emailOutcomeByKey = toOutcomeMap(emailPushMeta.outcomes);
  for (const { employee, campaignBucket } of batch.candidates) {
    const entry = toCampaignPushEntry(employee, emailOutcomeByKey, batch.companyName);
    if (campaignBucket === "sre") {
      campaignPushData.emailSre.push(entry);
    } else if (campaignBucket === "engLead") {
      campaignPushData.emailEngLead.push(entry);
    } else {
      campaignPushData.emailEng.push(entry);
    }
  }
  onTotals({
    successful: emailPushMeta.successful,
    failed: emailPushMeta.failed,
    skipped: emailPushMeta.outcomes.filter((outcome) => outcome.status === "skipped").length,
  });
}


function dedupeEmployeesByKey(employees: EnrichedEmployee[]): EnrichedEmployee[] {
  const seen = new Set<string>();
  const deduped: EnrichedEmployee[] = [];
  for (const employee of employees) {
    const key = toEmployeeKey(employee);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(employee);
  }
  return deduped;
}

function logPipelineInfo(_line: string): void {
  // Intentionally muted to reduce noisy normal-color logs.
}

function logCurrentSrePrefilterResults(companyName: string, prospects: Prospect[], maxResults: number): void {
  const cappedSuffix = prospects.length >= maxResults ? ` (returned cap of ${maxResults})` : "";
  console.error("");
  console.error(`SRE pre-filter results for ${companyName}: ${prospects.length}${cappedSuffix}`);

  if (prospects.length === 0) {
    console.error("  (no SREs returned)");
    return;
  }

  for (const [index, prospect] of prospects.entries()) {
    const name = prospect.name.trim() || "Unknown";
    const title = prospect.title.trim() || "Unknown title";
    console.error(`  ${index + 1}. ${name} | ${title} | ${prospect.id}`);
  }
}

function canTrustSrePrefilter(company: ResolvedCompany): boolean {
  return company.domain.trim().length > 0;
}

function createPipelineStepLogger(jobId: string): (
  step: string,
  message: string,
  companyContext?: { index: number; total: number; companyName: string }
)=> void {
  void jobId;
  return (
    _step: string,
    _message: string,
    _companyContext?: { index: number; total: number; companyName: string }
  ): void => {
    // Intentionally muted to remove verbose pipeline stage logs.
  };
}

async function resolveCompanyForApolloInput(row: {
  companyName: string;
  companyDomain: string;
}): Promise<ResolvedCompany> {
  if (row.companyDomain) {
    return getCompany(row.companyDomain);
  }
  return {
    companyName: row.companyName,
    domain: "",
  };
}

export async function runResearchPipeline(
  jobId: string,
  csvBuffer: string,
  config: PipelineConfig,
  selectedUser: SelectedUser,
  weekStartMs: number
): Promise<void> {
  const logPipelineStage = createPipelineStepLogger(jobId);
  const outputRows: OutputRow[] = [];
  const syncableOutputRows: OutputRow[] = [];
  const rejectedOutputRows: RejectedOutputRow[] = [];
  const rejectedCompanies: string[] = [];
  const skippedCompanies: string[] = [];
  let batchEmailTasks: Promise<void>[] = [];
  let companiesSinceLastCheckpoint = 0;
  let lastCheckpointSyncableIndex = 0;
  let lastCheckpointRejectedIndex = 0;
  let totalRows = 0;
  let skippedMissingWebsiteAndApolloAccountIdCount = 0;
  let apolloProcessedCompanyCount = 0;
  let totalSreFound = 0;
  let totalLinkedinCampaignSuccessful = 0;
  let totalLinkedinCampaignFailed = 0;
  let totalLinkedinCampaignSkipped = 0;
  let totalLemlistSuccessful = 0;
  let totalLemlistFailed = 0;
  let totalLemlistSkipped = 0;
  let totalEmailCampaignSuccessful = 0;
  let totalEmailCampaignFailed = 0;
  let totalEmailCampaignSkipped = 0;
  let eligibleCompanyCount = 0;
  let weeklyLimitSkippedCompanyCount = 0;
  const weeklyCounts = getWeeklySuccessCounts({ selectedUser, weekStartMs });
  let sessionLinkedinSuccessfulCount = 0;
  let weeklyLimitWarningAdded = false;

  const campaignPushData: CampaignPushData = {
    linkedinSre: [],
    linkedinEngLead: [],
    linkedinEng: [],
    emailSre: [],
    emailEng: [],
    emailEngLead: [],
    filteredOutCandidates: [],
    normalEngineerApifyWarnings: [],
  };

  async function flushCheckpoint(): Promise<void> {
    if (batchEmailTasks.length > 0) {
      await Promise.allSettled(batchEmailTasks);
      batchEmailTasks = [];
    }

    const newSyncableRows = syncableOutputRows.slice(lastCheckpointSyncableIndex);
    const newRejectedRows: OutputRow[] = rejectedOutputRows.slice(lastCheckpointRejectedIndex).map((row) => ({
      company_name: row.company_name,
      company_domain: row.company_domain,
      company_linkedin_url: row.company_linkedin_url,
      apollo_account_id: row.apollo_account_id,
      observability_tool_research: row.observability_tool_research,
      stage: row.status,
      sre_count: row.sre_count,
      notes: row.notes,
    }));
    lastCheckpointSyncableIndex = syncableOutputRows.length;
    lastCheckpointRejectedIndex = rejectedOutputRows.length;

    const [apolloOutcome, attioOutcome] = await Promise.allSettled([
      syncApolloAccountsFromOutputRows([...newSyncableRows, ...newRejectedRows]),
      syncAttioCompaniesFromOutputRows([...newSyncableRows, ...newRejectedRows]),
    ]);
    if (apolloOutcome.status === "rejected") {
      const msg = apolloOutcome.reason instanceof Error ? apolloOutcome.reason.message : "Unknown";
      addJobWarning(jobId, `Apollo bulk account sync failed (checkpoint): ${msg}`);
    } else {
      for (const w of apolloOutcome.value.warnings) {
        addJobWarning(jobId, w);
      }
    }
    if (attioOutcome.status === "rejected") {
      const msg = attioOutcome.reason instanceof Error ? attioOutcome.reason.message : "Unknown";
      addJobWarning(jobId, `Attio sync failed (checkpoint): ${msg}`);
    } else {
      for (const w of attioOutcome.value.warnings) {
        addJobWarning(jobId, w);
      }
    }

    saveWeeklySuccessForJob({
      jobId,
      selectedUser,
      completedAtMs: Date.now(),
      linkedinSuccessCount: totalLinkedinCampaignSuccessful,
      emailSuccessCount: totalEmailCampaignSuccessful,
    });

    const allRejectedSoFar: OutputRow[] = rejectedOutputRows.map((row) => ({
      company_name: row.company_name,
      company_domain: row.company_domain,
      company_linkedin_url: row.company_linkedin_url,
      apollo_account_id: row.apollo_account_id,
      observability_tool_research: row.observability_tool_research,
      stage: row.status,
      sre_count: row.sre_count,
      notes: row.notes,
    }));
    const partialCsvString = await rowsToCsvString([...outputRows, ...allRejectedSoFar]);
    const partialCsvBase64 = Buffer.from(partialCsvString, "utf8").toString("base64");
    setJobPartialResults(jobId, partialCsvBase64, campaignPushData);

    companiesSinceLastCheckpoint = 0;
  }

  try {
    setJobStatus(jobId, "processing");
    logPipelineStage("JOB_START", `Job started. selected_user=${selectedUser}`);
    setJobMessage(jobId, "Starting engineer and SRE pre-filter...");

    const lemlistEnabled = getEnvBoolean("LEMLIST_PUSH_ENABLED", true);
    const progressTotalRows = Math.min(
      await countProcessableCompanies({
        csvBuffer,
        domainColumn: config.domainColumn,
        apolloAccountIdColumn: config.apolloAccountIdColumn,
      }),
      MAX_ROWS
    );

    for await (const row of readCompanies({
      csvBuffer,
      nameColumn: config.nameColumn,
      domainColumn: config.domainColumn,
      linkedinUrlColumn: COMPANY_LINKEDIN_URL_COLUMN,
      apolloAccountIdColumn: config.apolloAccountIdColumn,
      onSkipRow: (skipInfo) => {
        if (skipInfo.reason === "missing_website_and_apollo_account_id") {
          skippedMissingWebsiteAndApolloAccountIdCount += 1;
          skippedCompanies.push(skipInfo.companyName || `Row ${skipInfo.rowNumber}`);
        }
      },
    })) {
      if (isCancelled(jobId)) {
        return;
      }

      totalRows += 1;
      if (totalRows > MAX_ROWS) {
        addJobWarning(jobId, `Row limit reached (${MAX_ROWS}). Remaining rows skipped.`);
        break;
      }

      setJobProgress(jobId, { currentRow: totalRows, totalRows: progressTotalRows });
      setJobMessage(jobId, `Engineer/SRE pre-filter row ${row.rowNumber}: ${row.companyName}`);
      const companyContext = { index: totalRows - 1, total: progressTotalRows, companyName: row.companyName };

      let shouldBreakAfterCompany = false;

      if (weeklyCounts.linkedinCount + sessionLinkedinSuccessfulCount >= WEEKLY_LINKEDIN_PUSH_LIMIT) {
        weeklyLimitSkippedCompanyCount += 1;
        skippedCompanies.push(`${row.companyName} (weekly LinkedIn limit reached)`);
        outputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          observability_tool_research: "",
          stage: "",
          sre_count: "",
          notes: "",
        });
        if (!weeklyLimitWarningAdded) {
          addJobWarning(
            jobId,
            `Weekly LinkedIn push limit (${WEEKLY_LINKEDIN_PUSH_LIMIT}) reached. Remaining companies were fully skipped.`
          );
          weeklyLimitWarningAdded = true;
        }
      } else {

      const company = await resolveCompanyForApolloInput(row);
      const peopleSearchFilters: PeopleSearchFilters = {
        apolloOrganizationId: row.apolloAccountId,
      };

      logPipelineStage("SEARCH_CURRENT_SRE", "Searching current SRE candidates for pre-filter.", companyContext);
      const trustCurrentSrePrefilter = canTrustSrePrefilter(company);
      const currentSreProspects = trustCurrentSrePrefilter
        ? dedupeProspectsById(
            await searchPeople(company, MAX_RESULTS, SRE_PERSON_TITLES, linkedinApolloPeopleFilters(peopleSearchFilters))
          )
        : [];
      const apolloSreCount = currentSreProspects.length;
      let rawSreCount = apolloSreCount;
      let exportedSreCount = apolloSreCount;
      if (trustCurrentSrePrefilter) {
        logCurrentSrePrefilterResults(row.companyName, currentSreProspects, MAX_RESULTS);
      } else {
        console.error("");
        console.error(`SRE pre-filter skipped for ${row.companyName}: missing company domain makes Apollo org-id-only search unreliable`);
      }
      logPipelineStage("SEARCH_CURRENT_SRE_DONE", `Current SRE candidates found. count=${rawSreCount}`, companyContext);
      if (trustCurrentSrePrefilter && rawSreCount > MAX_SRE_COUNT) {
        const rejectionNote = `${row.companyName} got rejected because it has ${rawSreCount} number of SREs`;
        logPipelineStage("REJECT_SRE_MAX", `Company rejected by SRE maximum. count=${rawSreCount}`, companyContext);
        rejectedCompanies.push(rejectionNote);
        rejectedOutputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          observability_tool_research: "",
          sre_count: rawSreCount,
          status: "NotActionableNow",
          notes: rejectionNote,
        });
        continue;
      }

      setJobMessage(jobId, `Observability research row ${row.rowNumber}: ${row.companyName}`);
      const observability = await researchCompany(row.companyName, row.companyDomain, {
        apiKey: config.azureOpenAiApiKey,
        baseUrl: config.azureOpenAiBaseUrl,
        model: config.model,
        maxCompletionTokens: config.maxCompletionTokens,
        searchApiKey: config.searchApiKey,
      });
      if (!shouldProcessByObservability(observability)) {
        rejectedCompanies.push(
          `Company ${row.companyName} was rejected because it was using ${observability.trim() || "other observability tools"}`
        );
        rejectedOutputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          observability_tool_research: observability,
          sre_count: rawSreCount,
          status: "NotActionableNow",
          notes: observability.trim() || REJECTED_REASON,
        });
        continue;
      }

      eligibleCompanyCount += 1;
      setJobMessage(jobId, `Apollo stage row ${row.rowNumber}: ${row.companyName}`);
      logPipelineStage("COMPANY_START", "Company processing started.", companyContext);

      try {
        logPipelineInfo(`\n${"═".repeat(78)}\n  APIFY COMPANY POOL — ${row.companyName} (${row.companyDomain})\n${"═".repeat(78)}\n\n`);
        logPipelineStage("APIFY_COMPANY_POOL_START", "Fetching company-wide engineering pool from Apify.", companyContext);
        const poolResult = await scrapeCompanyEmployees({
          companyName: company.companyName,
          companyDomain: company.domain,
          companyLinkedinUrl: row.companyLinkedinUrl,
          maxItemsPerCompany: 30,
        });
        const apifyCache = poolResult.apifyCache;
        const profilePool = dedupeEmployeesByKey(poolResult.employees);
        logPipelineStage(
          "APIFY_COMPANY_POOL_DONE",
          `Company pool loaded. profiles=${poolResult.profileCount} mapped=${profilePool.length}`,
          companyContext
        );

        const linkedinCandidates: LinkedinPoolCandidate[] = [];
        let keywordMatchedEmailRecycled: EnrichedEmployee[] = [];
        let prePlatformKeys: Set<string> | null = null;

        const currentSrePool = profilePool.filter((employee) =>
          SRE_PERSON_TITLES.some(
            (keyword) =>
              employee.currentTitle.toLowerCase().includes(keyword.toLowerCase()) ||
              (employee.headline ?? "").toLowerCase().includes(keyword.toLowerCase())
          )
        );
        const linkedinCurrentSreCount = currentSrePool.length;
        if (!trustCurrentSrePrefilter) {
          rawSreCount = linkedinCurrentSreCount;
          exportedSreCount = rawSreCount;
          console.error("");
          console.error(`SRE count from Apify for ${row.companyName}: ${rawSreCount}`);
          for (const [index, employee] of currentSrePool.entries()) {
            console.error(`  ${index + 1}. ${employee.name} | ${employee.currentTitle}`);
          }
          logPipelineStage("SEARCH_CURRENT_SRE_DONE", `Apify SRE count derived. count=${rawSreCount}`, companyContext);
          if (rawSreCount > MAX_SRE_COUNT) {
            const rejectionNote = `${row.companyName} got rejected because it has ${rawSreCount} number of SREs`;
            logPipelineStage("REJECT_SRE_MAX", `Company rejected by SRE maximum (Apify count). count=${rawSreCount}`, companyContext);
            rejectedCompanies.push(rejectionNote);
            rejectedOutputRows.push({
              company_name: row.companyName,
              company_domain: row.companyDomain,
              company_linkedin_url: row.companyLinkedinUrl,
              apollo_account_id: row.apolloAccountId ?? "",
              observability_tool_research: "",
              sre_count: rawSreCount,
              status: "NotActionableNow",
              notes: rejectionNote,
            });
            continue;
          }
        } else {
          exportedSreCount = Math.max(apolloSreCount, linkedinCurrentSreCount);
        }
        const currentSreKeys = new Set(currentSrePool.map(toEmployeeKey));
        const nonSrePool = profilePool.filter((emp) => !currentSreKeys.has(toEmployeeKey(emp)));
        const pastSrePool = filterByPastExperienceKeywords(nonSrePool, apifyCache, PAST_SRE_EXPERIENCE_KEYWORDS);
        logPipelineStage(
          "PAST_SRE_POOL_DERIVED",
          `Past SRE pool derived locally. count=${pastSrePool.length}`,
          companyContext
        );
        const currentSreFiltered = filterOpenToWorkFromCache(currentSrePool, apifyCache, {
          companyName: row.companyName,
          companyDomain: row.companyDomain,
        });
        for (const warning of currentSreFiltered.warnings) {
          addJobWarning(jobId, warning);
        }
        addFilteredOutCounts(
          campaignPushData,
          company.companyName,
          currentSreFiltered.filteredOut.map(({ reason }) => reason)
        );
        const selectedCurrentSre = selectTopSreForLemlist(currentSreFiltered.kept, 7);
        linkedinCandidates.push(
          ...selectedCurrentSre.map((employee) => ({
            employee,
            linkedinBucket: toLinkedinPoolBucket(employee),
          }))
        );

        {
          logPipelineInfo(`\n${"═".repeat(78)}\n  LINKEDIN KEYWORD EXPANSION — ${row.companyName} (${row.companyDomain})\n${"═".repeat(78)}\n\n`);
          logPipelineStage("KEYWORD_EXPANSION_START", "LinkedIn keyword expansion started.", companyContext);
          const allKeywordMatched: EnrichedEmployee[] = [];
          const alreadyLinkedinKeys = new Set(linkedinCandidates.map((candidate) => toEmployeeKey(candidate.employee)));

          for (const { label, config: stageConfig } of LINKEDIN_KEYWORD_STAGES) {
            logPipelineInfo(`  ▸ Filtering ${label} candidates from local pool...\n`);
            const stagePool = filterPoolByStage(profilePool, apifyCache, {
              currentTitles: stageConfig.currentTitles,
              pastTitles: stageConfig.pastTitles,
              notTitles: stageConfig.notTitles,
              notPastTitles: stageConfig.notPastTitles,
            }).filter((employee) => !alreadyLinkedinKeys.has(toEmployeeKey(employee)));
            const stageFiltered = filterOpenToWorkFromCache(stagePool, apifyCache, {
              companyName: row.companyName,
              companyDomain: row.companyDomain,
            });
            for (const warning of stageFiltered.warnings) {
              addJobWarning(jobId, warning);
            }
            addFilteredOutCounts(
              campaignPushData,
              company.companyName,
              stageFiltered.filteredOut.map(({ reason }) => reason)
            );
            const { matched } = filterByKeywordsInApifyData(stageFiltered.kept, apifyCache, SRE_WORK_KEYWORDS);
            allKeywordMatched.push(...matched);
            logPipelineInfo(`  ▸ ${label}: ${matched.length} matched SRE keywords\n`);
          }

          if (allKeywordMatched.length > 0) {
            const selectedForLinkedin = linkedinCandidates.map((candidate) => candidate.employee);
            const { forLinkedin, forEmailRecycling } = selectKeywordMatchedByTenure(
              allKeywordMatched,
              selectedForLinkedin,
              7
            );
            linkedinCandidates.push(
              ...forLinkedin.map((employee) => ({
                employee,
                linkedinBucket: toLinkedinPoolBucket(employee),
              }))
            );
            keywordMatchedEmailRecycled = forEmailRecycling;
          }
          logPipelineStage(
            "KEYWORD_EXPANSION_DONE",
            `Keyword expansion complete. linkedin_total=${linkedinCandidates.length} recycled=${keywordMatchedEmailRecycled.length}`,
            companyContext
          );
        }

        let selectedForLemlist = dedupeEmployeesByKey(linkedinCandidates.map((candidate) => candidate.employee));
        if (selectedForLemlist.length < 7) {
          logPipelineStage("BACKFILL_PHASE_1_START", "Backfill phase 1 (past SRE) started.", companyContext);
          const pastSreFiltered = filterOpenToWorkFromCache(pastSrePool, apifyCache, {
            companyName: row.companyName,
            companyDomain: row.companyDomain,
          });
          addFilteredOutCounts(
            campaignPushData,
            company.companyName,
            pastSreFiltered.filteredOut.map(({ reason }) => reason)
          );
          selectedForLemlist = fillToMinimumWithBackfill(selectedForLemlist, pastSreFiltered.kept, [], {
            minimum: 7,
            max: 7,
          });
          logPipelineStage(
            "BACKFILL_PHASE_1_DONE",
            `Backfill phase 1 complete. selected_after_phase1=${selectedForLemlist.length}`,
            companyContext
          );

          if (selectedForLemlist.length < 5) {
            prePlatformKeys = new Set(selectedForLemlist.map(toEmployeeKey));
            logPipelineStage("BACKFILL_PHASE_2_START", "Backfill phase 2 (platform) started.", companyContext);
            const platformPool = filterPoolByStage(profilePool, apifyCache, {
              currentTitles: ["platform engineer"],
            });
            const platformFiltered = filterOpenToWorkFromCache(platformPool, apifyCache, {
              companyName: row.companyName,
              companyDomain: row.companyDomain,
            });
            addFilteredOutCounts(
              campaignPushData,
              company.companyName,
              platformFiltered.filteredOut.map(({ reason }) => reason)
            );
            selectedForLemlist = fillToMinimumWithBackfill(selectedForLemlist, [], platformFiltered.kept, {
              minimum: 5,
              max: 5,
            });
            logPipelineStage(
              "BACKFILL_PHASE_2_DONE",
              `Backfill phase 2 complete. selected_after_phase2=${selectedForLemlist.length}`,
              companyContext
            );
          }
        }

        apolloProcessedCompanyCount += 1;
        totalSreFound += rawSreCount;

        let lemlistSuccessful = 0;
        let lemlistFailed = 0;
        if (lemlistEnabled && selectedForLemlist.length > 0) {
          const taggedForLemlist: TaggedLinkedinCandidate[] = selectedForLemlist
            .filter((emp) => !EMAIL_TITLE_REJECT_REGEX.test(emp.currentTitle))
            .map((emp) => {
              const fromPrePlatformPhase = prePlatformKeys === null || prePlatformKeys.has(toEmployeeKey(emp));
              const isLeadershipTitle = isLinkedinLeadershipTitle(emp.currentTitle);
              const linkedinBucket = fromPrePlatformPhase
                ? (isLeadershipTitle ? "engLead" as const : "sre" as const)
                : (isLeadershipTitle ? "engLead" as const : "eng" as const);
              return {
                employee: emp,
                linkedinBucket,
              };
            });
          {
            const sreForLinkedin = taggedForLemlist.filter((c) => c.linkedinBucket === "sre");
            const divider = "─".repeat(70);
            console.error("");
            console.error("╔" + "═".repeat(70) + "╗");
            console.error(`║  LINKEDIN SRE CANDIDATES — ${row.companyName} (${sreForLinkedin.length} candidate${sreForLinkedin.length === 1 ? "" : "s"})`.padEnd(71) + "║");
            console.error("╚" + "═".repeat(70) + "╝");
            for (const [i, { employee }] of sreForLinkedin.entries()) {
              const cached = employee.linkedinUrl
                ? apifyCache.get(normalizeLinkedinUrlForLookup(employee.linkedinUrl))
                : null;
              const currentExp = cached?.experience.find((e) => !e.endDate || e.endDate.text?.trim().toLowerCase() === "present")
                ?? cached?.experience[0]
                ?? null;
              const desc = currentExp?.description?.trim() || "—";
              const expSkills = currentExp?.skills?.length ? currentExp.skills.join(", ") : null;
              const profileSkills = cached?.profileSkills?.length
                ? cached.profileSkills.map((s: { name: string }) => s.name).join(", ")
                : null;
              const skills = expSkills ?? profileSkills ?? "—";
              console.error("");
              console.error(`  ${i + 1}. ${employee.name}`);
              console.error(`     Title  : ${employee.currentTitle}`);
              console.error(`     Desc   : ${desc}`);
              console.error(`     Skills : ${skills}`);
              if (i < sreForLinkedin.length - 1) {
                console.error("");
                console.error(`  ${divider}`);
              }
            }
            if (sreForLinkedin.length === 0) {
              console.error("  (no SRE candidates)");
            }
            console.error("");
          }

          logPipelineInfo(`  ▸ Pushing ${taggedForLemlist.length} candidates to LinkedIn campaign...\n`);
          logPipelineStage(
            "PUSH_LINKEDIN_START",
            `Pushing LinkedIn campaigns. candidates=${taggedForLemlist.length}`,
            companyContext
          );
          const lemlistMeta = await pushPeopleToLemlistCampaign(
            taggedForLemlist,
            company.companyName,
            company.domain,
            selectedUser
          );
          const linkedinOutcomeByKey = toOutcomeMap(lemlistMeta.outcomes);
          for (const tagged of taggedForLemlist) {
            const entry = toCampaignPushEntry(tagged.employee, linkedinOutcomeByKey, row.companyName);
            if (tagged.linkedinBucket === "sre") {
              campaignPushData.linkedinSre.push(entry);
            } else if (tagged.linkedinBucket === "engLead") {
              campaignPushData.linkedinEngLead.push(entry);
            } else {
              campaignPushData.linkedinEng.push(entry);
            }
          }
          lemlistSuccessful = lemlistMeta.successful;
          lemlistFailed = lemlistMeta.failed;
          const lemlistSkipped = lemlistMeta.outcomes.filter((outcome) => outcome.status === "skipped").length;
          sessionLinkedinSuccessfulCount += lemlistSuccessful;
          totalLinkedinCampaignSuccessful += lemlistSuccessful;
          totalLinkedinCampaignFailed += lemlistFailed;
          totalLinkedinCampaignSkipped += lemlistSkipped;
          totalLemlistSuccessful += lemlistSuccessful;
          totalLemlistFailed += lemlistFailed;
          totalLemlistSkipped += lemlistSkipped;
          logPipelineInfo(
            `  ▸ LinkedIn push done — ${lemlistSuccessful} successful, ${lemlistFailed} failed, ${lemlistSkipped} skipped\n`
          );
          logPipelineStage(
            "PUSH_LINKEDIN_DONE",
            `LinkedIn push complete. successful=${lemlistSuccessful} failed=${lemlistFailed} skipped=${lemlistSkipped}`,
            companyContext
          );
        }

        if (lemlistEnabled) {
          const attemptedLinkedinKeys = new Set(selectedForLemlist.map((employee) => toEmployeeKey(employee)));
          logPipelineInfo(`  ▸ Starting email candidate waterfall...\n`);
          logPipelineStage("EMAIL_WATERFALL_START", "Email candidate waterfall started.", companyContext);
          const waterfallResult = await runEmailCandidateWaterfall(
            company,
            attemptedLinkedinKeys,
            profilePool,
            apifyCache,
            { rawSreCount, recycledKeywordMatched: keywordMatchedEmailRecycled }
          );
          logPipelineStage(
            "EMAIL_WATERFALL_DONE",
            `Email candidate waterfall complete. candidates=${waterfallResult.candidates.length}`,
            companyContext
          );

          for (const warning of waterfallResult.warnings) {
            addJobWarning(jobId, warning);
          }

          if (waterfallResult.normalEngineerApifyWarnings.length > 0) {
            addWarningProblemCounts(
              campaignPushData,
              company.companyName,
              waterfallResult.normalEngineerApifyWarnings.map(({ problem }) => problem)
            );
          }

          if (waterfallResult.filteredOutCandidates.length > 0) {
            addFilteredOutCounts(
              campaignPushData,
              company.companyName,
              waterfallResult.filteredOutCandidates.map(({ reason }) => reason)
            );
          }

          const emailCandidates = waterfallResult.candidates.filter(
            ({ employee, campaignBucket }) => {
              if (EMAIL_TITLE_REJECT_REGEX.test(employee.currentTitle)) return false;
              if (campaignBucket === "engLead" && !ENG_LEAD_EMAIL_TITLE_REGEX.test(employee.currentTitle)) return false;
              return true;
            }
          );

          if (emailCandidates.length > 0) {
            const emailBatch: PendingEmailPushBatch = {
              companyName: company.companyName,
              companyDomain: company.domain,
              candidates: emailCandidates,
            };
            batchEmailTasks.push(
              enrichAndPushEmailBatch(
                emailBatch,
                jobId,
                companyContext,
                selectedUser,
                campaignPushData,
                ({ successful, failed, skipped }) => {
                  totalLemlistSuccessful += successful;
                  totalLemlistFailed += failed;
                  totalLemlistSkipped += skipped;
                  totalEmailCampaignSuccessful += successful;
                  totalEmailCampaignFailed += failed;
                  totalEmailCampaignSkipped += skipped;
                }
              ).catch((error) => {
                const message = error instanceof Error ? error.message : "Unknown email company task error";
                addJobWarning(jobId, `Email push failed for ${emailBatch.companyName}: ${message}`);
              })
            );
          }
        }

        const shouldStopAfterCurrentCompany =
          weeklyCounts.linkedinCount + sessionLinkedinSuccessfulCount >= WEEKLY_LINKEDIN_PUSH_LIMIT;

        outputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          observability_tool_research: observability,
          stage: "ChasingPOC",
          sre_count: exportedSreCount,
          notes: "",
        });
        syncableOutputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          observability_tool_research: observability,
          stage: "ChasingPOC",
          sre_count: exportedSreCount,
          notes: "",
        });
        logPipelineStage("COMPANY_DONE", "Company processing complete.", companyContext);

        if (shouldStopAfterCurrentCompany) {
          const estimatedRemainingCompanies = Math.max(progressTotalRows - totalRows, 0);
          weeklyLimitSkippedCompanyCount += estimatedRemainingCompanies;
          if (!weeklyLimitWarningAdded) {
            addJobWarning(
              jobId,
              `Weekly LinkedIn push limit (${WEEKLY_LINKEDIN_PUSH_LIMIT}) reached. Remaining companies were fully skipped.`
            );
            weeklyLimitWarningAdded = true;
          }
          logPipelineStage(
            "WEEKLY_LINKEDIN_LIMIT_REACHED",
            `LinkedIn weekly limit reached after company processing. remaining_companies_estimate=${estimatedRemainingCompanies}`,
            companyContext
          );
          shouldBreakAfterCompany = true;
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown pipeline error";
        addJobWarning(jobId, `Apollo/Lemlist failed for ${row.companyName}: ${message}`);
        logPipelineStage("COMPANY_FAILED", `Company failed. error=${message}`, companyContext);
        outputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          observability_tool_research: observability,
          stage: "ChasingPOC",
          sre_count: 0,
          notes: "",
        });
        syncableOutputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          observability_tool_research: observability,
          stage: "ChasingPOC",
          sre_count: 0,
          notes: "",
        });
      }

      } // end else (not weekly-limit skipped)

      companiesSinceLastCheckpoint += 1;
      if (companiesSinceLastCheckpoint >= CHECKPOINT_SIZE) {
        await flushCheckpoint();
      }

      if (shouldBreakAfterCompany) {
        break;
      }
    }

    await flushCheckpoint();

    setJobMessage(jobId, "Finalizing results and syncing company updates.");
    setRejectedCompanies(jobId, rejectedCompanies, REJECTED_REASON);
    setSkippedCompanies(jobId, skippedCompanies);

    const summary: JobSummary = {
      totalRows,
      eligibleCompanyCount,
      rejectedCompanyCount: rejectedCompanies.length,
      skippedMissingWebsiteAndApolloAccountIdCount,
      apolloProcessedCompanyCount,
      totalSreFound,
      totalLinkedinCampaignSuccessful,
      totalLinkedinCampaignFailed,
      totalLinkedinCampaignSkipped,
      totalLemlistSuccessful,
      totalLemlistFailed,
      totalLemlistSkipped,
      totalEmailCampaignSuccessful,
      totalEmailCampaignFailed,
      totalEmailCampaignSkipped,
      weeklyLimitSkippedCompanyCount,
    };
    setJobSummary(jobId, summary);
    setCampaignPushData(jobId, campaignPushData);

    const rejectedAsOutputRows: OutputRow[] = rejectedOutputRows.map((row) => ({
        company_name: row.company_name,
        company_domain: row.company_domain,
        company_linkedin_url: row.company_linkedin_url,
        apollo_account_id: row.apollo_account_id,
        observability_tool_research: row.observability_tool_research,
        stage: row.status,
        sre_count: row.sre_count,
        notes: row.notes,
      }));
    const combinedOutputRows: OutputRow[] = [
      ...outputRows,
      ...rejectedAsOutputRows,
    ];

    const csvString = await rowsToCsvString(combinedOutputRows);
    const csvBase64 = Buffer.from(csvString, "utf8").toString("base64");
    setJobMessage(jobId, "Completed. CSV and PDF are ready to download.");
    markJobDone(jobId, csvBase64);
    logPipelineStage(
      "JOB_DONE",
      `Job done: processed=${apolloProcessedCompanyCount} linkedin_success=${totalLinkedinCampaignSuccessful} linkedin_skipped=${totalLinkedinCampaignSkipped} lemlist_success=${totalLemlistSuccessful} lemlist_failed=${totalLemlistFailed} lemlist_skipped=${totalLemlistSkipped}`
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected job failure";
    markJobError(jobId, message);
    logPipelineStage("JOB_FAILED", `Job failed. error=${message}`);
  }
}
