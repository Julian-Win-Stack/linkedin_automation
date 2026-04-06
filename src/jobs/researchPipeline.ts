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
  FilteredOutCampaignEntry,
  markJobDone,
  markJobError,
  setJobMessage,
  setJobProgress,
  setSkippedCompanies,
  setJobStatus,
  setJobSummary,
  setCampaignPushData,
  setRejectedCompanies,
} from "./jobStore";
import { EnrichedEmployee, ApifyOpenToWorkCache, LemlistPushOutcome, Prospect } from "../types/prospect";
import { SelectedUser } from "../shared/selectedUser";
import { runEmailCandidateWaterfall, TaggedEmailCandidate, LINKEDIN_KEYWORD_STAGE_INFRA, LINKEDIN_KEYWORD_STAGE_DEVOPS, LINKEDIN_KEYWORD_STAGE_NORMAL_ENG } from "../services/emailCandidateWaterfall";
import { filterOpenToWorkFromCache, splitByTenure, filterByKeywordsInApifyData } from "../services/apifyClient";
import { syncApolloAccountsFromOutputRows } from "../services/apolloBulkUpdateAccounts";
import { syncAttioCompaniesFromOutputRows } from "../services/attioAssertCompanyRecords";
import { getWeeklySuccessCounts, saveWeeklySuccessForJob } from "../services/weeklySuccessStore";
import { scrapeCompanyEmployees, scrapePastSreEmployees, filterPoolByStage } from "../services/apifyCompanyEmployees";

const MAX_ROWS = 500;
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
const COMPANY_LINKEDIN_URL_COLUMN = "Company Linkedin Url";
const WEEKLY_LINKEDIN_PUSH_LIMIT = 100;
const LINKEDIN_LEADERSHIP_TITLE_REGEX = /\b(director|svp|vp|head|chief)\b/i;

const SRE_WORK_KEYWORDS: string[] = [
  "on-call",
  "on call",
  "incident response",
  "incident management",
  "production incidents",
  "incident handling",
  "pagerduty",
  "pager duty",
  "opsgenie",
  "postmortem",
  "post-mortem",
  "root cause analysis",
  "RCA",
  "alerting",
  "alerting systems",
  "alert fatigue",
  "alert noise",
  "alert tuning",
  "datadog",
  "grafana",
  "prometheus",
  "SLO",
  "SLI",
  "error budget",
  "MTTR",
  "new relic",
  "elastic",
  "elasticsearch",
  "kibana",
  "opentelemetry",
  "jaeger",
  "zipkin",
  "alertmanager",
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
  const pendingEmailPushBatches: PendingEmailPushBatch[] = [];
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

  const _originalConsoleLog = console.log;
  try {
    console.log = () => {};

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
        continue;
      }

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
      const rawSreCount = currentSreProspects.length;
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
        const [poolResult, pastSreResult] = await Promise.all([
          scrapeCompanyEmployees({
            companyName: company.companyName,
            companyDomain: company.domain,
            companyLinkedinUrl: row.companyLinkedinUrl,
            maxItemsPerCompany: 100,
          }),
          scrapePastSreEmployees({
            companyName: company.companyName,
            companyDomain: company.domain,
            companyLinkedinUrl: row.companyLinkedinUrl,
            maxItemsPerCompany: 100,
          }),
        ]);
        const apifyCache = poolResult.apifyCache;
        for (const [key, value] of pastSreResult.apifyCache) {
          if (!apifyCache.has(key)) {
            apifyCache.set(key, value);
          }
        }
        const profilePool = dedupeEmployeesByKey(poolResult.employees);
        const pastSrePool = dedupeEmployeesByKey(pastSreResult.employees);
        logPipelineStage(
          "APIFY_COMPANY_POOL_DONE",
          `Company pool loaded. profiles=${poolResult.profileCount} mapped=${profilePool.length}`,
          companyContext
        );
        logPipelineStage(
          "APIFY_PAST_SRE_POOL_DONE",
          `Past SRE pool loaded. profiles=${pastSreResult.profileCount} mapped=${pastSrePool.length}`,
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
        const { eligible: tenureEligibleSre } = splitByTenure(currentSrePool, 3);
        const currentSreFiltered = filterOpenToWorkFromCache(tenureEligibleSre, apifyCache, {
          companyName: row.companyName,
          companyDomain: row.companyDomain,
        });
        for (const warning of currentSreFiltered.warnings) {
          addJobWarning(jobId, warning);
        }
        campaignPushData.filteredOutCandidates.push(
          ...currentSreFiltered.filteredOut.map(({ employee, reason }) => ({
            companyName: company.companyName,
            name: employee.name,
            title: employee.currentTitle,
            linkedinUrl: employee.linkedinUrl ?? null,
            reason,
          }))
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
            const { eligible: tenureEligible } = splitByTenure(stagePool, 3);
            const stageFiltered = filterOpenToWorkFromCache(tenureEligible, apifyCache, {
              companyName: row.companyName,
              companyDomain: row.companyDomain,
            });
            for (const warning of stageFiltered.warnings) {
              addJobWarning(jobId, warning);
            }
            campaignPushData.filteredOutCandidates.push(
              ...stageFiltered.filteredOut.map(({ employee, reason }) => ({
                companyName: company.companyName,
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                reason,
              }))
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
          const { eligible: tenureEligiblePastSre } = splitByTenure(pastSrePool, 3);
          const pastSreFiltered = filterOpenToWorkFromCache(tenureEligiblePastSre, apifyCache, {
            companyName: row.companyName,
            companyDomain: row.companyDomain,
          });
          campaignPushData.filteredOutCandidates.push(
            ...pastSreFiltered.filteredOut.map(({ employee, reason }) => ({
              companyName: company.companyName,
              name: employee.name,
              title: employee.currentTitle,
              linkedinUrl: employee.linkedinUrl ?? null,
              reason,
            }))
          );
          selectedForLemlist = fillToMinimumWithBackfill(selectedCurrentSre, pastSreFiltered.kept, [], {
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
            const { eligible: tenureEligiblePlatform } = splitByTenure(platformPool, 12);
            const platformFiltered = filterOpenToWorkFromCache(tenureEligiblePlatform, apifyCache, {
              companyName: row.companyName,
              companyDomain: row.companyDomain,
            });
            campaignPushData.filteredOutCandidates.push(
              ...platformFiltered.filteredOut.map(({ employee, reason }) => ({
                companyName: company.companyName,
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                reason,
              }))
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
          const taggedForLemlist: TaggedLinkedinCandidate[] = selectedForLemlist.map((emp) => {
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
            campaignPushData.normalEngineerApifyWarnings.push(
              ...waterfallResult.normalEngineerApifyWarnings.map(({ employee, problem }) => ({
                companyName: company.companyName,
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                problem,
              }))
            );
          }

          if (waterfallResult.filteredOutCandidates.length > 0) {
            const filteredEntries: FilteredOutCampaignEntry[] = waterfallResult.filteredOutCandidates.map(
              ({ employee, reason }) => ({
                companyName: company.companyName,
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                reason,
              })
            );
            campaignPushData.filteredOutCandidates.push(...filteredEntries);
          }

          if (waterfallResult.candidates.length > 0) {
            const emailBatch: PendingEmailPushBatch = {
              companyName: company.companyName,
              companyDomain: company.domain,
              candidates: waterfallResult.candidates,
            };
            pendingEmailPushBatches.push(emailBatch);
            logPipelineStage(
              "QUEUE_EMAIL_BATCH",
              `Email batch queued. candidates=${waterfallResult.candidates.length}`,
              companyContext
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
          sre_count: rawSreCount,
          notes: "",
        });
        syncableOutputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          observability_tool_research: observability,
          stage: "ChasingPOC",
          sre_count: rawSreCount,
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
          break;
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
    }

    if (lemlistEnabled && pendingEmailPushBatches.length > 0) {
      setJobMessage(jobId, `Pushing selected contacts to email campaigns (0/${pendingEmailPushBatches.length} companies).`);
      logPipelineStage(
        "EMAIL_PUSH_STAGE_START",
        `Email campaign push stage started. companies=${pendingEmailPushBatches.length}`
      );
      for (let emailBatchIndex = 0; emailBatchIndex < pendingEmailPushBatches.length; emailBatchIndex += 1) {
        const batch = pendingEmailPushBatches[emailBatchIndex];
        if (isCancelled(jobId)) {
          return;
        }

        setJobMessage(
          jobId,
          `Pushing selected contacts to email campaigns (${emailBatchIndex + 1}/${pendingEmailPushBatches.length} companies).`
        );
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
        totalLemlistSuccessful += emailPushMeta.successful;
        totalLemlistFailed += emailPushMeta.failed;
        const emailSkipped = emailPushMeta.outcomes.filter((outcome) => outcome.status === "skipped").length;
        totalLemlistSkipped += emailSkipped;
        totalEmailCampaignSuccessful += emailPushMeta.successful;
        totalEmailCampaignFailed += emailPushMeta.failed;
        totalEmailCampaignSkipped += emailSkipped;
        logPipelineStage(
          "EMAIL_PUSH_COMPANY_DONE",
          `Email push complete. successful=${emailPushMeta.successful} failed=${emailPushMeta.failed} skipped=${emailSkipped}`,
          {
            index: pendingEmailPushBatches.indexOf(batch),
            total: pendingEmailPushBatches.length,
            companyName: batch.companyName,
          }
        );
      }
    }

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
    saveWeeklySuccessForJob({
      jobId,
      selectedUser,
      completedAtMs: Date.now(),
      linkedinSuccessCount: totalLinkedinCampaignSuccessful,
      emailSuccessCount: totalEmailCampaignSuccessful,
    });
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
    const syncRows: OutputRow[] = [
      ...syncableOutputRows,
      ...rejectedAsOutputRows,
    ];

    const [apolloSyncOutcome, attioSyncOutcome] = await Promise.allSettled([
      syncApolloAccountsFromOutputRows(syncRows),
      syncAttioCompaniesFromOutputRows(syncRows),
    ]);

    if (apolloSyncOutcome.status === "fulfilled") {
      for (const warning of apolloSyncOutcome.value.warnings) {
        addJobWarning(jobId, warning);
      }
    } else {
      const message =
        apolloSyncOutcome.reason instanceof Error
          ? apolloSyncOutcome.reason.message
          : "Unknown Apollo bulk account sync error";
      addJobWarning(jobId, `Apollo bulk account sync failed: ${message}`);
      logPipelineStage("APOLLO_BULK_ACCOUNT_SYNC_FAILED", `Apollo bulk account sync failed. error=${message}`);
    }

    if (attioSyncOutcome.status === "fulfilled") {
      for (const warning of attioSyncOutcome.value.warnings) {
        addJobWarning(jobId, warning);
      }
    } else {
      const message =
        attioSyncOutcome.reason instanceof Error ? attioSyncOutcome.reason.message : "Unknown Attio assert sync error";
      addJobWarning(jobId, `Attio company sync failed: ${message}`);
      logPipelineStage("ATTIO_COMPANY_SYNC_FAILED", `Attio company sync failed. error=${message}`);
    }

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
  } finally {
    console.log = _originalConsoleLog;
  }
}
