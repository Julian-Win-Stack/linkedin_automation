import { getEnvBoolean } from "../config/env";
import { PipelineConfig } from "../config/pipelineConfig";
import {
  bulkEnrichPeople,
  EnrichmentCache,
  runWaterfallEmailForPersonIds,
} from "../services/bulkEnrichPeople";
import { getCompany, ResolvedCompany } from "../services/getCompany";
import { enrichMissingEmailsWithLemlist } from "../services/lemlistBulkEmailEnrichment";
import { pushPeopleToLemlistEmailCampaign } from "../services/lemlistEmailPushQueue";
import { pushPeopleToLemlistCampaign, TaggedLinkedinCandidate } from "../services/lemlistPushQueue";
import {
  searchCurrentPlatformEngineerPeople,
  searchPastSrePeople,
  PeopleSearchFilters,
  searchPeople,
  searchEmailCandidatePeopleCached,
  ApolloSearchCache,
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
import { scrapeAndFilterOpenToWork, splitByTenure, filterByKeywordsInApifyData } from "../services/apifyClient";
import { syncApolloAccountsFromOutputRows } from "../services/apolloBulkUpdateAccounts";
import { syncAttioCompaniesFromOutputRows } from "../services/attioAssertCompanyRecords";
import { getWeeklySuccessCounts, saveWeeklySuccessForJob } from "../services/weeklySuccessStore";

const MAX_ROWS = 500;
const SRE_PERSON_TITLES = [
  "SRE",
  "Site Reliability",
  "Site Reliability Engineer",
  "Site Reliability Engineering",
  "Head of Reliability",
  "observability",
];
const MAX_RESULTS = 30;
/** Current-title exclusions for LinkedIn Apollo searches (SRE, past SRE, platform backfill). */
const LINKEDIN_APOLLO_NOT_TITLES = ["contract", "junior", "jr"];

function linkedinApolloPeopleFilters(filters: PeopleSearchFilters): PeopleSearchFilters {
  return { ...filters, notTitles: LINKEDIN_APOLLO_NOT_TITLES };
}
const REJECTED_REASON = "rejected because they were using other observability tools";
const MAX_SRE_COUNT = 15;
const EMAIL_WATERFALL_WAIT_MS = 20 * 60 * 1000;
const COMPANY_LINKEDIN_URL_COLUMN = "Company Linkedin Url";
const WEEKLY_LINKEDIN_PUSH_LIMIT = 100;
const LINKEDIN_LEADERSHIP_TITLE_REGEX = /\b(director|svp|vp|head|chief)\b/i;

const SRE_WORK_KEYWORDS: string[] = [
  "incident response",
  "SRE",
  "reliability",
  "on-call",
  "on call",
  "incident management",
  "postmortem",
  "post-mortem",
  "RCA",
  "alerting systems",
  "alert fatigue",
  "escalation",
  "uptime",
  "SLA",
  "SLO",
  "SLI",
  "error budgets",
  "high availability",
  "fault tolerance",
  "pager duty",
  "PagerDuty",
  "Opsgenie",
  "incidents",
  "incident",
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

function logPipelineStage(
  step: string,
  message: string,
  companyContext?: { index: number; total: number; companyName: string }
): void {
  const companyTag = companyContext
    ? `[COMPANY:${companyContext.index + 1}/${companyContext.total}:${companyContext.companyName}]`
    : "";
  console.log(`[Pipeline][STEP:${step}]${companyTag} ${message}`);
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
  const outputRows: OutputRow[] = [];
  const syncableOutputRows: OutputRow[] = [];
  const rejectedOutputRows: RejectedOutputRow[] = [];
  const rejectedCompanies: string[] = [];
  const skippedCompanies: string[] = [];
  const pendingEmailPushBatches: PendingEmailPushBatch[] = [];
  const globalMissingEmailPersonIds = new Set<string>();
  const enrichmentCache: EnrichmentCache = new Map();
  let totalRows = 0;
  let skippedMissingWebsiteAndApolloAccountIdCount = 0;
  let apolloProcessedCompanyCount = 0;
  let totalSreFound = 0;
  let totalLinkedinCampaignSuccessful = 0;
  let totalLinkedinCampaignFailed = 0;
  let totalLemlistSuccessful = 0;
  let totalLemlistFailed = 0;
  let totalEmailCampaignSuccessful = 0;
  let totalEmailCampaignFailed = 0;
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
    const waterfallEnabled = getEnvBoolean("APOLLO_WATERFALL_ENABLED", false);
    const lemlistBulkFindEmailEnabled = getEnvBoolean("LEMLIST_BULK_FIND_EMAIL_ENABLED", true);
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
      const currentSreProspects = dedupeProspectsById(
        await searchPeople(company, MAX_RESULTS, SRE_PERSON_TITLES, linkedinApolloPeopleFilters(peopleSearchFilters))
      );
      const rawSreCount = currentSreProspects.length;
      logPipelineStage("SEARCH_CURRENT_SRE_DONE", `Current SRE candidates found. count=${rawSreCount}`, companyContext);
      if (rawSreCount > MAX_SRE_COUNT) {
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
        const apifyCache: ApifyOpenToWorkCache = new Map();
        process.stdout.write(`\n${"═".repeat(78)}\n  LINKEDIN CAMPAIGN — SRE Search — ${row.companyName} (${row.companyDomain})\n${"═".repeat(78)}\n\n`);
        process.stdout.write(`  ▸ Reusing ${rawSreCount} pre-filtered current SRE candidates\n`);
        process.stdout.write(`  ▸ Enriching ${currentSreProspects.length} current SRE candidates...\n`);
        logPipelineStage("ENRICH_CURRENT_SRE", "Enriching current SRE candidates.", companyContext);
        const enrichedEmployees = await bulkEnrichPeople(currentSreProspects, enrichmentCache);
        const { eligible: tenureEligibleSre } = splitByTenure(enrichedEmployees, 2);
        process.stdout.write(`  ▸ Checking openToWork for ${tenureEligibleSre.length} current SRE candidates...\n`);
        logPipelineStage("APIFY_CURRENT_SRE", `Checking openToWork for ${tenureEligibleSre.length} current SRE candidates.`, companyContext);
        const {
          kept: apifyFilteredSre,
          warnings: apifyWarnsSre,
          filteredOut: apifyFilteredOutSre,
        } = await scrapeAndFilterOpenToWork(tenureEligibleSre, apifyCache, {
          companyName: row.companyName,
          companyDomain: row.companyDomain,
        });
        for (const w of apifyWarnsSre) { addJobWarning(jobId, w); }
        campaignPushData.filteredOutCandidates.push(
          ...apifyFilteredOutSre.map(({ employee, reason }) => ({
            companyName: company.companyName,
            name: employee.name,
            title: employee.currentTitle,
            linkedinUrl: employee.linkedinUrl ?? null,
            reason,
          }))
        );
        const selectedCurrentSre = selectTopSreForLemlist(apifyFilteredSre, 7);
        process.stdout.write(`  ▸ Selected ${selectedCurrentSre.length} current SRE for LinkedIn seed\n`);
        logPipelineStage(
          "SELECT_CURRENT_SRE",
          `Current SRE selected for LinkedIn seed. selected=${selectedCurrentSre.length}`,
          companyContext
        );
        let selectedForLemlist = selectedCurrentSre;
        let prePlatformKeys: Set<string> | null = null;
        let keywordMatchedEmailRecycled: EnrichedEmployee[] = [];
        const apolloSearchCache: ApolloSearchCache = new Map();

        // LinkedIn Keyword Expansion: search DevOps/Infra/Normal Eng for SRE-keyword matches
        {
          process.stdout.write(`\n${"═".repeat(78)}\n  LINKEDIN KEYWORD EXPANSION — ${row.companyName} (${row.companyDomain})\n${"═".repeat(78)}\n\n`);
          logPipelineStage("KEYWORD_EXPANSION_START", "LinkedIn keyword expansion started.", companyContext);

          const allKeywordMatched: EnrichedEmployee[] = [];
          const sreProspectIds = new Set(currentSreProspects.map((p) => p.id));

          for (const { label, config: stageConfig } of LINKEDIN_KEYWORD_STAGES) {
            process.stdout.write(`  ▸ Searching ${label} candidates...\n`);
            const searchParams = {
              currentTitles: stageConfig.currentTitles,
              pastTitles: stageConfig.pastTitles,
              notTitles: stageConfig.notTitles,
              notPastTitles: stageConfig.notPastTitles,
            };
            const rawProspects = await searchEmailCandidatePeopleCached(
              company,
              MAX_RESULTS,
              searchParams,
              peopleSearchFilters,
              apolloSearchCache
            );
            const prospects = dedupeProspectsById(rawProspects).filter((p) => !sreProspectIds.has(p.id));
            process.stdout.write(`  ▸ ${label}: ${rawProspects.length} raw → ${prospects.length} after dedup\n`);

            if (prospects.length === 0) continue;

            const enriched = await bulkEnrichPeople(prospects, enrichmentCache);
            const { eligible: tenureEligible } = splitByTenure(enriched, 2);
            process.stdout.write(`  ▸ ${label}: ${enriched.length} enriched → ${tenureEligible.length} after tenure filter (2mo)\n`);

            if (tenureEligible.length === 0) continue;

            const {
              kept: apifyFiltered,
              warnings: apifyWarns,
              filteredOut: apifyFilteredOut,
            } = await scrapeAndFilterOpenToWork(tenureEligible, apifyCache, {
              companyName: row.companyName,
              companyDomain: row.companyDomain,
            });
            for (const w of apifyWarns) { addJobWarning(jobId, w); }
            campaignPushData.filteredOutCandidates.push(
              ...apifyFilteredOut.map(({ employee, reason }) => ({
                companyName: company.companyName,
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                reason,
              }))
            );

            if (apifyFiltered.length === 0) continue;

            const { matched } = filterByKeywordsInApifyData(apifyFiltered, apifyCache, SRE_WORK_KEYWORDS);
            process.stdout.write(`  ▸ ${label}: ${matched.length} matched SRE keywords\n`);
            allKeywordMatched.push(...matched);
          }

          if (allKeywordMatched.length > 0) {
            const { forLinkedin, forEmailRecycling } = selectKeywordMatchedByTenure(
              allKeywordMatched,
              selectedForLemlist,
              7
            );
            selectedForLemlist = [...selectedForLemlist, ...forLinkedin];
            keywordMatchedEmailRecycled = forEmailRecycling;
            process.stdout.write(`  ▸ Keyword expansion: ${forLinkedin.length} added to LinkedIn, ${forEmailRecycling.length} recycled to email\n`);
          }

          logPipelineStage(
            "KEYWORD_EXPANSION_DONE",
            `Keyword expansion complete. linkedin_total=${selectedForLemlist.length} recycled=${keywordMatchedEmailRecycled.length}`,
            companyContext
          );
        }

        if (selectedForLemlist.length < 7) {
          process.stdout.write(`  ▸ Backfill Phase 1 — Searching past SRE candidates...\n`);
          logPipelineStage("BACKFILL_PHASE_1_START", "Backfill phase 1 (past SRE) started.", companyContext);
          const pastSreProspects = dedupeProspectsById(
            await searchPastSrePeople(
              company,
              MAX_RESULTS,
              linkedinApolloPeopleFilters({ apolloOrganizationId: row.apolloAccountId })
            )
          );
          process.stdout.write(`  ▸ Enriching ${pastSreProspects.length} past SRE candidates...\n`);
          const pastSreEnriched = await bulkEnrichPeople(pastSreProspects, enrichmentCache);
          const { eligible: tenureEligiblePastSre } = splitByTenure(pastSreEnriched, 2);
          process.stdout.write(`  ▸ Checking openToWork for ${tenureEligiblePastSre.length} past SRE candidates...\n`);
          logPipelineStage("APIFY_PAST_SRE", `Checking openToWork for ${tenureEligiblePastSre.length} past SRE candidates.`, companyContext);
          const {
            kept: apifyFilteredPastSre,
            warnings: apifyWarnsPastSre,
            filteredOut: apifyFilteredOutPastSre,
          } = await scrapeAndFilterOpenToWork(tenureEligiblePastSre, apifyCache, {
            companyName: row.companyName,
            companyDomain: row.companyDomain,
          });
          for (const w of apifyWarnsPastSre) { addJobWarning(jobId, w); }
          campaignPushData.filteredOutCandidates.push(
            ...apifyFilteredOutPastSre.map(({ employee, reason }) => ({
              companyName: company.companyName,
              name: employee.name,
              title: employee.currentTitle,
              linkedinUrl: employee.linkedinUrl ?? null,
              reason,
            }))
          );
          selectedForLemlist = fillToMinimumWithBackfill(selectedCurrentSre, apifyFilteredPastSre, [], {
            minimum: 7,
            max: 7,
          });
          process.stdout.write(`  ▸ Backfill Phase 1 done — ${selectedForLemlist.length} selected so far\n`);
          logPipelineStage(
            "BACKFILL_PHASE_1_DONE",
            `Backfill phase 1 complete. selected_after_phase1=${selectedForLemlist.length}`,
            companyContext
          );

          if (selectedForLemlist.length < 5) {
            prePlatformKeys = new Set(selectedForLemlist.map(toEmployeeKey));
            process.stdout.write(`  ▸ Backfill Phase 2 — Searching platform candidates...\n`);
            logPipelineStage("BACKFILL_PHASE_2_START", "Backfill phase 2 (platform) started.", companyContext);
            const platformProspects = dedupeProspectsById(
              await searchCurrentPlatformEngineerPeople(
                company,
                MAX_RESULTS,
                linkedinApolloPeopleFilters({ apolloOrganizationId: row.apolloAccountId })
              )
            );
            process.stdout.write(`  ▸ Enriching ${platformProspects.length} platform candidates...\n`);
            const platformEnriched = await bulkEnrichPeople(platformProspects, enrichmentCache);
            const { eligible: tenureEligiblePlatform } = splitByTenure(platformEnriched, 11);
            process.stdout.write(`  ▸ Checking openToWork for ${tenureEligiblePlatform.length} platform candidates...\n`);
            logPipelineStage("APIFY_PLATFORM", `Checking openToWork for ${tenureEligiblePlatform.length} platform candidates.`, companyContext);
            const {
              kept: apifyFilteredPlatform,
              warnings: apifyWarnsPlatform,
              filteredOut: apifyFilteredOutPlatform,
            } = await scrapeAndFilterOpenToWork(tenureEligiblePlatform, apifyCache, {
              companyName: row.companyName,
              companyDomain: row.companyDomain,
            });
            for (const w of apifyWarnsPlatform) { addJobWarning(jobId, w); }
            campaignPushData.filteredOutCandidates.push(
              ...apifyFilteredOutPlatform.map(({ employee, reason }) => ({
                companyName: company.companyName,
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                reason,
              }))
            );
            selectedForLemlist = fillToMinimumWithBackfill(selectedForLemlist, [], apifyFilteredPlatform, {
              minimum: 5,
              max: 5,
            });
            process.stdout.write(`  ▸ Backfill Phase 2 done — ${selectedForLemlist.length} selected so far\n`);
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
          process.stdout.write(`  ▸ Pushing ${taggedForLemlist.length} candidates to LinkedIn campaign...\n`);
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
          sessionLinkedinSuccessfulCount += lemlistSuccessful;
          totalLinkedinCampaignSuccessful += lemlistSuccessful;
          totalLinkedinCampaignFailed += lemlistFailed;
          totalLemlistSuccessful += lemlistSuccessful;
          totalLemlistFailed += lemlistFailed;
          process.stdout.write(`  ▸ LinkedIn push done — ${lemlistSuccessful} successful, ${lemlistFailed} failed\n`);
          logPipelineStage(
            "PUSH_LINKEDIN_DONE",
            `LinkedIn push complete. successful=${lemlistSuccessful} failed=${lemlistFailed}`,
            companyContext
          );
        }

        if (lemlistEnabled) {
          const attemptedLinkedinKeys = new Set(selectedForLemlist.map((employee) => toEmployeeKey(employee)));
          process.stdout.write(`  ▸ Starting email candidate waterfall...\n`);
          logPipelineStage("EMAIL_WATERFALL_START", "Email candidate waterfall started.", companyContext);
          const waterfallResult = await runEmailCandidateWaterfall(
            company,
            attemptedLinkedinKeys,
            enrichmentCache,
            peopleSearchFilters,
            apifyCache,
            { rawSreCount, apolloSearchCache, recycledKeywordMatched: keywordMatchedEmailRecycled }
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
            pendingEmailPushBatches.push({
              companyName: company.companyName,
              companyDomain: company.domain,
              candidates: waterfallResult.candidates,
            });
            for (const { employee } of waterfallResult.candidates) {
              if (!employee.email && employee.id) {
                globalMissingEmailPersonIds.add(employee.id);
              }
            }
            logPipelineStage(
              "QUEUE_EMAIL_BATCH",
              `Email batch queued. missing_email_so_far=${globalMissingEmailPersonIds.size}`,
              companyContext
            );
          }
        }

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

    let recoveredEmailsByPersonId = new Map<string, string>();
    if (lemlistEnabled && lemlistBulkFindEmailEnabled && pendingEmailPushBatches.length > 0) {
      const missingEmailCandidates = pendingEmailPushBatches.flatMap((batch) =>
        batch.candidates
          .filter(({ employee }) => !employee.email || employee.email.trim().length === 0)
          .map(({ employee }) => ({
            employee,
            companyName: batch.companyName,
            companyDomain: batch.companyDomain,
          }))
      );

      if (missingEmailCandidates.length > 0) {
        logPipelineStage(
          "LEMLIST_EMAIL_ENRICH_START",
          `Lemlist bulk find_email started. candidates=${missingEmailCandidates.length}`
        );
        setJobMessage(
          jobId,
          `Missing ${missingEmailCandidates.length} emails. Started an additional search for missing emails.`
        );
        const summary = await enrichMissingEmailsWithLemlist(missingEmailCandidates, (progress) => {
          setJobMessage(
            jobId,
            `Missing email search update: found=${progress.recovered}, not found=${progress.notFound}, pending=${progress.pending}.`
          );
        });
        logPipelineStage(
          "LEMLIST_EMAIL_ENRICH_DONE",
          `Lemlist bulk find_email complete. attempted=${summary.attempted} accepted=${summary.accepted} recovered=${summary.recovered} not_found=${summary.notFound}`
        );
      }
    }

    if (lemlistEnabled && lemlistBulkFindEmailEnabled && waterfallEnabled && globalMissingEmailPersonIds.size > 0) {
      logPipelineStage(
        "GLOBAL_WATERFALL_SKIPPED",
        `Global waterfall skipped because LEMLIST_BULK_FIND_EMAIL_ENABLED is true. missing_people=${globalMissingEmailPersonIds.size}`
      );
    } else if (lemlistEnabled && waterfallEnabled && globalMissingEmailPersonIds.size > 0) {
      logPipelineStage(
        "GLOBAL_WATERFALL_START",
        `Global waterfall started: missing_people=${globalMissingEmailPersonIds.size} wait_ms=${EMAIL_WATERFALL_WAIT_MS}`
      );
      setJobMessage(
        jobId,
        `Running Apollo waterfall for ${globalMissingEmailPersonIds.size} missing emails (max wait ${Math.round(EMAIL_WATERFALL_WAIT_MS / 60000)} minutes).`
      );
      try {
        recoveredEmailsByPersonId = await runWaterfallEmailForPersonIds(
          [...globalMissingEmailPersonIds],
          EMAIL_WATERFALL_WAIT_MS
        );
        logPipelineStage(
          "GLOBAL_WATERFALL_DONE",
          `Global waterfall complete: recovered_emails=${recoveredEmailsByPersonId.size}`
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown Apollo waterfall error";
        addJobWarning(jobId, `Apollo waterfall batch failed: ${message}`);
        logPipelineStage("GLOBAL_WATERFALL_FAILED", `Global waterfall failed. error=${message}`);
      }
    } else if (lemlistEnabled && !waterfallEnabled && globalMissingEmailPersonIds.size > 0) {
      logPipelineStage(
        "GLOBAL_WATERFALL_SKIPPED",
        `Global waterfall skipped because APOLLO_WATERFALL_ENABLED is false. missing_people=${globalMissingEmailPersonIds.size}`
      );
    }

    if (lemlistEnabled && pendingEmailPushBatches.length > 0) {
      logPipelineStage(
        "EMAIL_PUSH_STAGE_START",
        `Email campaign push stage started. companies=${pendingEmailPushBatches.length}`
      );
      for (const batch of pendingEmailPushBatches) {
        if (isCancelled(jobId)) {
          return;
        }

        for (const { employee } of batch.candidates) {
          if (employee.email || !employee.id) {
            continue;
          }
          const recoveredEmail = recoveredEmailsByPersonId.get(employee.id);
          if (recoveredEmail) {
            employee.email = recoveredEmail;
          }
        }

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
        totalEmailCampaignSuccessful += emailPushMeta.successful;
        totalEmailCampaignFailed += emailPushMeta.failed;
        logPipelineStage(
          "EMAIL_PUSH_COMPANY_DONE",
          `Email push complete. successful=${emailPushMeta.successful} failed=${emailPushMeta.failed}`,
          {
            index: pendingEmailPushBatches.indexOf(batch),
            total: pendingEmailPushBatches.length,
            companyName: batch.companyName,
          }
        );
      }
    }

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
      totalLemlistSuccessful,
      totalLemlistFailed,
      totalEmailCampaignSuccessful,
      totalEmailCampaignFailed,
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
    markJobDone(jobId, csvBase64);
    logPipelineStage(
      "JOB_DONE",
      `Job done: processed=${apolloProcessedCompanyCount} linkedin_success=${totalLinkedinCampaignSuccessful} lemlist_success=${totalLemlistSuccessful} lemlist_failed=${totalLemlistFailed}`
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected job failure";
    markJobError(jobId, message);
    logPipelineStage("JOB_FAILED", `Job failed. error=${message}`);
  } finally {
    console.log = _originalConsoleLog;
  }
}
