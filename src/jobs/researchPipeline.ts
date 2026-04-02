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
  countEngineerPeople,
  searchCurrentPlatformEngineerPeople,
  searchPastSrePeople,
  PeopleSearchFilters,
  searchPeople,
} from "../services/searchPeople";
import { readCompanies } from "../services/observability/csvReader";
import { researchCompany } from "../services/observability/openaiClient";
import { fillToMinimumWithBackfill, selectTopSreForLemlist } from "../services/sreSelection";
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
import { runEmailCandidateWaterfall, TaggedEmailCandidate } from "../services/emailCandidateWaterfall";
import { scrapeAndFilterOpenToWork, splitByTenure } from "../services/apifyClient";

const MAX_ROWS = 500;
const SRE_PERSON_TITLES = ["SRE", "Site Reliability", "Site Reliability Engineer", "Site Reliability Engineering", "Head of Reliability"];
const MAX_RESULTS = 30;
/** Current-title exclusions for LinkedIn Apollo searches (SRE, past SRE, platform backfill). */
const LINKEDIN_APOLLO_NOT_TITLES = ["contract"];

function linkedinApolloPeopleFilters(filters: PeopleSearchFilters): PeopleSearchFilters {
  return { ...filters, notTitles: LINKEDIN_APOLLO_NOT_TITLES };
}
const REJECTED_REASON = "rejected because they were using other observability tools";
const MIN_ENGINEER_COUNT = 18;
const MAX_ENGINEER_COUNT = 700;
const MAX_SRE_COUNT = 15;
const ENGINEER_RANGE_REJECTION_NOTE = "Engineer count not in BACCA's optimal range";
const LARGE_ENGINEER_COUNT_MASK = "> 1000";
const EMAIL_WATERFALL_WAIT_MS = 20 * 60 * 1000;
const COMPANY_LINKEDIN_URL_COLUMN = "Company Linkedin Url";

interface RowResearchResult {
  companyName: string;
  companyDomain: string;
  companyLinkedinUrl: string;
  apolloAccountId?: string;
  company: ResolvedCompany;
  peopleSearchFilters: PeopleSearchFilters;
  engineerCount: number;
  engineerCountDisplayValue: number | "> 1000";
  currentSreProspects: Prospect[];
  rawSreCount: number;
  observability: string;
  eligible: boolean;
}

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

function getEngineerCountDisplayValue(engineerCount: number): number | "> 1000" {
  if (engineerCount > 1000) {
    return LARGE_ENGINEER_COUNT_MASK;
  }
  return engineerCount;
}

function toEmployeeKey(employee: EnrichedEmployee): string {
  return employee.id ?? `${employee.name}|${employee.currentTitle}|${employee.linkedinUrl ?? ""}`;
}

function toOutcomeMap(outcomes: LemlistPushOutcome[]): Map<string, LemlistPushOutcome> {
  return new Map(outcomes.map((outcome) => [outcome.key, outcome]));
}

function toCampaignPushEntry(
  employee: EnrichedEmployee,
  outcomeByKey: Map<string, LemlistPushOutcome>
): CampaignPushEntry {
  const outcome = outcomeByKey.get(toEmployeeKey(employee));
  if (!outcome) {
    return {
      name: employee.name,
      title: employee.currentTitle,
      linkedinUrl: employee.linkedinUrl ?? null,
      lemlistStatus: "failed",
      lemlistError: "Lemlist result missing for this candidate.",
    };
  }
  return {
    name: employee.name,
    title: employee.currentTitle,
    linkedinUrl: employee.linkedinUrl ?? null,
    lemlistStatus: outcome.status,
    ...(outcome.error ? { lemlistError: outcome.error } : {}),
  };
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
  selectedUser: SelectedUser
): Promise<void> {
  const rowResults: RowResearchResult[] = [];
  const outputRows: OutputRow[] = [];
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
  let totalLemlistSuccessful = 0;
  let totalLemlistFailed = 0;
  let totalEmailCampaignSuccessful = 0;
  let totalEmailCampaignFailed = 0;

  const campaignPushData: CampaignPushData = {
    linkedinSre: [],
    linkedinEng: [],
    emailSre: [],
    emailEng: [],
    emailEngLead: [],
  };

  const _originalConsoleLog = console.log;
  try {
    console.log = () => {};

    setJobStatus(jobId, "processing");
    logPipelineStage("JOB_START", `Job started. selected_user=${selectedUser}`);
    setJobMessage(jobId, "Starting engineer and SRE pre-filter...");

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

      setJobProgress(jobId, { currentRow: row.rowNumber, totalRows: MAX_ROWS });
      setJobMessage(jobId, `Engineer/SRE pre-filter row ${row.rowNumber}: ${row.companyName}`);
      const companyContext = { index: totalRows - 1, total: MAX_ROWS, companyName: row.companyName };
      const company = await resolveCompanyForApolloInput(row);
      const peopleSearchFilters: PeopleSearchFilters = {
        apolloOrganizationId: row.apolloAccountId,
      };

      logPipelineStage("COUNT_ENGINEERS", "Counting engineers.", companyContext);
      const engineerCount = await countEngineerPeople(company, peopleSearchFilters);
      const engineerCountDisplayValue = getEngineerCountDisplayValue(engineerCount);
      logPipelineStage("COUNT_ENGINEERS_DONE", `Engineer count computed. count=${engineerCountDisplayValue}`, companyContext);
      if (engineerCount < MIN_ENGINEER_COUNT || engineerCount > MAX_ENGINEER_COUNT) {
        logPipelineStage(
          "REJECT_ENGINEER_RANGE",
          `Company rejected by engineer range. count=${engineerCountDisplayValue}`,
          companyContext
        );
        rejectedCompanies.push(
          `${row.companyName} was rejected because engineer count (${engineerCountDisplayValue}) is not in BACCA's optimal range`
        );
        rejectedOutputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          observability_tool_research: "",
          sre_count: "",
          engineer_count: engineerCountDisplayValue,
          status: "NotActionableNow",
          notes: ENGINEER_RANGE_REJECTION_NOTE,
        });
        continue;
      }

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
          observability_tool_research: "",
          sre_count: rawSreCount,
          engineer_count: engineerCountDisplayValue,
          status: "NotActionableNow",
          notes: rejectionNote,
        });
        continue;
      }

      rowResults.push({
        companyName: row.companyName,
        companyDomain: row.companyDomain,
        companyLinkedinUrl: row.companyLinkedinUrl,
        apolloAccountId: row.apolloAccountId,
        company,
        peopleSearchFilters,
        engineerCount,
        engineerCountDisplayValue,
        currentSreProspects,
        rawSreCount,
        observability: "",
        eligible: false,
      });
    }

    setJobMessage(jobId, `Pre-filter complete. Running observability research for ${rowResults.length} companies.`);
    logPipelineStage("PREFILTER_DONE", `Pre-filter complete. companies_for_observability=${rowResults.length}`);

    for (let index = 0; index < rowResults.length; index += 1) {
      if (isCancelled(jobId)) {
        return;
      }
      const row = rowResults[index];
      setJobProgress(jobId, { currentRow: totalRows + index + 1, totalRows: totalRows + rowResults.length });
      setJobMessage(jobId, `Observability research ${index + 1}/${rowResults.length}: ${row.companyName}`);
      const observability = await researchCompany(row.companyName, row.companyDomain, {
        apiKey: config.azureOpenAiApiKey,
        baseUrl: config.azureOpenAiBaseUrl,
        model: config.model,
        maxCompletionTokens: config.maxCompletionTokens,
        searchApiKey: config.searchApiKey,
      });
      row.observability = observability;
      row.eligible = shouldProcessByObservability(observability);
      if (!row.eligible) {
        rejectedCompanies.push(
          `Company ${row.companyName} was rejected because it was using ${observability.trim() || "other observability tools"}`
        );
      }
    }

    const eligibleRows = rowResults.filter((row) => row.eligible);
    setJobMessage(jobId, `Observability stage complete. Processing ${eligibleRows.length} eligible companies.`);
    logPipelineStage(
      "OBSERVABILITY_DONE",
      `Observability complete. eligible_companies=${eligibleRows.length} rejected_by_observability=${rowResults.length - eligibleRows.length}`
    );

    const lemlistEnabled = getEnvBoolean("LEMLIST_PUSH_ENABLED", true);
    const waterfallEnabled = getEnvBoolean("APOLLO_WATERFALL_ENABLED", false);
    const lemlistBulkFindEmailEnabled = getEnvBoolean("LEMLIST_BULK_FIND_EMAIL_ENABLED", true);

    for (let index = 0; index < eligibleRows.length; index += 1) {
      if (isCancelled(jobId)) {
        return;
      }

      const row = eligibleRows[index];
      const progressRow = totalRows + index + 1;
      setJobProgress(jobId, { currentRow: progressRow, totalRows: totalRows + eligibleRows.length });
      setJobMessage(jobId, `Apollo stage ${index + 1}/${eligibleRows.length}: ${row.companyName}`);
      const companyContext = { index, total: eligibleRows.length, companyName: row.companyName };
      logPipelineStage("COMPANY_START", "Company processing started.", companyContext);

      try {
        const apifyCache: ApifyOpenToWorkCache = new Map();
        const company = row.company;
        const engineerCount = row.engineerCount;
        const currentSreProspects = row.currentSreProspects;
        const rawSreCount = row.rawSreCount;
        process.stdout.write(`\n${"═".repeat(78)}\n  LINKEDIN CAMPAIGN — SRE Search — ${row.companyName} (${row.companyDomain})\n${"═".repeat(78)}\n\n`);
        process.stdout.write(`  ▸ Reusing ${rawSreCount} pre-filtered current SRE candidates\n`);
        process.stdout.write(`  ▸ Enriching ${currentSreProspects.length} current SRE candidates...\n`);
        logPipelineStage("ENRICH_CURRENT_SRE", "Enriching current SRE candidates.", companyContext);
        const enrichedEmployees = await bulkEnrichPeople(currentSreProspects, enrichmentCache);
        const { eligible: tenureEligibleSre } = splitByTenure(enrichedEmployees, 2);
        process.stdout.write(`  ▸ Checking openToWork for ${tenureEligibleSre.length} current SRE candidates...\n`);
        logPipelineStage("APIFY_CURRENT_SRE", `Checking openToWork for ${tenureEligibleSre.length} current SRE candidates.`, companyContext);
        const { kept: apifyFilteredSre, warnings: apifyWarnsSre } = await scrapeAndFilterOpenToWork(tenureEligibleSre, apifyCache, { companyName: row.companyName, companyDomain: row.companyDomain });
        for (const w of apifyWarnsSre) { addJobWarning(jobId, w); }
        const selectedCurrentSre = selectTopSreForLemlist(apifyFilteredSre, 7);
        process.stdout.write(`  ▸ Selected ${selectedCurrentSre.length} current SRE for LinkedIn seed\n`);
        logPipelineStage(
          "SELECT_CURRENT_SRE",
          `Current SRE selected for LinkedIn seed. selected=${selectedCurrentSre.length}`,
          companyContext
        );
        let selectedForLemlist = selectedCurrentSre;
        let prePlatformKeys: Set<string> | null = null;

        // Backfill only runs if we have at least one current SRE candidate selected.
        if (selectedCurrentSre.length > 0 && selectedCurrentSre.length < 5 && rawSreCount > 0) {
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
          const { kept: apifyFilteredPastSre, warnings: apifyWarnsPastSre } = await scrapeAndFilterOpenToWork(tenureEligiblePastSre, apifyCache, { companyName: row.companyName, companyDomain: row.companyDomain });
          for (const w of apifyWarnsPastSre) { addJobWarning(jobId, w); }
          selectedForLemlist = fillToMinimumWithBackfill(selectedCurrentSre, apifyFilteredPastSre, [], {
            minimum: 5,
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
            const { kept: apifyFilteredPlatform, warnings: apifyWarnsPlatform } = await scrapeAndFilterOpenToWork(tenureEligiblePlatform, apifyCache, { companyName: row.companyName, companyDomain: row.companyDomain });
            for (const w of apifyWarnsPlatform) { addJobWarning(jobId, w); }
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
          const taggedForLemlist: TaggedLinkedinCandidate[] = selectedForLemlist.map((emp) => ({
            employee: emp,
            linkedinBucket: prePlatformKeys === null || prePlatformKeys.has(toEmployeeKey(emp))
              ? "sre" as const
              : "eng" as const,
          }));
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
            const entry = toCampaignPushEntry(tagged.employee, linkedinOutcomeByKey);
            if (tagged.linkedinBucket === "sre") {
              campaignPushData.linkedinSre.push(entry);
            } else {
              campaignPushData.linkedinEng.push(entry);
            }
          }
          lemlistSuccessful = lemlistMeta.successful;
          lemlistFailed = lemlistMeta.failed;
          totalLinkedinCampaignSuccessful += lemlistSuccessful;
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
            row.peopleSearchFilters,
            apifyCache
          );
          logPipelineStage(
            "EMAIL_WATERFALL_DONE",
            `Email candidate waterfall complete. candidates=${waterfallResult.candidates.length}`,
            companyContext
          );

          for (const warning of waterfallResult.warnings) {
            addJobWarning(jobId, warning);
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
          observability_tool_research: row.observability,
          stage: "ChasingPOC",
          sre_count: rawSreCount,
          engineer_count: engineerCount,
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
          observability_tool_research: row.observability,
          stage: "ChasingPOC",
          sre_count: 0,
          engineer_count: 0,
          notes: "",
        });
      }
    }

    for (const row of rowResults.filter((entry) => !entry.eligible)) {
      const rejectionNotes = row.observability.trim() || REJECTED_REASON;
      rejectedOutputRows.push({
        company_name: row.companyName,
        company_domain: row.companyDomain,
        company_linkedin_url: row.companyLinkedinUrl,
        observability_tool_research: row.observability,
        sre_count: row.rawSreCount,
        engineer_count: row.engineerCountDisplayValue,
        status: "NotActionableNow",
        notes: rejectionNotes,
      });
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
          const entry = toCampaignPushEntry(employee, emailOutcomeByKey);
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
      eligibleCompanyCount: eligibleRows.length,
      rejectedCompanyCount: rejectedCompanies.length,
      skippedMissingWebsiteAndApolloAccountIdCount,
      apolloProcessedCompanyCount,
      totalSreFound,
      totalLinkedinCampaignSuccessful,
      totalLemlistSuccessful,
      totalLemlistFailed,
      totalEmailCampaignSuccessful,
      totalEmailCampaignFailed,
    };
    setJobSummary(jobId, summary);
    setCampaignPushData(jobId, campaignPushData);

    const combinedOutputRows: OutputRow[] = [
      ...outputRows,
      ...rejectedOutputRows.map((row) => ({
        company_name: row.company_name,
        company_domain: row.company_domain,
        company_linkedin_url: row.company_linkedin_url,
        observability_tool_research: row.observability_tool_research,
        stage: row.status,
        sre_count: row.sre_count,
        engineer_count: row.engineer_count,
        notes: row.notes,
      })),
    ];

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
