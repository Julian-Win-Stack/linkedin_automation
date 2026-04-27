import { getEnvBoolean } from "../config/env";
import { PipelineConfig } from "../config/pipelineConfig";
import { getCompany, ResolvedCompany } from "../services/getCompany";
import { pushPeopleToLemlistCampaign, TaggedLinkedinCandidate } from "../services/lemlistPushQueue";
import { countProcessableCompanies, readCompanies } from "../services/observability/csvReader";
import { fillToMinimumWithBackfill, selectTopSreForLemlist, selectKeywordMatchedByTenure, runBackfillStages } from "../services/sreSelection";
import {
  OutputRow,
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
  setJobPartialResults,
} from "./jobStore";
import { EnrichedEmployee, LemlistPushOutcome } from "../types/prospect";
import { SelectedUser } from "../shared/selectedUser";
import { filterOpenToWorkFromCache, filterByKeywordsInApifyData, filterOutHardwareHeavyPeople } from "../services/apifyClient";
import { syncApolloAccountsFromOutputRows, formatCurrentWeekLabel } from "../services/apolloBulkUpdateAccounts";
import { syncAttioCompaniesFromOutputRows } from "../services/attioAssertCompanyRecords";
import { getWeeklySuccessCounts, saveWeeklySuccessForJob } from "../services/weeklySuccessStore";
import { scrapeCompanyEmployees, filterByPastExperienceKeywords } from "../services/apifyCompanyEmployees";

const MAX_ROWS = 500;
const CHECKPOINT_SIZE = 50;
const SRE_PERSON_TITLES = [
  "SRE",
  "Site Reliability",
  "Site Reliability Engineer",
  "Site Reliability Engineering",
  "Head of Reliability",
];
const PAST_SRE_EXPERIENCE_KEYWORDS = ["SRE", "Site Reliability"];
const COMPANY_LINKEDIN_URL_COLUMN = "Company Linkedin Url";
const WEEKLY_LINKEDIN_PUSH_LIMIT = 100;
const LINKEDIN_LEADERSHIP_TITLE_REGEX = /\b(director|svp|vp|head|chief)\b/i;
const EMAIL_TITLE_REJECT_REGEX = /\b(data|front[\s-]?end)\b/i;

const DEVOPS_TITLE_REGEX = /\b(devops|dev[\s-]?ops)\b/i;
const QA_TITLE_REJECT_REGEX = /\bQA\b/i;

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
  "AI adoption",
  "Terraform",
];


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
      hardwareHeavyCount: 0,
      qaTitleCount: 0,
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
    } else if (reason === "hardware_heavy") {
      summary.hardwareHeavyCount += 1;
    } else if (reason === "qa_title") {
      summary.qaTitleCount += 1;
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
  const skippedCompanies: string[] = [];
  let companiesSinceLastCheckpoint = 0;
  let lastCheckpointSyncableIndex = 0;
  let totalRows = 0;
  let skippedMissingWebsiteAndApolloAccountIdCount = 0;
  let apolloProcessedCompanyCount = 0;
  let totalLinkedinCampaignSuccessful = 0;
  let totalLinkedinCampaignFailed = 0;
  let totalLinkedinCampaignSkipped = 0;
  let totalCompaniesReachedOutTo = 0;
  let eligibleCompanyCount = 0;
  let weeklyLimitSkippedCompanyCount = 0;
  const weeklyCounts = getWeeklySuccessCounts({ selectedUser, weekStartMs });
  let sessionLinkedinSuccessfulCount = 0;
  let weeklyLimitWarningAdded = false;

  const campaignPushData: CampaignPushData = {
    linkedinSre: [],
    linkedinEngLead: [],
    linkedinEng: [],
    filteredOutCandidates: [],
    normalEngineerApifyWarnings: [],
  };

  async function flushCheckpoint(): Promise<void> {
    const newSyncableRows = syncableOutputRows.slice(lastCheckpointSyncableIndex);
    lastCheckpointSyncableIndex = syncableOutputRows.length;

    const [apolloOutcome, attioOutcome] = await Promise.allSettled([
      syncApolloAccountsFromOutputRows(newSyncableRows),
      syncAttioCompaniesFromOutputRows(newSyncableRows),
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
      companiesReachedOutToCount: totalCompaniesReachedOutTo,
    });

    const partialCsvString = await rowsToCsvString(outputRows);
    const partialCsvBase64 = Buffer.from(partialCsvString, "utf8").toString("base64");
    setJobPartialResults(jobId, partialCsvBase64, campaignPushData);

    companiesSinceLastCheckpoint = 0;
  }

  try {
    setJobStatus(jobId, "processing");
    logPipelineStage("JOB_START", `Job started. selected_user=${selectedUser}`);
    setJobMessage(jobId, "Starting pipeline...");

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
      setJobMessage(jobId, `Processing row ${row.rowNumber}: ${row.companyName}`);
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
          stage: "",
        });
        if (!weeklyLimitWarningAdded) {
          addJobWarning(
            jobId,
            `Weekly LinkedIn push limit (${WEEKLY_LINKEDIN_PUSH_LIMIT}) reached. Remaining companies were fully skipped.`
          );
          weeklyLimitWarningAdded = true;
        }
      } else {

      eligibleCompanyCount += 1;
      const company = await resolveCompanyForApolloInput(row);
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
        const rawProfilePool = dedupeEmployeesByKey(poolResult.employees);
        const hardwareFilteredPool = filterOutHardwareHeavyPeople(rawProfilePool, apifyCache);
        addFilteredOutCounts(
          campaignPushData,
          company.companyName,
          hardwareFilteredPool.rejected.map(() => "hardware_heavy")
        );
        const profilePool = hardwareFilteredPool.kept;
        logPipelineStage(
          "APIFY_COMPANY_POOL_DONE",
          `Company pool loaded. profiles=${poolResult.profileCount} mapped=${profilePool.length}`,
          companyContext
        );

        const linkedinCandidates: LinkedinPoolCandidate[] = [];
        let preBackfillKeys: Set<string> | null = null;

        const currentSrePool = profilePool.filter((employee) =>
          SRE_PERSON_TITLES.some(
            (keyword) =>
              employee.currentTitle.toLowerCase().includes(keyword.toLowerCase()) ||
              (employee.headline ?? "").toLowerCase().includes(keyword.toLowerCase())
          )
        );
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
          const alreadyLinkedinKeys = new Set(linkedinCandidates.map((candidate) => toEmployeeKey(candidate.employee)));

          const devopsPool = profilePool.filter((employee) => {
            const title = employee.currentTitle ?? "";
            return (
              DEVOPS_TITLE_REGEX.test(title) &&
              !QA_TITLE_REJECT_REGEX.test(title) &&
              !alreadyLinkedinKeys.has(toEmployeeKey(employee))
            );
          });
          logPipelineInfo(`  ▸ DevOps pool size: ${devopsPool.length}\n`);

          const devopsFiltered = filterOpenToWorkFromCache(devopsPool, apifyCache, {
            companyName: row.companyName,
            companyDomain: row.companyDomain,
          });
          for (const warning of devopsFiltered.warnings) {
            addJobWarning(jobId, warning);
          }
          addFilteredOutCounts(
            campaignPushData,
            company.companyName,
            devopsFiltered.filteredOut.map(({ reason }) => reason)
          );
          const devopsHardwareFiltered = filterOutHardwareHeavyPeople(devopsFiltered.kept, apifyCache);
          const { matched: allKeywordMatched } = filterByKeywordsInApifyData(
            devopsHardwareFiltered.kept,
            apifyCache,
            SRE_WORK_KEYWORDS
          );
          logPipelineInfo(`  ▸ DevOps keyword matched: ${allKeywordMatched.length}\n`);

          if (allKeywordMatched.length > 0) {
            const selectedForLinkedin = linkedinCandidates.map((candidate) => candidate.employee);
            const { forLinkedin } = selectKeywordMatchedByTenure(
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
          }
          logPipelineStage(
            "KEYWORD_EXPANSION_DONE",
            `Keyword expansion complete. linkedin_total=${linkedinCandidates.length}`,
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
            preBackfillKeys = new Set(selectedForLemlist.map(toEmployeeKey));
            logPipelineStage("BACKFILL_STAGES_START", "Backfill stages (Infra→Platform→DevOps→NormalEng→EngLeader) started.", companyContext);
            const backfillResult = runBackfillStages(
              selectedForLemlist,
              profilePool,
              apifyCache,
              { companyName: row.companyName, companyDomain: row.companyDomain }
            );
            for (const w of backfillResult.warnings) addJobWarning(jobId, w);
            addFilteredOutCounts(campaignPushData, company.companyName, backfillResult.filteredOutReasons);
            if (backfillResult.normalEngineerApifyWarnings.length > 0) {
              addWarningProblemCounts(
                campaignPushData,
                company.companyName,
                backfillResult.normalEngineerApifyWarnings.map(({ problem }) => problem)
              );
            }
            selectedForLemlist = dedupeEmployeesByKey([
              ...selectedForLemlist,
              ...backfillResult.candidates.map((c) => c.employee),
            ]);
            logPipelineStage(
              "BACKFILL_STAGES_DONE",
              `Backfill stages complete. selected_after_backfill=${selectedForLemlist.length}`,
              companyContext
            );
          }
        }

        apolloProcessedCompanyCount += 1;

        let lemlistSuccessful = 0;
        let lemlistFailed = 0;
        if (lemlistEnabled && selectedForLemlist.length > 0) {
          const taggedForLemlist: TaggedLinkedinCandidate[] = selectedForLemlist
            .filter((emp) => !EMAIL_TITLE_REJECT_REGEX.test(emp.currentTitle))
            .map((emp) => {
              const fromStages123 = preBackfillKeys === null || preBackfillKeys.has(toEmployeeKey(emp));
              const isLeadershipTitle = isLinkedinLeadershipTitle(emp.currentTitle);
              const linkedinBucket = fromStages123
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
          logPipelineInfo(
            `  ▸ LinkedIn push done — ${lemlistSuccessful} successful, ${lemlistFailed} failed, ${lemlistSkipped} skipped\n`
          );
          logPipelineStage(
            "PUSH_LINKEDIN_DONE",
            `LinkedIn push complete. successful=${lemlistSuccessful} failed=${lemlistFailed} skipped=${lemlistSkipped}`,
            companyContext
          );
        }

        const shouldStopAfterCurrentCompany =
          weeklyCounts.linkedinCount + sessionLinkedinSuccessfulCount >= WEEKLY_LINKEDIN_PUSH_LIMIT;

        const outreachDateLabel = formatCurrentWeekLabel();

        outputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          stage: "ChasingPOC",
          outreach_date: outreachDateLabel,
        });
        syncableOutputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          stage: "ChasingPOC",
          outreach_date: outreachDateLabel,
        });
        totalCompaniesReachedOutTo += 1;
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
        const outreachDateLabel = formatCurrentWeekLabel();
        outputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          stage: "ChasingPOC",
          outreach_date: outreachDateLabel,
        });
        syncableOutputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          company_linkedin_url: row.companyLinkedinUrl,
          apollo_account_id: row.apolloAccountId ?? "",
          stage: "ChasingPOC",
          outreach_date: outreachDateLabel,
        });
        totalCompaniesReachedOutTo += 1;
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
    setSkippedCompanies(jobId, skippedCompanies);

    const summary: JobSummary = {
      totalRows,
      eligibleCompanyCount,
      skippedMissingWebsiteAndApolloAccountIdCount,
      apolloProcessedCompanyCount,
      totalLinkedinCampaignSuccessful,
      totalLinkedinCampaignFailed,
      totalLinkedinCampaignSkipped,
      weeklyLimitSkippedCompanyCount,
    };
    setJobSummary(jobId, summary);
    setCampaignPushData(jobId, campaignPushData);

    const csvString = await rowsToCsvString(outputRows);
    const csvBase64 = Buffer.from(csvString, "utf8").toString("base64");
    setJobMessage(jobId, "Completed. CSV and PDF are ready to download.");
    markJobDone(jobId, csvBase64);
    logPipelineStage(
      "JOB_DONE",
      `Job done: processed=${apolloProcessedCompanyCount} linkedin_success=${totalLinkedinCampaignSuccessful} linkedin_skipped=${totalLinkedinCampaignSkipped}`
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected job failure";
    markJobError(jobId, message);
    logPipelineStage("JOB_FAILED", `Job failed. error=${message}`);
  }
}
