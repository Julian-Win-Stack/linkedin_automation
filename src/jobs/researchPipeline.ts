import { getEnvBoolean } from "../config/env";
import { PipelineConfig } from "../config/pipelineConfig";
import {
  bulkEnrichPeople,
  EnrichmentCache,
  runWaterfallEmailForPersonIds,
} from "../services/bulkEnrichPeople";
import { getCompany } from "../services/getCompany";
import { pushPeopleToLemlistEmailCampaign } from "../services/lemlistEmailPushQueue";
import { pushPeopleToLemlistCampaign } from "../services/lemlistPushQueue";
import {
  countEngineerPeople,
  searchCurrentEngineeringEmailCandidates,
  searchCurrentPlatformEngineerPeople,
  searchPastSrePeople,
  searchPeople,
} from "../services/searchPeople";
import { readCompanies } from "../services/observability/csvReader";
import { researchCompany } from "../services/observability/openaiClient";
import { fillToMinimumWithBackfill, selectTopSreForLemlist } from "../services/sreSelection";
import {
  OutputRow,
  RejectedOutputRow,
  rejectedRowsToCsvString,
  rowsToCsvString,
} from "../services/observability/csvWriter";
import {
  addJobWarning,
  getJob,
  JobSummary,
  markJobDone,
  markJobError,
  setJobMessage,
  setJobProgress,
  setSkippedCompanies,
  setJobStatus,
  setJobSummary,
  setRejectedCompanies,
} from "./jobStore";
import { EnrichedEmployee } from "../types/prospect";

const MAX_ROWS = 500;
const SRE_PERSON_TITLES = ["SRE", "Site Reliability", "Head of Reliability"];
const MAX_RESULTS = 30;
const EMAIL_CANDIDATE_MAX_RESULTS = 100;
const REJECTED_REASON = "rejected because they were using other observability tools";
const MIN_ENGINEER_COUNT = 18;
const MAX_ENGINEER_COUNT = 700;
const MAX_SRE_COUNT = 15;
const ENGINEER_RANGE_REJECTION_NOTE = "Engineer count not in BACCA's optimal range";
const LARGE_ENGINEER_COUNT_MASK = "> 1000";
const EMAIL_WATERFALL_WAIT_MS = 20 * 60 * 1000;

interface RowResearchResult {
  companyName: string;
  companyDomain: string;
  apolloAccountId?: string;
  observability: string;
  eligible: boolean;
}

interface PendingEmailPushBatch {
  companyName: string;
  companyDomain: string;
  employees: EnrichedEmployee[];
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

export async function runResearchPipeline(
  jobId: string,
  csvBuffer: string,
  config: PipelineConfig
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

  try {
    setJobStatus(jobId, "processing");
    setJobMessage(jobId, "Starting observability research...");

    for await (const row of readCompanies({
      csvBuffer,
      nameColumn: config.nameColumn,
      domainColumn: config.domainColumn,
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
      setJobMessage(jobId, `Observability research row ${row.rowNumber}: ${row.companyName}`);

      const observability = await researchCompany(row.companyName, row.companyDomain, {
        apiKey: config.azureOpenAiApiKey,
        baseUrl: config.azureOpenAiBaseUrl,
        model: config.model,
        maxCompletionTokens: config.maxCompletionTokens,
        searchApiKey: config.searchApiKey,
      });

      const eligible = shouldProcessByObservability(observability);
      if (!eligible) {
        rejectedCompanies.push(
          `Company ${row.companyName} was rejected because it was using other observability tools`
        );
      }

      rowResults.push({
        companyName: row.companyName,
        companyDomain: row.companyDomain,
        apolloAccountId: row.apolloAccountId,
        observability,
        eligible,
      });
    }

    const eligibleRows = rowResults.filter((row) => row.eligible);
    setJobMessage(jobId, `Observability stage complete. Processing ${eligibleRows.length} eligible companies.`);
    logPipelineStage(
      "OBSERVABILITY_DONE",
      `Observability complete. eligible_companies=${eligibleRows.length} rejected_by_observability=${rowResults.length - eligibleRows.length}`
    );

    const lemlistEnabled = getEnvBoolean("LEMLIST_PUSH_ENABLED", true);
    const waterfallEnabled = getEnvBoolean("APOLLO_WATERFALL_ENABLED", false);

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
        logPipelineStage("RESOLVE_COMPANY", "Resolving company profile.", companyContext);
        const company = row.companyDomain
          ? await getCompany(row.companyDomain)
          : {
              companyName: row.companyName,
              domain: "",
            };
        logPipelineStage("COUNT_ENGINEERS", "Counting engineers.", companyContext);
        const engineerCount = await countEngineerPeople(company, {
          apolloOrganizationId: row.apolloAccountId,
        });
        logPipelineStage("COUNT_ENGINEERS_DONE", `Engineer count computed. count=${engineerCount}`, companyContext);
        const engineerCountDisplayValue = getEngineerCountDisplayValue(engineerCount);
        if (engineerCount < MIN_ENGINEER_COUNT) {
          logPipelineStage(
            "REJECT_ENGINEER_MIN",
            `Company rejected by engineer minimum. count=${engineerCountDisplayValue}`,
            companyContext
          );
          rejectedCompanies.push(
            `${row.companyName} was rejected because engineer count (${engineerCountDisplayValue}) is not in BACCA's optimal range`
          );
          rejectedOutputRows.push({
            company_name: row.companyName,
            company_domain: row.companyDomain,
            observability_tool_research: row.observability,
            sre_count: "",
            engineer_count: engineerCountDisplayValue,
            status: "NotActionableNow",
            notes: ENGINEER_RANGE_REJECTION_NOTE,
          });
          continue;
        }
        if (engineerCount > MAX_ENGINEER_COUNT) {
          logPipelineStage(
            "REJECT_ENGINEER_MAX",
            `Company rejected by engineer maximum. count=${engineerCountDisplayValue}`,
            companyContext
          );
          rejectedCompanies.push(
            `${row.companyName} was rejected because engineer count (${engineerCountDisplayValue}) is not in BACCA's optimal range`
          );
          rejectedOutputRows.push({
            company_name: row.companyName,
            company_domain: row.companyDomain,
            observability_tool_research: row.observability,
            sre_count: "",
            engineer_count: engineerCountDisplayValue,
            status: "NotActionableNow",
            notes: ENGINEER_RANGE_REJECTION_NOTE,
          });
          continue;
        }
        logPipelineStage("SEARCH_CURRENT_SRE", "Searching current SRE candidates.", companyContext);
        const prospects = await searchPeople(company, MAX_RESULTS, SRE_PERSON_TITLES, {
          apolloOrganizationId: row.apolloAccountId,
        });
        const dedupedProspects = dedupeProspectsById(prospects);
        const rawSreCount = dedupedProspects.length;
        logPipelineStage("SEARCH_CURRENT_SRE_DONE", `Current SRE candidates found. count=${rawSreCount}`, companyContext);
        if (rawSreCount > MAX_SRE_COUNT) {
          logPipelineStage(
            "REJECT_SRE_MAX",
            `Company rejected by SRE maximum. count=${rawSreCount}`,
            companyContext
          );
          const rejectionNote = `${row.companyName} got rejected because it has ${rawSreCount} number of SREs`;
          rejectedCompanies.push(rejectionNote);
          rejectedOutputRows.push({
            company_name: row.companyName,
            company_domain: row.companyDomain,
            observability_tool_research: row.observability,
            sre_count: rawSreCount,
            engineer_count: "",
            status: "NotActionableNow",
            notes: rejectionNote,
          });
          continue;
        }
        logPipelineStage("ENRICH_CURRENT_SRE", "Enriching current SRE candidates.", companyContext);
        const enrichedEmployees = await bulkEnrichPeople(dedupedProspects, enrichmentCache);
        const selectedCurrentSre = selectTopSreForLemlist(enrichedEmployees, 7);
        logPipelineStage(
          "SELECT_CURRENT_SRE",
          `Current SRE selected for LinkedIn seed. selected=${selectedCurrentSre.length}`,
          companyContext
        );
        let selectedForLemlist = selectedCurrentSre;

        // Backfill only runs if we have at least one current SRE candidate selected.
        if (selectedCurrentSre.length > 0 && selectedCurrentSre.length < 5 && rawSreCount > 0) {
          logPipelineStage("BACKFILL_PHASE_1_START", "Backfill phase 1 (past SRE) started.", companyContext);
          const pastSreProspects = dedupeProspectsById(
            await searchPastSrePeople(company, MAX_RESULTS, {
              apolloOrganizationId: row.apolloAccountId,
            })
          );
          const pastSreEnriched = await bulkEnrichPeople(pastSreProspects, enrichmentCache);
          selectedForLemlist = fillToMinimumWithBackfill(selectedCurrentSre, pastSreEnriched, [], {
            minimum: 5,
            max: 7,
          });
          logPipelineStage(
            "BACKFILL_PHASE_1_DONE",
            `Backfill phase 1 complete. selected_after_phase1=${selectedForLemlist.length}`,
            companyContext
          );

          if (selectedForLemlist.length < 5) {
            logPipelineStage("BACKFILL_PHASE_2_START", "Backfill phase 2 (platform) started.", companyContext);
            const platformProspects = dedupeProspectsById(
              await searchCurrentPlatformEngineerPeople(company, MAX_RESULTS, {
                apolloOrganizationId: row.apolloAccountId,
              })
            );
            const platformEnriched = await bulkEnrichPeople(platformProspects, enrichmentCache);
            selectedForLemlist = fillToMinimumWithBackfill(selectedForLemlist, [], platformEnriched, {
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
          logPipelineStage(
            "PUSH_LINKEDIN_START",
            `Pushing LinkedIn campaigns. candidates=${selectedForLemlist.length}`,
            companyContext
          );
          const lemlistMeta = await pushPeopleToLemlistCampaign(
            selectedForLemlist,
            company.companyName,
            company.domain
          );
          lemlistSuccessful = lemlistMeta.successful;
          lemlistFailed = lemlistMeta.failed;
          totalLinkedinCampaignSuccessful += lemlistSuccessful;
          totalLemlistSuccessful += lemlistSuccessful;
          totalLemlistFailed += lemlistFailed;
          logPipelineStage(
            "PUSH_LINKEDIN_DONE",
            `LinkedIn push complete. successful=${lemlistSuccessful} failed=${lemlistFailed}`,
            companyContext
          );
        }

        if (lemlistEnabled) {
          logPipelineStage("SEARCH_EMAIL_CANDIDATES", "Searching broad email candidates.", companyContext);
          const attemptedLinkedinKeys = new Set(selectedForLemlist.map((employee) => toEmployeeKey(employee)));
          const broadEmailProspects = dedupeProspectsById(
            await searchCurrentEngineeringEmailCandidates(company, EMAIL_CANDIDATE_MAX_RESULTS, {
              apolloOrganizationId: row.apolloAccountId,
            })
          );
          const emailEnriched = await bulkEnrichPeople(broadEmailProspects, enrichmentCache);
          logPipelineStage(
            "ENRICH_EMAIL_CANDIDATES_DONE",
            `Email candidates enriched. count=${emailEnriched.length}`,
            companyContext
          );
          const listA = emailEnriched.filter((employee) => {
            if (attemptedLinkedinKeys.has(toEmployeeKey(employee))) {
              return false;
            }
            return employee.tenure !== null && employee.tenure >= 11;
          });
          logPipelineStage("BUILD_LIST_A_DONE", `List A prepared. count=${listA.length}`, companyContext);

          if (listA.length > 0) {
            pendingEmailPushBatches.push({
              companyName: company.companyName,
              companyDomain: company.domain,
              employees: listA,
            });
            for (const employee of listA) {
              if (!employee.email && employee.id) {
                globalMissingEmailPersonIds.add(employee.id);
              }
            }
            logPipelineStage(
              "QUEUE_LIST_A",
              `List A queued for email push. missing_email_so_far=${globalMissingEmailPersonIds.size}`,
              companyContext
            );
          }
        }

        outputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          observability_tool_research: row.observability,
          status: "ChasingPOC",
          sre_count: rawSreCount,
          engineer_count: engineerCount,
        });
        logPipelineStage("COMPANY_DONE", "Company processing complete.", companyContext);
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown pipeline error";
        addJobWarning(jobId, `Apollo/Lemlist failed for ${row.companyName}: ${message}`);
        logPipelineStage("COMPANY_FAILED", `Company failed. error=${message}`, companyContext);
        outputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          observability_tool_research: row.observability,
          status: "ChasingPOC",
          sre_count: 0,
          engineer_count: 0,
        });
      }
    }

    for (const row of rowResults.filter((entry) => !entry.eligible)) {
      const rejectionNotes = row.observability.trim() || REJECTED_REASON;
      rejectedOutputRows.push({
        company_name: row.companyName,
        company_domain: row.companyDomain,
        observability_tool_research: row.observability,
        sre_count: "",
        engineer_count: "",
        status: "NotActionableNow",
        notes: rejectionNotes,
      });
    }

    let recoveredEmailsByPersonId = new Map<string, string>();
    if (lemlistEnabled && waterfallEnabled && globalMissingEmailPersonIds.size > 0) {
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

        for (const employee of batch.employees) {
          if (employee.email || !employee.id) {
            continue;
          }
          const recoveredEmail = recoveredEmailsByPersonId.get(employee.id);
          if (recoveredEmail) {
            employee.email = recoveredEmail;
          }
        }

        const emailPushMeta = await pushPeopleToLemlistEmailCampaign(
          batch.employees,
          batch.companyName,
          batch.companyDomain
        );
        totalLemlistSuccessful += emailPushMeta.successful;
        totalLemlistFailed += emailPushMeta.failed;
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
    };
    setJobSummary(jobId, summary);

    const csvString = await rowsToCsvString(outputRows);
    const rejectsCsvString = await rejectedRowsToCsvString(rejectedOutputRows);
    const csvBase64 = Buffer.from(csvString, "utf8").toString("base64");
    const rejectsCsvBase64 = Buffer.from(rejectsCsvString, "utf8").toString("base64");
    markJobDone(jobId, csvBase64, rejectsCsvBase64);
    logPipelineStage(
      "JOB_DONE",
      `Job done: processed=${apolloProcessedCompanyCount} linkedin_success=${totalLinkedinCampaignSuccessful} lemlist_success=${totalLemlistSuccessful} lemlist_failed=${totalLemlistFailed}`
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected job failure";
    markJobError(jobId, message);
    logPipelineStage("JOB_FAILED", `Job failed. error=${message}`);
  }
}
