import { getEnvBoolean } from "../config/env";
import { PipelineConfig } from "../config/pipelineConfig";
import { bulkEnrichPeople } from "../services/bulkEnrichPeople";
import { getCompany } from "../services/getCompany";
import { pushPeopleToLemlistCampaign } from "../services/lemlistPushQueue";
import { countEngineerPeople, searchPeople } from "../services/searchPeople";
import { readCompanies } from "../services/observability/csvReader";
import { researchCompany } from "../services/observability/openaiClient";
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
  setJobStatus,
  setJobSummary,
  setRejectedCompanies,
} from "./jobStore";

const MAX_ROWS = 500;
const SRE_PERSON_TITLES = ["SRE", "Site Reliability"];
const MAX_RESULTS = 30;
const REJECTED_REASON = "rejected because they were using other observability tools";
const MIN_ENGINEER_COUNT = 20;

interface RowResearchResult {
  companyName: string;
  companyDomain: string;
  observability: string;
  eligible: boolean;
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

export async function runResearchPipeline(
  jobId: string,
  csvBuffer: string,
  config: PipelineConfig
): Promise<void> {
  const rowResults: RowResearchResult[] = [];
  const outputRows: OutputRow[] = [];
  const rejectedOutputRows: RejectedOutputRow[] = [];
  const rejectedCompanies: string[] = [];
  let totalRows = 0;
  let apolloProcessedCompanyCount = 0;
  let totalSreFound = 0;
  let totalLemlistSuccessful = 0;
  let totalLemlistFailed = 0;

  try {
    setJobStatus(jobId, "processing");
    setJobMessage(jobId, "Starting observability research...");

    for await (const row of readCompanies({
      csvBuffer,
      nameColumn: config.nameColumn,
      domainColumn: config.domainColumn,
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
        observability,
        eligible,
      });
    }

    const eligibleRows = rowResults.filter((row) => row.eligible);
    setJobMessage(jobId, `Observability stage complete. Processing ${eligibleRows.length} eligible companies.`);

    const lemlistEnabled = getEnvBoolean("LEMLIST_PUSH_ENABLED", true);

    for (let index = 0; index < eligibleRows.length; index += 1) {
      if (isCancelled(jobId)) {
        return;
      }

      const row = eligibleRows[index];
      const progressRow = totalRows + index + 1;
      setJobProgress(jobId, { currentRow: progressRow, totalRows: totalRows + eligibleRows.length });
      setJobMessage(jobId, `Apollo stage ${index + 1}/${eligibleRows.length}: ${row.companyName}`);

      try {
        const company = await getCompany(row.companyDomain);
        const engineerCount = await countEngineerPeople(company);
        if (engineerCount < MIN_ENGINEER_COUNT) {
          rejectedCompanies.push(
            `${row.companyName} was rejected because it has only ${engineerCount} number of software engineers`
          );
          rejectedOutputRows.push({
            company_name: row.companyName,
            company_domain: row.companyDomain,
            observability_tool_research: row.observability,
            sre_count: "",
            engineer_count: "",
            status: "NotActionableNow",
            notes: `Software engineer count: "${engineerCount}"`,
          });
          continue;
        }
        const prospects = await searchPeople(company, MAX_RESULTS, SRE_PERSON_TITLES);
        const dedupedProspects = dedupeProspectsById(prospects);
        const enrichedEmployees = await bulkEnrichPeople(dedupedProspects);

        apolloProcessedCompanyCount += 1;
        totalSreFound += enrichedEmployees.length;

        let lemlistSuccessful = 0;
        let lemlistFailed = 0;
        if (lemlistEnabled && enrichedEmployees.length > 0) {
          const lemlistMeta = await pushPeopleToLemlistCampaign(
            enrichedEmployees,
            company.companyName,
            company.domain
          );
          lemlistSuccessful = lemlistMeta.successful;
          lemlistFailed = lemlistMeta.failed;
          totalLemlistSuccessful += lemlistSuccessful;
          totalLemlistFailed += lemlistFailed;
        }

        outputRows.push({
          company_name: row.companyName,
          company_domain: row.companyDomain,
          observability_tool_research: row.observability,
          status: "ChasingPOC",
          sre_count: enrichedEmployees.length,
          engineer_count: engineerCount,
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : "Unknown pipeline error";
        addJobWarning(jobId, `Apollo/Lemlist failed for ${row.companyName}: ${message}`);
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

    setRejectedCompanies(jobId, rejectedCompanies, REJECTED_REASON);

    const summary: JobSummary = {
      totalRows,
      eligibleCompanyCount: eligibleRows.length,
      rejectedCompanyCount: rejectedCompanies.length,
      apolloProcessedCompanyCount,
      totalSreFound,
      totalLemlistSuccessful,
      totalLemlistFailed,
    };
    setJobSummary(jobId, summary);

    const csvString = await rowsToCsvString(outputRows);
    const rejectsCsvString = await rejectedRowsToCsvString(rejectedOutputRows);
    const csvBase64 = Buffer.from(csvString, "utf8").toString("base64");
    const rejectsCsvBase64 = Buffer.from(rejectsCsvString, "utf8").toString("base64");
    markJobDone(jobId, csvBase64, rejectsCsvBase64);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected job failure";
    markJobError(jobId, message);
  }
}
