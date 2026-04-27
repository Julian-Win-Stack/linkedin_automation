import { randomUUID } from "node:crypto";

export type JobStatus = "pending" | "processing" | "cancelled" | "done" | "error";

export interface CampaignPushEntry {
  companyName: string;
  name: string;
  title: string;
  linkedinUrl: string | null;
  lemlistStatus: "succeed" | "failed" | "skipped";
  lemlistError?: string;
}

export type FilteredOutReason = "open_to_work" | "frontend_role" | "contract_employment" | "hardware_heavy" | "qa_title";

export interface FilteredOutCampaignSummary {
  companyName: string;
  openToWorkCount: number;
  frontendRoleCount: number;
  contractEmploymentCount: number;
  hardwareHeavyCount: number;
  qaTitleCount: number;
}

export interface WarningProblemSummary {
  problem: string;
  count: number;
}

export interface NormalEngineerApifyWarningSummary {
  companyName: string;
  totalCount: number;
  problems: WarningProblemSummary[];
}

export interface CampaignPushData {
  linkedinSre: CampaignPushEntry[];
  linkedinEngLead: CampaignPushEntry[];
  linkedinEng: CampaignPushEntry[];
  filteredOutCandidates: FilteredOutCampaignSummary[];
  normalEngineerApifyWarnings: NormalEngineerApifyWarningSummary[];
}

export interface JobSummary {
  totalRows: number;
  eligibleCompanyCount: number;
  skippedMissingWebsiteAndApolloAccountIdCount: number;
  apolloProcessedCompanyCount: number;
  totalLinkedinCampaignSuccessful: number;
  totalLinkedinCampaignFailed: number;
  totalLinkedinCampaignSkipped: number;
  weeklyLimitSkippedCompanyCount: number;
}

export type JobState = {
  status: JobStatus;
  message?: string;
  totalRows?: number;
  currentRow?: number;
  warnings: string[];
  skippedCompanies: string[];
  csvBase64?: string;
  error?: string;
  summary?: JobSummary;
  campaignPushData?: CampaignPushData;
  partialCsvBase64?: string;
  partialCampaignPushData?: CampaignPushData;
  createdAtMs: number;
  updatedAtMs: number;
};

const jobs = new Map<string, JobState>();
const TERMINAL_JOB_TTL_MS = 60_000;
const CLEANUP_THROTTLE_MS = 60_000;
let lastCleanupAtMs = 0;

function cleanup(nowMs: number): void {
  if (nowMs - lastCleanupAtMs < CLEANUP_THROTTLE_MS) {
    return;
  }
  lastCleanupAtMs = nowMs;

  for (const [jobId, job] of jobs) {
    if (nowMs - job.updatedAtMs > TERMINAL_JOB_TTL_MS) {
      jobs.delete(jobId);
    }
  }
}

export function createJob(): string {
  const nowMs = Date.now();
  cleanup(nowMs);

  const jobId = randomUUID();
  jobs.set(jobId, {
    status: "pending",
    warnings: [],
    skippedCompanies: [],
    createdAtMs: nowMs,
    updatedAtMs: nowMs,
  });
  return jobId;
}

export function getJob(jobId: string): JobState | undefined {
  cleanup(Date.now());
  return jobs.get(jobId);
}

export function removeJob(jobId: string): void {
  jobs.delete(jobId);
}

export function setJobStatus(jobId: string, status: JobStatus): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.status = status;
  job.updatedAtMs = Date.now();
}

export function setJobMessage(jobId: string, message: string | undefined): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.message = message;
  job.updatedAtMs = Date.now();
}

export function setJobProgress(jobId: string, opts: { currentRow?: number; totalRows?: number }): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  if (typeof opts.currentRow === "number") {
    job.currentRow = opts.currentRow;
  }
  if (typeof opts.totalRows === "number") {
    job.totalRows = opts.totalRows;
  }
  job.updatedAtMs = Date.now();
}

export function addJobWarning(jobId: string, warning: string): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.warnings.push(warning);
  job.updatedAtMs = Date.now();
}

export function setSkippedCompanies(jobId: string, companies: string[]): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.skippedCompanies = companies;
  job.updatedAtMs = Date.now();
}

export function setJobSummary(jobId: string, summary: JobSummary): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.summary = summary;
  job.updatedAtMs = Date.now();
}

export function setCampaignPushData(jobId: string, data: CampaignPushData): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.campaignPushData = data;
  job.updatedAtMs = Date.now();
}

export function setJobPartialResults(
  jobId: string,
  csvBase64: string,
  campaignPushData: CampaignPushData
): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.partialCsvBase64 = csvBase64;
  job.partialCampaignPushData = campaignPushData;
  job.updatedAtMs = Date.now();
}

export function markJobDone(jobId: string, csvBase64: string): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.status = "done";
  job.csvBase64 = csvBase64;
  job.updatedAtMs = Date.now();
}

export function markJobError(jobId: string, error: string): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.status = "error";
  job.error = error;
  job.updatedAtMs = Date.now();
}

export function markJobCancelled(jobId: string, message = "Job cancelled by user"): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.status = "cancelled";
  job.message = message;
  job.updatedAtMs = Date.now();
}
