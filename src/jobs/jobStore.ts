import { randomUUID } from "node:crypto";

export type JobStatus = "pending" | "processing" | "cancelled" | "done" | "error";

export interface JobSummary {
  totalRows: number;
  eligibleCompanyCount: number;
  rejectedCompanyCount: number;
  skippedMissingWebsiteAndApolloAccountIdCount: number;
  apolloProcessedCompanyCount: number;
  totalSreFound: number;
  totalLemlistSuccessful: number;
  totalLemlistFailed: number;
}

export type JobState = {
  status: JobStatus;
  message?: string;
  totalRows?: number;
  currentRow?: number;
  warnings: string[];
  skippedCompanies: string[];
  csvBase64?: string;
  rejectsCsvBase64?: string;
  error?: string;
  rejectedCompanies: string[];
  rejectedReason?: string;
  summary?: JobSummary;
  createdAtMs: number;
  updatedAtMs: number;
};

const jobs = new Map<string, JobState>();
const MAX_JOB_AGE_MS = 60 * 60 * 1000;
const MAX_JOBS = 20;
let lastCleanupAtMs = 0;

function cleanup(nowMs: number): void {
  if (nowMs - lastCleanupAtMs < 10_000) {
    return;
  }
  lastCleanupAtMs = nowMs;

  for (const [jobId, job] of jobs) {
    if (nowMs - job.createdAtMs > MAX_JOB_AGE_MS) {
      jobs.delete(jobId);
    }
  }

  if (jobs.size <= MAX_JOBS) {
    return;
  }
  const sorted = [...jobs.entries()].sort((a, b) => a[1].createdAtMs - b[1].createdAtMs);
  const toDelete = sorted.length - MAX_JOBS;
  for (let index = 0; index < toDelete; index += 1) {
    jobs.delete(sorted[index][0]);
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
    rejectedCompanies: [],
    createdAtMs: nowMs,
    updatedAtMs: nowMs,
  });
  return jobId;
}

export function getJob(jobId: string): JobState | undefined {
  cleanup(Date.now());
  return jobs.get(jobId);
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

export function setRejectedCompanies(jobId: string, companies: string[], rejectedReason: string): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.rejectedCompanies = companies;
  job.rejectedReason = rejectedReason;
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

export function markJobDone(jobId: string, csvBase64: string, rejectsCsvBase64?: string): void {
  const job = jobs.get(jobId);
  if (!job) {
    return;
  }
  job.status = "done";
  job.csvBase64 = csvBase64;
  job.rejectsCsvBase64 = rejectsCsvBase64;
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
