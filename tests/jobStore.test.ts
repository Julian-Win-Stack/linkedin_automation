import { beforeEach, describe, expect, it, vi } from "vitest";

async function loadJobStore() {
  return import("../src/jobs/jobStore");
}

describe("jobStore lifecycle", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-01-01T00:00:00.000Z"));
  });

  it("evicts jobs older than one hour during cleanup", async () => {
    const { createJob, getJob } = await loadJobStore();
    const oldJobId = createJob();
    expect(getJob(oldJobId)).toBeDefined();

    vi.setSystemTime(new Date("2026-01-01T01:00:12.000Z"));
    const triggerJobId = createJob();
    expect(getJob(triggerJobId)).toBeDefined();
    expect(getJob(oldJobId)).toBeUndefined();
  });

  it("evicts oldest jobs when max job count is exceeded", async () => {
    const { createJob, getJob } = await loadJobStore();
    const ids: string[] = [];
    for (let index = 0; index < 22; index += 1) {
      vi.setSystemTime(new Date(`2026-01-01T00:00:${String(index).padStart(2, "0")}.000Z`));
      ids.push(createJob());
    }

    // Advance >10s so cleanup runs again and enforces MAX_JOBS.
    vi.setSystemTime(new Date("2026-01-01T00:01:00.000Z"));
    const latestId = createJob();

    expect(getJob(latestId)).toBeDefined();
    expect(getJob(ids[0])).toBeUndefined();
    expect(getJob(ids[1])).toBeUndefined();
    expect(getJob(ids[2])).toBeDefined();
  });

  it("updates status fields for done and error jobs", async () => {
    const {
      addJobWarning,
      createJob,
      getJob,
      markJobDone,
      markJobError,
      setJobMessage,
      setJobProgress,
    } = await loadJobStore();
    const jobId = createJob();
    setJobMessage(jobId, "running");
    setJobProgress(jobId, { currentRow: 2, totalRows: 10 });
    addJobWarning(jobId, "warn");
    markJobDone(jobId, "csv_base64", "rejects_base64");

    const doneJob = getJob(jobId);
    expect(doneJob?.status).toBe("done");
    expect(doneJob?.message).toBe("running");
    expect(doneJob?.currentRow).toBe(2);
    expect(doneJob?.totalRows).toBe(10);
    expect(doneJob?.warnings).toEqual(["warn"]);
    expect(doneJob?.csvBase64).toBe("csv_base64");

    markJobError(jobId, "failed");
    const errorJob = getJob(jobId);
    expect(errorJob?.status).toBe("error");
    expect(errorJob?.error).toBe("failed");
  });
});
