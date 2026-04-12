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
    markJobDone(jobId, "csv_base64");

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

  it("does not evict an actively updating job even after 1+ minutes from creation", async () => {
    const { createJob, getJob, setJobMessage } = await loadJobStore();

    const jobId = createJob(); // T=0, updatedAtMs=0
    expect(getJob(jobId)).toBeDefined();

    // Simulate the job being updated at 59 seconds (still alive)
    vi.setSystemTime(new Date("2026-01-01T00:00:59.000Z"));
    setJobMessage(jobId, "still running"); // updatedAtMs = 59 sec

    // Advance to 61 seconds from start, but only 2 seconds since last update
    vi.setSystemTime(new Date("2026-01-01T00:01:01.000Z"));

    // Trigger cleanup — job should NOT be evicted (only 2 sec since last update)
    const job = getJob(jobId);
    expect(job).toBeDefined();
    expect(job?.message).toBe("still running");
  });

  it("evicts a stale job that has not been updated for 1+ minutes", async () => {
    const { createJob, getJob, setJobMessage } = await loadJobStore();

    const jobId = createJob(); // T=0
    setJobMessage(jobId, "started"); // updatedAtMs = T=0

    // Advance 61 seconds — no updates since creation
    vi.setSystemTime(new Date("2026-01-01T00:01:01.000Z"));

    // Another job triggers cleanup
    createJob();
    expect(getJob(jobId)).toBeUndefined();
  });

  it("setJobPartialResults stores partial csv and campaignPushData on the job", async () => {
    const { createJob, getJob, setJobPartialResults } = await loadJobStore();

    const jobId = createJob();
    const fakeCampaignPushData = {
      linkedinSre: [],
      linkedinEngLead: [],
      linkedinEng: [],
      emailSre: [],
      emailEng: [],
      emailEngLead: [],
      filteredOutCandidates: [],
      normalEngineerApifyWarnings: [],
    };

    setJobPartialResults(jobId, "partial_csv_base64", fakeCampaignPushData);

    const job = getJob(jobId);
    expect(job?.partialCsvBase64).toBe("partial_csv_base64");
    expect(job?.partialCampaignPushData).toEqual(fakeCampaignPushData);
  });
});
