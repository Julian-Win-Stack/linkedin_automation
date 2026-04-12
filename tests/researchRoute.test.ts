import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import researchRouter from "../src/routes/research";
import {
  setJobMessage,
  setJobProgress,
  createJob,
  getJob,
  markJobDone,
  setJobPartialResults,
} from "../src/jobs/jobStore";

const runResearchPipelineMock = vi.fn();
const loadPipelineConfigMock = vi.fn();
const getWeeklySuccessCountsMock = vi.fn();
const enqueueQueueItemMock = vi.fn();
const claimNextQueuedItemForUserMock = vi.fn();
const setQueueItemJobIdMock = vi.fn();
const recoverRunningItemsToQueuedMock = vi.fn();
const listQueueItemsForUserMock = vi.fn();
const getQueueItemByIdMock = vi.fn();
const getQueueItemByJobIdMock = vi.fn();
const completeQueueItemMock = vi.fn();
const toQueueLabelMock = vi.fn();

vi.mock("../src/jobs/researchPipeline", () => ({
  runResearchPipeline: (...args: unknown[]) => runResearchPipelineMock(...args),
}));

vi.mock("../src/config/pipelineConfig", () => ({
  loadPipelineConfig: (...args: unknown[]) => loadPipelineConfigMock(...args),
}));

vi.mock("../src/services/weeklySuccessStore", () => ({
  getWeeklySuccessCounts: (...args: unknown[]) => getWeeklySuccessCountsMock(...args),
}));

vi.mock("../src/services/queueStore", () => ({
  enqueueQueueItem: (...args: unknown[]) => enqueueQueueItemMock(...args),
  claimNextQueuedItemForUser: (...args: unknown[]) => claimNextQueuedItemForUserMock(...args),
  setQueueItemJobId: (...args: unknown[]) => setQueueItemJobIdMock(...args),
  recoverRunningItemsToQueued: (...args: unknown[]) => recoverRunningItemsToQueuedMock(...args),
  listQueueItemsForUser: (...args: unknown[]) => listQueueItemsForUserMock(...args),
  getQueueItemById: (...args: unknown[]) => getQueueItemByIdMock(...args),
  getQueueItemByJobId: (...args: unknown[]) => getQueueItemByJobIdMock(...args),
  completeQueueItem: (...args: unknown[]) => completeQueueItemMock(...args),
  toQueueLabel: (...args: unknown[]) => toQueueLabelMock(...args),
}));

function createTestApp() {
  const app = express();
  app.use(express.json());
  app.use(researchRouter);
  return app;
}

describe("research job routes", () => {
  beforeEach(() => {
    runResearchPipelineMock.mockReset();
    loadPipelineConfigMock.mockReset();
    getWeeklySuccessCountsMock.mockReset();
    enqueueQueueItemMock.mockReset();
    claimNextQueuedItemForUserMock.mockReset();
    setQueueItemJobIdMock.mockReset();
    recoverRunningItemsToQueuedMock.mockReset();
    listQueueItemsForUserMock.mockReset();
    getQueueItemByIdMock.mockReset();
    getQueueItemByJobIdMock.mockReset();
    completeQueueItemMock.mockReset();
    toQueueLabelMock.mockReset();
    loadPipelineConfigMock.mockReturnValue({
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "gpt-5.4",
      maxCompletionTokens: 2048,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    });
    getWeeklySuccessCountsMock.mockReturnValue({
      linkedinCount: 0,
      emailCount: 0,
      companiesReachedOutToCount: 0,
    });
    enqueueQueueItemMock.mockReturnValue({
      queueItemId: "queue-1",
      queueOrder: 1,
      status: "queued",
    });
    claimNextQueuedItemForUserMock.mockReturnValue(null);
    recoverRunningItemsToQueuedMock.mockReturnValue(0);
    listQueueItemsForUserMock.mockReturnValue([]);
    getQueueItemByJobIdMock.mockReturnValue(null);
    toQueueLabelMock.mockReturnValue("1st queue");
  });

  it("returns 400 when no csv file is sent", async () => {
    const app = createTestApp();
    const response = await request(app).post("/research").send({});
    expect(response.status).toBe(400);
  });

  it("enqueues a csv and returns queue item metadata", async () => {
    const app = createTestApp();
    const csv = "Company Name,Website\nAcme,acme.com\n";
    const response = await request(app)
      .post("/research")
      .field("selectedUser", "julian")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(200);
    expect(response.body.queueItemId).toBe("queue-1");
    expect(response.body.queueOrder).toBe(1);
    expect(response.body.queueLabel).toBe("1st queue");
    expect(enqueueQueueItemMock).toHaveBeenCalledTimes(1);
  });

  it("starts a job when website column is missing but apollo account id column exists", async () => {
    const app = createTestApp();
    const csv = "Company Name,Apollo Account Id\nAcme,apollo-123\n";
    const response = await request(app)
      .post("/research")
      .field("selectedUser", "cherry")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(200);
    expect(response.body.queueItemId).toBe("queue-1");
  });

  it("returns 400 when company name column is missing", async () => {
    const app = createTestApp();
    const csv = "Website,Apollo Account Id\nacme.com,apollo-123\n";
    const response = await request(app)
      .post("/research")
      .field("selectedUser", "raihan")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("Company Name");
  });

  it("returns 400 when both website and apollo account id columns are missing", async () => {
    const app = createTestApp();
    const csv = "Company Name\nAcme\n";
    const response = await request(app)
      .post("/research")
      .field("selectedUser", "julian")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(400);
    expect(response.body.error).toContain('at least one of "Website" or "Apollo Account Id"');
  });

  it("returns 400 when selectedUser is missing", async () => {
    const app = createTestApp();
    const csv = "Company Name,Website\nAcme,acme.com\n";
    const response = await request(app)
      .post("/research")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("selectedUser is required");
  });

  it("returns 400 when selectedUser is invalid", async () => {
    const app = createTestApp();
    const csv = "Company Name,Website\nAcme,acme.com\n";
    const response = await request(app)
      .post("/research")
      .field("selectedUser", "someoneelse")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("selectedUser is required");
  });

  it("lists queue items for selected user", async () => {
    const app = createTestApp();
    listQueueItemsForUserMock.mockReturnValueOnce([
      {
        queueItemId: "queue-1",
        selectedUser: "julian",
        queueOrder: 1,
        status: "done",
        weekStartMs: 0,
        csvInput: "Company Name,Website\nAcme,acme.com\n",
        jobId: null,
        csvOutputBase64: "YQ==",
        summary: null,
        warnings: [],
        skippedCompanies: [],
        rejectedCompanies: [],
        rejectedReason: null,
        errorMessage: null,
        campaignPushData: null,
        createdAtMs: 1,
        updatedAtMs: 2,
        startedAtMs: 3,
        completedAtMs: 4,
      },
    ]);
    toQueueLabelMock.mockReturnValueOnce("1st queue");

    const response = await request(app).get("/queue").query({ selectedUser: "julian" });
    expect(response.status).toBe(200);
    expect(Array.isArray(response.body.items)).toBe(true);
    expect(response.body.items[0].queueLabel).toBe("1st queue");
    expect(response.body.items[0].status).toBe("done");
  });

  it("returns 409 when queue limit is reached", async () => {
    const app = createTestApp();
    enqueueQueueItemMock.mockImplementationOnce(() => {
      throw new Error("Queue limit reached (10) for julian.");
    });
    const csv = "Company Name,Website\nAcme,acme.com\n";
    const response = await request(app)
      .post("/research")
      .field("selectedUser", "julian")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(409);
    expect(response.body.error).toContain("Queue limit reached");
  });

  it("returns weekly counts for valid selected user and week start", async () => {
    const app = createTestApp();
    getWeeklySuccessCountsMock.mockReturnValueOnce({
      linkedinCount: 7,
      emailCount: 9,
      companiesReachedOutToCount: 11,
    });
    const weekStartMs = Date.now();

    const response = await request(app).get("/weekly-counts").query({
      selectedUser: "julian",
      weekStartMs,
    });

    expect(response.status).toBe(200);
    expect(response.body).toEqual({
      linkedinCount: 7,
      emailCount: 9,
      companiesReachedOutToCount: 11,
    });
    expect(getWeeklySuccessCountsMock).toHaveBeenCalledWith({
      selectedUser: "julian",
      weekStartMs,
    });
  });

  it("returns 400 for weekly counts when selectedUser is invalid", async () => {
    const app = createTestApp();
    const response = await request(app).get("/weekly-counts").query({
      selectedUser: "someone",
      weekStartMs: Date.now(),
    });

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("selectedUser is required");
  });

  it("returns 400 for weekly counts when weekStartMs is invalid", async () => {
    const app = createTestApp();
    const response = await request(app).get("/weekly-counts").query({
      selectedUser: "julian",
      weekStartMs: "not-a-number",
    });

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("weekStartMs");
  });


  it("downloads queue csv with clean filename", async () => {
    const app = createTestApp();
    getQueueItemByIdMock.mockReturnValueOnce({
      queueItemId: "queue-1",
      selectedUser: "julian",
      queueOrder: 1,
      status: "done",
      weekStartMs: 0,
      csvInput: "Company Name,Website\nAcme,acme.com\n",
      jobId: null,
      csvOutputBase64: Buffer.from("Company Name,Website\nAcme,acme.com\n", "utf8").toString("base64"),
      summary: null,
      warnings: [],
      skippedCompanies: [],
      rejectedCompanies: [],
      rejectedReason: null,
      errorMessage: null,
      campaignPushData: null,
      createdAtMs: 1,
      updatedAtMs: 2,
      startedAtMs: 3,
      completedAtMs: 4,
    });

    const response = await request(app).get("/queue/queue-1/csv");
    expect(response.status).toBe(200);
    expect(response.headers["content-disposition"]).toContain('attachment; filename="research-results.csv"');
  });

  it("downloads pdf for a persisted finished job", async () => {
    const app = createTestApp();
    getQueueItemByJobIdMock.mockReturnValueOnce({
      queueItemId: "queue-1",
      selectedUser: "julian",
      queueOrder: 1,
      status: "done",
      weekStartMs: 0,
      csvInput: "x",
      jobId: "finished-job",
      csvOutputBase64: null,
      summary: null,
      warnings: [],
      skippedCompanies: [],
      rejectedCompanies: [],
      rejectedReason: null,
      errorMessage: null,
      campaignPushData: {
        linkedinSre: [
          {
            companyName: "Acme",
            name: "Jane Doe",
            title: "SRE",
            linkedinUrl: "https://linkedin.com/in/jane",
            lemlistStatus: "succeed",
          },
        ],
        linkedinEngLead: [],
        linkedinEng: [],
        emailSre: [],
        emailEng: [],
        emailEngLead: [],
        filteredOutCandidates: [],
        normalEngineerApifyWarnings: [],
      },
      createdAtMs: 1,
      updatedAtMs: 2,
      startedAtMs: 3,
      completedAtMs: 4,
    });

    const response = await request(app).get("/pdf/finished-job");
    expect(response.status).toBe(200);
    expect(response.headers["content-type"]).toContain("application/pdf");
  });

  it("evicts a finished job from memory after persisting the queue item", async () => {
    const app = createTestApp();
    enqueueQueueItemMock.mockReturnValueOnce({
      queueItemId: "queue-1",
      queueOrder: 1,
      status: "queued",
    });
    claimNextQueuedItemForUserMock
      .mockReturnValueOnce({
        queueItemId: "queue-1",
        selectedUser: "julian",
        queueOrder: 1,
        status: "running",
        weekStartMs: 0,
        csvInput: "Company Name,Website\nAcme,acme.com\n",
        jobId: null,
        csvOutputBase64: null,
        summary: null,
        warnings: [],
        skippedCompanies: [],
        rejectedCompanies: [],
        rejectedReason: null,
        errorMessage: null,
        campaignPushData: null,
        createdAtMs: 1,
        updatedAtMs: 2,
        startedAtMs: 3,
        completedAtMs: null,
      })
      .mockReturnValueOnce(null);
    getQueueItemByIdMock.mockReturnValue({
      queueItemId: "queue-1",
      selectedUser: "julian",
      queueOrder: 1,
      status: "running",
      weekStartMs: 0,
      csvInput: "Company Name,Website\nAcme,acme.com\n",
      jobId: "dynamic",
      csvOutputBase64: null,
      summary: null,
      warnings: [],
      skippedCompanies: [],
      rejectedCompanies: [],
      rejectedReason: null,
      errorMessage: null,
      campaignPushData: null,
      createdAtMs: 1,
      updatedAtMs: 2,
      startedAtMs: 3,
      completedAtMs: null,
    });
    runResearchPipelineMock.mockImplementationOnce(async (jobId: string) => {
      markJobDone(jobId, Buffer.from("a,b\n1,2\n", "utf8").toString("base64"));
    });

    const response = await request(app)
      .post("/research")
      .field("selectedUser", "julian")
      .attach("csv", Buffer.from("Company Name,Website\nAcme,acme.com\n", "utf8"), "input.csv");

    expect(response.status).toBe(200);

    let persistedJobId = "";
    for (let attempt = 0; attempt < 20; attempt += 1) {
      const jobId = setQueueItemJobIdMock.mock.calls[0]?.[1] as string | undefined;
      if (jobId && completeQueueItemMock.mock.calls.length > 0 && !getJob(jobId)) {
        persistedJobId = jobId;
        break;
      }
      await Promise.resolve();
    }

    expect(persistedJobId).not.toBe("");
    expect(getJob(persistedJobId)).toBeUndefined();
    expect(completeQueueItemMock).toHaveBeenCalledTimes(1);
  });

  it("cancels all active queue items for selected user", async () => {
    const app = createTestApp();
    const runningJobId = createJob();
    listQueueItemsForUserMock.mockReturnValueOnce([
      {
        queueItemId: "queue-running",
        selectedUser: "julian",
        queueOrder: 1,
        status: "running",
        weekStartMs: 0,
        csvInput: "x",
        jobId: runningJobId,
        csvOutputBase64: null,
        summary: null,
        warnings: ["w1"],
        skippedCompanies: [],
        rejectedCompanies: [],
        rejectedReason: null,
        errorMessage: null,
        campaignPushData: null,
        createdAtMs: 1,
        updatedAtMs: 2,
        startedAtMs: 3,
        completedAtMs: null,
      },
      {
        queueItemId: "queue-queued",
        selectedUser: "julian",
        queueOrder: 2,
        status: "queued",
        weekStartMs: 0,
        csvInput: "y",
        jobId: null,
        csvOutputBase64: null,
        summary: null,
        warnings: [],
        skippedCompanies: [],
        rejectedCompanies: [],
        rejectedReason: null,
        errorMessage: null,
        campaignPushData: null,
        createdAtMs: 1,
        updatedAtMs: 2,
        startedAtMs: null,
        completedAtMs: null,
      },
      {
        queueItemId: "queue-done",
        selectedUser: "julian",
        queueOrder: 3,
        status: "done",
        weekStartMs: 0,
        csvInput: "z",
        jobId: null,
        csvOutputBase64: null,
        summary: null,
        warnings: [],
        skippedCompanies: [],
        rejectedCompanies: [],
        rejectedReason: null,
        errorMessage: null,
        campaignPushData: null,
        createdAtMs: 1,
        updatedAtMs: 2,
        startedAtMs: null,
        completedAtMs: 4,
      },
    ]);

    const response = await request(app)
      .post("/queue/cancel-all")
      .send({ selectedUser: "julian" });

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({
      status: "cancelled",
      cancelledCount: 2,
    });
    expect(completeQueueItemMock).toHaveBeenCalledTimes(2);
  });

  it("returns 400 when cancel-all selectedUser is invalid", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/queue/cancel-all")
      .send({ selectedUser: "someoneelse" });
    expect(response.status).toBe(400);
    expect(response.body.error).toContain("selectedUser is required");
  });

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

  function makeRunningQueueItem(jobId: string) {
    return {
      queueItemId: "queue-1",
      selectedUser: "julian",
      queueOrder: 1,
      status: "running" as const,
      weekStartMs: 0,
      csvInput: "Company Name,Website\nAcme,acme.com\n",
      jobId,
      csvOutputBase64: null,
      summary: null,
      warnings: [],
      skippedCompanies: [],
      rejectedCompanies: [],
      rejectedReason: null,
      errorMessage: null,
      campaignPushData: null,
      createdAtMs: 1,
      updatedAtMs: 2,
      startedAtMs: 3,
      completedAtMs: null,
    };
  }

  it("reports hasCsv and hasPdf true for a running job with partial results", async () => {
    const app = createTestApp();
    const jobId = createJob();
    setJobPartialResults(jobId, Buffer.from("partial csv").toString("base64"), fakeCampaignPushData);

    listQueueItemsForUserMock.mockReturnValueOnce([makeRunningQueueItem(jobId)]);
    toQueueLabelMock.mockReturnValueOnce("1st queue");

    const response = await request(app).get("/queue").query({ selectedUser: "julian" });
    expect(response.status).toBe(200);
    expect(response.body.items[0].hasCsv).toBe(true);
    expect(response.body.items[0].hasPdf).toBe(true);
  });

  it("reports hasCsv and hasPdf false for a running job with no partial results yet", async () => {
    const app = createTestApp();
    const jobId = createJob(); // no partial results set

    listQueueItemsForUserMock.mockReturnValueOnce([makeRunningQueueItem(jobId)]);
    toQueueLabelMock.mockReturnValueOnce("1st queue");

    const response = await request(app).get("/queue").query({ selectedUser: "julian" });
    expect(response.status).toBe(200);
    expect(response.body.items[0].hasCsv).toBe(false);
    expect(response.body.items[0].hasPdf).toBe(false);
  });

  it("serves partial csv from running job when queue item has no final csv", async () => {
    const app = createTestApp();
    const jobId = createJob();
    const partialCsv = "company_name,company_domain\nAcme,acme.com\n";
    setJobPartialResults(jobId, Buffer.from(partialCsv, "utf8").toString("base64"), fakeCampaignPushData);

    getQueueItemByIdMock.mockReturnValueOnce(makeRunningQueueItem(jobId));

    const response = await request(app).get("/queue/queue-1/csv");
    expect(response.status).toBe(200);
    expect(response.headers["content-disposition"]).toContain("research-results-partial.csv");
    expect(response.text).toBe(partialCsv);
  });

  it("serves partial pdf from running job when queue item has no final pdf", async () => {
    const app = createTestApp();
    const jobId = createJob();
    setJobPartialResults(jobId, "csv_base64", fakeCampaignPushData);

    getQueueItemByIdMock.mockReturnValueOnce(makeRunningQueueItem(jobId));

    const response = await request(app).get("/queue/queue-1/pdf");
    expect(response.status).toBe(200);
    expect(response.headers["content-type"]).toContain("application/pdf");
    expect(response.headers["content-disposition"]).toContain("people-partial.pdf");
  });

  it("returns 400 for partial csv when running job has no partial results yet", async () => {
    const app = createTestApp();
    const jobId = createJob(); // no partial results

    getQueueItemByIdMock.mockReturnValueOnce(makeRunningQueueItem(jobId));

    const response = await request(app).get("/queue/queue-1/csv");
    expect(response.status).toBe(400);
    expect(response.body.error).toContain("CSV is not available");
  });
});
