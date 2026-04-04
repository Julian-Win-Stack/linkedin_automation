import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import researchRouter from "../src/routes/research";
import {
  setJobMessage,
  setJobProgress,
  createJob,
  markJobDone,
  markJobCancelled,
  markJobError,
  setSkippedCompanies,
  setJobSummary,
  setRejectedCompanies,
} from "../src/jobs/jobStore";

const runResearchPipelineMock = vi.fn();
const loadPipelineConfigMock = vi.fn();
const getWeeklySuccessCountsMock = vi.fn();

vi.mock("../src/jobs/researchPipeline", () => ({
  runResearchPipeline: (...args: unknown[]) => runResearchPipelineMock(...args),
}));

vi.mock("../src/config/pipelineConfig", () => ({
  loadPipelineConfig: (...args: unknown[]) => loadPipelineConfigMock(...args),
}));

vi.mock("../src/services/weeklySuccessStore", () => ({
  getWeeklySuccessCounts: (...args: unknown[]) => getWeeklySuccessCountsMock(...args),
}));

function createTestApp() {
  const app = express();
  app.use(researchRouter);
  return app;
}

describe("research job routes", () => {
  beforeEach(() => {
    runResearchPipelineMock.mockReset();
    loadPipelineConfigMock.mockReset();
    getWeeklySuccessCountsMock.mockReset();
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
    });
  });

  it("returns 400 when no csv file is sent", async () => {
    const app = createTestApp();
    const response = await request(app).post("/research").send({});
    expect(response.status).toBe(400);
  });

  it("starts a job and returns jobId", async () => {
    const app = createTestApp();
    const csv = "Company Name,Website\nAcme,acme.com\n";
    const response = await request(app)
      .post("/research")
      .field("selectedUser", "julian")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(200);
    expect(typeof response.body.jobId).toBe("string");
    expect(runResearchPipelineMock).toHaveBeenCalledTimes(1);
    expect(runResearchPipelineMock).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(String),
      expect.any(Object),
      "julian"
    );
  });

  it("starts a job when website column is missing but apollo account id column exists", async () => {
    const app = createTestApp();
    const csv = "Company Name,Apollo Account Id\nAcme,apollo-123\n";
    const response = await request(app)
      .post("/research")
      .field("selectedUser", "cherry")
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(200);
    expect(typeof response.body.jobId).toBe("string");
    expect(runResearchPipelineMock).toHaveBeenCalledTimes(1);
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

  it("returns done status with summary and rejected companies", async () => {
    const app = createTestApp();
    const jobId = createJob();
    setSkippedCompanies(jobId, []);
    setRejectedCompanies(jobId, ["Company X", "Company Y"], "rejected because they were using other observability tools");
    setJobSummary(jobId, {
      totalRows: 2,
      eligibleCompanyCount: 1,
      rejectedCompanyCount: 1,
      skippedMissingWebsiteAndApolloAccountIdCount: 0,
      apolloProcessedCompanyCount: 1,
      totalSreFound: 3,
      totalLinkedinCampaignSuccessful: 1,
      totalLinkedinCampaignFailed: 0,
      totalLemlistSuccessful: 2,
      totalLemlistFailed: 1,
      totalEmailCampaignSuccessful: 1,
      totalEmailCampaignFailed: 0,
    });
    markJobDone(jobId, Buffer.from("a,b\n1,2\n", "utf8").toString("base64"));

    const response = await request(app).get(`/status/${jobId}`);
    expect(response.status).toBe(200);
    expect(response.body.status).toBe("done");
    expect(response.body.skippedCompanies).toEqual([]);
    expect(response.body.rejectedCompanies).toEqual(["Company X", "Company Y"]);
    expect(response.body.summary.apolloProcessedCompanyCount).toBe(1);
    expect(response.body.summary.skippedMissingWebsiteAndApolloAccountIdCount).toBe(0);
    expect(response.body.summary.totalLinkedinCampaignSuccessful).toBe(1);
  });

  it("returns 404 when status job does not exist", async () => {
    const app = createTestApp();
    const response = await request(app).get("/status/not-a-real-job");
    expect(response.status).toBe(404);
  });

  it("returns weekly counts for valid selected user and week start", async () => {
    const app = createTestApp();
    getWeeklySuccessCountsMock.mockReturnValueOnce({
      linkedinCount: 7,
      emailCount: 9,
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

  it("returns processing payload contract for in-progress job", async () => {
    const app = createTestApp();
    const jobId = createJob();
    setJobMessage(jobId, "Working");
    setJobProgress(jobId, { currentRow: 3, totalRows: 20 });

    const response = await request(app).get(`/status/${jobId}`);
    expect(response.status).toBe(200);
    expect(response.body.status).toBe("pending");
    expect(response.body.message).toBe("Working");
    expect(response.body.currentRow).toBe(3);
    expect(response.body.totalRows).toBe(20);
    expect(Array.isArray(response.body.warnings)).toBe(true);
    expect(response.body.csv).toBeUndefined();
    expect(response.body.summary).toBeUndefined();
  });

  it("returns error payload contract when job failed", async () => {
    const app = createTestApp();
    const jobId = createJob();
    markJobError(jobId, "pipeline exploded");

    const response = await request(app).get(`/status/${jobId}`);
    expect(response.status).toBe(200);
    expect(response.body.status).toBe("error");
    expect(response.body.error).toBe("pipeline exploded");
    expect(response.body.csv).toBeUndefined();
  });

  it("cancels an in-progress job", async () => {
    const app = createTestApp();
    const jobId = createJob();
    const response = await request(app).post(`/cancel/${jobId}`);
    expect(response.status).toBe(200);
    expect(response.body.status).toBe("cancelled");
  });

  it("returns 409 when cancelling an already cancelled job", async () => {
    const app = createTestApp();
    const jobId = createJob();
    markJobCancelled(jobId);
    const response = await request(app).post(`/cancel/${jobId}`);
    expect(response.status).toBe(409);
  });

  it("returns 409 when cancelling a done job", async () => {
    const app = createTestApp();
    const jobId = createJob();
    markJobDone(jobId, Buffer.from("a,b\n1,2\n", "utf8").toString("base64"));
    const response = await request(app).post(`/cancel/${jobId}`);
    expect(response.status).toBe(409);
  });
});
