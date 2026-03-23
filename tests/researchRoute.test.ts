import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import researchRouter from "../src/routes/research";
import {
  createJob,
  markJobDone,
  setJobSummary,
  setRejectedCompanies,
} from "../src/jobs/jobStore";

const runResearchPipelineMock = vi.fn();
const loadPipelineConfigMock = vi.fn();

vi.mock("../src/jobs/researchPipeline", () => ({
  runResearchPipeline: (...args: unknown[]) => runResearchPipelineMock(...args),
}));

vi.mock("../src/config/pipelineConfig", () => ({
  loadPipelineConfig: (...args: unknown[]) => loadPipelineConfigMock(...args),
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
    loadPipelineConfigMock.mockReturnValue({
      azureOpenAiApiKey: "k",
      azureOpenAiBaseUrl: "u",
      searchApiKey: "s",
      model: "gpt-5.4",
      maxCompletionTokens: 2048,
      nameColumn: "Company Name",
      domainColumn: "Website",
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
      .attach("csv", Buffer.from(csv, "utf8"), "input.csv");

    expect(response.status).toBe(200);
    expect(typeof response.body.jobId).toBe("string");
    expect(runResearchPipelineMock).toHaveBeenCalledTimes(1);
  });

  it("returns done status with summary and rejected companies", async () => {
    const app = createTestApp();
    const jobId = createJob();
    setRejectedCompanies(jobId, ["Company X", "Company Y"], "rejected because they were using other observability tools");
    setJobSummary(jobId, {
      totalRows: 2,
      eligibleCompanyCount: 1,
      rejectedCompanyCount: 1,
      apolloProcessedCompanyCount: 1,
      totalSreFound: 3,
      totalLemlistSuccessful: 2,
      totalLemlistFailed: 1,
    });
    markJobDone(jobId, Buffer.from("a,b\n1,2\n", "utf8").toString("base64"));

    const response = await request(app).get(`/status/${jobId}`);
    expect(response.status).toBe(200);
    expect(response.body.status).toBe("done");
    expect(response.body.rejectedCompanies).toEqual(["Company X", "Company Y"]);
    expect(response.body.summary.apolloProcessedCompanyCount).toBe(1);
  });
});
