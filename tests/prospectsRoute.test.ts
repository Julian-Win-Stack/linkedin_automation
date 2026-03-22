import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import prospectsRouter from "../src/routes/prospects";

const bulkEnrichPeopleMock = vi.fn();
const countEngineerPeopleMock = vi.fn();
const getCompanyMock = vi.fn();
const pushPeopleToLemlistCampaignMock = vi.fn();
const searchPeopleMock = vi.fn();

vi.mock("../src/services/bulkEnrichPeople", () => ({
  bulkEnrichPeople: (...args: unknown[]) => bulkEnrichPeopleMock(...args),
}));

vi.mock("../src/services/getCompany", () => ({
  getCompany: (...args: unknown[]) => getCompanyMock(...args),
}));

vi.mock("../src/services/lemlistPushQueue", () => ({
  pushPeopleToLemlistCampaign: (...args: unknown[]) => pushPeopleToLemlistCampaignMock(...args),
}));

vi.mock("../src/services/searchPeople", () => ({
  countEngineerPeople: (...args: unknown[]) => countEngineerPeopleMock(...args),
  searchPeople: (...args: unknown[]) => searchPeopleMock(...args),
}));

function createTestApp() {
  const app = express();
  app.use(express.json());
  app.use("/api/v1/prospects", prospectsRouter);
  return app;
}

describe("POST /api/v1/prospects/search", () => {
  beforeEach(() => {
    bulkEnrichPeopleMock.mockReset();
    countEngineerPeopleMock.mockReset();
    getCompanyMock.mockReset();
    pushPeopleToLemlistCampaignMock.mockReset();
    searchPeopleMock.mockReset();
    delete process.env.LEMLIST_PUSH_ENABLED;
  });

  it("returns enriched people for SRE search", async () => {
    getCompanyMock.mockResolvedValue({
      companyName: "Acme",
      domain: "acme.com",
    });
    countEngineerPeopleMock.mockResolvedValue(42);

    searchPeopleMock.mockResolvedValue([
      {
        id: "person_1",
        name: "A Person",
        title: "Platform Engineer",
      },
    ]);
    bulkEnrichPeopleMock.mockResolvedValue([
      {
        startDate: "2024-01-01",
        endDate: null,
        name: "A Person",
        linkedinUrl: "https://linkedin.com/in/a",
        currentTitle: "SRE",
        tenure: "2 years 2 months",
      },
    ]);
    pushPeopleToLemlistCampaignMock.mockResolvedValue({
      attempted: 1,
      successful: 1,
      failed: 0,
      successItems: ["A Person"],
      failedItems: [],
    });

    const app = createTestApp();
    const response = await request(app).post("/api/v1/prospects/search").send({ companyUrl: "acme.com" });

    expect(response.status).toBe(200);
    expect(response.body.data).toHaveLength(1);
    expect(response.body.meta.searchedCount).toBe(1);
    expect(response.body.meta.enrichedCount).toBe(1);
    expect(response.body.meta.engineerCount).toBe(42);
    expect(response.body.meta.lemlist).toEqual({
      attempted: 1,
      successful: 1,
      failed: 0,
      successItems: ["A Person"],
      failedItems: [],
    });
    expect(searchPeopleMock).toHaveBeenCalledWith(
      {
        companyName: "Acme",
        domain: "acme.com",
      },
      30,
      ["SRE", "Site Reliability"]
    );
    expect(countEngineerPeopleMock).toHaveBeenCalledWith({
      companyName: "Acme",
      domain: "acme.com",
    });
    expect(countEngineerPeopleMock.mock.invocationCallOrder[0]).toBeLessThan(
      searchPeopleMock.mock.invocationCallOrder[0]
    );
    expect(bulkEnrichPeopleMock).toHaveBeenCalledWith([
      {
        id: "person_1",
        name: "A Person",
        title: "Platform Engineer",
      },
    ]);
    expect(getCompanyMock).toHaveBeenCalledWith("acme.com");
    expect(pushPeopleToLemlistCampaignMock).toHaveBeenCalledWith(
      [
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "A Person",
          linkedinUrl: "https://linkedin.com/in/a",
          currentTitle: "SRE",
          tenure: "2 years 2 months",
        },
      ],
      "Acme",
      "acme.com"
    );
  });

  it("deduplicates prospects by id before enrichment", async () => {
    getCompanyMock.mockResolvedValue({
      companyName: "Acme",
      domain: "acme.com",
    });
    countEngineerPeopleMock.mockResolvedValue(3);

    searchPeopleMock.mockResolvedValue([
      { id: "person_1", name: "A Person", title: "SRE" },
      { id: "person_1", name: "A Person", title: "SRE" },
      { id: "person_2", name: "B Person", title: "SRE" },
    ]);

    bulkEnrichPeopleMock.mockResolvedValue([
      {
        startDate: "2024-01-01",
        endDate: null,
        name: "A Person",
        linkedinUrl: "https://linkedin.com/in/a",
        currentTitle: "SRE",
        tenure: "2 years 2 months",
      },
    ]);
    pushPeopleToLemlistCampaignMock.mockResolvedValue({
      attempted: 1,
      successful: 1,
      failed: 0,
      successItems: ["A Person"],
      failedItems: [],
    });

    const app = createTestApp();
    const response = await request(app).post("/api/v1/prospects/search").send({ companyUrl: "acme.com" });

    expect(response.status).toBe(200);
    expect(bulkEnrichPeopleMock).toHaveBeenCalledWith([
      { id: "person_1", name: "A Person", title: "SRE" },
      { id: "person_2", name: "B Person", title: "SRE" },
    ]);
    expect(response.body.meta.searchedCount).toBe(3);
    expect(response.body.meta.enrichedCount).toBe(1);
    expect(response.body.meta.engineerCount).toBe(3);
  });

  it("returns 404 when exact company does not exist in Apollo", async () => {
    const notFoundError = new Error("Exact company not found in Apollo for query: acme");
    notFoundError.name = "CompanyNotFoundError";
    getCompanyMock.mockRejectedValue(notFoundError);

    const app = createTestApp();
    const response = await request(app)
      .post("/api/v1/prospects/search")
      .send({ companyUrl: "acme.com" });

    expect(response.status).toBe(404);
    expect(searchPeopleMock).not.toHaveBeenCalled();
  });

  it("returns 400 when company input is not a domain", async () => {
    const invalidCompanyInputError = new Error(
      "Invalid company input 'Acme'. Please provide a company domain."
    );
    invalidCompanyInputError.name = "InvalidCompanyInputError";
    getCompanyMock.mockRejectedValue(invalidCompanyInputError);

    const app = createTestApp();
    const response = await request(app)
      .post("/api/v1/prospects/search")
      .send({ companyUrl: "Acme" });

    expect(response.status).toBe(400);
    expect(searchPeopleMock).not.toHaveBeenCalled();
  });

  it("returns 400 when companyUrl is missing", async () => {
    const app = createTestApp();
    const response = await request(app).post("/api/v1/prospects/search").send({});
    expect(response.status).toBe(400);
  });

  it("skips Lemlist push when request toggle is false", async () => {
    getCompanyMock.mockResolvedValue({
      companyName: "Acme",
      domain: "acme.com",
    });
    countEngineerPeopleMock.mockResolvedValue(10);
    searchPeopleMock.mockResolvedValue([{ id: "person_1", name: "A Person", title: "SRE" }]);
    bulkEnrichPeopleMock.mockResolvedValue([
      {
        startDate: "2024-01-01",
        endDate: null,
        name: "A Person",
        linkedinUrl: "https://linkedin.com/in/a",
        currentTitle: "SRE",
        tenure: "2 years 2 months",
      },
    ]);

    const app = createTestApp();
    const response = await request(app)
      .post("/api/v1/prospects/search")
      .send({ companyUrl: "acme.com", pushToLemlist: false });

    expect(response.status).toBe(200);
    expect(pushPeopleToLemlistCampaignMock).not.toHaveBeenCalled();
    expect(response.body.meta.lemlist).toEqual({
      attempted: 0,
      successful: 0,
      failed: 0,
      successItems: [],
      failedItems: [],
      skipped: true,
      reason: "Lemlist push disabled by config or request.",
      enabledByEnv: true,
      enabledByRequest: false,
    });
  });

  it("skips Lemlist push when env toggle is false", async () => {
    process.env.LEMLIST_PUSH_ENABLED = "false";
    getCompanyMock.mockResolvedValue({
      companyName: "Acme",
      domain: "acme.com",
    });
    countEngineerPeopleMock.mockResolvedValue(10);
    searchPeopleMock.mockResolvedValue([{ id: "person_1", name: "A Person", title: "SRE" }]);
    bulkEnrichPeopleMock.mockResolvedValue([
      {
        startDate: "2024-01-01",
        endDate: null,
        name: "A Person",
        linkedinUrl: "https://linkedin.com/in/a",
        currentTitle: "SRE",
        tenure: "2 years 2 months",
      },
    ]);

    const app = createTestApp();
    const response = await request(app).post("/api/v1/prospects/search").send({ companyUrl: "acme.com" });

    expect(response.status).toBe(200);
    expect(pushPeopleToLemlistCampaignMock).not.toHaveBeenCalled();
    expect(response.body.meta.lemlist).toEqual({
      attempted: 0,
      successful: 0,
      failed: 0,
      successItems: [],
      failedItems: [],
      skipped: true,
      reason: "Lemlist push disabled by config or request.",
      enabledByEnv: false,
      enabledByRequest: true,
    });
  });

});
