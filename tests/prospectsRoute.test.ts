import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import prospectsRouter from "../src/routes/prospects";

const getCompanyMock = vi.fn();
const searchPeopleMock = vi.fn();

vi.mock("../src/services/getCompany", () => ({
  getCompany: (...args: unknown[]) => getCompanyMock(...args),
}));

vi.mock("../src/services/searchPeople", () => ({
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
    getCompanyMock.mockReset();
    searchPeopleMock.mockReset();
  });

  it("returns matching people with preset keywords", async () => {
    getCompanyMock.mockResolvedValue({
      companyName: "Acme",
      domain: "acme.com",
      linkedinUrl: null,
    });

    searchPeopleMock.mockResolvedValue([
      {
        name: "A Person",
        title: "Platform Engineer",
        company: "Acme",
        linkedinUrl: null,
        tenureMonths: "1 year 0 months",
      },
    ]);

    const app = createTestApp();
    const response = await request(app)
      .post("/api/v1/prospects/search")
      .send({ companyUrl: "acme.com", maxResults: 25, filterPreset: "srePlatform" });

    expect(response.status).toBe(200);
    expect(response.body.data).toHaveLength(1);
    expect(searchPeopleMock).toHaveBeenCalledWith(
      {
        companyName: "Acme",
        domain: "acme.com",
        linkedinUrl: null,
      },
      25,
      ["SRE", "Site Reliability", "Platform"]
    );
    expect(getCompanyMock).toHaveBeenCalledWith("acme.com");
  });

  it("returns 404 when exact company does not exist in Apollo", async () => {
    const notFoundError = new Error("Exact company not found in Apollo for query: acme");
    notFoundError.name = "CompanyNotFoundError";
    getCompanyMock.mockRejectedValue(notFoundError);

    const app = createTestApp();
    const response = await request(app)
      .post("/api/v1/prospects/search")
      .send({ companyUrl: "acme.com", maxResults: 25 });

    expect(response.status).toBe(404);
    expect(searchPeopleMock).not.toHaveBeenCalled();
  });

  it("returns 400 when company input is not domain or linkedin url", async () => {
    const invalidCompanyInputError = new Error(
      "Invalid company input 'Acme'. Please provide a company domain."
    );
    invalidCompanyInputError.name = "InvalidCompanyInputError";
    getCompanyMock.mockRejectedValue(invalidCompanyInputError);

    const app = createTestApp();
    const response = await request(app)
      .post("/api/v1/prospects/search")
      .send({ companyUrl: "Acme", maxResults: 25 });

    expect(response.status).toBe(400);
    expect(searchPeopleMock).not.toHaveBeenCalled();
  });

  it("returns 400 when companyUrl is missing", async () => {
    const app = createTestApp();
    const response = await request(app).post("/api/v1/prospects/search").send({});
    expect(response.status).toBe(400);
  });

  it("uses custom titleKeywords when provided", async () => {
    getCompanyMock.mockResolvedValue({
      companyName: "Acme",
      domain: "acme.com",
      linkedinUrl: null,
    });
    searchPeopleMock.mockResolvedValue([]);

    const app = createTestApp();
    const response = await request(app)
      .post("/api/v1/prospects/search")
      .send({ companyUrl: "acme.com", titleKeywords: ["Engineer"] });

    expect(response.status).toBe(200);
    expect(searchPeopleMock).toHaveBeenCalledWith(
      {
        companyName: "Acme",
        domain: "acme.com",
        linkedinUrl: null,
      },
      100,
      ["Engineer"]
    );
  });
});
