import { afterEach, describe, expect, it, vi } from "vitest";
import { loadPipelineConfig } from "../src/config/pipelineConfig";

describe("loadPipelineConfig", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("uses default column names when not specified", () => {
    delete process.env.NAME_COLUMN;
    delete process.env.DOMAIN_COLUMN;
    delete process.env.APOLLO_ACCOUNT_ID_COLUMN;
    delete process.env.LINKEDIN_URL_COLUMN;

    const config = loadPipelineConfig();

    expect(config.nameColumn).toEqual(["Company Name"]);
    expect(config.domainColumn).toEqual(["Website"]);
    expect(config.apolloAccountIdColumn).toEqual(["Apollo Account Id"]);
    expect(config.linkedinUrlColumn).toEqual(["Company Linkedin Url"]);
  });

  it("uses custom column names from env", () => {
    vi.stubEnv("NAME_COLUMN", "Name");
    vi.stubEnv("DOMAIN_COLUMN", "Domain");
    vi.stubEnv("APOLLO_ACCOUNT_ID_COLUMN", "ApolloId");
    vi.stubEnv("LINKEDIN_URL_COLUMN", "LinkedIn");

    const config = loadPipelineConfig();

    expect(config.nameColumn).toEqual(["Name"]);
    expect(config.domainColumn).toEqual(["Domain"]);
    expect(config.apolloAccountIdColumn).toEqual(["ApolloId"]);
    expect(config.linkedinUrlColumn).toEqual(["LinkedIn"]);
  });

  it("trims whitespace from column name env vars", () => {
    vi.stubEnv("NAME_COLUMN", "  Company Name  ");
    vi.stubEnv("DOMAIN_COLUMN", "  Website  ");
    vi.stubEnv("APOLLO_ACCOUNT_ID_COLUMN", "  Apollo Account Id  ");

    const config = loadPipelineConfig();

    expect(config.nameColumn).toEqual(["Company Name"]);
    expect(config.domainColumn).toEqual(["Website"]);
    expect(config.apolloAccountIdColumn).toEqual(["Apollo Account Id"]);
  });

  it("parses comma-separated env values into arrays", () => {
    vi.stubEnv("NAME_COLUMN", "Company Name, Parent Record > Company name");
    vi.stubEnv("DOMAIN_COLUMN", "Website,Domain,URL");

    const config = loadPipelineConfig();

    expect(config.nameColumn).toEqual(["Company Name", "Parent Record > Company name"]);
    expect(config.domainColumn).toEqual(["Website", "Domain", "URL"]);
  });

  it("falls back to defaults when env value is only whitespace or commas", () => {
    vi.stubEnv("NAME_COLUMN", "  ,  ,  ");

    const config = loadPipelineConfig();

    expect(config.nameColumn).toEqual(["Company Name"]);
  });
});
