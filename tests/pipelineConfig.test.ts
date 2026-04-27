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

    const config = loadPipelineConfig();

    expect(config.nameColumn).toBe("Company Name");
    expect(config.domainColumn).toBe("Website");
    expect(config.apolloAccountIdColumn).toBe("Apollo Account Id");
  });

  it("uses custom column names from env", () => {
    vi.stubEnv("NAME_COLUMN", "Name");
    vi.stubEnv("DOMAIN_COLUMN", "Domain");
    vi.stubEnv("APOLLO_ACCOUNT_ID_COLUMN", "ApolloId");

    const config = loadPipelineConfig();

    expect(config.nameColumn).toBe("Name");
    expect(config.domainColumn).toBe("Domain");
    expect(config.apolloAccountIdColumn).toBe("ApolloId");
  });

  it("trims whitespace from column name env vars", () => {
    vi.stubEnv("NAME_COLUMN", "  Company Name  ");
    vi.stubEnv("DOMAIN_COLUMN", "  Website  ");
    vi.stubEnv("APOLLO_ACCOUNT_ID_COLUMN", "  Apollo Account Id  ");

    const config = loadPipelineConfig();

    expect(config.nameColumn).toBe("Company Name");
    expect(config.domainColumn).toBe("Website");
    expect(config.apolloAccountIdColumn).toBe("Apollo Account Id");
  });
});
