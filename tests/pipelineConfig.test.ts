import { afterEach, describe, expect, it, vi } from "vitest";
import { loadPipelineConfig } from "../src/config/pipelineConfig";

describe("loadPipelineConfig", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  it("loads config from environment variables", () => {
    vi.stubEnv("AZURE_OPENAI_API_KEY", "test-key");
    vi.stubEnv("AZURE_OPENAI_BASE_URL", "https://api.test.com");
    vi.stubEnv("SEARCHAPI_API_KEY", "search-key");
    vi.stubEnv("MODEL", "gpt-5.4");
    vi.stubEnv("MAX_COMPLETION_TOKENS", "4096");

    const config = loadPipelineConfig();

    expect(config.azureOpenAiApiKey).toBe("test-key");
    expect(config.azureOpenAiBaseUrl).toBe("https://api.test.com");
    expect(config.searchApiKey).toBe("search-key");
    expect(config.model).toBe("gpt-5.4");
    expect(config.maxCompletionTokens).toBe(4096);
  });

  it("uses default column names when not specified", () => {
    vi.stubEnv("AZURE_OPENAI_API_KEY", "test-key");
    vi.stubEnv("AZURE_OPENAI_BASE_URL", "https://api.test.com");
    vi.stubEnv("SEARCHAPI_API_KEY", "search-key");
    delete process.env.NAME_COLUMN;
    delete process.env.DOMAIN_COLUMN;
    delete process.env.APOLLO_ACCOUNT_ID_COLUMN;

    const config = loadPipelineConfig();

    expect(config.nameColumn).toBe("Company Name");
    expect(config.domainColumn).toBe("Website");
    expect(config.apolloAccountIdColumn).toBe("Apollo Account Id");
  });

  it("uses custom column names from env", () => {
    vi.stubEnv("AZURE_OPENAI_API_KEY", "test-key");
    vi.stubEnv("AZURE_OPENAI_BASE_URL", "https://api.test.com");
    vi.stubEnv("SEARCHAPI_API_KEY", "search-key");
    vi.stubEnv("NAME_COLUMN", "Name");
    vi.stubEnv("DOMAIN_COLUMN", "Domain");
    vi.stubEnv("APOLLO_ACCOUNT_ID_COLUMN", "ApolloId");

    const config = loadPipelineConfig();

    expect(config.nameColumn).toBe("Name");
    expect(config.domainColumn).toBe("Domain");
    expect(config.apolloAccountIdColumn).toBe("ApolloId");
  });

  it("defaults MAX_COMPLETION_TOKENS to 2048", () => {
    vi.stubEnv("AZURE_OPENAI_API_KEY", "test-key");
    vi.stubEnv("AZURE_OPENAI_BASE_URL", "https://api.test.com");
    vi.stubEnv("SEARCHAPI_API_KEY", "search-key");
    delete process.env.MAX_COMPLETION_TOKENS;

    const config = loadPipelineConfig();

    expect(config.maxCompletionTokens).toBe(2048);
  });

  it("throws when MAX_COMPLETION_TOKENS is not a positive number", () => {
    vi.stubEnv("AZURE_OPENAI_API_KEY", "test-key");
    vi.stubEnv("AZURE_OPENAI_BASE_URL", "https://api.test.com");
    vi.stubEnv("SEARCHAPI_API_KEY", "search-key");
    vi.stubEnv("MAX_COMPLETION_TOKENS", "abc");

    expect(() => loadPipelineConfig()).toThrow("MAX_COMPLETION_TOKENS must be a positive number");
  });

  it("throws when MAX_COMPLETION_TOKENS is zero", () => {
    vi.stubEnv("AZURE_OPENAI_API_KEY", "test-key");
    vi.stubEnv("AZURE_OPENAI_BASE_URL", "https://api.test.com");
    vi.stubEnv("SEARCHAPI_API_KEY", "search-key");
    vi.stubEnv("MAX_COMPLETION_TOKENS", "0");

    expect(() => loadPipelineConfig()).toThrow("MAX_COMPLETION_TOKENS must be a positive number");
  });

  it("throws when required env var is missing", () => {
    delete process.env.AZURE_OPENAI_API_KEY;
    vi.stubEnv("AZURE_OPENAI_BASE_URL", "https://api.test.com");
    vi.stubEnv("SEARCHAPI_API_KEY", "search-key");

    expect(() => loadPipelineConfig()).toThrow("Missing AZURE_OPENAI_API_KEY");
  });

  it("defaults model to gpt-5.4 when not set", () => {
    vi.stubEnv("AZURE_OPENAI_API_KEY", "test-key");
    vi.stubEnv("AZURE_OPENAI_BASE_URL", "https://api.test.com");
    vi.stubEnv("SEARCHAPI_API_KEY", "search-key");
    delete process.env.MODEL;

    const config = loadPipelineConfig();

    expect(config.model).toBe("gpt-5.4");
  });
});
