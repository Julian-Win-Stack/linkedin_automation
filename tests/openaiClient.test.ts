import { beforeEach, describe, expect, it, vi } from "vitest";
import { researchCompany } from "../src/services/observability/openaiClient";

const searchGoogleMock = vi.fn();

vi.mock("../src/services/observability/searchApiClient", () => ({
  searchGoogle: (...args: unknown[]) => searchGoogleMock(...args),
}));

describe("researchCompany", () => {
  const config = {
    apiKey: "k",
    baseUrl: "https://example.openai.azure.com/openai/deployments/test",
    model: "gpt-5.4",
    maxCompletionTokens: 512,
    searchApiKey: "search-key",
  };

  beforeEach(() => {
    searchGoogleMock.mockReset();
    vi.restoreAllMocks();
  });

  it("returns content from successful completion response", async () => {
    searchGoogleMock.mockResolvedValue([{ title: "t", link: "https://example.com", snippet: "s" }]);
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ choices: [{ message: { content: "Not found" } }] }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await researchCompany("Acme", "acme.com", config);
    expect(result).toBe("Not found");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("retries and returns Error: message after repeated failures", async () => {
    searchGoogleMock.mockResolvedValue([]);
    const fetchMock = vi.fn().mockRejectedValue(new Error("network down"));
    vi.stubGlobal("fetch", fetchMock);

    const result = await researchCompany("Acme", "acme.com", config);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(result).toContain("Error: network down");
  }, 12000);

  it("retries when API returns ok but missing content", async () => {
    searchGoogleMock.mockResolvedValue([]);
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ choices: [{ message: { content: "" } }] }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ choices: [{ message: { content: "1. Datadog : https://docs.example.com" } }] }),
      });
    vi.stubGlobal("fetch", fetchMock);

    const result = await researchCompany("Acme", "acme.com", config);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(result).toContain("Datadog");
  });
});
