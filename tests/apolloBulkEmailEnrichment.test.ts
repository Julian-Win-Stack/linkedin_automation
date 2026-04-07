import { beforeEach, describe, expect, it, vi } from "vitest";
import { findEmailsInBulk } from "../src/services/apolloBulkEmailEnrichment";

const getRequiredEnvMock = vi.fn();

vi.mock("../src/config/env", () => ({
  getRequiredEnv: (...args: unknown[]) => getRequiredEnvMock(...args),
}));

describe("findEmailsInBulk", () => {
  beforeEach(() => {
    getRequiredEnvMock.mockReset();
    getRequiredEnvMock.mockReturnValue("test-apollo-key");
    vi.stubGlobal("fetch", vi.fn());
  });

  it("returns map of linkedin to email", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        matches: [
          {
            linkedin_url: "https://www.linkedin.com/in/alice/",
            email: "alice@example.com",
          },
        ],
      }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const result = await findEmailsInBulk([
      { name: "Alice Smith", domain: "example.com", linkedinUrl: "https://linkedin.com/in/alice" },
    ]);

    expect(result.get("linkedin.com/in/alice")).toBe("alice@example.com");
  });

  it("sends correct headers and body to Apollo", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => ({ matches: [] }),
    });
    vi.stubGlobal("fetch", mockFetch);

    await findEmailsInBulk([
      { name: "Bob Jones", domain: "acme.com", linkedinUrl: "https://linkedin.com/in/bob" },
    ]);

    expect(mockFetch).toHaveBeenCalledTimes(1);
    const [url, options] = mockFetch.mock.calls[0];
    expect(url).toBe("https://api.apollo.io/api/v1/people/bulk_match");
    expect(options.headers["x-api-key"]).toBe("test-apollo-key");
    const body = JSON.parse(options.body);
    expect(body.details[0]).toMatchObject({
      name: "Bob Jones",
      domain: "acme.com",
      linkedin_url: "https://linkedin.com/in/bob",
    });
  });

  it("returns empty map for empty input", async () => {
    const result = await findEmailsInBulk([]);
    expect(result.size).toBe(0);
  });

  it("chunks into batches of 10", async () => {
    const candidates = Array.from({ length: 12 }, (_, i) => ({
      name: `Person ${i}`,
      domain: "example.com",
      linkedinUrl: `https://linkedin.com/in/person${i}`,
    }));
    const mockFetch = vi
      .fn()
      .mockResolvedValueOnce({ ok: true, json: async () => ({ matches: [] }) })
      .mockResolvedValueOnce({ ok: true, json: async () => ({ matches: [] }) });
    vi.stubGlobal("fetch", mockFetch);

    await findEmailsInBulk(candidates);

    expect(mockFetch).toHaveBeenCalledTimes(2);
    const firstBody = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(firstBody.details).toHaveLength(10);
    const secondBody = JSON.parse(mockFetch.mock.calls[1][1].body);
    expect(secondBody.details).toHaveLength(2);
  });

  it("throws after retries on failure", async () => {
    vi.useFakeTimers();
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      json: async () => ({}),
    });
    vi.stubGlobal("fetch", mockFetch);

    const expectation = expect(
      findEmailsInBulk([
        { name: "Bad Person", domain: "bad.com", linkedinUrl: "https://linkedin.com/in/bad" },
      ])
    ).rejects.toThrow("Apollo bulk match returned HTTP 500");
    await vi.runAllTimersAsync();
    await expectation;
    vi.useRealTimers();
  });

  it("skips matches with missing email or linkedin", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        matches: [
          { linkedin_url: "https://linkedin.com/in/alice/", email: null },
          { linkedin_url: null, email: "orphan@example.com" },
          { linkedin_url: "https://linkedin.com/in/bob/", email: "bob@example.com" },
        ],
      }),
    });
    vi.stubGlobal("fetch", mockFetch);

    const result = await findEmailsInBulk([
      { name: "Alice", domain: "example.com", linkedinUrl: "https://linkedin.com/in/alice" },
      { name: "Bob", domain: "example.com", linkedinUrl: "https://linkedin.com/in/bob" },
    ]);

    expect(result.size).toBe(1);
    expect(result.get("linkedin.com/in/bob")).toBe("bob@example.com");
  });
});
