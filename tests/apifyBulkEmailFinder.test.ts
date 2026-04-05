import { beforeEach, describe, expect, it, vi } from "vitest";
import { findEmailsInBulk } from "../src/services/apifyBulkEmailFinder";

const getRequiredEnvMock = vi.fn();

vi.mock("../src/config/env", () => ({
  getRequiredEnv: (...args: unknown[]) => getRequiredEnvMock(...args),
}));

describe("findEmailsInBulk", () => {
  beforeEach(() => {
    getRequiredEnvMock.mockReset();
    getRequiredEnvMock.mockReturnValue("test-apify-key");
    vi.stubGlobal("fetch", vi.fn());
  });

  it("returns map of linkedin to email", async () => {
    const mockFetch = vi.fn().mockResolvedValueOnce({
      ok: true,
      json: async () => [
        {
          linkedinUrl: "https://www.linkedin.com/in/alice/",
          email: "alice@example.com",
        },
      ],
    });
    vi.stubGlobal("fetch", mockFetch);

    const result = await findEmailsInBulk(["https://linkedin.com/in/alice"]);
    expect(result.get("linkedin.com/in/alice")).toBe("alice@example.com");
  });

  it("returns empty map for empty input", async () => {
    const result = await findEmailsInBulk([]);
    expect(result.size).toBe(0);
  });

  it("throws after retries on failure", async () => {
    vi.useFakeTimers();
    const mockFetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      json: async () => [],
    });
    vi.stubGlobal("fetch", mockFetch);

    const expectation = expect(findEmailsInBulk(["https://linkedin.com/in/bad"])).rejects.toThrow(
      "Apify bulk email finder returned HTTP 500"
    );
    await vi.runAllTimersAsync();
    await expectation;
    vi.useRealTimers();
  });
});
