import { beforeEach, describe, expect, it, vi } from "vitest";
import { apolloPost, apolloPostWithQuery } from "../src/services/apolloClient";

const { postMock, createMock, isAxiosErrorMock } = vi.hoisted(() => ({
  postMock: vi.fn(),
  createMock: vi.fn(),
  isAxiosErrorMock: vi.fn(),
}));

vi.mock("axios", () => ({
  default: {
    create: createMock,
    isAxiosError: isAxiosErrorMock,
  },
  create: createMock,
  isAxiosError: isAxiosErrorMock,
}));

function axiosError(status?: number): { isAxiosError: true; response?: { status: number; data: unknown }; message: string } {
  if (typeof status === "number") {
    return {
      isAxiosError: true,
      response: {
        status,
        data: { message: `status-${status}` },
      },
      message: `status-${status}`,
    };
  }

  return {
    isAxiosError: true,
    message: "network-error",
  };
}

describe("apolloClient retries", () => {
  beforeEach(() => {
    process.env.APOLLO_API_KEY = "apollo-test-key";
    postMock.mockReset();
    createMock.mockReset();
    isAxiosErrorMock.mockReset();
    createMock.mockReturnValue({ post: postMock });
    isAxiosErrorMock.mockImplementation(
      (error: unknown) => Boolean((error as { isAxiosError?: boolean })?.isAxiosError)
    );
  });

  it("retries on 429 and succeeds", async () => {
    postMock.mockRejectedValueOnce(axiosError(429)).mockResolvedValueOnce({ data: { ok: true } });

    const result = await apolloPost<{ ok: boolean }>("/mixed_people/api_search", { page: 1 });

    expect(result.ok).toBe(true);
    expect(postMock).toHaveBeenCalledTimes(2);
  });

  it("does not retry on non-retryable 4xx", async () => {
    postMock.mockRejectedValueOnce(axiosError(400));

    await expect(apolloPost("/mixed_people/api_search", { page: 1 })).rejects.toThrow(
      "Apollo API error (400): status-400"
    );
    expect(postMock).toHaveBeenCalledTimes(1);
  });

  it("retries on network error for query-based calls", async () => {
    postMock.mockRejectedValueOnce(axiosError()).mockResolvedValueOnce({ data: { people: [] } });

    const result = await apolloPostWithQuery<{ people: unknown[] }>(
      "/mixed_people/api_search",
      { page: 1, per_page: 100 }
    );

    expect(result.people).toEqual([]);
    expect(postMock).toHaveBeenCalledTimes(2);
  });
});
