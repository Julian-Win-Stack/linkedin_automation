import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  apolloGetWithQuery,
  apolloPost,
  apolloPostWithQuery,
  fetchApolloAccountCustomFieldNameToIdMap,
  fetchApolloAccountStageNameToIdMap,
} from "../src/services/apolloClient";

const { postMock, getMock, createMock, isAxiosErrorMock } = vi.hoisted(() => ({
  postMock: vi.fn(),
  getMock: vi.fn(),
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
    getMock.mockReset();
    createMock.mockReset();
    isAxiosErrorMock.mockReset();
    createMock.mockReturnValue({ post: postMock, get: getMock });
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

  it("retries on network error for GET query-based calls", async () => {
    getMock.mockRejectedValueOnce(axiosError()).mockResolvedValueOnce({ data: { fields: [] } });

    const result = await apolloGetWithQuery<{ fields: unknown[] }>(
      "/fields",
      { source: "custom" }
    );

    expect(result.fields).toEqual([]);
    expect(getMock).toHaveBeenCalledTimes(2);
  });

  it("maps only account custom fields by exact label", async () => {
    getMock.mockResolvedValueOnce({
      data: {
        fields: [
          { id: "account.custom_1", label: "Signal Score", modality: "account" },
          { id: "contact.custom_1", label: "Signal Score", modality: "contact" },
          { id: "account.custom_2", label: "Company Tier", modality: "account" },
          { id: "", label: "Bad Field", modality: "account" },
        ],
      },
    });

    const map = await fetchApolloAccountCustomFieldNameToIdMap();

    expect(getMock).toHaveBeenCalledWith("/fields?source=custom");
    expect(map.get("Signal Score")).toBe("account.custom_1");
    expect(map.get("Company Tier")).toBe("account.custom_2");
    expect(map.has("Bad Field")).toBe(false);
  });

  it("maps account stage names and display names to ids", async () => {
    getMock.mockResolvedValueOnce({
      data: {
        account_stages: [
          { id: "stage_1", name: "ChasingPOC", display_name: "Chasing POC" },
          { id: "stage_2", name: "NotActionableNow", display_name: "Not Actionable Now" },
          { id: "", name: "Bad Stage", display_name: "Bad Stage" },
        ],
      },
    });

    const map = await fetchApolloAccountStageNameToIdMap();

    expect(getMock).toHaveBeenCalledWith("/account_stages");
    expect(map.get("ChasingPOC")).toBe("stage_1");
    expect(map.get("Chasing POC")).toBe("stage_1");
    expect(map.get("NotActionableNow")).toBe("stage_2");
    expect(map.get("Not Actionable Now")).toBe("stage_2");
    expect(map.has("Bad Stage")).toBe(false);
  });
});
