import { beforeEach, describe, expect, it, vi } from "vitest";
import { attioGet, attioPut } from "../src/services/attioClient";

const { getMock, putMock, createMock, isAxiosErrorMock } = vi.hoisted(() => ({
  getMock: vi.fn(),
  putMock: vi.fn(),
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

function axiosError(
  status?: number,
  retryAfter?: string
): { isAxiosError: true; response?: { status: number; data: unknown; headers?: Record<string, string> }; message: string } {
  if (typeof status === "number") {
    return {
      isAxiosError: true,
      response: {
        status,
        data: { message: `status-${status}` },
        headers: retryAfter ? { "retry-after": retryAfter } : {},
      },
      message: `status-${status}`,
    };
  }

  return {
    isAxiosError: true,
    message: "network-error",
  };
}

describe("attioClient retries", () => {
  beforeEach(() => {
    process.env.ATTIO_API_KEY = "attio-test-key";
    getMock.mockReset();
    putMock.mockReset();
    createMock.mockReset();
    isAxiosErrorMock.mockReset();
    createMock.mockReturnValue({ get: getMock, put: putMock });
    isAxiosErrorMock.mockImplementation(
      (error: unknown) => Boolean((error as { isAxiosError?: boolean })?.isAxiosError)
    );
  });

  it("retries GET after 429 and succeeds", async () => {
    getMock
      .mockRejectedValueOnce(axiosError(429, "0"))
      .mockResolvedValueOnce({ data: { data: [] } });

    const result = await attioGet<{ data: unknown[] }>("/v2/objects/companies/attributes");

    expect(result.data).toEqual([]);
    expect(getMock).toHaveBeenCalledTimes(2);
  });

  it("retries PUT on retryable 5xx and succeeds", async () => {
    putMock
      .mockRejectedValueOnce(axiosError(503))
      .mockResolvedValueOnce({ data: { ok: true } });

    const result = await attioPut<{ ok: boolean }>(
      "/v2/objects/companies/records?matching_attribute=domains",
      { data: { values: { domains: ["acme.com"] } } }
    );

    expect(result.ok).toBe(true);
    expect(putMock).toHaveBeenCalledTimes(2);
  });

  it("does not retry non-retryable 4xx", async () => {
    putMock.mockRejectedValueOnce(axiosError(400));

    await expect(
      attioPut("/v2/objects/companies/records?matching_attribute=domains", {
        data: { values: { domains: ["acme.com"] } },
      })
    ).rejects.toThrow("Attio API error (400): status-400");

    expect(putMock).toHaveBeenCalledTimes(1);
  });
});
