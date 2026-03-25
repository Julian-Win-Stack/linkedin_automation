import { beforeEach, describe, expect, it, vi } from "vitest";
import { bulkEnrichPeople, runWaterfallEmailForPersonIds } from "../src/services/bulkEnrichPeople";

const apolloPostMock = vi.fn();
const registerPendingWaterfallRequestMock = vi.fn();
const waitForWaterfallRequestsMock = vi.fn();
const getRecoveredEmailsForRequestsMock = vi.fn();

vi.mock("../src/services/apolloClient", () => ({
  apolloPost: (...args: unknown[]) => apolloPostMock(...args),
}));

vi.mock("../src/services/apolloWaterfallStore", () => ({
  registerPendingWaterfallRequest: (...args: unknown[]) => registerPendingWaterfallRequestMock(...args),
  waitForWaterfallRequests: (...args: unknown[]) => waitForWaterfallRequestsMock(...args),
  getRecoveredEmailsForRequests: (...args: unknown[]) => getRecoveredEmailsForRequestsMock(...args),
}));

describe("bulkEnrichPeople", () => {
  beforeEach(() => {
    apolloPostMock.mockReset();
    registerPendingWaterfallRequestMock.mockReset();
    waitForWaterfallRequestsMock.mockReset();
    getRecoveredEmailsForRequestsMock.mockReset();
    waitForWaterfallRequestsMock.mockResolvedValue({
      completedRequestCount: 0,
      timedOut: false,
    });
    getRecoveredEmailsForRequestsMock.mockReturnValue(new Map());
    registerPendingWaterfallRequestMock.mockReturnValue({
      appliedBufferedCallback: false,
      recoveredEmailCount: 0,
    });
    process.env.APOLLO_WEBHOOK_URL = "https://example.com/webhooks/apollo/waterfall";
  });

  it("enriches in batches of 10 and maps required fields", async () => {
    const people = Array.from({ length: 21 }).map((_, idx) => ({
      id: `person_${idx + 1}`,
      name: `Person ${idx + 1}`,
      title: "SRE",
    }));

    apolloPostMock
      .mockResolvedValueOnce({
        matches: [
          {
            organization_id: "org_1",
            name: "A Person",
            email: "a.person@example.com",
            linkedin_url: "https://linkedin.com/in/a",
            title: "SRE",
            employment_history: [
              {
                organization_id: "org_1",
                title: "SRE",
                start_date: "2024-01-01",
                end_date: null,
                current: true,
              },
            ],
          },
        ],
      })
      .mockResolvedValueOnce({ matches: [] })
      .mockResolvedValueOnce({ matches: [] });

    const result = await bulkEnrichPeople(people);

    expect(apolloPostMock).toHaveBeenCalledTimes(3);
    const firstPayload = apolloPostMock.mock.calls[0]?.[1] as Record<string, unknown>;
    expect(firstPayload.run_waterfall_email).toBeUndefined();
    expect(firstPayload.webhook_url).toBeUndefined();
    expect(result[0]).toEqual({
      id: undefined,
      startDate: "2024-01-01",
      endDate: null,
      name: "A Person",
      email: "a.person@example.com",
      linkedinUrl: "https://linkedin.com/in/a",
      currentTitle: "SRE",
      tenure: expect.any(Number),
    });
  });

  it("caps enrichment input at 30 people", async () => {
    const people = Array.from({ length: 35 }).map((_, idx) => ({
      id: `person_${idx + 1}`,
      name: `Person ${idx + 1}`,
      title: "SRE",
    }));

    apolloPostMock.mockResolvedValue({ matches: [] });
    await bulkEnrichPeople(people);

    expect(apolloPostMock).toHaveBeenCalledTimes(3);
    const firstBatch = apolloPostMock.mock.calls[0]?.[1] as { details: Array<{ id: string }> };
    const secondBatch = apolloPostMock.mock.calls[1]?.[1] as { details: Array<{ id: string }> };
    const thirdBatch = apolloPostMock.mock.calls[2]?.[1] as { details: Array<{ id: string }> };
    expect(firstBatch.details).toHaveLength(10);
    expect(secondBatch.details).toHaveLength(10);
    expect(thirdBatch.details).toHaveLength(10);
    expect(thirdBatch.details.at(-1)?.id).toBe("person_30");
  });

  it("merges overlapping roles before summing company tenure", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2022-01-01T00:00:00.000Z"));

    apolloPostMock.mockResolvedValueOnce({
      matches: [
        {
          organization_id: "org_1",
          name: "A Person",
          linkedin_url: "https://linkedin.com/in/a",
          title: "SRE",
          employment_history: [
            {
              organization_id: "org_1",
              title: "SRE",
              start_date: "2020-01-01",
              end_date: "2021-01-01",
              current: false,
            },
            {
              organization_id: "org_1",
              title: "Senior SRE",
              start_date: "2020-06-01",
              end_date: null,
              current: true,
            },
          ],
        },
      ],
    });

    const result = await bulkEnrichPeople([{ id: "person_1", name: "A Person", title: "SRE" }]);
    expect(result).toHaveLength(1);
    expect(result[0].tenure).toBe(24);
    vi.useRealTimers();
  });

  it("does not return people whose current role has non-null end_date", async () => {
    apolloPostMock.mockResolvedValueOnce({
      matches: [
        {
          organization_id: "org_1",
          name: "Former Employee",
          linkedin_url: "https://linkedin.com/in/former",
          title: "SRE",
          employment_history: [
            {
              organization_id: "org_1",
              title: "SRE",
              start_date: "2020-01-01",
              end_date: "2021-01-01",
              current: false,
            },
          ],
        },
      ],
    });

    const result = await bulkEnrichPeople([{ id: "person_1", name: "Former Employee", title: "SRE" }]);
    expect(result).toEqual([]);
  });

});

describe("runWaterfallEmailForPersonIds", () => {
  beforeEach(() => {
    apolloPostMock.mockReset();
    registerPendingWaterfallRequestMock.mockReset();
    waitForWaterfallRequestsMock.mockReset();
    getRecoveredEmailsForRequestsMock.mockReset();
    waitForWaterfallRequestsMock.mockResolvedValue({
      completedRequestCount: 0,
      timedOut: false,
    });
    getRecoveredEmailsForRequestsMock.mockReturnValue(new Map());
    registerPendingWaterfallRequestMock.mockReturnValue({
      appliedBufferedCallback: false,
      recoveredEmailCount: 0,
    });
    process.env.APOLLO_WEBHOOK_URL = "https://example.com/webhooks/apollo/waterfall";
  });

  it("runs waterfall in batches and waits for registered request ids", async () => {
    apolloPostMock
      .mockResolvedValueOnce({ request_id: "req_1" })
      .mockResolvedValueOnce({ request_id: "req_2" });
    waitForWaterfallRequestsMock.mockResolvedValueOnce({
      completedRequestCount: 2,
      timedOut: false,
    });
    getRecoveredEmailsForRequestsMock.mockReturnValueOnce(
      new Map([["person_2", "recovered@example.com"]])
    );

    const recovered = await runWaterfallEmailForPersonIds(
      ["person_1", "person_2", "person_3", "person_4", "person_5", "person_6", "person_7", "person_8", "person_9", "person_10", "person_11"],
      1234
    );

    expect(apolloPostMock).toHaveBeenCalledTimes(2);
    expect(apolloPostMock.mock.calls[0]?.[1]).toEqual(
      expect.objectContaining({
        run_waterfall_email: true,
        webhook_url: expect.stringMatching(
          /^https:\/\/example\.com\/webhooks\/apollo\/waterfall\?client_req_id=/
        ),
      })
    );
    expect(registerPendingWaterfallRequestMock).toHaveBeenCalledWith(expect.any(String), [
      "person_1",
      "person_2",
      "person_3",
      "person_4",
      "person_5",
      "person_6",
      "person_7",
      "person_8",
      "person_9",
      "person_10",
    ]);
    expect(registerPendingWaterfallRequestMock).toHaveBeenCalledWith(expect.any(String), [
      "person_11",
    ]);
    const firstTrackedId = registerPendingWaterfallRequestMock.mock.calls[0]?.[0] as string;
    const secondTrackedId = registerPendingWaterfallRequestMock.mock.calls[1]?.[0] as string;
    expect(waitForWaterfallRequestsMock).toHaveBeenCalledWith(
      [firstTrackedId, secondTrackedId],
      1234
    );
    expect(getRecoveredEmailsForRequestsMock).toHaveBeenCalledWith([
      firstTrackedId,
      secondTrackedId,
    ]);
    expect(recovered.get("person_2")).toBe("recovered@example.com");
  });

  it("fails fast when APOLLO_WEBHOOK_URL is missing", async () => {
    delete process.env.APOLLO_WEBHOOK_URL;
    await expect(runWaterfallEmailForPersonIds(["person_1"], 1000)).rejects.toThrow(
      "Missing APOLLO_WEBHOOK_URL environment variable."
    );
  });
});
