import { beforeEach, describe, expect, it, vi } from "vitest";
import { bulkEnrichPeople } from "../src/services/bulkEnrichPeople";

const apolloPostMock = vi.fn();

vi.mock("../src/services/apolloClient", () => ({
  apolloPost: (...args: unknown[]) => apolloPostMock(...args),
}));

describe("bulkEnrichPeople", () => {
  beforeEach(() => {
    apolloPostMock.mockReset();
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
    expect(result[0]).toEqual({
      id: undefined,
      startDate: "2024-01-01",
      endDate: null,
      name: "A Person",
      email: "a.person@example.com",
      linkedinUrl: "https://linkedin.com/in/a",
      currentTitle: "SRE",
      headline: "",
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

  it("reuses per-job cache and only enriches uncached person ids", async () => {
    const cache = new Map();
    cache.set("person_cached", {
      id: "person_cached",
      startDate: "2023-01-01",
      endDate: null,
      name: "Cached Person",
      email: "cached@example.com",
      linkedinUrl: "https://linkedin.com/in/cached",
      currentTitle: "SRE",
      headline: "",
      tenure: 12,
    });
    cache.set("person_missing", null);

    apolloPostMock.mockResolvedValueOnce({
      matches: [
        {
          id: "person_new",
          organization_id: "org_1",
          name: "New Person",
          email: "new.person@example.com",
          linkedin_url: "https://linkedin.com/in/new",
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
    });

    const result = await bulkEnrichPeople(
      [
        { id: "person_cached", name: "Cached Person", title: "SRE" },
        { id: "person_missing", name: "Missing Person", title: "SRE" },
        { id: "person_new", name: "New Person", title: "SRE" },
      ],
      cache
    );

    expect(apolloPostMock).toHaveBeenCalledTimes(1);
    expect(apolloPostMock).toHaveBeenCalledWith("/people/bulk_match", {
      details: [{ id: "person_new" }],
    });
    expect(result.map((employee) => employee.id)).toEqual(["person_cached", "person_new"]);
    expect(cache.get("person_new")).toEqual(
      expect.objectContaining({
        id: "person_new",
        email: "new.person@example.com",
      })
    );
  });

});
