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
      startDate: "2024-01-01",
      endDate: null,
      name: "A Person",
      linkedinUrl: "https://linkedin.com/in/a",
      currentTitle: "SRE",
      tenure: expect.any(String),
    });
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
    expect(result[0].tenure).toBe("2 years 0 months");
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
