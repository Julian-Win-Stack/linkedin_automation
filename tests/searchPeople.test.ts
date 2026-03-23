import { beforeEach, describe, expect, it, vi } from "vitest";
import { countEngineerPeople, searchPeople } from "../src/services/searchPeople";
import { ResolvedCompany } from "../src/services/getCompany";

const apolloPostWithQueryMock = vi.fn();

vi.mock("../src/services/apolloClient", () => ({
  apolloPostWithQuery: (...args: unknown[]) => apolloPostWithQueryMock(...args),
}));

describe("searchPeople", () => {
  const company: ResolvedCompany = {
    companyName: "Acme",
    domain: "acme.com",
  };

  beforeEach(() => {
    apolloPostWithQueryMock.mockReset();
  });

  it("passes person_titles to Apollo and maps returned people", async () => {
    apolloPostWithQueryMock.mockResolvedValue({
      people: [
        {
          name: "A Person",
          title: "Platform Engineer",
          organization_name: "Acme",
        },
        {
          name: "B Person",
          title: "Frontend Engineer",
          organization_name: "Acme",
        },
      ],
      pagination: { page: 1, total_pages: 1 },
    });

    const result = await searchPeople(company, 25);
    expect(result).toHaveLength(2);
    expect(result[0].name).toBe("A Person");
    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: true,
      "q_organization_domains_list[]": ["acme.com"],
      "person_titles[]": ["SRE", "Site Reliability"],
    });
  });

  it("supports custom person_titles filters", async () => {
    apolloPostWithQueryMock.mockResolvedValue({
      people: [
        {
          name: "A Person",
          title: "Backend Engineer",
          organization_name: "Acme",
        },
        {
          name: "B Person",
          title: "Designer",
          organization_name: "Acme",
        },
      ],
      pagination: { page: 1, total_pages: 1 },
    });

    const result = await searchPeople(company, 25, ["Site Reliability Engineer"]);
    expect(result).toHaveLength(2);
    expect(result[0].title).toBe("Backend Engineer");
  });

  it("counts engineers as union of past and current title lists", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        people: [
          { id: "person-1", name: "Past One", title: "Backend Engineer" },
          { id: "person-2", name: "Past Two", title: "Platform Engineer" },
        ],
        pagination: { page: 1, total_pages: 1 },
      })
      .mockResolvedValueOnce({
        people: [
          { id: "person-2", name: "Past Two", title: "Platform Engineer" },
          { id: "person-3", name: "Current Three", title: "Technical Lead" },
          { id: "person-4", name: "Current Four", title: "DevOps Engineer" },
        ],
        pagination: { page: 1, total_pages: 1 },
      });

    const count = await countEngineerPeople(company);
    expect(count).toBe(4);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(2);

    const [firstEndpoint, firstQuery] = apolloPostWithQueryMock.mock.calls[0] as [
      string,
      Record<string, unknown>
    ];
    const [secondEndpoint, secondQuery] = apolloPostWithQueryMock.mock.calls[1] as [
      string,
      Record<string, unknown>
    ];

    expect(firstEndpoint).toBe("/mixed_people/api_search");
    expect(secondEndpoint).toBe("/mixed_people/api_search");
    expect(firstQuery.page).toBe(1);
    expect(secondQuery.page).toBe(1);
    expect(firstQuery.per_page).toBe(100);
    expect(secondQuery.per_page).toBe(100);
    expect(firstQuery["q_organization_domains_list[]"]).toEqual(["acme.com"]);
    expect(secondQuery["q_organization_domains_list[]"]).toEqual(["acme.com"]);
    expect(firstQuery["person_past_titles[]"]).toBeTruthy();
    expect(secondQuery["person_titles[]"]).toBeTruthy();
  });

  it("returns zero when both title searches return no people", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        people: [],
        pagination: { page: 1, total_pages: 1 },
      })
      .mockResolvedValueOnce({
      people: [],
      pagination: { page: 1, total_pages: 1 },
    });

    const count = await countEngineerPeople(company);
    expect(count).toBe(0);
  });

  it("short-circuits when past-title engineer count is above 20", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: Array.from({ length: 21 }, (_, index) => ({
        id: `past-${index + 1}`,
        name: `Past ${index + 1}`,
        title: "Engineer",
      })),
      pagination: { page: 1, total_pages: 1 },
    });

    const count = await countEngineerPeople(company);
    expect(count).toBe(21);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(1);
  });
});
