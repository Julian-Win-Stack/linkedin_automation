import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  countEngineerPeople,
  searchCurrentPlatformEngineerPeople,
  searchPastSrePeople,
  searchPeople,
} from "../src/services/searchPeople";
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
  const domainlessCompany: ResolvedCompany = {
    companyName: "NoDomainCo",
    domain: "",
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
      include_similar_titles: false,
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

  it("includes q_organization_ids[] when apollo organization id is provided", async () => {
    apolloPostWithQueryMock.mockResolvedValue({
      people: [{ id: "person-1", name: "A Person", title: "SRE" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchPeople(company, 25, ["SRE"], { apolloOrganizationId: "org_123" });

    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "q_organization_ids[]": ["org_123"],
      "person_titles[]": ["SRE"],
    });
  });

  it("counts engineers as union of past and current title lists", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        total_entries: 10,
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
        total_entries: 0,
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

  it("falls back to full counting when total_entries is missing", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        people: [{ id: "past-1", name: "Past One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      })
      .mockResolvedValueOnce({
        people: [{ id: "current-1", name: "Current One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      });

    const count = await countEngineerPeople(company);
    expect(count).toBe(2);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(2);
  });

  it("reuses first past-title response and does not refetch page 1", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        total_entries: 3,
        people: [{ id: "past-1", name: "Past One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 2 },
      })
      .mockResolvedValueOnce({
        people: [{ id: "past-2", name: "Past Two", title: "Engineer" }],
        pagination: { page: 2, total_pages: 2 },
      })
      .mockResolvedValueOnce({
        people: [{ id: "current-1", name: "Current One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      });

    const count = await countEngineerPeople(company);
    expect(count).toBe(3);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(3);

    const firstPastQuery = apolloPostWithQueryMock.mock.calls[0]?.[1] as Record<string, unknown>;
    const secondPastQuery = apolloPostWithQueryMock.mock.calls[1]?.[1] as Record<string, unknown>;
    expect(firstPastQuery["person_past_titles[]"]).toBeTruthy();
    expect(firstPastQuery.page).toBe(1);
    expect(secondPastQuery["person_past_titles[]"]).toBeTruthy();
    expect(secondPastQuery.page).toBe(2);
  });

  it("does not include domain filter when company domain is empty", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "person-1", name: "No Domain", title: "SRE" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchPeople(domainlessCompany, 10, ["SRE"], { apolloOrganizationId: "org_123" });
    const requestQuery = apolloPostWithQueryMock.mock.calls[0]?.[1] as Record<string, unknown>;
    expect(requestQuery["q_organization_domains_list[]"]).toBeUndefined();
    expect(requestQuery["q_organization_ids[]"]).toEqual(["org_123"]);
  });

  it("short-circuits when past-title engineer count is above 20", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      total_entries: 34,
      people: [{ id: "past-1", name: "Past 1", title: "Engineer" }],
      pagination: { page: 1, total_pages: 1 },
    });

    const count = await countEngineerPeople(company);
    expect(count).toBe(34);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(1);
  });

  it("short-circuits from total_entries even when first page people is empty", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      total_entries: 200,
      people: [],
      pagination: { page: 1, total_pages: 2 },
    });

    const count = await countEngineerPeople(company);
    expect(count).toBe(200);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(1);
  });

  it("passes q_organization_ids[] for engineer counting when provided", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        total_entries: 1,
        people: [{ id: "past-1", name: "Past One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      })
      .mockResolvedValueOnce({
        people: [{ id: "current-1", name: "Current One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      });

    await countEngineerPeople(company, { apolloOrganizationId: "org_abc" });

    expect(apolloPostWithQueryMock).toHaveBeenNthCalledWith(1, "/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: true,
      "q_organization_domains_list[]": ["acme.com"],
      "q_organization_ids[]": ["org_abc"],
      "person_past_titles[]": expect.any(Array),
    });
    expect(apolloPostWithQueryMock).toHaveBeenNthCalledWith(2, "/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: true,
      "q_organization_domains_list[]": ["acme.com"],
      "q_organization_ids[]": ["org_abc"],
      "person_titles[]": expect.any(Array),
    });
  });

  it("queries targeted past SRE helper with person_past_titles[]", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "past-1", name: "Past One", title: "Principal Engineer" }],
      pagination: { page: 1, total_pages: 1 },
    });

    const result = await searchPastSrePeople(company, 10);
    expect(result).toHaveLength(1);
    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "person_past_titles[]": ["SRE", "Site Reliability", "Head of Reliability"],
    });
  });

  it("includes q_organization_ids[] for past SRE helper when provided", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "past-1", name: "Past One", title: "Principal Engineer" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchPastSrePeople(company, 10, { apolloOrganizationId: "org_456" });

    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "q_organization_ids[]": ["org_456"],
      "person_past_titles[]": ["SRE", "Site Reliability", "Head of Reliability"],
    });
  });

  it("queries targeted platform helper with current title param", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "platform-1", name: "Platform One", title: "Platform Engineer" }],
      pagination: { page: 1, total_pages: 1 },
    });

    const result = await searchCurrentPlatformEngineerPeople(company, 10);
    expect(result).toHaveLength(1);
    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "person_titles[]": ["platform engineer"],
    });
  });

  it("includes q_organization_ids[] for platform helper when provided", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "platform-1", name: "Platform One", title: "Platform Engineer" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchCurrentPlatformEngineerPeople(company, 10, { apolloOrganizationId: "org_789" });

    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "q_organization_ids[]": ["org_789"],
      "person_titles[]": ["platform engineer"],
    });
  });
});
