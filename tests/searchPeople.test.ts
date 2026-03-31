import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  countEngineerPeople,
  searchEmailCandidatePeople,
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

  it("returns current-title total_entries when current count is at least 18", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        total_entries: 26,
        people: [{ id: "current-1", name: "Current 1", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      });

    const count = await countEngineerPeople(company);
    expect(count).toBe(26);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(1);

    const [firstEndpoint, firstQuery] = apolloPostWithQueryMock.mock.calls[0] as [
      string,
      Record<string, unknown>
    ];

    expect(firstEndpoint).toBe("/mixed_people/api_search");
    expect(firstQuery.page).toBe(1);
    expect(firstQuery.per_page).toBe(100);
    expect(firstQuery["q_organization_domains_list[]"]).toEqual(["acme.com"]);
    expect(firstQuery["person_titles[]"]).toBeTruthy();
  });

  it("falls back to past-title total_entries when current is below 18", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        total_entries: 12,
        people: [{ id: "current-1", name: "Current One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      })
      .mockResolvedValueOnce({
        total_entries: 22,
        people: [{ id: "past-1", name: "Past One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      });

    const count = await countEngineerPeople(company);
    expect(count).toBe(22);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(2);
  });

  it("returns past-title count when both current and past are below threshold", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        total_entries: 17,
        people: [{ id: "current-1", name: "Current One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      })
      .mockResolvedValueOnce({
        total_entries: 10,
        people: [{ id: "past-1", name: "Past One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      });

    const count = await countEngineerPeople(company);
    expect(count).toBe(10);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(2);
  });

  it("uses current-page people length when total_entries is missing", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        people: Array.from({ length: 18 }, (_, index) => ({
          id: `current-${index}`,
          name: `Current ${index}`,
          title: "Engineer",
        })),
        pagination: { page: 1, total_pages: 1 },
      });

    const count = await countEngineerPeople(company);
    expect(count).toBe(18);
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(1);
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

  it("passes q_organization_ids[] for engineer counting when provided", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      total_entries: 18,
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
      "person_titles[]": expect.any(Array),
    });
    expect(apolloPostWithQueryMock).toHaveBeenCalledTimes(1);
  });

  it("queries past titles when current title count is below 18 and includes org id", async () => {
    apolloPostWithQueryMock
      .mockResolvedValueOnce({
        total_entries: 12,
        people: [{ id: "current-1", name: "Current One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      })
      .mockResolvedValueOnce({
        total_entries: 17,
        people: [{ id: "past-1", name: "Past One", title: "Engineer" }],
        pagination: { page: 1, total_pages: 1 },
      });

    await countEngineerPeople(company, { apolloOrganizationId: "org_abc" });

    expect(apolloPostWithQueryMock).toHaveBeenNthCalledWith(1, "/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: true,
      "q_organization_domains_list[]": ["acme.com"],
      "q_organization_ids[]": ["org_abc"],
      "person_titles[]": expect.any(Array),
    });
    expect(apolloPostWithQueryMock).toHaveBeenNthCalledWith(2, "/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: true,
      "q_organization_domains_list[]": ["acme.com"],
      "q_organization_ids[]": ["org_abc"],
      "person_past_titles[]": expect.any(Array),
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
      "person_past_titles[]": ["SRE", "Site Reliability", "Site Reliability Engineer", "Head of Reliability"],
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
      "person_past_titles[]": ["SRE", "Site Reliability", "Site Reliability Engineer", "Head of Reliability"],
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

  it("searches email candidates with current titles only", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "sre-1", name: "SRE One", title: "SRE" }],
      pagination: { page: 1, total_pages: 1 },
    });

    const result = await searchEmailCandidatePeople(company, 10, {
      currentTitles: ["SRE", "Site Reliability"],
    });
    expect(result).toHaveLength(1);
    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "person_titles[]": ["SRE", "Site Reliability"],
    });
  });

  it("searches email candidates with past titles only", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "past-1", name: "Past SRE", title: "SRE" }],
      pagination: { page: 1, total_pages: 1 },
    });

    const result = await searchEmailCandidatePeople(company, 10, {
      pastTitles: ["SRE", "Site Reliability"],
    });
    expect(result).toHaveLength(1);
    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "person_past_titles[]": ["SRE", "Site Reliability"],
    });
  });

  it("searches email candidates with both current and past titles", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "plat-1", name: "Platform One", title: "Platform Engineer" }],
      pagination: { page: 1, total_pages: 1 },
    });

    const result = await searchEmailCandidatePeople(company, 10, {
      currentTitles: ["platform"],
      pastTitles: ["engineer"],
    });
    expect(result).toHaveLength(1);
    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "person_titles[]": ["platform"],
      "person_past_titles[]": ["engineer"],
    });
  });

  it("includes q_organization_ids[] for email candidate search when provided", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "eng-1", name: "Eng One", title: "Infrastructure" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchEmailCandidatePeople(
      company,
      10,
      { currentTitles: ["Infrastructure"] },
      { apolloOrganizationId: "org_123" }
    );

    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "q_organization_ids[]": ["org_123"],
      "person_titles[]": ["Infrastructure"],
    });
  });

  it("throws when no titles are provided to email candidate search", async () => {
    await expect(
      searchEmailCandidatePeople(company, 10, {})
    ).rejects.toThrow("At least one current or past title is required.");
  });

  it("includes person_not_titles[] when notTitles is provided", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "infra-1", name: "Infra One", title: "Infrastructure Engineer" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchEmailCandidatePeople(
      company,
      10,
      { currentTitles: ["Infrastructure"], notTitles: ["data"] }
    );

    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "person_titles[]": ["Infrastructure"],
      "person_not_titles[]": ["data"],
    });
  });

  it("omits person_not_titles[] when notTitles is empty or absent", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "sre-2", name: "SRE Two", title: "SRE" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchEmailCandidatePeople(company, 10, {
      currentTitles: ["SRE"],
      notTitles: [],
    });

    const params = apolloPostWithQueryMock.mock.calls[0][1];
    expect(params).not.toHaveProperty("person_not_titles[]");
  });

  it("includes person_not_past_titles[] when notPastTitles is provided", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "past-sre-1", name: "Past SRE", title: "Software Engineer" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchEmailCandidatePeople(company, 10, {
      pastTitles: ["SRE"],
      notPastTitles: ["client", "sales"],
    });

    expect(apolloPostWithQueryMock).toHaveBeenCalledWith("/mixed_people/api_search", {
      page: 1,
      per_page: 100,
      include_similar_titles: false,
      "q_organization_domains_list[]": ["acme.com"],
      "person_past_titles[]": ["SRE"],
      "person_not_past_titles[]": ["client", "sales"],
    });
  });

  it("omits person_not_past_titles[] when notPastTitles is empty or absent", async () => {
    apolloPostWithQueryMock.mockResolvedValueOnce({
      people: [{ id: "sre-3", name: "SRE Three", title: "SRE" }],
      pagination: { page: 1, total_pages: 1 },
    });

    await searchEmailCandidatePeople(company, 10, {
      pastTitles: ["SRE"],
      notPastTitles: [],
    });

    const params = apolloPostWithQueryMock.mock.calls[0][1];
    expect(params).not.toHaveProperty("person_not_past_titles[]");
  });
});
