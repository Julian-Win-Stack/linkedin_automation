import { beforeEach, describe, expect, it, vi } from "vitest";
import { searchPeople } from "../src/services/searchPeople";
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
});
