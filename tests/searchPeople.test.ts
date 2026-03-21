import { beforeEach, describe, expect, it, vi } from "vitest";
import { searchPeople } from "../src/services/searchPeople";
import { ResolvedCompany } from "../src/services/getCompany";

const apolloPostMock = vi.fn();

vi.mock("../src/services/apolloClient", () => ({
  apolloPost: (...args: unknown[]) => apolloPostMock(...args),
}));

describe("searchPeople", () => {
  const company: ResolvedCompany = {
    companyName: "Acme",
    domain: "acme.com",
    linkedinUrl: null,
  };

  beforeEach(() => {
    apolloPostMock.mockReset();
  });

  it("filters people by title keywords", async () => {
    apolloPostMock.mockResolvedValue({
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
    expect(result).toHaveLength(1);
    expect(result[0].name).toBe("A Person");
  });

  it("supports custom keyword filters", async () => {
    apolloPostMock.mockResolvedValue({
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

    const result = await searchPeople(company, 25, ["engineer"]);
    expect(result).toHaveLength(1);
    expect(result[0].title).toBe("Backend Engineer");
  });
});
