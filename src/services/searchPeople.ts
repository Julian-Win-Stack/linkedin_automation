import { apolloPostWithQuery } from "./apolloClient";
import { ResolvedCompany } from "./getCompany";
import { ApolloPerson, Prospect } from "../types/prospect";

const DEFAULT_PERSON_TITLES = ["SRE", "Site Reliability"];
const APOLLO_PAGE_SIZE = 100;
const MAX_APOLLO_PAGES = 500;

interface PeopleSearchResponse {
  total_entries?: number;
  people?: ApolloPerson[];
  pagination?: {
    page?: number;
    total_pages?: number;
  };
}

function toName(person: ApolloPerson): string {
  if (person.name) {
    return person.name;
  }

  return [person.first_name, person.last_name].filter(Boolean).join(" ").trim();
}

function toProspect(person: ApolloPerson, companyName: string): Prospect {
  const fallbackId = `${companyName}:${toName(person)}:${person.title ?? ""}`;
  return {
    id: person.id ?? fallbackId,
    name: toName(person),
    title: person.title ?? "",
  };
}

function toPeopleSearchQueryParams(
  company: ResolvedCompany,
  page: number,
  personTitles: string[],
  perPage = APOLLO_PAGE_SIZE,
  includeSimilarTitles = true
): Record<string, string | number | boolean | Array<string | number | boolean>> {
  const params: Record<string, string | number | boolean | Array<string | number | boolean>> = {
    page,
    per_page: perPage,
    "person_titles[]": personTitles,
    include_similar_titles: includeSimilarTitles,
  };

  // Follow the People Search parameter names from Apollo docs.
  params["q_organization_domains_list[]"] = [company.domain];

  return params;
}

export async function countEngineerPeople(company: ResolvedCompany): Promise<number> {
  const engineerPastTitles = [
    "engineer",
    "developer",
    "builder",
    "technical lead",
    "platform",
    "infrastructure",
    "SRE",
    "backend",
    "cloud",
    "devops",
    "Site Reliability",
    "software",
    "IOS",
    "Android",
    "mobile",
    "frontend",
    "backend",
    "fullstack",
    "python",
    "java",
    "javascript",
    "typescript",
    "ruby",
    "php",
    "sql",
    "nosql",
    "database",
    "machine learning",
    "artificial intelligence",
    "ai",
    "ml",
    "dl",
    "deep learning",  
    "kubernetes",
    "docker",
    "cloud",
    "aws",
    "azure",
    "gcp",
    "devops",
    "site reliability",
    "network engineer",
    "technical lead",
    "tech lead",
    "software architect",
    "CTO",
    "platform engineer",
    "platform architect",
    "ai"
  ];

  const response = await apolloPostWithQuery<PeopleSearchResponse>(
    "/mixed_people/api_search",
    {
      page: 1,
      per_page: 1,
      "person_past_titles[]": engineerPastTitles,
      include_similar_titles: true,
      "q_organization_domains_list[]": [company.domain],
    }
  );

  const totalEntries = response.total_entries;
  if (typeof totalEntries !== "number" || !Number.isFinite(totalEntries) || totalEntries < 0) {
    return 0;
  }

  return totalEntries;
}

export async function searchPeople(
  company: ResolvedCompany,
  maxResults = 100,
  personTitles: string[] = DEFAULT_PERSON_TITLES
): Promise<Prospect[]> {
  const normalizedMaxResults = Math.max(1, Math.min(maxResults, 100));
  const normalizedPersonTitles = personTitles
    .map((title) => title.trim())
    .filter((title) => title.length > 0);

  if (normalizedPersonTitles.length === 0) {
    throw new Error("At least one person title is required.");
  }

  const prospects: Prospect[] = [];
  let page = 1;

  while (prospects.length < normalizedMaxResults) {
    const response = await apolloPostWithQuery<PeopleSearchResponse>(
      "/mixed_people/api_search",
      toPeopleSearchQueryParams(company, page, normalizedPersonTitles)
    );

    const people = response.people ?? [];
    if (people.length === 0) {
      break;
    }

    const matchingProspects = people.map((person) => toProspect(person, company.companyName));

    prospects.push(...matchingProspects);

    const totalPages = response.pagination?.total_pages;
    const reachedLastPage =
      page >= MAX_APOLLO_PAGES || (totalPages ? page >= totalPages : people.length < APOLLO_PAGE_SIZE);
    if (reachedLastPage) {
      break;
    }

    page += 1;
  }

  return prospects.slice(0, normalizedMaxResults);
}
