import { apolloPostWithQuery } from "./apolloClient";
import { ResolvedCompany } from "./getCompany";
import { ApolloPerson, Prospect } from "../types/prospect";

const DEFAULT_PERSON_TITLES = ["SRE", "Site Reliability"];
const APOLLO_PAGE_SIZE = 100;
const MAX_APOLLO_PAGES = 500;
const ENGINEER_TITLE_KEYWORDS = [
  "developer",
  "builder",
  "platform",
  "infrastructure",
  "SRE",
  "backend",
  "cloud",
  "devops",
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
  "Chief Technology Officer",
  "application engineer",
  "founding engineer",
  "product engineer",
  "fullstack engineer",
  "full stack engineer",
  "data engineer",
  "data architect",
  "data scientist",
  "systems engineer",
  "distributed engineer",
  "reliability engineer",
  "security engineer",
  "head of engineering",
  "vp of engineering",
  "svp of engineering",
  "director of engineering",
  "engineering manager",
  "software engineering",
  "head of systems",
  "engineering director",
  "engineering lead",
  "lead engineer",
  "staff engineer",
  "principal engineer",
  "principal software engineer",
  "principal engineer",
  "distinguished engineer",
  "architect",
  "eng manager",
  "eng lead",
  "eng director",
  "eng manager",
  "eng lead",
  "eng director",
  "eng manager",
  "eng lead",
  "eng director",
];

interface PeopleSearchResponse {
  total_entries?: number;
  people?: ApolloPerson[];
  pagination?: {
    page?: number;
    total_pages?: number;
  };
}

type TitleParamKey = "person_titles[]" | "person_past_titles[]";
export interface PeopleSearchFilters {
  apolloOrganizationId?: string;
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
  titleParamKey: TitleParamKey,
  filters: PeopleSearchFilters = {},
  perPage = APOLLO_PAGE_SIZE,
  includeSimilarTitles = true
): Record<string, string | number | boolean | Array<string | number | boolean>> {
  const params: Record<string, string | number | boolean | Array<string | number | boolean>> = {
    page,
    per_page: perPage,
    [titleParamKey]: personTitles,
    include_similar_titles: includeSimilarTitles,
  };

  const domain = company.domain.trim();
  if (domain) {
    // Follow the People Search parameter names from Apollo docs.
    params["q_organization_domains_list[]"] = [domain];
  }

  const apolloOrganizationId = filters.apolloOrganizationId?.trim();
  if (apolloOrganizationId) {
    params["q_organization_ids[]"] = [apolloOrganizationId];
  }

  return params;
}

async function searchPeopleByTitleParam(
  company: ResolvedCompany,
  maxResults: number,
  titles: string[],
  titleParamKey: TitleParamKey,
  filters: PeopleSearchFilters = {},
  includeSimilarTitles = true
): Promise<Prospect[]> {
  const normalizedMaxResults = Math.max(1, Math.min(maxResults, 100));
  const normalizedTitles = [...new Set(titles.map((title) => title.trim()).filter(Boolean))];

  if (normalizedTitles.length === 0) {
    throw new Error("At least one person title is required.");
  }

  const prospects: Prospect[] = [];
  let page = 1;

  while (prospects.length < normalizedMaxResults) {
    const response = await apolloPostWithQuery<PeopleSearchResponse>(
      "/mixed_people/api_search",
      toPeopleSearchQueryParams(
        company,
        page,
        normalizedTitles,
        titleParamKey,
        filters,
        APOLLO_PAGE_SIZE,
        includeSimilarTitles
      )
    );

    const people = response.people ?? [];
    if (people.length === 0) {
      break;
    }

    prospects.push(...people.map((person) => toProspect(person, company.companyName)));

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

export async function countEngineerPeople(
  company: ResolvedCompany,
  filters: PeopleSearchFilters = {}
): Promise<number> {
  const queryTitles = [...new Set(ENGINEER_TITLE_KEYWORDS.map((keyword) => keyword.trim()).filter(Boolean))];
  const ENGINEER_COUNT_MINIMUM = 18;

  async function fetchEngineerPeoplePage(
    titleParamKey: "person_past_titles[]" | "person_titles[]",
    page: number
  ): Promise<PeopleSearchResponse> {
    return apolloPostWithQuery<PeopleSearchResponse>("/mixed_people/api_search", {
      page,
      per_page: APOLLO_PAGE_SIZE,
      [titleParamKey]: queryTitles,
      include_similar_titles: true,
      ...(company.domain.trim() ? { "q_organization_domains_list[]": [company.domain.trim()] } : {}),
      ...(filters.apolloOrganizationId?.trim()
        ? { "q_organization_ids[]": [filters.apolloOrganizationId.trim()] }
        : {}),
    });
  }

  function toCountFromFirstPage(response: PeopleSearchResponse): number {
    if (typeof response.total_entries === "number") {
      return response.total_entries;
    }
    return (response.people ?? []).length;
  }

  const firstCurrentTitleResponse = await fetchEngineerPeoplePage("person_titles[]", 1);
  const currentTitleCount = toCountFromFirstPage(firstCurrentTitleResponse);
  if (currentTitleCount >= ENGINEER_COUNT_MINIMUM) {
    return currentTitleCount;
  }

  const firstPastTitleResponse = await fetchEngineerPeoplePage("person_past_titles[]", 1);
  const pastTitleCount = toCountFromFirstPage(firstPastTitleResponse);
  if (pastTitleCount > ENGINEER_COUNT_MINIMUM) {
    return pastTitleCount;
  }

  return pastTitleCount;
}

export async function searchPeople(
  company: ResolvedCompany,
  maxResults = 100,
  personTitles: string[] = DEFAULT_PERSON_TITLES,
  filters: PeopleSearchFilters = {}
): Promise<Prospect[]> {
  return searchPeopleByTitleParam(company, maxResults, personTitles, "person_titles[]", filters, false);
}

const BACKFILL_PAST_SRE_TITLES = ["SRE", "Site Reliability", "Site Reliability Engineer", "Site Reliability Engineering", "Head of Reliability"];
const BACKFILL_PLATFORM_TITLES = ["platform engineer"];

export async function searchPastSrePeople(
  company: ResolvedCompany,
  maxResults = 30,
  filters: PeopleSearchFilters = {}
): Promise<Prospect[]> {
  return searchPeopleByTitleParam(
    company,
    maxResults,
    BACKFILL_PAST_SRE_TITLES,
    "person_past_titles[]",
    filters,
    false
  );
}

export async function searchCurrentPlatformEngineerPeople(
  company: ResolvedCompany,
  maxResults = 30,
  filters: PeopleSearchFilters = {}
): Promise<Prospect[]> {
  return searchPeopleByTitleParam(company, maxResults, BACKFILL_PLATFORM_TITLES, "person_titles[]", filters, false);
}

export interface EmailCandidateSearchParams {
  currentTitles?: string[];
  pastTitles?: string[];
  notTitles?: string[];
  notPastTitles?: string[];
}

export async function searchEmailCandidatePeople(
  company: ResolvedCompany,
  maxResults: number,
  searchParams: EmailCandidateSearchParams,
  filters: PeopleSearchFilters = {}
): Promise<Prospect[]> {
  const normalizedMaxResults = Math.max(1, Math.min(maxResults, 100));
  const currentTitles = [...new Set((searchParams.currentTitles ?? []).map((t) => t.trim()).filter(Boolean))];
  const pastTitles = [...new Set((searchParams.pastTitles ?? []).map((t) => t.trim()).filter(Boolean))];
  const notTitles = [...new Set((searchParams.notTitles ?? []).map((t) => t.trim()).filter(Boolean))];
  const notPastTitles = [...new Set((searchParams.notPastTitles ?? []).map((t) => t.trim()).filter(Boolean))];

  if (currentTitles.length === 0 && pastTitles.length === 0) {
    throw new Error("At least one current or past title is required.");
  }

  const prospects: Prospect[] = [];
  let page = 1;

  while (prospects.length < normalizedMaxResults) {
    const params: Record<string, string | number | boolean | Array<string | number | boolean>> = {
      page,
      per_page: APOLLO_PAGE_SIZE,
      include_similar_titles: false,
    };

    if (currentTitles.length > 0) {
      params["person_titles[]"] = currentTitles;
    }
    if (pastTitles.length > 0) {
      params["person_past_titles[]"] = pastTitles;
    }
    if (notTitles.length > 0) {
      params["person_not_titles[]"] = notTitles;
    }
    if (notPastTitles.length > 0) {
      params["person_not_past_titles[]"] = notPastTitles;
    }

    const domain = company.domain.trim();
    if (domain) {
      params["q_organization_domains_list[]"] = [domain];
    }

    const apolloOrganizationId = filters.apolloOrganizationId?.trim();
    if (apolloOrganizationId) {
      params["q_organization_ids[]"] = [apolloOrganizationId];
    }

    const response = await apolloPostWithQuery<PeopleSearchResponse>(
      "/mixed_people/api_search",
      params
    );

    const people = response.people ?? [];
    if (people.length === 0) {
      break;
    }

    prospects.push(...people.map((person) => toProspect(person, company.companyName)));

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
