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
  const ENGINEER_PAST_TITLE_SHORT_CIRCUIT_THRESHOLD = 20;

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

  async function fetchAllPeopleByTitleParam(
    titleParamKey: "person_past_titles[]" | "person_titles[]",
    firstResponse?: PeopleSearchResponse
  ): Promise<ApolloPerson[]> {
    const allPeople: ApolloPerson[] = [];
    let page = 1;
    let response = firstResponse ?? (await fetchEngineerPeoplePage(titleParamKey, page));

    while (page <= MAX_APOLLO_PAGES) {
      const people = response.people ?? [];
      if (people.length === 0) {
        break;
      }

      allPeople.push(...people);

      const totalPages = response.pagination?.total_pages;
      const reachedLastPage =
        page >= MAX_APOLLO_PAGES || (totalPages ? page >= totalPages : people.length < APOLLO_PAGE_SIZE);
      if (reachedLastPage) {
        break;
      }
      page += 1;
      response = await fetchEngineerPeoplePage(titleParamKey, page);
    }

    return allPeople;
  }

  const firstPastTitleResponse = await fetchEngineerPeoplePage("person_past_titles[]", 1);
  if (
    typeof firstPastTitleResponse.total_entries === "number" &&
    firstPastTitleResponse.total_entries > ENGINEER_PAST_TITLE_SHORT_CIRCUIT_THRESHOLD
  ) {
    return firstPastTitleResponse.total_entries;
  }

  const pastTitlePeople = await fetchAllPeopleByTitleParam("person_past_titles[]", firstPastTitleResponse);
  const pastTitleUniqueEngineerIds = new Set<string>();
  for (const person of pastTitlePeople) {
    const stableId = person.id ?? `${toName(person)}|${person.title ?? ""}`;
    pastTitleUniqueEngineerIds.add(stableId);
  }

  // Optimization: if past-title count is already high enough, skip current-title search and merging.
  if (pastTitleUniqueEngineerIds.size > ENGINEER_PAST_TITLE_SHORT_CIRCUIT_THRESHOLD) {
    return pastTitleUniqueEngineerIds.size;
  }

  const currentTitlePeople = await fetchAllPeopleByTitleParam("person_titles[]");

  const uniqueEngineerIds = new Set<string>();

  const addPersonToSet = (person: ApolloPerson): void => {
    const stableId = person.id ?? `${toName(person)}|${person.title ?? ""}`;
    uniqueEngineerIds.add(stableId);
  };

  for (const person of pastTitlePeople) addPersonToSet(person);
  for (const person of currentTitlePeople) {
    addPersonToSet(person);
  }

  return uniqueEngineerIds.size;
}

export async function searchPeople(
  company: ResolvedCompany,
  maxResults = 100,
  personTitles: string[] = DEFAULT_PERSON_TITLES,
  filters: PeopleSearchFilters = {}
): Promise<Prospect[]> {
  return searchPeopleByTitleParam(company, maxResults, personTitles, "person_titles[]", filters, false);
}

const BACKFILL_PAST_SRE_TITLES = ["SRE", "Site Reliability", "Head of Reliability"];
const BACKFILL_PLATFORM_TITLES = ["platform engineer"];
const EMAIL_CANDIDATE_TITLES = [
  "platform engineer",
  "SRE",
  "Site Reliability",
  "staff engineer",
  "principal engineer",
  "vp of platform",
  "cto",
  "tech lead",
  "technical lead",
  "devops",
  "infrastructure",
  "head of engineering",
  "vp of engineering",
  "svp of engineering",
  "chief technology officer",
  "vice president of engineering",
  "vp of software engineering",
  "vp of technology",
  "head of backend",
  "head of infrastructure",
  "head of systems",
  "director of engineering",
  "director of software engineering",
  "director of backend engineering",
  "lead software engineer",
];

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

export async function searchCurrentEngineeringEmailCandidates(
  company: ResolvedCompany,
  maxResults = 100,
  filters: PeopleSearchFilters = {}
): Promise<Prospect[]> {
  return searchPeopleByTitleParam(company, maxResults, EMAIL_CANDIDATE_TITLES, "person_titles[]", filters, false);
}
