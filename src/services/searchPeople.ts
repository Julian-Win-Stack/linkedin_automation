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

type TitleParamKey = "person_titles[]" | "person_past_titles[]";
export interface PeopleSearchFilters {
  apolloOrganizationId?: string;
  /** Substrings excluded from current job title (Apollo `person_not_titles[]`). */
  notTitles?: string[];
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
    params["organization_ids[]"] = [apolloOrganizationId];
  }

  const notTitles = [...new Set((filters.notTitles ?? []).map((t) => t.trim()).filter(Boolean))];
  if (notTitles.length > 0) {
    params["person_not_titles[]"] = notTitles;
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

export async function searchPeople(
  company: ResolvedCompany,
  maxResults = 100,
  personTitles: string[] = DEFAULT_PERSON_TITLES,
  filters: PeopleSearchFilters = {}
): Promise<Prospect[]> {
  return searchPeopleByTitleParam(company, maxResults, personTitles, "person_titles[]", filters, false);
}

const BACKFILL_PAST_SRE_TITLES = [
  "SRE",
  "Site Reliability",
  "Site Reliability Engineer",
  "Site Reliability Engineering",
  "Head of Reliability",
  "observability",
];
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
      params["organization_ids[]"] = [apolloOrganizationId];
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

export type ApolloSearchCache = Map<string, Prospect[]>;

function toSearchCacheKey(
  company: ResolvedCompany,
  searchParams: EmailCandidateSearchParams,
  filters: PeopleSearchFilters
): string {
  const parts = {
    domain: company.domain.trim().toLowerCase(),
    orgId: filters.apolloOrganizationId?.trim() ?? "",
    currentTitles: [...(searchParams.currentTitles ?? [])].sort(),
    pastTitles: [...(searchParams.pastTitles ?? [])].sort(),
    notTitles: [...(searchParams.notTitles ?? [])].sort(),
    notPastTitles: [...(searchParams.notPastTitles ?? [])].sort(),
  };
  return JSON.stringify(parts);
}

export async function searchEmailCandidatePeopleCached(
  company: ResolvedCompany,
  maxResults: number,
  searchParams: EmailCandidateSearchParams,
  filters: PeopleSearchFilters,
  cache: ApolloSearchCache
): Promise<Prospect[]> {
  const key = toSearchCacheKey(company, searchParams, filters);
  const cached = cache.get(key);
  if (cached) {
    return cached.slice(0, maxResults);
  }

  const results = await searchEmailCandidatePeople(company, maxResults, searchParams, filters);
  cache.set(key, results);
  return results;
}