import { apolloPost } from "./apolloClient";
import { ResolvedCompany } from "./getCompany";
import { ApolloPerson, Prospect } from "../types/prospect";
import { computeTenure } from "./computeTenure";

const DEFAULT_TITLE_KEYWORDS = ["sre", "site reliability", "platform"];
const APOLLO_PAGE_SIZE = 100;

interface PeopleSearchResponse {
  people?: ApolloPerson[];
  pagination?: {
    page?: number;
    total_pages?: number;
  };
}

export interface SearchPeoplePageDebug {
  page: number;
  rawPeopleCount: number;
  matchedPeopleCount: number;
  sampleTitles: string[];
  apolloRequestBody: Record<string, unknown>;
}

export interface SearchPeopleDebugInfo {
  keywordsUsed: string[];
  totalRawPeople: number;
  totalMatchedPeople: number;
  pages: SearchPeoplePageDebug[];
}

export interface SearchPeopleResult {
  prospects: Prospect[];
  debug: SearchPeopleDebugInfo;
}

function personMatchesTitleKeywords(title: string | undefined, titleKeywords: string[]): boolean {
  if (!title) {
    return false;
  }

  const normalizedTitle = title.toLowerCase();
  return titleKeywords.some((keyword) => normalizedTitle.includes(keyword));
}

function toName(person: ApolloPerson): string {
  if (person.name) {
    return person.name;
  }

  return [person.first_name, person.last_name].filter(Boolean).join(" ").trim();
}

function toProspect(person: ApolloPerson, companyName: string): Prospect {
  return {
    name: toName(person),
    title: person.title ?? "",
    company: person.organization_name ?? companyName,
    linkedinUrl: person.linkedin_url ?? null,
    tenureMonths: computeTenure(person.employment_history, companyName),
  };
}

function toPeopleSearchBody(company: ResolvedCompany, page: number): Record<string, unknown> {
  const body: Record<string, unknown> = {
    page,
    per_page: APOLLO_PAGE_SIZE,
  };

  // Domain is the most reliable identifier when we have it.
  if (company.domain) {
    body.q_organization_domains = [company.domain];
  } else if (company.linkedinUrl) {
    body.q_organization_linkedin_urls = [company.linkedinUrl];
  } else {
    body.q_organization_name = company.companyName;
  }

  return body;
}

export async function searchPeople(
  company: ResolvedCompany,
  maxResults = 100,
  titleKeywords: string[] = DEFAULT_TITLE_KEYWORDS
): Promise<Prospect[]> {
  const result = await searchPeopleWithDiagnostics(company, maxResults, titleKeywords);
  return result.prospects;
}

export async function searchPeopleWithDiagnostics(
  company: ResolvedCompany,
  maxResults = 100,
  titleKeywords: string[] = DEFAULT_TITLE_KEYWORDS
): Promise<SearchPeopleResult> {
  const normalizedMaxResults = Math.max(1, Math.min(maxResults, 100));
  const normalizedKeywords = titleKeywords
    .map((keyword) => keyword.trim().toLowerCase())
    .filter((keyword) => keyword.length > 0);

  if (normalizedKeywords.length === 0) {
    throw new Error("At least one title keyword is required.");
  }

  const prospects: Prospect[] = [];
  const debug: SearchPeopleDebugInfo = {
    keywordsUsed: normalizedKeywords,
    totalRawPeople: 0,
    totalMatchedPeople: 0,
    pages: [],
  };
  let page = 1;

  while (prospects.length < normalizedMaxResults) {
    const requestBody = toPeopleSearchBody(company, page);
    const response = await apolloPost<PeopleSearchResponse>("/mixed_people/api_search", requestBody);

    const people = response.people ?? [];
    debug.totalRawPeople += people.length;
    if (people.length === 0) {
      debug.pages.push({
        page,
        rawPeopleCount: 0,
        matchedPeopleCount: 0,
        sampleTitles: [],
        apolloRequestBody: requestBody,
      });
      break;
    }

    const matchingPeople = people.filter((person) =>
      personMatchesTitleKeywords(person.title, normalizedKeywords)
    );
    debug.totalMatchedPeople += matchingPeople.length;
    debug.pages.push({
      page,
      rawPeopleCount: people.length,
      matchedPeopleCount: matchingPeople.length,
      sampleTitles: people
        .map((person) => person.title ?? "")
        .filter((title) => title.length > 0)
        .slice(0, 10),
      apolloRequestBody: requestBody,
    });

    const matchingProspects = matchingPeople.map((person) => toProspect(person, company.companyName));

    prospects.push(...matchingProspects);

    const totalPages = response.pagination?.total_pages;
    const reachedLastPage = totalPages ? page >= totalPages : people.length < APOLLO_PAGE_SIZE;
    if (reachedLastPage) {
      break;
    }

    page += 1;
  }

  return {
    prospects: prospects.slice(0, normalizedMaxResults),
    debug,
  };
}
