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
  const normalizedMaxResults = Math.max(1, Math.min(maxResults, 100));
  const normalizedKeywords = titleKeywords
    .map((keyword) => keyword.trim().toLowerCase())
    .filter((keyword) => keyword.length > 0);

  if (normalizedKeywords.length === 0) {
    throw new Error("At least one title keyword is required.");
  }

  const prospects: Prospect[] = [];
  let page = 1;

  while (prospects.length < normalizedMaxResults) {
    const response = await apolloPost<PeopleSearchResponse>(
      "/mixed_people/api_search",
      toPeopleSearchBody(company, page)
    );

    const people = response.people ?? [];
    if (people.length === 0) {
      break;
    }

    const matchingPeople = people.filter((person) =>
      personMatchesTitleKeywords(person.title, normalizedKeywords)
    );

    const matchingProspects = matchingPeople.map((person) => toProspect(person, company.companyName));

    prospects.push(...matchingProspects);

    const totalPages = response.pagination?.total_pages;
    const reachedLastPage = totalPages ? page >= totalPages : people.length < APOLLO_PAGE_SIZE;
    if (reachedLastPage) {
      break;
    }

    page += 1;
  }

  return prospects.slice(0, normalizedMaxResults);
}
