export interface ResolvedCompany {
  companyName: string;
  domain: string | null;
  linkedinUrl: string | null;
}

export class InvalidCompanyInputError extends Error {
  constructor(companyQuery: string) {
    super(
      `Invalid company input '${companyQuery}'. Please provide a company domain (example: acme.com) or a LinkedIn company URL.`
    );
    this.name = "InvalidCompanyInputError";
  }
}

function looksLikeDomain(value: string): boolean {
  return /^[a-z0-9.-]+\.[a-z]{2,}$/i.test(value.trim());
}

function looksLikeLinkedinUrl(value: string): boolean {
  return /^https?:\/\/(www\.)?linkedin\.com\/company\/.+/i.test(value.trim());
}

export async function getCompany(companyQuery: string): Promise<ResolvedCompany> {
  const normalizedQuery = companyQuery.trim();
  if (!normalizedQuery) {
    throw new Error("companyQuery cannot be empty.");
  }

  if (looksLikeDomain(normalizedQuery)) {
    return {
      companyName: normalizedQuery,
      domain: normalizedQuery.toLowerCase(),
      linkedinUrl: null,
    };
  }

  if (looksLikeLinkedinUrl(normalizedQuery)) {
    return {
      companyName: normalizedQuery,
      domain: null,
      linkedinUrl: normalizedQuery,
    };
  }

  throw new InvalidCompanyInputError(normalizedQuery);
}
