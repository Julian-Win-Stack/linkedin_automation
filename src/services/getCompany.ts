export interface ResolvedCompany {
  companyName: string;
  domain: string;
}

export class InvalidCompanyInputError extends Error {
  constructor(companyQuery: string) {
    super(
      `Invalid company input '${companyQuery}'. Please provide a company domain (example: acme.com).`
    );
    this.name = "InvalidCompanyInputError";
  }
}

function looksLikeDomain(value: string): boolean {
  return /^[a-z0-9.-]+\.[a-z]{2,}$/i.test(value.trim());
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
    };
  }

  throw new InvalidCompanyInputError(normalizedQuery);
}
