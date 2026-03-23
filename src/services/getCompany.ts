export interface ResolvedCompany {
  companyName: string;
  domain: string;
}

export class InvalidCompanyInputError extends Error {
  constructor(companyQuery: string) {
    super(
      `Invalid company input '${companyQuery}'. Please provide a company domain or website URL (example: acme.com or https://acme.com).`
    );
    this.name = "InvalidCompanyInputError";
  }
}

function looksLikeDomain(value: string): boolean {
  return /^[a-z0-9.-]+\.[a-z]{2,}$/i.test(value.trim());
}

function toDomainFromWebsiteUrl(value: string): string | null {
  try {
    const parsedUrl = new URL(value.trim());
    if (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") {
      return null;
    }

    const hostname = parsedUrl.hostname.toLowerCase().replace(/^www\./, "");
    if (!looksLikeDomain(hostname)) {
      return null;
    }

    return hostname;
  } catch {
    return null;
  }
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

  const domainFromUrl = toDomainFromWebsiteUrl(normalizedQuery);
  if (domainFromUrl) {
    return {
      companyName: domainFromUrl,
      domain: domainFromUrl,
    };
  }

  throw new InvalidCompanyInputError(normalizedQuery);
}
