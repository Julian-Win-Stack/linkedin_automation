import { getRequiredEnv } from "../config/env";

const APOLLO_BASE_URL = "https://api.apollo.io/api/v1";
const APOLLO_BULK_MATCH_BATCH_SIZE = 10;
const MAX_RETRIES = 2;
const RETRY_DELAYS_MS = [2000, 4000];

export interface EmailEnrichmentInput {
  name: string;
  domain: string;
  linkedinUrl: string;
}

interface ApolloPersonDetail {
  name: string;
  domain: string;
  linkedin_url: string;
}

interface ApolloMatch {
  email?: string | null;
  linkedin_url?: string | null;
}

interface ApolloBulkMatchResponse {
  matches?: ApolloMatch[];
}

function normalizeLinkedinUrl(url: string): string {
  return url
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/+$/, "")
    .toLowerCase();
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function callBulkMatchApi(
  details: ApolloPersonDetail[],
  apiKey: string
): Promise<ApolloMatch[]> {
  const endpoint = `${APOLLO_BASE_URL}/people/bulk_match`;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Cache-Control": "no-cache",
          "x-api-key": apiKey,
        },
        body: JSON.stringify({ details }),
      });
      if (!response.ok) {
        throw new Error(`Apollo bulk match returned HTTP ${response.status}`);
      }
      const data = (await response.json()) as ApolloBulkMatchResponse;
      return Array.isArray(data.matches) ? data.matches : [];
    } catch (error) {
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAYS_MS[attempt]);
        continue;
      }
      throw error;
    }
  }
  return [];
}

export async function findEmailsInBulk(
  candidates: EmailEnrichmentInput[]
): Promise<Map<string, string>> {
  const valid = candidates.filter((c) => c.linkedinUrl.trim().length > 0);
  if (valid.length === 0) {
    return new Map();
  }

  const apiKey = getRequiredEnv("APOLLO_API_KEY");
  const byLinkedin = new Map<string, string>();

  for (let i = 0; i < valid.length; i += APOLLO_BULK_MATCH_BATCH_SIZE) {
    const chunk = valid.slice(i, i + APOLLO_BULK_MATCH_BATCH_SIZE);
    const details: ApolloPersonDetail[] = chunk.map((c) => ({
      name: c.name,
      domain: c.domain,
      linkedin_url: c.linkedinUrl.trim(),
    }));

    const matches = await callBulkMatchApi(details, apiKey);

    for (const match of matches) {
      const linkedinRaw = match.linkedin_url?.trim() ?? "";
      const email = match.email?.trim() ?? "";
      if (!linkedinRaw || !email) {
        continue;
      }
      byLinkedin.set(normalizeLinkedinUrl(linkedinRaw), email);
    }
  }

  return byLinkedin;
}
