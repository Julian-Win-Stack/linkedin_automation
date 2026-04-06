import { getRequiredEnv } from "../config/env";

const APIFY_BASE_URL = "https://api.apify.com/v2";
const APIFY_ACTOR_ID = "snipercoder~bulk-linkedin-email-finder";
const RUN_TIMEOUT_SECONDS = 120;
const MAX_RETRIES = 2;
const RETRY_DELAYS_MS = [2000, 4000];

interface EmailFinderItem {
  linkedinUrl?: string;
  linkedin_url?: string;
  email?: string;
  workEmail?: string;
  personalEmail?: string;
  [key: string]: unknown;
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

function pickEmail(item: EmailFinderItem): string | null {
  const candidates = [
    item.email,
    item.workEmail,
    item.personalEmail,
    item["04_Email"],
  ];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate.trim();
    }
  }
  for (const [key, value] of Object.entries(item)) {
    if (!key.toLowerCase().includes("email")) {
      continue;
    }
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return null;
}

function pickLinkedin(item: EmailFinderItem): string | null {
  const candidates = [
    item.linkedinUrl,
    item.linkedin_url,
    item["06_Linkedin_url"],
  ];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim().length > 0) {
      return candidate.trim();
    }
  }
  for (const [key, value] of Object.entries(item)) {
    const normalizedKey = key.toLowerCase();
    if (!normalizedKey.includes("linkedin")) {
      continue;
    }
    if (normalizedKey.includes("query")) {
      continue;
    }
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return null;
}

async function runBulkEmailFinder(linkedinUrls: string[], apiKey: string): Promise<EmailFinderItem[]> {
  const endpoint =
    `${APIFY_BASE_URL}/acts/${APIFY_ACTOR_ID}/run-sync-get-dataset-items` +
    `?token=${apiKey}&timeout=${RUN_TIMEOUT_SECONDS}`;

  const payload = { linkedin_url_or_ids: linkedinUrls };

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        throw new Error(`Apify bulk email finder returned HTTP ${response.status}`);
      }
      const data = (await response.json()) as unknown;
      if (!Array.isArray(data)) {
        throw new Error("Invalid bulk email finder response");
      }
      return data as EmailFinderItem[];
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

export async function findEmailsInBulk(linkedinUrls: string[]): Promise<Map<string, string>> {
  const cleaned = [...new Set(linkedinUrls.map((url) => url.trim()).filter(Boolean))];
  if (cleaned.length === 0) {
    return new Map();
  }

  const apiKey = getRequiredEnv("APIFY_API_KEY");
  const results = await runBulkEmailFinder(cleaned, apiKey);
  const byLinkedin = new Map<string, string>();

  for (const item of results) {
    const linkedinRaw = pickLinkedin(item) ?? "";
    const email = pickEmail(item);
    if (!linkedinRaw || !email) {
      continue;
    }
    byLinkedin.set(normalizeLinkedinUrl(linkedinRaw), email);
  }

  return byLinkedin;
}
