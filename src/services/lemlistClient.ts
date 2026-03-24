import axios, { AxiosInstance } from "axios";
import { getRequiredEnv } from "../config/env";

const LEMLIST_BASE_URL = "https://api.lemlist.com/api";
const DEFAULT_TIMEOUT_MS = 12_000;
const DEFAULT_RETRIES = 1;

export interface LemlistCreateLeadPayload {
  email?: string;
  firstName?: string;
  lastName?: string;
  companyName?: string;
  jobTitle?: string;
  linkedinUrl?: string;
  phone?: string;
  companyDomain?: string;
  timezone?: string;
  contactOwner?: string;
}

export interface LemlistCreateLeadQuery {
  deduplicate?: boolean;
  linkedinEnrichment?: boolean;
  findEmail?: boolean;
  verifyEmail?: boolean;
  findPhone?: boolean;
}

interface LemlistError extends Error {
  status?: number;
}

function getLemlistApiKey(): string {
  return getRequiredEnv("LEMLIST_API_KEY");
}

export interface LemlistLinkedinCampaignIds {
  sreCampaignId: string;
  engLeadCampaignId: string;
  engCampaignId: string;
}

export function getLemlistLinkedinCampaignIds(): LemlistLinkedinCampaignIds {
  return {
    sreCampaignId: getRequiredEnv("LEMLIST_LINKEDIN_SRE_CAMPAIGN_ID"),
    engLeadCampaignId: getRequiredEnv("LEMLIST_LINKEDIN_ENG_LEAD_CAMPAIGN_ID"),
    engCampaignId: getRequiredEnv("LEMLIST_LINKEDIN_ENG_CAMPAIGN_ID"),
  };
}

export function toBasicAuthHeader(apiKey: string): string {
  const token = Buffer.from(`:${apiKey}`).toString("base64");
  return `Basic ${token}`;
}

function createLemlistClient(): AxiosInstance {
  const apiKey = getLemlistApiKey();
  return axios.create({
    baseURL: LEMLIST_BASE_URL,
    timeout: DEFAULT_TIMEOUT_MS,
    headers: {
      Authorization: toBasicAuthHeader(apiKey),
      "Content-Type": "application/json",
      accept: "application/json",
    },
  });
}

function toQueryString(query: LemlistCreateLeadQuery): string {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (typeof value === "boolean") {
      params.append(key, String(value));
    }
  }
  return params.toString();
}

function toLemlistError(error: unknown): LemlistError {
  if (!axios.isAxiosError(error)) {
    return new Error(error instanceof Error ? error.message : "Unknown Lemlist request error.");
  }

  const status = error.response?.status;
  const payload = error.response?.data;
  const message =
    typeof payload === "string"
      ? payload
      : payload && typeof payload === "object"
        ? JSON.stringify(payload)
        : error.message;

  const lemlistError = new Error(
    status ? `Lemlist API error (${status}): ${message}` : `Lemlist API error: ${message}`
  ) as LemlistError;
  lemlistError.status = status;
  return lemlistError;
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export async function createLeadInCampaign(
  campaignId: string,
  payload: LemlistCreateLeadPayload,
  query: LemlistCreateLeadQuery = {},
  retries = DEFAULT_RETRIES
): Promise<void> {
  const client = createLemlistClient();
  const queryString = toQueryString(query);
  const path = queryString
    ? `/campaigns/${campaignId}/leads/?${queryString}`
    : `/campaigns/${campaignId}/leads/`;

  let attempt = 0;
  while (true) {
    try {
      await client.post(path, payload);
      return;
    } catch (error) {
      attempt += 1;
      if (attempt > retries) {
        throw toLemlistError(error);
      }
      await sleep(300 * attempt);
    }
  }
}
