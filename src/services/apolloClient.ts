import axios, { AxiosInstance } from "axios";
import { getRequiredEnv } from "../config/env";

const APOLLO_BASE_URL = "https://api.apollo.io/api/v1";
const DEFAULT_TIMEOUT_MS = 12_000;
const DEFAULT_RETRIES = 2;
type QueryValue = string | number | boolean;

export interface ApolloField {
  id: string;
  label: string;
  modality: string;
  source?: string;
}

interface ApolloFieldsResponse {
  fields?: ApolloField[];
}

interface ApolloAccountStage {
  id?: string;
  name?: string;
  display_name?: string;
}

interface ApolloAccountStagesResponse {
  account_stages?: ApolloAccountStage[];
}

function getApiKey(): string {
  return getRequiredEnv("APOLLO_API_KEY");
}

export function createApolloClient(): AxiosInstance {
  return axios.create({
    baseURL: APOLLO_BASE_URL,
    timeout: DEFAULT_TIMEOUT_MS,
    headers: {
      "x-api-key": getApiKey(),
      "Content-Type": "application/json",
      accept: "text/plain",
      "Cache-Control": "no-cache",
    },
  });
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function toApolloErrorMessage(error: unknown): string {
  if (!axios.isAxiosError(error)) {
    return error instanceof Error ? error.message : "Unknown Apollo request error.";
  }

  const status = error.response?.status ?? "unknown";
  const payload = error.response?.data;

  if (typeof payload === "string") {
    return `Apollo API error (${status}): ${payload}`;
  }

  if (payload && typeof payload === "object") {
    const record = payload as Record<string, unknown>;
    const message =
      (typeof record.error === "string" && record.error) ||
      (typeof record.message === "string" && record.message) ||
      (typeof record.detail === "string" && record.detail);

    if (message) {
      return `Apollo API error (${status}): ${message}`;
    }

    return `Apollo API error (${status}): ${JSON.stringify(record)}`;
  }

  return `Apollo API error (${status}): ${error.message}`;
}

function isRetryableApolloError(error: unknown): boolean {
  if (!axios.isAxiosError(error)) {
    return false;
  }

  const status = error.response?.status;
  if (typeof status !== "number") {
    // No HTTP response means a transport/network failure.
    return true;
  }

  return status === 429 || status >= 500;
}

export async function apolloPost<TResponse>(
  path: string,
  body: Record<string, unknown>,
  retries = DEFAULT_RETRIES
): Promise<TResponse> {
  const client = createApolloClient();
  let attempt = 0;

  while (true) {
    try {
      const response = await client.post<TResponse>(path, body);
      return response.data;
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !isRetryableApolloError(error)) {
        throw new Error(toApolloErrorMessage(error));
      }

      // Simple backoff keeps prototype resilient to temporary failures.
      await sleep(300 * attempt);
    }
  }
}

function toQueryString(queryParams: Record<string, QueryValue | QueryValue[]>): string {
  const params = new URLSearchParams();

  for (const [key, rawValue] of Object.entries(queryParams)) {
    if (Array.isArray(rawValue)) {
      for (const value of rawValue) {
        params.append(key, String(value));
      }
    } else {
      params.append(key, String(rawValue));
    }
  }

  return params.toString();
}

export async function apolloPostWithQuery<TResponse>(
  path: string,
  queryParams: Record<string, QueryValue | QueryValue[]>,
  retries = DEFAULT_RETRIES
): Promise<TResponse> {
  const client = createApolloClient();
  const queryString = toQueryString(queryParams);
  const requestPath = queryString.length > 0 ? `${path}?${queryString}` : path;
  let attempt = 0;

  while (true) {
    try {
      const response = await client.post<TResponse>(requestPath);
      return response.data;
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !isRetryableApolloError(error)) {
        throw new Error(toApolloErrorMessage(error));
      }

      await sleep(300 * attempt);
    }
  }
}

export async function apolloGetWithQuery<TResponse>(
  path: string,
  queryParams: Record<string, QueryValue | QueryValue[]>,
  retries = DEFAULT_RETRIES
): Promise<TResponse> {
  const client = createApolloClient();
  const queryString = toQueryString(queryParams);
  const requestPath = queryString.length > 0 ? `${path}?${queryString}` : path;
  let attempt = 0;

  while (true) {
    try {
      const response = await client.get<TResponse>(requestPath);
      return response.data;
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !isRetryableApolloError(error)) {
        throw new Error(toApolloErrorMessage(error));
      }

      await sleep(300 * attempt);
    }
  }
}

export async function fetchApolloAccountCustomFieldNameToIdMap(): Promise<Map<string, string>> {
  const response = await apolloGetWithQuery<ApolloFieldsResponse>("/fields", { source: "custom" });
  const fields = Array.isArray(response.fields) ? response.fields : [];
  const nameToId = new Map<string, string>();

  for (const field of fields) {
    if (!field || typeof field !== "object") {
      continue;
    }

    const id = typeof field.id === "string" ? field.id.trim() : "";
    const label = typeof field.label === "string" ? field.label.trim() : "";
    const modality = typeof field.modality === "string" ? field.modality.trim().toLowerCase() : "";

    if (!id || !label || modality !== "account") {
      continue;
    }

    nameToId.set(label, id);
  }

  return nameToId;
}

export async function fetchApolloAccountStageNameToIdMap(): Promise<Map<string, string>> {
  const response = await apolloGetWithQuery<ApolloAccountStagesResponse>("/account_stages", {});
  const stages = Array.isArray(response.account_stages) ? response.account_stages : [];
  const nameToId = new Map<string, string>();

  for (const stage of stages) {
    if (!stage || typeof stage !== "object") {
      continue;
    }

    const id = typeof stage.id === "string" ? stage.id.trim() : "";
    const name = typeof stage.name === "string" ? stage.name.trim() : "";
    const displayName = typeof stage.display_name === "string" ? stage.display_name.trim() : "";

    if (!id) {
      continue;
    }
    if (name) {
      nameToId.set(name, id);
    }
    if (displayName) {
      nameToId.set(displayName, id);
    }
  }

  return nameToId;
}
