import axios, { AxiosInstance } from "axios";

const APOLLO_BASE_URL = "https://api.apollo.io/api/v1";
const DEFAULT_TIMEOUT_MS = 12_000;
const DEFAULT_RETRIES = 2;

function getApiKey(): string {
  const apiKey = process.env.APOLLO_API_KEY;
  if (!apiKey) {
    throw new Error("Missing APOLLO_API_KEY environment variable.");
  }

  return apiKey;
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
      if (attempt > retries) {
        throw new Error(toApolloErrorMessage(error));
      }

      // Simple backoff keeps prototype resilient to temporary failures.
      await sleep(300 * attempt);
    }
  }
}
