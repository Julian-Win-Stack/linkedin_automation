import axios, { AxiosError, AxiosInstance } from "axios";
import { getRequiredEnv } from "../config/env";

const ATTIO_BASE_URL = "https://api.attio.com";
const DEFAULT_TIMEOUT_MS = 12_000;
const DEFAULT_RETRIES = 1;

function getAttioApiKey(): string {
  return getRequiredEnv("ATTIO_API_KEY");
}

function createAttioClient(): AxiosInstance {
  return axios.create({
    baseURL: ATTIO_BASE_URL,
    timeout: DEFAULT_TIMEOUT_MS,
    headers: {
      Authorization: `Bearer ${getAttioApiKey()}`,
      "Content-Type": "application/json",
      accept: "application/json",
    },
  });
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function toAttioErrorMessage(error: unknown): string {
  if (!axios.isAxiosError(error)) {
    return error instanceof Error ? error.message : "Unknown Attio request error.";
  }

  const status = error.response?.status ?? "unknown";
  const payload = error.response?.data;

  if (typeof payload === "string") {
    return `Attio API error (${status}): ${payload}`;
  }

  if (payload && typeof payload === "object") {
    const record = payload as Record<string, unknown>;
    const message =
      (typeof record.message === "string" && record.message) ||
      (typeof record.code === "string" && record.code) ||
      (typeof record.type === "string" && record.type);

    if (message) {
      return `Attio API error (${status}): ${message}`;
    }

    return `Attio API error (${status}): ${JSON.stringify(record)}`;
  }

  return `Attio API error (${status}): ${error.message}`;
}

function isRetryableAttioError(error: unknown): boolean {
  if (!axios.isAxiosError(error)) {
    return false;
  }

  const status = error.response?.status;
  if (typeof status !== "number") {
    return true;
  }

  return status === 429 || status >= 500;
}

function parseRetryAfterMs(error: AxiosError): number | undefined {
  const retryAfterRaw = error.response?.headers?.["retry-after"];
  if (!retryAfterRaw) {
    return undefined;
  }

  const retryAfterValue = Array.isArray(retryAfterRaw) ? retryAfterRaw[0] : retryAfterRaw;
  if (!retryAfterValue) {
    return undefined;
  }

  const seconds = Number(retryAfterValue);
  if (Number.isFinite(seconds)) {
    return Math.max(0, Math.ceil(seconds * 1000));
  }

  const retryAt = Date.parse(retryAfterValue);
  if (!Number.isNaN(retryAt)) {
    return Math.max(0, retryAt - Date.now());
  }

  return undefined;
}

async function withRetry<T>(requestFn: () => Promise<T>, retries = DEFAULT_RETRIES): Promise<T> {
  let attempt = 0;

  while (true) {
    try {
      return await requestFn();
    } catch (error) {
      attempt += 1;
      if (attempt > retries || !isRetryableAttioError(error)) {
        throw new Error(toAttioErrorMessage(error));
      }

      const retryDelayFromHeader =
        axios.isAxiosError(error) && error.response?.status === 429 ? parseRetryAfterMs(error) : undefined;
      const fallbackDelay = 300 * attempt;
      await sleep(retryDelayFromHeader ?? fallbackDelay);
    }
  }
}

export async function attioGet<TResponse>(path: string, retries = DEFAULT_RETRIES): Promise<TResponse> {
  const client = createAttioClient();
  return withRetry(async () => {
    const response = await client.get<TResponse>(path);
    return response.data;
  }, retries);
}

export async function attioPut<TResponse>(
  path: string,
  body: Record<string, unknown>,
  retries = DEFAULT_RETRIES
): Promise<TResponse> {
  const client = createAttioClient();
  return withRetry(async () => {
    const response = await client.put<TResponse>(path, body);
    return response.data;
  }, retries);
}
