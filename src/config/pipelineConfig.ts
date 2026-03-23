import { getRequiredEnv } from "./env";

export interface PipelineConfig {
  azureOpenAiApiKey: string;
  azureOpenAiBaseUrl: string;
  searchApiKey: string;
  model: string;
  maxCompletionTokens: number;
  nameColumn: string;
  domainColumn: string;
}

export function loadPipelineConfig(): PipelineConfig {
  const maxCompletionTokensRaw = process.env.MAX_COMPLETION_TOKENS ?? "2048";
  const maxCompletionTokens = Number(maxCompletionTokensRaw);
  if (!Number.isFinite(maxCompletionTokens) || maxCompletionTokens <= 0) {
    throw new Error("MAX_COMPLETION_TOKENS must be a positive number.");
  }

  return {
    azureOpenAiApiKey: getRequiredEnv("AZURE_OPENAI_API_KEY"),
    azureOpenAiBaseUrl: getRequiredEnv("AZURE_OPENAI_BASE_URL"),
    searchApiKey: getRequiredEnv("SEARCHAPI_API_KEY"),
    model: (process.env.MODEL ?? "gpt-5.4").trim(),
    maxCompletionTokens,
    nameColumn: (process.env.NAME_COLUMN ?? "Company Name").trim(),
    domainColumn: (process.env.DOMAIN_COLUMN ?? "Website").trim(),
  };
}
