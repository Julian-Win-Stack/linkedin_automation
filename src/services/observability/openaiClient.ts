import { searchGoogle, SearchResult } from "./searchApiClient";

interface OpenAIConfig {
  apiKey: string;
  baseUrl: string;
  model: string;
  maxCompletionTokens: number;
  searchApiKey: string;
}

interface ChatCompletionsResponse {
  choices?: Array<{
    message?: {
      content?: string | null;
    };
  }>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildSystemPrompt(): string {
  return [
    "You are an observability-tool judge.",
    "You are given a company and a list of candidate web pages (URLs and snippets).",
    "Your job is to decide, based ONLY on those pages, which observability, monitoring, logging, or APM tools this company actually uses.",
    "",
    "IMPORTANT:",
    "- Do NOT invent tools or URLs.",
    '- If the evidence is ambiguous or weak, you may mark a tool as "(low confidence)".',
    '- If none of the pages clearly mention an observability/monitoring/APM tool, return exactly: Not found',
    "",
    "Output format (no markdown, no brackets, numbered list):",
    "1. <tool name> : https://example.com/path",
    "2. <tool name> : https://example.com/path",
    "3. <tool name> : https://example.com/path",
    "",
    "Only return up to 3 tools.",
    "It is better to return 'Not found' than to guess.",
  ].join("\n");
}

function buildUserPrompt(companyName: string, domain: string, candidates: SearchResult[]): string {
  const lines = [
    `Company name: ${companyName}`,
    `Company domain: ${domain}`,
    "",
    "Here are candidate pages that may contain evidence of observability/monitoring/APM tools:",
  ];

  if (candidates.length === 0) {
    lines.push("- (no candidate pages were found)");
  } else {
    for (const candidate of candidates) {
      lines.push(`- URL: ${candidate.link}`);
      if (candidate.title) {
        lines.push(`  Title: ${candidate.title}`);
      }
      if (candidate.snippet) {
        lines.push(`  Snippet: ${candidate.snippet}`);
      }
      lines.push("");
    }
  }

  lines.push(
    "",
    "From ONLY this evidence, decide which observability/monitoring/logging/APM tools this company uses.",
    "If none of the pages clearly indicate a tool, return exactly: Not found."
  );

  return lines.join("\n");
}

function cleanDomain(domain: string): string {
  return domain
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/.*$/, "");
}

function buildQueries(companyName: string, companyDomain: string): string[] {
  const normalizedDomain = cleanDomain(companyDomain);
  const baseQuery = `${companyName}${normalizedDomain ? ` ${normalizedDomain}` : ""} Datadog OR "Grafana" OR "New Relic" OR "Prometheus" OR "Splunk" OR "Dynatrace" OR "Elastic" OR "PagerDuty" OR "Honeycomb"`;
  return [baseQuery];
}

async function gatherSearchCandidates(
  companyName: string,
  companyDomain: string,
  searchApiKey: string
): Promise<SearchResult[]> {
  const queries = buildQueries(companyName, companyDomain);
  const allResults: SearchResult[] = [];

  for (const query of queries) {
    try {
      const results = await searchGoogle(query, searchApiKey);
      allResults.push(...results);
    } catch {
      // Keep behavior resilient for partial SearchAPI failures.
    }
  }

  const seen = new Set<string>();
  const unique = allResults.filter((result) => {
    if (!result.link || seen.has(result.link)) {
      return false;
    }
    seen.add(result.link);
    return true;
  });

  return unique.slice(0, 10);
}

function extractContent(payload: ChatCompletionsResponse): string | null {
  const content = payload.choices?.[0]?.message?.content;
  if (typeof content !== "string") {
    return null;
  }
  const trimmed = content.trim();
  return trimmed ? trimmed : null;
}

export async function researchCompany(
  companyName: string,
  domain: string,
  config: OpenAIConfig
): Promise<string> {
  const url = `${config.baseUrl}/chat/completions`;
  const companyDomain = cleanDomain(domain);
  const candidates = await gatherSearchCandidates(companyName, companyDomain, config.searchApiKey);
  const system = buildSystemPrompt();
  const user = buildUserPrompt(companyName, companyDomain, candidates);

  const body = {
    model: config.model,
    max_completion_tokens: config.maxCompletionTokens,
    stream: false,
    messages: [
      { role: "system", content: system },
      { role: "user", content: user },
    ],
  };

  const backoffsMs = [1000, 2000, 4000];
  let lastError: unknown = null;

  for (let attempt = 0; attempt < backoffsMs.length; attempt += 1) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          "api-key": config.apiKey,
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(90_000),
      });

      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(
          `OpenAI API ${response.status} ${response.statusText}${text ? `: ${text}` : ""}`
        );
      }

      const payload = (await response.json()) as ChatCompletionsResponse;
      const content = extractContent(payload);
      if (!content) {
        throw new Error("OpenAI response missing message content");
      }
      return content;
    } catch (error) {
      lastError = error;
      await sleep(backoffsMs[attempt]);
    }
  }

  const message = lastError instanceof Error ? lastError.message : "Unknown error calling OpenAI";
  return `Error: ${message}`;
}
