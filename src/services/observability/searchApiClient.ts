const SEARCH_API_ENDPOINT = "https://www.searchapi.io/api/v1/search";

export type SearchResult = {
  title: string;
  link: string;
  snippet: string;
};

interface SearchApiResponse {
  organic_results?: Array<{
    title?: string;
    link?: string;
    snippet?: string;
  }>;
}

export async function searchGoogle(query: string, apiKey: string): Promise<SearchResult[]> {
  const url = new URL(SEARCH_API_ENDPOINT);
  url.searchParams.set("engine", "google");
  url.searchParams.set("q", query);
  url.searchParams.set("api_key", apiKey);

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(15_000),
  });

  if (!response.ok) {
    const text = await response.text().catch(() => "");
    throw new Error(`SearchAPI ${response.status} ${response.statusText}${text ? `: ${text}` : ""}`);
  }

  const payload = (await response.json()) as SearchApiResponse;
  const organicResults = payload.organic_results ?? [];
  return organicResults
    .map((item) => ({
      title: item.title ?? "",
      link: item.link ?? "",
      snippet: item.snippet ?? "",
    }))
    .filter((item) => item.link);
}
