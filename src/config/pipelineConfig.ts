export interface PipelineConfig {
  nameColumn: string[];
  domainColumn: string[];
  apolloAccountIdColumn: string[];
  linkedinUrlColumn: string[];
}

function parseColumnList(envValue: string | undefined, defaults: string[]): string[] {
  if (!envValue) return defaults;
  const parsed = envValue.split(",").map((s) => s.trim()).filter(Boolean);
  return parsed.length > 0 ? parsed : defaults;
}

export function loadPipelineConfig(): PipelineConfig {
  return {
    nameColumn: parseColumnList(process.env.NAME_COLUMN, ["Company Name"]),
    domainColumn: parseColumnList(process.env.DOMAIN_COLUMN, ["Website"]),
    apolloAccountIdColumn: parseColumnList(process.env.APOLLO_ACCOUNT_ID_COLUMN, ["Apollo Account Id"]),
    linkedinUrlColumn: parseColumnList(process.env.LINKEDIN_URL_COLUMN, ["Company Linkedin Url"]),
  };
}
