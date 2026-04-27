export interface PipelineConfig {
  nameColumn: string;
  domainColumn: string;
  apolloAccountIdColumn: string;
}

export function loadPipelineConfig(): PipelineConfig {
  return {
    nameColumn: (process.env.NAME_COLUMN ?? "Company Name").trim(),
    domainColumn: (process.env.DOMAIN_COLUMN ?? "Website").trim(),
    apolloAccountIdColumn: (process.env.APOLLO_ACCOUNT_ID_COLUMN ?? "Apollo Account Id").trim(),
  };
}
