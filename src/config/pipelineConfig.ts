export interface PipelineConfig {
  nameColumn: string[];
  domainColumn: string[];
  apolloAccountIdColumn: string[];
  linkedinUrlColumn: string[];
}

const NAME_COLUMN_CANDIDATES = ["Company Name", "Parent Record > Company name"];
const DOMAIN_COLUMN_CANDIDATES = ["Website", "Parent Record > Website"];
const APOLLO_ACCOUNT_ID_COLUMN_CANDIDATES = ["Apollo Account Id", "Parent Record > Apollo ID", "Apollo ID"];
const LINKEDIN_URL_COLUMN_CANDIDATES = ["Company Linkedin Url", "Parent Record > LinkedIn Page", "LinkedIn Page"];

export function loadPipelineConfig(): PipelineConfig {
  return {
    nameColumn: NAME_COLUMN_CANDIDATES,
    domainColumn: DOMAIN_COLUMN_CANDIDATES,
    apolloAccountIdColumn: APOLLO_ACCOUNT_ID_COLUMN_CANDIDATES,
    linkedinUrlColumn: LINKEDIN_URL_COLUMN_CANDIDATES,
  };
}
