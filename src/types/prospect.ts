export interface Prospect {
  id: string;
  name: string;
  title: string;
}

export interface EnrichedEmployee {
  id?: string;
  startDate: string | null;
  endDate: string | null;
  name: string;
  email?: string | null;
  linkedinUrl: string | null;
  currentTitle: string;
  tenure: number | null;
}

export interface LemlistFailedLead {
  name: string;
  error: string;
}

export type LemlistPushStatus = "succeed" | "failed" | "skipped";

export interface LemlistPushOutcome {
  key: string;
  name: string;
  title: string;
  linkedinUrl: string | null;
  status: LemlistPushStatus;
  error?: string;
}

export interface LemlistPushMeta {
  attempted: number;
  successful: number;
  failed: number;
  successItems: string[];
  failedItems: LemlistFailedLead[];
  outcomes: LemlistPushOutcome[];
  skipped?: boolean;
  reason?: string;
  enabledByEnv?: boolean;
  enabledByRequest?: boolean;
}

export interface EmploymentHistoryItem {
  organization_name?: string;
  company_name?: string;
  title?: string;
  start_date?: string;
  end_date?: string | null;
  current?: boolean;
}

export interface ApolloPerson {
  id?: string;
  name?: string;
  first_name?: string;
  last_name?: string;
  title?: string;
  organization_name?: string;
  linkedin_url?: string;
  employment_history?: EmploymentHistoryItem[];
}

export interface ApifyExperienceEntry {
  companyName?: string;
  companyUniversalName?: string;
  companyLinkedinUrl?: string;
  description?: string;
  employmentType?: string;
  position?: string;
  endDate?: { text?: string } | null;
  skills?: string[];
}

export interface ApifyProfileSkill {
  name: string;
}

export interface ApifyCacheEntry {
  openToWork: boolean;
  experience: ApifyExperienceEntry[];
  profileSkills: ApifyProfileSkill[];
}

export type ApifyOpenToWorkCache = Map<string, ApifyCacheEntry>;
