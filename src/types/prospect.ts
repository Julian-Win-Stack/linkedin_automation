export interface Prospect {
  id: string;
  name: string;
  title: string;
}

export interface EnrichedEmployee {
  startDate: string | null;
  endDate: string | null;
  name: string;
  linkedinUrl: string | null;
  currentTitle: string;
  tenure: string | null;
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
