export interface Prospect {
  name: string;
  title: string;
  company: string;
  linkedinUrl: string | null;
  tenureMonths: string | null;
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
  name?: string;
  first_name?: string;
  last_name?: string;
  title?: string;
  organization_name?: string;
  linkedin_url?: string;
  employment_history?: EmploymentHistoryItem[];
}
