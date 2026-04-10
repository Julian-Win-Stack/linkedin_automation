import { stringify } from "csv-stringify";

export type OutputRow = {
  company_name: string;
  company_domain: string;
  company_linkedin_url: string;
  apollo_account_id?: string;
  observability_tool_research: string;
  stage: "ChasingPOC" | "NotActionableNow" | "";
  sre_count: "" | number;
  notes: string;
  outreach_date?: string;
};

export type RejectedOutputRow = {
  company_name: string;
  company_domain: string;
  company_linkedin_url: string;
  apollo_account_id?: string;
  observability_tool_research: string;
  sre_count: "" | number;
  status: "NotActionableNow";
  notes: string;
};

export function rowsToCsvString(rows: OutputRow[]): Promise<string> {
  return new Promise((resolve, reject) => {
    stringify(
      rows,
      {
        header: true,
        columns: [
          { key: "company_name", header: "Company Name" },
          { key: "company_domain", header: "Website" },
          { key: "company_linkedin_url", header: "Company Linkedin Url" },
          { key: "apollo_account_id", header: "Apollo Account Id" },
          { key: "observability_tool_research", header: "observability_tool" },
          { key: "stage", header: "Stage" },
          { key: "sre_count", header: "Number of SREs" },
          { key: "notes", header: "Notes" },
          { key: "outreach_date", header: "Outreach Date" },
        ],
      },
      (error, output) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(output ?? "");
      }
    );
  });
}

export function rejectedRowsToCsvString(rows: RejectedOutputRow[]): Promise<string> {
  return new Promise((resolve, reject) => {
    stringify(
      rows,
      {
        header: true,
        columns: [
          { key: "company_name", header: "Company Name" },
          { key: "company_domain", header: "Website" },
          { key: "company_linkedin_url", header: "Company Linkedin Url" },
          { key: "apollo_account_id", header: "Apollo Account Id" },
          { key: "observability_tool_research", header: "observability_tool" },
          { key: "sre_count", header: "Number of SREs" },
          { key: "status", header: "Stage" },
          { key: "notes", header: "Notes" },
        ],
      },
      (error, output) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(output ?? "");
      }
    );
  });
}
