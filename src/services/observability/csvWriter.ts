import { stringify } from "csv-stringify";

export type OutputRow = {
  company_name: string;
  company_domain: string;
  company_linkedin_url: string;
  apollo_account_id?: string;
  stage: "ChasingPOC" | "";
  outreach_date?: string;
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
          { key: "stage", header: "Stage" },
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
