import { stringify } from "csv-stringify";

export type OutputRow = {
  company_name: string;
  company_domain: string;
  observability_tool_research: string;
  pipeline_status: string;
  sre_count: number;
  engineer_count: number;
  lemlist_successful: number;
  lemlist_failed: number;
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
          { key: "observability_tool_research", header: "observability_tool" },
          { key: "pipeline_status", header: "pipeline_status" },
          { key: "sre_count", header: "sre_count" },
          { key: "engineer_count", header: "engineer_count" },
          { key: "lemlist_successful", header: "lemlist_successful" },
          { key: "lemlist_failed", header: "lemlist_failed" },
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
