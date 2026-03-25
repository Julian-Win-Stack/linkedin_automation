import { stringify } from "csv-stringify";

export type OutputRow = {
  company_name: string;
  company_domain: string;
  observability_tool_research: string;
  stage: "ChasingPOC" | "NotActionableNow";
  sre_count: "" | number;
  engineer_count: "" | number | "> 1000";
  notes: string;
};

export type RejectedOutputRow = {
  company_name: string;
  company_domain: string;
  observability_tool_research: string;
  sre_count: "" | number;
  engineer_count: "" | number | "> 1000";
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
          { key: "observability_tool_research", header: "observability_tool" },
          { key: "stage", header: "Stage" },
          { key: "sre_count", header: "Number of SREs" },
          { key: "engineer_count", header: "Number of Engineers" },
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

export function rejectedRowsToCsvString(rows: RejectedOutputRow[]): Promise<string> {
  return new Promise((resolve, reject) => {
    stringify(
      rows,
      {
        header: true,
        columns: [
          { key: "company_name", header: "Company Name" },
          { key: "company_domain", header: "Website" },
          { key: "observability_tool_research", header: "observability_tool" },
          { key: "sre_count", header: "Number of SREs" },
          { key: "engineer_count", header: "Number of Engineers" },
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
