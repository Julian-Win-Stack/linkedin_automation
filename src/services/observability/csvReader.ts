import { parse } from "csv-parse";
import { Readable } from "node:stream";

export type CompanyRow = {
  companyName: string;
  companyDomain: string;
  companyLinkedinUrl: string;
  apolloAccountId?: string;
  rowNumber: number;
};

export type CompanyRowSkipReason = "missing_website_and_apollo_account_id";
export interface CompanyRowSkipInfo {
  reason: CompanyRowSkipReason;
  companyName: string;
  rowNumber: number;
}

interface ReadCompaniesOptions {
  csvBuffer: string;
  nameColumn: string;
  domainColumn: string;
  linkedinUrlColumn?: string;
  apolloAccountIdColumn?: string;
  onSkipRow?: (skipInfo: CompanyRowSkipInfo) => void;
}

interface CountProcessableCompaniesOptions {
  csvBuffer: string;
  domainColumn: string;
  apolloAccountIdColumn?: string;
}

function cleanCell(value: unknown): string {
  return String(value ?? "").trim();
}

const parseOptions = {
  columns: true,
  bom: true,
  skip_empty_lines: true,
  relax_quotes: true,
  relax_column_count: true,
  trim: false,
} as const;

export async function* readCompanies(options: ReadCompaniesOptions): AsyncGenerator<CompanyRow> {
  const inputStream = Readable.from([options.csvBuffer]);
  const parser = inputStream.pipe(parse(parseOptions));

  let rowNumber = 1;

  for await (const record of parser as AsyncIterable<Record<string, unknown>>) {
    rowNumber += 1;

    const companyName = cleanCell(record[options.nameColumn]);
    const companyDomain = cleanCell(record[options.domainColumn]);
    const companyLinkedinUrl = options.linkedinUrlColumn
      ? cleanCell(record[options.linkedinUrlColumn])
      : "";
    const apolloAccountId = options.apolloAccountIdColumn
      ? cleanCell(record[options.apolloAccountIdColumn]) || undefined
      : undefined;

    if (!companyDomain && !apolloAccountId) {
      options.onSkipRow?.({
        reason: "missing_website_and_apollo_account_id",
        companyName,
        rowNumber,
      });
      continue;
    }

    yield {
      companyName,
      companyDomain,
      companyLinkedinUrl,
      apolloAccountId,
      rowNumber,
    };
  }
}

export async function countProcessableCompanies(options: CountProcessableCompaniesOptions): Promise<number> {
  const inputStream = Readable.from([options.csvBuffer]);
  const parser = inputStream.pipe(parse(parseOptions));
  let count = 0;

  for await (const record of parser as AsyncIterable<Record<string, unknown>>) {
    const companyDomain = cleanCell(record[options.domainColumn]);
    const apolloAccountId = options.apolloAccountIdColumn
      ? cleanCell(record[options.apolloAccountIdColumn]) || undefined
      : undefined;
    if (!companyDomain && !apolloAccountId) {
      continue;
    }
    count += 1;
  }

  return count;
}
