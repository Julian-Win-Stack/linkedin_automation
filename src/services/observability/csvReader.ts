import { parse } from "csv-parse";
import { Readable } from "node:stream";

export type CompanyRow = {
  companyName: string;
  companyDomain: string;
  companyLinkedinUrl: string;
  apolloAccountId?: string;
  rowNumber: number;
};

export type CompanyRowSkipReason = "missing_apollo_account_id";
export interface CompanyRowSkipInfo {
  reason: CompanyRowSkipReason;
  companyName: string;
  companyDomain: string;
  rowNumber: number;
}

interface ReadCompaniesOptions {
  csvBuffer: string;
  nameColumn: string[];
  domainColumn: string[];
  linkedinUrlColumn?: string[];
  apolloAccountIdColumn?: string[];
  onSkipRow?: (skipInfo: CompanyRowSkipInfo) => void;
}

interface CountProcessableCompaniesOptions {
  csvBuffer: string;
  domainColumn: string[];
  apolloAccountIdColumn?: string[];
}

function cleanCell(value: unknown): string {
  return String(value ?? "").trim();
}

// Returns the actual header key from the CSV whose trimmed lowercase matches any candidate.
function resolveHeader(candidates: string[], headerKeys: string[]): string | undefined {
  const lowerKeys = headerKeys.map((k) => k.trim().toLowerCase());
  for (const candidate of candidates) {
    const idx = lowerKeys.indexOf(candidate.trim().toLowerCase());
    if (idx !== -1) return headerKeys[idx];
  }
  return undefined;
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
  let nameHeader: string | undefined;
  let domainHeader: string | undefined;
  let linkedinHeader: string | undefined;
  let apolloHeader: string | undefined;
  let headersResolved = false;

  for await (const record of parser as AsyncIterable<Record<string, unknown>>) {
    rowNumber += 1;

    if (!headersResolved) {
      const keys = Object.keys(record);
      nameHeader = resolveHeader(options.nameColumn, keys);
      domainHeader = resolveHeader(options.domainColumn, keys);
      linkedinHeader = options.linkedinUrlColumn
        ? resolveHeader(options.linkedinUrlColumn, keys)
        : undefined;
      apolloHeader = options.apolloAccountIdColumn
        ? resolveHeader(options.apolloAccountIdColumn, keys)
        : undefined;
      headersResolved = true;
    }

    const companyName = cleanCell(nameHeader ? record[nameHeader] : "");
    const companyDomain = cleanCell(domainHeader ? record[domainHeader] : "");
    const companyLinkedinUrl = linkedinHeader ? cleanCell(record[linkedinHeader]) : "";
    const apolloAccountId = apolloHeader
      ? cleanCell(record[apolloHeader]) || undefined
      : undefined;

    if (!apolloAccountId) {
      options.onSkipRow?.({
        reason: "missing_apollo_account_id",
        companyName,
        companyDomain,
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
  let apolloHeader: string | undefined;
  let headersResolved = false;

  for await (const record of parser as AsyncIterable<Record<string, unknown>>) {
    if (!headersResolved) {
      const keys = Object.keys(record);
      apolloHeader = options.apolloAccountIdColumn
        ? resolveHeader(options.apolloAccountIdColumn, keys)
        : undefined;
      headersResolved = true;
    }

    const apolloAccountId = apolloHeader
      ? cleanCell(record[apolloHeader]) || undefined
      : undefined;
    if (!apolloAccountId) {
      continue;
    }
    count += 1;
  }

  return count;
}
