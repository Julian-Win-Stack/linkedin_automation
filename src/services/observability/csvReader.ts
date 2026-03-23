import { parse } from "csv-parse";
import { Readable } from "node:stream";

export type CompanyRow = {
  companyName: string;
  companyDomain: string;
  rowNumber: number;
};

interface ReadCompaniesOptions {
  csvBuffer: string;
  nameColumn: string;
  domainColumn: string;
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

    if (!companyName || !companyDomain) {
      continue;
    }

    yield {
      companyName,
      companyDomain,
      rowNumber,
    };
  }
}
