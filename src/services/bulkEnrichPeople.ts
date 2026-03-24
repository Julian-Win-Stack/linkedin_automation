import { apolloPost } from "./apolloClient";
import { EnrichedEmployee, Prospect } from "../types/prospect";

interface EmploymentHistoryItem {
  organization_id?: string | null;
  organization_name?: string;
  title?: string;
  start_date?: string | null;
  end_date?: string | null;
  current?: boolean;
}

interface BulkMatchRecord {
  id?: string;
  organization_id?: string;
  name?: string;
  linkedin_url?: string | null;
  title?: string;
  employment_history?: EmploymentHistoryItem[];
}

interface BulkMatchResponse {
  matches?: BulkMatchRecord[];
}

function toMonthIndex(date: Date): number {
  return date.getUTCFullYear() * 12 + date.getUTCMonth();
}

function parseDate(rawDate?: string | null): Date | null {
  if (!rawDate) {
    return null;
  }

  const parsed = new Date(rawDate);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed;
}

interface MonthRange {
  start: number;
  end: number;
}

function toMonthRange(item: EmploymentHistoryItem, now: Date): MonthRange | null {
  const startDate = parseDate(item.start_date);
  if (!startDate) {
    return null;
  }

  const endDate = parseDate(item.end_date) ?? now;
  const start = toMonthIndex(startDate);
  const end = toMonthIndex(endDate);
  return {
    start,
    end: end >= start ? end : start,
  };
}

function mergeRanges(ranges: MonthRange[]): MonthRange[] {
  if (ranges.length === 0) {
    return [];
  }

  const sorted = [...ranges].sort((a, b) => a.start - b.start);
  const merged: MonthRange[] = [sorted[0]];

  for (let i = 1; i < sorted.length; i += 1) {
    const current = sorted[i];
    const previous = merged[merged.length - 1];

    if (current.start <= previous.end) {
      previous.end = Math.max(previous.end, current.end);
      continue;
    }

    merged.push(current);
  }

  return merged;
}

function computeCompanyTenure(record: BulkMatchRecord, now = new Date()): number | null {
  const history = record.employment_history ?? [];
  if (history.length === 0) {
    return null;
  }

  const targetOrganizationId = record.organization_id;
  if (!targetOrganizationId) {
    return null;
  }

  const ranges = history
    .filter((item) => item.organization_id === targetOrganizationId)
    .map((item) => toMonthRange(item, now))
    .filter((range): range is MonthRange => range !== null);

  if (ranges.length === 0) {
    return null;
  }

  const merged = mergeRanges(ranges);
  const totalMonths = merged.reduce((sum, range) => sum + (range.end - range.start), 0);
  return totalMonths >= 0 ? totalMonths : 0;
}

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function pickCurrentRole(employmentHistory: EmploymentHistoryItem[] | undefined): EmploymentHistoryItem | null {
  if (!employmentHistory || employmentHistory.length === 0) {
    return null;
  }

  return employmentHistory.find((item) => item.current === true || item.end_date == null) ?? null;
}

function toEnrichedEmployee(record: BulkMatchRecord): EnrichedEmployee | null {
  const currentRole = pickCurrentRole(record.employment_history);
  if (!currentRole || currentRole.end_date != null) {
    return null;
  }

  const tenure = computeCompanyTenure(record);
  return {
    id: record.id,
    startDate: currentRole?.start_date ?? null,
    endDate: currentRole?.end_date ?? null,
    name: record.name ?? "",
    linkedinUrl: record.linkedin_url ?? null,
    currentTitle: currentRole?.title ?? record.title ?? "",
    tenure,
  };
}

export async function bulkEnrichPeople(people: Prospect[]): Promise<EnrichedEmployee[]> {
  const MAX_TOTAL = 30;
  const BATCH_SIZE = 10;
  const selected = people.slice(0, MAX_TOTAL);
  const batches = chunkArray(selected, BATCH_SIZE).slice(0, 3);
  const enriched: EnrichedEmployee[] = [];

  for (const batch of batches) {
    if (batch.length === 0) {
      continue;
    }

    const response = await apolloPost<BulkMatchResponse>("/people/bulk_match", {
      details: batch.map((person) => ({ id: person.id })),
    });

    const matches = response.matches ?? [];
    const activeEmployees = matches
      .map((record) => toEnrichedEmployee(record))
      .filter((employee): employee is EnrichedEmployee => employee !== null);
    enriched.push(...activeEmployees);
  }

  return enriched;
}
