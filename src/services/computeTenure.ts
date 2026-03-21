import { EmploymentHistoryItem } from "../types/prospect";

function toMonthIndex(date: Date): number {
  return date.getUTCFullYear() * 12 + date.getUTCMonth();
}

function parseStartDate(rawDate?: string): Date | null {
  if (!rawDate) {
    return null;
  }

  const date = new Date(rawDate);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date;
}

function isSameCompany(historyCompany: string, targetCompany: string): boolean {
  return historyCompany.toLowerCase().trim() === targetCompany.toLowerCase().trim();
}

export function computeTenure(
  employmentHistory: EmploymentHistoryItem[] | undefined,
  targetCompany: string,
  now = new Date()
): string | null {
  if (!employmentHistory || employmentHistory.length === 0) {
    return null;
  }

  const currentRoleAtCompany = employmentHistory.find((item) => {
    const historyCompany = item.organization_name ?? item.company_name ?? "";
    const companyMatches = isSameCompany(historyCompany, targetCompany);
    const isCurrentRole = item.current === true || item.end_date == null;
    return companyMatches && isCurrentRole;
  });

  if (!currentRoleAtCompany) {
    return null;
  }

  const startDate = parseStartDate(currentRoleAtCompany.start_date);
  if (!startDate) {
    return null;
  }

  const totalMonths = toMonthIndex(now) - toMonthIndex(startDate);
  const safeMonths = totalMonths >= 0 ? totalMonths : 0;
  const years = Math.floor(safeMonths / 12);
  const months = safeMonths % 12;

  const yearsLabel = years === 1 ? "year" : "years";
  const monthsLabel = months === 1 ? "month" : "months";
  return `${years} ${yearsLabel} ${months} ${monthsLabel}`;
}
