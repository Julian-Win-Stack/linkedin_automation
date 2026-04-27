import { EnrichedEmployee, ApifyOpenToWorkCache, ApifyExperienceEntry } from "../types/prospect";

const LINE_WIDTH = 78;
const HEAVY_LINE = "═".repeat(LINE_WIDTH);
const FRONTEND_REGEX = /\b(front[\s-]?end|android|ios|ai|ml|machine[\s-]?learning)\b/i;
const FRONTEND_OVERRIDE_REGEX = /\b(back[\s-]?end|full[\s-]?stack|end[\s-]?to[\s-]?end)\b/i;
const TITLE_REJECT_REGEX = /\b(data|front[\s-]?end)\b/i;
const HARDWARE_REGEX = /\b(hardware|hw)\b/gi;
const HARDWARE_MIN_OCCURRENCES = 1;

function print(line: string): void {
  void line;
}

function normalizeLinkedinUrl(url: string): string {
  return url
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/+$/, "")
    .toLowerCase();
}

interface TableRow {
  name: string;
  linkedinUrl: string | null;
  openToWork: string;
  status: string;
}

function printApifyTable(
  companyName: string,
  companyDomain: string,
  rows: TableRow[],
  counts: { kept: number; removed: number; cached: number; skipped: number; errors: number },
  profilesSent: number,
  cachedCount: number,
  apiCallCount: number
): void {
  print("");
  print(HEAVY_LINE);
  print(`  APIFY OPEN-TO-WORK CHECK — ${companyName} (${companyDomain})`);
  print(`  Profiles sent: ${profilesSent}  ·  Cached: ${cachedCount}  ·  API calls: ${apiCallCount}`);
  print(HEAVY_LINE);
  print("");
  print(`    ${"Name".padEnd(22)}${"LinkedIn URL".padEnd(36)}${"OpenToWork".padEnd(12)}Status`);
  print(`    ${"─".repeat(22)}${"─".repeat(36)}${"─".repeat(12)}${"─".repeat(8)}`);

  for (const row of rows) {
    const name = row.name.length > 20 ? row.name.slice(0, 19) + "…" : row.name;
    const url = row.linkedinUrl
      ? row.linkedinUrl.length > 34
        ? row.linkedinUrl.slice(0, 33) + "…"
        : row.linkedinUrl
      : "—";
    print(`    ${name.padEnd(22)}${url.padEnd(36)}${row.openToWork.padEnd(12)}${row.status}`);
  }

  print("");
  print(
    `    Summary: ${counts.kept} kept · ${counts.removed} removed · ${counts.cached} cached · ${counts.skipped} skipped · ${counts.errors} error`
  );
  print(HEAVY_LINE);
  print("");
}


export interface ApifyFilterResult {
  kept: EnrichedEmployee[];
  warnings: string[];
  filteredOut: ApifyFilteredCandidate[];
}

export type ApifyFilteredReason = "open_to_work" | "contract_employment";

export interface ApifyFilteredCandidate {
  employee: EnrichedEmployee;
  reason: ApifyFilteredReason;
}

export function filterOpenToWorkFromCache(
  employees: EnrichedEmployee[],
  cache: ApifyOpenToWorkCache,
  context: { companyName: string; companyDomain: string }
): ApifyFilterResult {
  if (employees.length === 0) {
    return { kept: [], warnings: [], filteredOut: [] };
  }

  const kept: EnrichedEmployee[] = [];
  const filteredOut: ApifyFilteredCandidate[] = [];
  const warnings: string[] = [];
  const tableRows: TableRow[] = [];
  const counts = { kept: 0, removed: 0, cached: 0, skipped: 0, errors: 0 };

  for (const employee of employees) {
    const rawUrl = employee.linkedinUrl?.trim() ?? "";
    if (!rawUrl) {
      kept.push(employee);
      counts.skipped += 1;
      tableRows.push({
        name: employee.name,
        linkedinUrl: null,
        openToWork: "—",
        status: "SKIPPED (no URL)",
      });
      continue;
    }

    const key = normalizeLinkedinUrl(rawUrl);
    const cached = cache.get(key);
    if (!cached) {
      kept.push(employee);
      counts.errors += 1;
      tableRows.push({
        name: employee.name,
        linkedinUrl: employee.linkedinUrl,
        openToWork: "—",
        status: "KEPT (cache miss)",
      });
      continue;
    }

    counts.cached += 1;
    if (cached.canonicalLinkedinUrl) {
      employee.linkedinUrl = cached.canonicalLinkedinUrl;
    }

    if (cached.openToWork) {
      filteredOut.push({ employee, reason: "open_to_work" });
      counts.removed += 1;
      tableRows.push({
        name: employee.name,
        linkedinUrl: employee.linkedinUrl,
        openToWork: "true",
        status: "REMOVED (cached)",
      });
      continue;
    }

    if (shouldRejectForContractEmployment(cached.experience)) {
      filteredOut.push({ employee, reason: "contract_employment" });
      counts.removed += 1;
      tableRows.push({
        name: employee.name,
        linkedinUrl: employee.linkedinUrl,
        openToWork: "false",
        status: "REMOVED (employment type, cached)",
      });
      continue;
    }

    kept.push(employee);
    counts.kept += 1;
    tableRows.push({
      name: employee.name,
      linkedinUrl: employee.linkedinUrl,
      openToWork: "false",
      status: "KEPT (cached)",
    });
  }

  if (counts.skipped > 0) {
    const names = employees
      .filter((employee) => !employee.linkedinUrl || employee.linkedinUrl.trim().length === 0)
      .map((employee) => employee.name)
      .join(", ");
    warnings.push(
      `${context.companyName}: ${counts.skipped} candidate(s) skipped from Apify openToWork check — no LinkedIn URL (${names})`
    );
  }

  printApifyTable(
    context.companyName,
    context.companyDomain,
    tableRows,
    counts,
    employees.length,
    counts.cached,
    0
  );

  return { kept, warnings, filteredOut };
}

function isCurrentRole(entry: ApifyExperienceEntry): boolean {
  if (!entry.endDate) return true;
  const text = entry.endDate.text?.toLowerCase().trim() ?? "";
  return text === "present" || text === "";
}

function getMostRecentExperienceEntry(experience: ApifyExperienceEntry[]): ApifyExperienceEntry | null {
  if (experience.length === 0) {
    return null;
  }
  const currentEntry = experience.find((entry) => isCurrentRole(entry));
  return currentEntry ?? experience[0] ?? null;
}

/** Normalizes Apify employmentType for exact-set matching (hyphens vs spaces, casing). */
function normalizeEmploymentTypeForMatch(employmentType: string | undefined): string {
  return (employmentType ?? "")
    .trim()
    .toLowerCase()
    .replace(/-/g, " ")
    .replace(/\s+/g, " ");
}

const REJECTED_EMPLOYMENT_TYPES = new Set<string>([
  "contract",
  "contractor",
  "freelance",
  "freelancer",
  "trainee",
  "intern",
  "internship",
  "apprenticeship",
  "self employed",
  "consultant",
  "consulting",
  "agency",
  "part time",
  "temporary",
  "temp",
  "advisor",
  "fractional",
]);

function isContractEmploymentType(employmentType: string | undefined): boolean {
  const normalized = normalizeEmploymentTypeForMatch(employmentType);
  if (!normalized) {
    return false;
  }
  return REJECTED_EMPLOYMENT_TYPES.has(normalized);
}

function shouldRejectForContractEmployment(
  experience: ApifyExperienceEntry[]
): boolean {
  const mostRecentEntry = getMostRecentExperienceEntry(experience);
  if (!mostRecentEntry) {
    return false;
  }
  return isContractEmploymentType(mostRecentEntry.employmentType);
}

export interface FrontendFilterResult {
  kept: EnrichedEmployee[];
  rejectedFrontend: EnrichedEmployee[];
  warningCandidates: FrontendWarningCandidate[];
}

export type FrontendWarningReason = "company_not_matched" | "company_not_current_role";

export interface FrontendWarningCandidate {
  employee: EnrichedEmployee;
  reason: FrontendWarningReason;
  problem: string;
}

export function filterFrontendEngineers(
  employees: EnrichedEmployee[],
  cache: ApifyOpenToWorkCache,
  _company?: { companyName: string; companyDomain: string }
): FrontendFilterResult {
  const kept: EnrichedEmployee[] = [];
  const rejectedFrontend: EnrichedEmployee[] = [];
  const warningCandidates: FrontendWarningCandidate[] = [];
  const rows: { name: string; companyMatch: string; result: string }[] = [];

  for (const emp of employees) {
    const normalizedUrl = emp.linkedinUrl ? normalizeLinkedinUrl(emp.linkedinUrl) : null;
    const cached = normalizedUrl ? cache.get(normalizedUrl) : null;

    if (!cached || cached.experience.length === 0) {
      kept.push(emp);
      rows.push({ name: emp.name, companyMatch: "—", result: "KEPT (no data)" });
      continue;
    }

    const matchedEntry = getMostRecentExperienceEntry(cached.experience);

    if (!matchedEntry) {
      kept.push(emp);
      rows.push({ name: emp.name, companyMatch: "—", result: "KEPT (no data)" });
      continue;
    }

    const allTexts: string[] = [matchedEntry.description ?? ""];
    for (const entry of cached.experience) {
      if (entry.description) allTexts.push(entry.description);
      if (entry.skills) allTexts.push(...entry.skills);
    }
    for (const skill of cached.profileSkills) allTexts.push(skill.name);
    if (cached.about) allTexts.push(cached.about);
    const combinedText = allTexts.join(" ");

    const rejectedByTitle = TITLE_REJECT_REGEX.test(emp.currentTitle);
    const rejectedByDesc = FRONTEND_REGEX.test(combinedText) && !FRONTEND_OVERRIDE_REGEX.test(combinedText);

    if (rejectedByTitle || rejectedByDesc) {
      rejectedFrontend.push(emp);
      const reason = rejectedByTitle ? "REJECTED (title)" : "REJECTED (frontend)";
      rows.push({ name: emp.name, companyMatch: matchedEntry.companyName ?? "—", result: reason });
    } else {
      kept.push(emp);
      rows.push({ name: emp.name, companyMatch: matchedEntry.companyName ?? "—", result: "KEPT" });
    }
  }

  if (rows.length > 0) {
    print("");
    print(`    FRONTEND KEYWORD CHECK (Normal Engineer Search)`);
    print(`    ${"Name".padEnd(22)}${"Company Match".padEnd(24)}Result`);
    print(`    ${"─".repeat(22)}${"─".repeat(24)}${"─".repeat(24)}`);
    for (const row of rows) {
      const name = row.name.length > 20 ? row.name.slice(0, 19) + "…" : row.name;
      const match = row.companyMatch.length > 22 ? row.companyMatch.slice(0, 21) + "…" : row.companyMatch;
      print(`    ${name.padEnd(22)}${match.padEnd(24)}${row.result}`);
    }
    print(
      `    Result: ${kept.length} kept · ${rejectedFrontend.length} rejected · ${warningCandidates.length} warning`
    );
    print("");
  }

  return { kept, rejectedFrontend, warningCandidates };
}

export interface HardwareFilterResult {
  kept: EnrichedEmployee[];
  rejected: EnrichedEmployee[];
}

export function filterOutHardwareHeavyPeople(
  employees: EnrichedEmployee[],
  _cache: ApifyOpenToWorkCache
): HardwareFilterResult {
  const kept: EnrichedEmployee[] = [];
  const rejected: EnrichedEmployee[] = [];

  for (const emp of employees) {
    const title = emp.currentTitle ?? "";
    const matches = title.match(HARDWARE_REGEX);
    const count = matches ? matches.length : 0;

    if (count >= HARDWARE_MIN_OCCURRENCES) {
      rejected.push(emp);
    } else {
      kept.push(emp);
    }
  }

  return { kept, rejected };
}

export interface SreKeywordFilterResult {
  matched: EnrichedEmployee[];
  unmatched: EnrichedEmployee[];
}

export function filterByKeywordsInApifyData(
  employees: EnrichedEmployee[],
  cache: ApifyOpenToWorkCache,
  companyOrKeywords: { companyName: string; companyDomain: string } | string[],
  maybeKeywords?: string[]
): SreKeywordFilterResult {
  const keywords = Array.isArray(companyOrKeywords)
    ? companyOrKeywords
    : (maybeKeywords ?? []);
  const matched: EnrichedEmployee[] = [];
  const unmatched: EnrichedEmployee[] = [];
  const lowerKeywords = keywords.map((k) => k.toLowerCase());

  for (const emp of employees) {
    const normalizedUrl = emp.linkedinUrl ? normalizeLinkedinUrl(emp.linkedinUrl) : null;
    const cached = normalizedUrl ? cache.get(normalizedUrl) : null;

    const textsToSearch: string[] = [];

    if (emp.headline) {
      textsToSearch.push(emp.headline);
    }

    if (cached) {
      for (const entry of cached.experience) {
        if (entry.description) {
          textsToSearch.push(entry.description);
        }
        if (entry.skills) {
          textsToSearch.push(...entry.skills);
        }
      }

      for (const skill of cached.profileSkills) {
        textsToSearch.push(skill.name);
      }

      if (cached.about) {
        textsToSearch.push(cached.about);
      }
    }

    const combined = textsToSearch.join(" ").toLowerCase();
    const hasKeyword = lowerKeywords.some((keyword) => combined.includes(keyword));

    if (hasKeyword) {
      matched.push(emp);
    } else {
      unmatched.push(emp);
    }
  }

  return { matched, unmatched };
}
