import { getRequiredEnv } from "../config/env";
import { EnrichedEmployee, ApifyOpenToWorkCache, ApifyExperienceEntry } from "../types/prospect";

const APIFY_ACTOR_ID = "harvestapi~linkedin-profile-scraper";
const APIFY_BASE_URL = "https://api.apify.com/v2";
const BATCH_SIZE = 50;
const MAX_RETRIES = 2;
const RETRY_DELAYS_MS = [3000, 6000];
const PER_RUN_TIMEOUT_SECONDS = 120;
const OVERALL_TIMEOUT_MS = 180_000;

const LINE_WIDTH = 78;
const HEAVY_LINE = "═".repeat(LINE_WIDTH);

const FRONTEND_REGEX = /\bfront[\s-]?end\b/i;
const FRONTEND_OVERRIDE_REGEX = /\b(back[\s-]?end|full[\s-]?stack|end[\s-]?to[\s-]?end)\b/i;

const COMPANY_SUFFIXES_REGEX = /\b(inc|llc|ltd|corp|corporation|co|company|plc|ag|gmbh|sa|srl|pvt|pte|pty|nv|bv|se|oy|ab|as)\b\.?/gi;

function print(line: string): void {
  process.stdout.write(line + "\n");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function normalizeLinkedinUrl(url: string): string {
  return url
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/+$/, "")
    .toLowerCase();
}

function normalizeUrlForApify(url: string): string {
  let normalized = url.trim();
  normalized = normalized.replace(/^http:\/\//i, "https://");
  if (!/^https:\/\//i.test(normalized)) {
    normalized = "https://" + normalized;
  }
  if (!normalized.endsWith("/")) {
    normalized += "/";
  }
  return normalized;
}

interface ApifyDatasetItem {
  linkedinUrl?: string;
  openToWork?: boolean;
  experience?: ApifyExperienceEntry[];
  originalQuery?: { query?: string } | string;
  [key: string]: unknown;
}

async function scrapeBatch(
  urls: string[],
  apiKey: string,
  signal: AbortSignal
): Promise<Map<string, { openToWork: boolean; experience: ApifyExperienceEntry[] }>> {
  const endpoint =
    `${APIFY_BASE_URL}/acts/${APIFY_ACTOR_ID}/run-sync-get-dataset-items` +
    `?token=${apiKey}&timeout=${PER_RUN_TIMEOUT_SECONDS}`;

  const apifyUrls = urls.map(normalizeUrlForApify);

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          profileScraperMode: "Profile details no email ($4 per 1k)",
          queries: apifyUrls,
        }),
        signal,
      });

      if (!response.ok) {
        throw new Error(`Apify API returned HTTP ${response.status}`);
      }

      const data = (await response.json()) as ApifyDatasetItem[];
      if (!Array.isArray(data)) {
        throw new Error("Invalid response from Apify");
      }

      const resultMap = new Map<string, { openToWork: boolean; experience: ApifyExperienceEntry[] }>();
      for (const item of data) {
        if (!item.linkedinUrl) continue;
        const key = normalizeLinkedinUrl(item.linkedinUrl);
        const openToWork = item.openToWork === true;
        const experience = Array.isArray(item.experience)
          ? item.experience
          : [];
        const entry = { openToWork, experience };
        resultMap.set(key, entry);

        const oq = item.originalQuery;
        const oqUrl = typeof oq === "string" ? oq : oq?.query;
        if (oqUrl) {
          const oqKey = normalizeLinkedinUrl(oqUrl);
          if (!resultMap.has(oqKey)) {
            resultMap.set(oqKey, entry);
          }
        }
      }

      return resultMap;
    } catch (error) {
      if (signal.aborted) {
        throw new Error("Overall timeout reached");
      }
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAYS_MS[attempt]);
        continue;
      }
      throw error;
    }
  }

  throw new Error("Unreachable");
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

export function splitByTenure(
  employees: EnrichedEmployee[],
  minTenureMonths: number
): { eligible: EnrichedEmployee[]; droppedByTenure: EnrichedEmployee[] } {
  const eligible: EnrichedEmployee[] = [];
  const droppedByTenure: EnrichedEmployee[] = [];

  for (const emp of employees) {
    if (emp.tenure !== null && emp.tenure < minTenureMonths) {
      droppedByTenure.push(emp);
    } else {
      eligible.push(emp);
    }
  }

  return { eligible, droppedByTenure };
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

export async function scrapeAndFilterOpenToWork(
  employees: EnrichedEmployee[],
  cache: ApifyOpenToWorkCache,
  context: { companyName: string; companyDomain: string }
): Promise<ApifyFilterResult> {
  if (employees.length === 0) {
    return { kept: [], warnings: [], filteredOut: [] };
  }

  const apiKey = getRequiredEnv("APIFY_API_KEY");
  const controller = new AbortController();
  const overallTimeout = setTimeout(() => controller.abort(), OVERALL_TIMEOUT_MS);

  const tableRows: TableRow[] = [];
  const kept: EnrichedEmployee[] = [];
  const filteredOut: ApifyFilteredCandidate[] = [];
  const counts = { kept: 0, removed: 0, cached: 0, skipped: 0, errors: 0 };

  const withUrl: { employee: EnrichedEmployee; normalizedUrl: string }[] = [];
  const withoutUrl: EnrichedEmployee[] = [];

  for (const emp of employees) {
    if (emp.linkedinUrl && emp.linkedinUrl.trim().length > 0) {
      withUrl.push({ employee: emp, normalizedUrl: normalizeLinkedinUrl(emp.linkedinUrl) });
    } else {
      withoutUrl.push(emp);
    }
  }

  for (const emp of withoutUrl) {
    kept.push(emp);
    counts.skipped += 1;
    tableRows.push({ name: emp.name, linkedinUrl: null, openToWork: "—", status: "SKIPPED (no URL)" });
  }

  const needsScraping: { employee: EnrichedEmployee; normalizedUrl: string }[] = [];

  for (const { employee, normalizedUrl } of withUrl) {
    if (cache.has(normalizedUrl)) {
      const cached = cache.get(normalizedUrl)!;
      if (cached.openToWork) {
        filteredOut.push({ employee, reason: "open_to_work" });
        counts.removed += 1;
        tableRows.push({
          name: employee.name,
          linkedinUrl: employee.linkedinUrl,
          openToWork: "true",
          status: "REMOVED (cached)",
        });
      } else if (shouldRejectForContractEmployment(cached.experience, context.companyDomain, context.companyName)) {
        filteredOut.push({ employee, reason: "contract_employment" });
        counts.removed += 1;
        counts.cached += 1;
        tableRows.push({
          name: employee.name,
          linkedinUrl: employee.linkedinUrl,
          openToWork: "false",
          status: "REMOVED (contract, cached)",
        });
      } else {
        kept.push(employee);
        counts.kept += 1;
        counts.cached += 1;
        tableRows.push({
          name: employee.name,
          linkedinUrl: employee.linkedinUrl,
          openToWork: "false",
          status: "KEPT (cached)",
        });
      }
    } else {
      needsScraping.push({ employee, normalizedUrl });
    }
  }

  const apiCallCount = needsScraping.length > 0
    ? Math.ceil(needsScraping.length / BATCH_SIZE)
    : 0;

  if (needsScraping.length > 0) {
    const batches = chunkArray(needsScraping, BATCH_SIZE);

    for (const batch of batches) {
      if (controller.signal.aborted) {
        break;
      }

      try {
        const urls = batch.map(({ employee }) => employee.linkedinUrl!);
        const batchResults = await scrapeBatch(urls, apiKey, controller.signal);

        for (const { employee, normalizedUrl } of batch) {
          const result = batchResults.get(normalizedUrl);

          if (result) {
            cache.set(normalizedUrl, { openToWork: result.openToWork, experience: result.experience });

            if (result.openToWork) {
              filteredOut.push({ employee, reason: "open_to_work" });
              counts.removed += 1;
              tableRows.push({
                name: employee.name,
                linkedinUrl: employee.linkedinUrl,
                openToWork: "true",
                status: "REMOVED",
              });
            } else if (shouldRejectForContractEmployment(result.experience, context.companyDomain, context.companyName)) {
              filteredOut.push({ employee, reason: "contract_employment" });
              counts.removed += 1;
              tableRows.push({
                name: employee.name,
                linkedinUrl: employee.linkedinUrl,
                openToWork: "false",
                status: "REMOVED (contract)",
              });
            } else {
              kept.push(employee);
              counts.kept += 1;
              tableRows.push({
                name: employee.name,
                linkedinUrl: employee.linkedinUrl,
                openToWork: "false",
                status: "KEPT",
              });
            }
          } else {
            kept.push(employee);
            counts.errors += 1;
            tableRows.push({
              name: employee.name,
              linkedinUrl: employee.linkedinUrl,
              openToWork: "ERROR",
              status: "KEPT (profile not in response)",
            });
          }
        }
      } catch (error) {
        const errorMsg = error instanceof Error ? error.message : "Unknown error";
        for (const { employee } of batch) {
          kept.push(employee);
          counts.errors += 1;
          tableRows.push({
            name: employee.name,
            linkedinUrl: employee.linkedinUrl,
            openToWork: "ERROR",
            status: `KEPT (${errorMsg})`,
          });
        }
      }
    }
  }

  clearTimeout(overallTimeout);

  printApifyTable(
    context.companyName,
    context.companyDomain,
    tableRows,
    counts,
    withUrl.length,
    counts.cached,
    apiCallCount
  );

  const warnings: string[] = [];
  if (withoutUrl.length > 0) {
    const names = withoutUrl.map((e) => e.name).join(", ");
    warnings.push(
      `${context.companyName}: ${withoutUrl.length} candidate(s) skipped from Apify openToWork check — no LinkedIn URL (${names})`
    );
  }

  return { kept, warnings, filteredOut };
}

function extractDomainBase(domain: string): string {
  if (!domain) return "";
  return domain.split(".")[0].toLowerCase().replace(/[-_]/g, "");
}

function normalizeCompanyName(name: string): string {
  if (!name) return "";
  return name
    .replace(COMPANY_SUFFIXES_REGEX, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function matchesCompany(
  entry: ApifyExperienceEntry,
  companyDomain: string,
  companyName: string
): boolean {
  const domainBase = extractDomainBase(companyDomain);
  if (domainBase && entry.companyUniversalName) {
    const slug = entry.companyUniversalName.toLowerCase().replace(/[-_]/g, "");
    if (slug === domainBase) {
      return true;
    }
  }

  const normalizedTarget = normalizeCompanyName(companyName);
  const normalizedEntry = normalizeCompanyName(entry.companyName ?? "");
  if (!normalizedTarget || !normalizedEntry) return false;
  if (normalizedTarget === normalizedEntry) return true;

  const shorter = normalizedTarget.length <= normalizedEntry.length ? normalizedTarget : normalizedEntry;
  const longer = normalizedTarget.length <= normalizedEntry.length ? normalizedEntry : normalizedTarget;
  return shorter.length >= 3 && longer.includes(shorter);
}

function isCurrentRole(entry: ApifyExperienceEntry): boolean {
  if (!entry.endDate) return true;
  const text = entry.endDate.text?.toLowerCase().trim() ?? "";
  return text === "present" || text === "";
}

function findCompanyExperienceMatch(
  experience: ApifyExperienceEntry[],
  companyDomain: string,
  companyName: string
): { currentMatchedEntry: ApifyExperienceEntry | null; fallbackMatchedEntry: ApifyExperienceEntry | null } {
  for (const entry of experience) {
    if (isCurrentRole(entry) && matchesCompany(entry, companyDomain, companyName)) {
      return { currentMatchedEntry: entry, fallbackMatchedEntry: entry };
    }
  }
  for (const entry of experience) {
    if (matchesCompany(entry, companyDomain, companyName)) {
      return { currentMatchedEntry: null, fallbackMatchedEntry: entry };
    }
  }
  return { currentMatchedEntry: null, fallbackMatchedEntry: null };
}

function isContractEmploymentType(employmentType: string | undefined): boolean {
  const normalized = (employmentType ?? "").trim().toLowerCase();
  return normalized === "contract"
    || normalized === "contractor"
    || normalized === "freelance"
    || normalized === "freelancer";
}

function shouldRejectForContractEmployment(
  experience: ApifyExperienceEntry[],
  companyDomain: string,
  companyName: string
): boolean {
  const { currentMatchedEntry, fallbackMatchedEntry } = findCompanyExperienceMatch(
    experience,
    companyDomain,
    companyName
  );
  const matchedEntry = currentMatchedEntry ?? fallbackMatchedEntry;
  if (!matchedEntry) {
    return false;
  }
  return isContractEmploymentType(matchedEntry.employmentType);
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
  company: { companyName: string; companyDomain: string }
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

    const { currentMatchedEntry, fallbackMatchedEntry } = findCompanyExperienceMatch(
      cached.experience,
      company.companyDomain,
      company.companyName
    );
    const matchedEntry = currentMatchedEntry ?? fallbackMatchedEntry ?? null;

    if (!matchedEntry) {
      kept.push(emp);
      warningCandidates.push({
        employee: emp,
        reason: "company_not_matched",
        problem: `Could not match this profile to ${company.companyName} in Apify experience data.`,
      });
      rows.push({ name: emp.name, companyMatch: "no match", result: "KEPT (no company match)" });
      continue;
    }

    if (!currentMatchedEntry) {
      const desc = matchedEntry.description ?? "";
      if (FRONTEND_REGEX.test(desc) && !FRONTEND_OVERRIDE_REGEX.test(desc)) {
        rejectedFrontend.push(emp);
        rows.push({
          name: emp.name,
          companyMatch: matchedEntry.companyName ?? "—",
          result: "REJECTED (frontend, past role)",
        });
        continue;
      }

      kept.push(emp);
      warningCandidates.push({
        employee: emp,
        reason: "company_not_current_role",
        problem: `Found ${company.companyName} in history, but not as current/present role in Apify data.`,
      });
      rows.push({
        name: emp.name,
        companyMatch: matchedEntry.companyName ?? "—",
        result: "KEPT (not current role)",
      });
      continue;
    }

    const desc = matchedEntry.description ?? "";
    if (FRONTEND_REGEX.test(desc) && !FRONTEND_OVERRIDE_REGEX.test(desc)) {
      rejectedFrontend.push(emp);
      rows.push({ name: emp.name, companyMatch: matchedEntry.companyName ?? "—", result: "REJECTED (frontend)" });
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
