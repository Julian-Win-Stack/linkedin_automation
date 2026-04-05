import { attioGet, attioPut } from "./attioClient";
import { OutputRow } from "./observability/csvWriter";

const ATTIO_OBJECT_SLUG = "companies";
const ATTIO_MATCHING_ATTRIBUTE = "domains";
const ATTIO_CONCURRENCY = 10;
const ATTIO_API_RETRIES = 1;
const ATTIO_PAGE_SIZE = 200;
const ATTIO_ERROR_COLOR = "\x1b[31m";
const ANSI_RESET = "\x1b[0m";

interface AttioAttribute {
  api_slug?: string | null;
}

interface AttioListAttributesResponse {
  data?: AttioAttribute[];
}

type AttioRecordValue = string | number | boolean | string[] | number[];

interface AttioAssertTask {
  domain: string;
  companyName: string;
  values: Record<string, AttioRecordValue>;
}

interface BuildAssertTasksResult {
  tasks: AttioAssertTask[];
  skippedMissingDomainCount: number;
  skippedNoMappableFieldsCount: number;
  duplicateDomainCount: number;
  unmappedApiSlugs: string[];
}

export interface AttioAssertSyncResult {
  attemptedRows: number;
  dedupedDomains: number;
  assertedCount: number;
  failedCount: number;
  skippedMissingDomainCount: number;
  skippedNoMappableFieldsCount: number;
  duplicateDomainCount: number;
  warnings: string[];
}

const COLUMN_TO_ATTIO_SLUG_CANDIDATES: Record<keyof OutputRow, string[]> = {
  company_name: ["name", "company_name"],
  company_domain: ["domains", "domain", "website", "company_domain"],
  company_linkedin_url: ["company_linkedin_url", "linkedin_url", "linkedin", "company_linkedin"],
  apollo_account_id: [],
  observability_tool_research: ["observability_tool_research", "observability_tool"],
  stage: ["stage"],
  sre_count: ["sre_count"],
  notes: ["notes"],
};

function toDisplayValue(value: unknown): string | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  const text = String(value).trim();
  return text.length > 0 ? text : undefined;
}

function normalizeMappingToken(value: string): string {
  return value.toLowerCase().replace(/[_\s]+/g, "");
}

function buildNormalizedSlugMap(availableSlugs: Set<string>): Map<string, string> {
  const normalizedToSlug = new Map<string, string>();
  for (const slug of availableSlugs) {
    const normalizedSlug = normalizeMappingToken(slug);
    if (normalizedSlug.length === 0) {
      continue;
    }
    if (!normalizedToSlug.has(normalizedSlug)) {
      normalizedToSlug.set(normalizedSlug, slug);
    }
  }
  return normalizedToSlug;
}

function isDomainsSlug(slug: string): boolean {
  return normalizeMappingToken(slug) === normalizeMappingToken("domains");
}

function normalizeDomain(rawValue: unknown): string | undefined {
  const text = toDisplayValue(rawValue);
  if (!text) {
    return undefined;
  }

  const withProtocol = text.includes("://") ? text : `https://${text}`;
  try {
    const parsed = new URL(withProtocol);
    const host = parsed.hostname.trim().toLowerCase().replace(/^www\./, "");
    return host.length > 0 ? host : undefined;
  } catch {
    const host = text.toLowerCase().trim().replace(/^https?:\/\//, "").replace(/^www\./, "").split("/")[0];
    return host.length > 0 ? host : undefined;
  }
}

async function fetchCompanyAttributeSlugs(): Promise<Set<string>> {
  const slugs = new Set<string>();
  let offset = 0;

  while (true) {
    const response = await attioGet<AttioListAttributesResponse>(
      `/v2/objects/${ATTIO_OBJECT_SLUG}/attributes?limit=${ATTIO_PAGE_SIZE}&offset=${offset}`,
      ATTIO_API_RETRIES
    );
    const attributes = Array.isArray(response.data) ? response.data : [];
    for (const attribute of attributes) {
      const slug = typeof attribute.api_slug === "string" ? attribute.api_slug.trim() : "";
      if (slug) {
        slugs.add(slug);
      }
    }

    if (attributes.length < ATTIO_PAGE_SIZE) {
      break;
    }
    offset += ATTIO_PAGE_SIZE;
  }

  return slugs;
}

function buildAssertTasks(rows: OutputRow[], availableSlugs: Set<string>): BuildAssertTasksResult {
  const unmappedApiSlugs = new Set<string>();
  const domainToTask = new Map<string, AttioAssertTask>();
  const orderedDomains: string[] = [];
  const duplicateDomains = new Set<string>();
  const normalizedSlugMap = buildNormalizedSlugMap(availableSlugs);
  let skippedMissingDomainCount = 0;
  let skippedNoMappableFieldsCount = 0;

  for (const row of rows) {
    const normalizedDomain = normalizeDomain(row.company_domain);
    if (!normalizedDomain) {
      skippedMissingDomainCount += 1;
      continue;
    }

    const values: Record<string, AttioRecordValue> = {};
    const companyName = toDisplayValue(row.company_name) ?? normalizedDomain;

    for (const [columnKey, rawValue] of Object.entries(row) as Array<[keyof OutputRow, unknown]>) {
      if (columnKey === "apollo_account_id") {
        continue;
      }

      const displayValue = toDisplayValue(rawValue);
      if (!displayValue) {
        continue;
      }

      const slugCandidates = COLUMN_TO_ATTIO_SLUG_CANDIDATES[columnKey];
      if (!slugCandidates || slugCandidates.length === 0) {
        continue;
      }

      const matchedSlug = slugCandidates
        .map((slug) => normalizedSlugMap.get(normalizeMappingToken(slug)))
        .find((slug): slug is string => Boolean(slug));
      if (!matchedSlug) {
        unmappedApiSlugs.add(slugCandidates[0]);
        continue;
      }

      if (isDomainsSlug(matchedSlug)) {
        values[matchedSlug] = [normalizedDomain];
      } else if (columnKey === "sre_count") {
        const numericValue = Number(displayValue);
        values[matchedSlug] = Number.isFinite(numericValue) ? numericValue : displayValue;
      } else {
        values[matchedSlug] = displayValue;
      }
    }

    values[ATTIO_MATCHING_ATTRIBUTE] = [normalizedDomain];

    if (Object.keys(values).length === 0) {
      skippedNoMappableFieldsCount += 1;
      continue;
    }

    if (!domainToTask.has(normalizedDomain)) {
      orderedDomains.push(normalizedDomain);
    } else {
      duplicateDomains.add(normalizedDomain);
    }

    domainToTask.set(normalizedDomain, {
      domain: normalizedDomain,
      companyName,
      values,
    });
  }

  const tasks = orderedDomains
    .map((domain) => domainToTask.get(domain))
    .filter((task): task is AttioAssertTask => Boolean(task));

  return {
    tasks,
    skippedMissingDomainCount,
    skippedNoMappableFieldsCount,
    duplicateDomainCount: duplicateDomains.size,
    unmappedApiSlugs: [...unmappedApiSlugs].sort((a, b) => a.localeCompare(b)),
  };
}

async function assertSingleCompany(task: AttioAssertTask): Promise<void> {
  const path =
    `/v2/objects/${ATTIO_OBJECT_SLUG}/records` +
    `?matching_attribute=${encodeURIComponent(ATTIO_MATCHING_ATTRIBUTE)}`;
  await attioPut(path, { data: { values: task.values } }, ATTIO_API_RETRIES);
}

async function runWithConcurrency<T>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<void>
): Promise<void> {
  let nextIndex = 0;

  const runWorker = async (): Promise<void> => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= items.length) {
        return;
      }
      await worker(items[index], index);
    }
  };

  const runnerCount = Math.min(concurrency, items.length);
  const runners = Array.from({ length: runnerCount }, () => runWorker());
  await Promise.all(runners);
}

export async function syncAttioCompaniesFromOutputRows(rows: OutputRow[]): Promise<AttioAssertSyncResult> {
  const warnings: string[] = [];
  const availableSlugs = await fetchCompanyAttributeSlugs();
  const buildResult = buildAssertTasks(rows, availableSlugs);

  for (const apiSlug of buildResult.unmappedApiSlugs) {
    console.error(
      `${ATTIO_ERROR_COLOR}[Attio][Assert][ERROR] Unmapped output field slug "${apiSlug}" - skipping values.${ANSI_RESET}`
    );
  }

  if (buildResult.duplicateDomainCount > 0) {
    warnings.push(
      `Attio sync deduped ${buildResult.duplicateDomainCount} duplicate domain record(s); last row values were used.`
    );
  }

  let assertedCount = 0;
  let failedCount = 0;

  await runWithConcurrency(buildResult.tasks, ATTIO_CONCURRENCY, async (task) => {
    try {
      await assertSingleCompany(task);
      assertedCount += 1;
    } catch (error) {
      failedCount += 1;
      const message = error instanceof Error ? error.message : "Unknown Attio assert error";
      warnings.push(`Uploading ${task.companyName} to Attio failed. Please contact Julian`);
      console.error(
        `${ATTIO_ERROR_COLOR}[Attio][Assert][ERROR] company=${task.companyName} domain=${task.domain} message=${message}${ANSI_RESET}`
      );
    }
  });

  return {
    attemptedRows: rows.length,
    dedupedDomains: buildResult.tasks.length,
    assertedCount,
    failedCount,
    skippedMissingDomainCount: buildResult.skippedMissingDomainCount,
    skippedNoMappableFieldsCount: buildResult.skippedNoMappableFieldsCount,
    duplicateDomainCount: buildResult.duplicateDomainCount,
    warnings,
  };
}

export const __testOnly__ = {
  buildAssertTasks,
  normalizeDomain,
};
