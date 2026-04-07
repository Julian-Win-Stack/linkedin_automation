import { getRequiredEnv } from "../config/env";
import {
  ApifyCacheEntry,
  ApifyExperienceEntry,
  ApifyOpenToWorkCache,
  ApifyProfileSkill,
  EnrichedEmployee,
} from "../types/prospect";

const APIFY_BASE_URL = "https://api.apify.com/v2";
const APIFY_ACTOR_ID = "harvestapi~linkedin-company-employees";
const RUN_TIMEOUT_SECONDS = 180;
const MAX_RETRIES = 2;
const RETRY_DELAYS_MS = [3000, 6000];

const JOB_TITLES = [
  "SRE",
  "Site Reliability",
  "Infrastructure",
  "DevOps",
  "Staff Engineer",
  "Principal engineer",
  "Lead Software Engineer",
  "Tech Lead",
  "Platform Engineer",
];

const EXCLUDE_SENIORITY_LEVEL_IDS = ["100", "110", "310", "320"];
const EXCLUDE_FUNCTION_IDS = [
  "1",
  "2",
  "3",
  "4",
  "5",
  "6",
  "7",
  "9",
  "10",
  "11",
  "12",
  "13",
  "14",
  "15",
  "16",
  "17",
  "18",
  "19",
  "20",
  "21",
  "22",
  "23",
  "24",
  "25",
  "26",
];

const MONTH_TO_INDEX: Record<string, number> = {
  jan: 0,
  feb: 1,
  mar: 2,
  apr: 3,
  may: 4,
  jun: 5,
  jul: 6,
  aug: 7,
  sep: 8,
  oct: 9,
  nov: 10,
  dec: 11,
};

interface ApifyDateObject {
  month?: string;
  year?: number;
  text?: string;
}

interface CompanyEmployeesProfile {
  id?: string;
  publicIdentifier?: string;
  linkedinUrl?: string;
  firstName?: string;
  lastName?: string;
  headline?: string;
  openToWork?: boolean;
  skills?: Array<{ name?: string }>;
  experience?: Array<{
    companyName?: string;
    companyUniversalName?: string;
    companyLinkedinUrl?: string;
    description?: string;
    employmentType?: string;
    position?: string;
    endDate?: ApifyDateObject | null;
    startDate?: ApifyDateObject | null;
    skills?: string[];
  }>;
}

interface CompanyEmployeesInput {
  companyName: string;
  companyDomain: string;
  companyLinkedinUrl?: string;
  maxItemsPerCompany?: number;
}

interface CompanyEmployeesResult {
  employees: EnrichedEmployee[];
  apifyCache: ApifyOpenToWorkCache;
  profileCount: number;
}

interface MonthRange {
  start: number;
  end: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeLinkedinUrl(url: string): string {
  return url
    .replace(/^https?:\/\//i, "")
    .replace(/^www\./i, "")
    .replace(/\/+$/, "")
    .toLowerCase();
}

function normalizeCompanyName(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function normalizeLinkedinCompanyInput(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return trimmed;
  }
  const withoutProtocol = trimmed.replace(/^https?:\/\//i, "");
  return `https://${withoutProtocol.replace(/\/+$/, "")}`;
}

function isPresentDate(raw?: ApifyDateObject | null): boolean {
  const text = raw?.text?.trim().toLowerCase() ?? "";
  return text === "" || text === "present";
}

function toDate(raw?: ApifyDateObject | null): Date | null {
  if (!raw) {
    return null;
  }
  const year = typeof raw.year === "number" ? raw.year : null;
  if (!year) {
    return null;
  }
  const monthText = raw.month?.trim().slice(0, 3).toLowerCase() ?? "";
  const month = MONTH_TO_INDEX[monthText] ?? 0;
  return new Date(Date.UTC(year, month, 1));
}

function toMonthIndex(date: Date): number {
  return date.getUTCFullYear() * 12 + date.getUTCMonth();
}

function toMonthRange(
  exp: ApifyExperienceEntry & { startDate?: ApifyDateObject | null },
  now: Date
): MonthRange | null {
  const start = toDate(exp.startDate);
  if (!start) {
    return null;
  }
  const end = isPresentDate(exp.endDate as ApifyDateObject | null)
    ? now
    : toDate(exp.endDate as ApifyDateObject | null) ?? now;
  const startIndex = toMonthIndex(start);
  const endIndex = toMonthIndex(end);
  return { start: startIndex, end: endIndex >= startIndex ? endIndex : startIndex };
}

function mergeRanges(ranges: MonthRange[]): MonthRange[] {
  if (ranges.length === 0) {
    return [];
  }
  const sorted = [...ranges].sort((a, b) => a.start - b.start);
  const merged: MonthRange[] = [sorted[0]];
  for (let index = 1; index < sorted.length; index += 1) {
    const current = sorted[index];
    const previous = merged[merged.length - 1];
    if (current.start <= previous.end) {
      previous.end = Math.max(previous.end, current.end);
      continue;
    }
    merged.push(current);
  }
  return merged;
}

function isCurrentRole(exp: ApifyExperienceEntry): boolean {
  if (!exp.endDate) {
    return true;
  }
  const text = exp.endDate.text?.trim().toLowerCase() ?? "";
  return text === "" || text === "present";
}

function findCurrentRole(experience: ApifyExperienceEntry[]): ApifyExperienceEntry | null {
  if (experience.length === 0) {
    return null;
  }
  return experience.find((entry) => isCurrentRole(entry)) ?? experience[0];
}

function companyMatchesTarget(
  exp: ApifyExperienceEntry,
  targetCompanyName: string,
  targetCompanyLinkedinUrl?: string
): boolean {
  const targetName = normalizeCompanyName(targetCompanyName);
  const expName = normalizeCompanyName(exp.companyName ?? "");
  if (expName && expName === targetName) {
    return true;
  }
  if (!targetCompanyLinkedinUrl || !exp.companyLinkedinUrl) {
    return false;
  }
  return normalizeLinkedinUrl(exp.companyLinkedinUrl) === normalizeLinkedinUrl(targetCompanyLinkedinUrl);
}

export function computeTenureFromExperience(
  experience: Array<ApifyExperienceEntry & { startDate?: ApifyDateObject | null }>,
  targetCompanyName: string,
  targetCompanyLinkedinUrl?: string,
  now = new Date()
): number | null {
  const matching = experience.filter((exp) =>
    companyMatchesTarget(exp, targetCompanyName, targetCompanyLinkedinUrl)
  );
  if (matching.length === 0) {
    return null;
  }
  const ranges = matching
    .map((exp) => toMonthRange(exp, now))
    .filter((range): range is MonthRange => range !== null);
  if (ranges.length === 0) {
    return null;
  }
  const merged = mergeRanges(ranges);
  const totalMonths = merged.reduce((sum, range) => sum + (range.end - range.start), 0);
  return totalMonths >= 0 ? totalMonths : 0;
}

function mapExperience(profile: CompanyEmployeesProfile): Array<ApifyExperienceEntry & { startDate?: ApifyDateObject | null }> {
  const experience = Array.isArray(profile.experience) ? profile.experience : [];
  return experience.map((entry) => ({
    companyName: entry.companyName,
    companyUniversalName: entry.companyUniversalName,
    companyLinkedinUrl: entry.companyLinkedinUrl,
    description: entry.description,
    employmentType: entry.employmentType,
    position: entry.position,
    endDate: entry.endDate ?? undefined,
    startDate: entry.startDate ?? undefined,
    skills: Array.isArray(entry.skills) ? entry.skills : [],
  }));
}

export function mapProfileToEnrichedEmployee(
  profile: CompanyEmployeesProfile,
  input: CompanyEmployeesInput
): EnrichedEmployee | null {
  const linkedinUrl = profile.linkedinUrl?.trim() ?? "";
  if (!linkedinUrl) {
    return null;
  }

  const fullName = [profile.firstName?.trim(), profile.lastName?.trim()].filter(Boolean).join(" ").trim();
  if (!fullName) {
    return null;
  }

  const experience = mapExperience(profile);
  const currentRole = findCurrentRole(experience);
  const currentTitle = currentRole?.position?.trim() ?? "";
  const headline = profile.headline?.trim() ?? "";
  const startDateObj = currentRole?.startDate;
  const parsedStartDate = toDate(startDateObj);
  const startDate = parsedStartDate ? parsedStartDate.toISOString().slice(0, 10) : null;
  const tenure = computeTenureFromExperience(experience, input.companyName, input.companyLinkedinUrl);

  return {
    id: profile.id ?? profile.publicIdentifier ?? linkedinUrl,
    startDate,
    endDate: null,
    name: fullName,
    email: null,
    linkedinUrl,
    currentTitle,
    headline,
    tenure,
  };
}

function toProfileSkills(profile: CompanyEmployeesProfile): ApifyProfileSkill[] {
  const skills = Array.isArray(profile.skills) ? profile.skills : [];
  return skills
    .filter((item): item is { name: string } => typeof item?.name === "string" && item.name.trim().length > 0)
    .map((item) => ({ name: item.name.trim() }));
}

function populateApifyCache(profile: CompanyEmployeesProfile, cache: ApifyOpenToWorkCache): void {
  const linkedinUrl = profile.linkedinUrl?.trim() ?? "";
  if (!linkedinUrl) {
    return;
  }
  const normalized = normalizeLinkedinUrl(linkedinUrl);
  const experience = mapExperience(profile).map((entry) => ({
    companyName: entry.companyName,
    companyUniversalName: entry.companyUniversalName,
    companyLinkedinUrl: entry.companyLinkedinUrl,
    description: entry.description,
    employmentType: entry.employmentType,
    position: entry.position,
    endDate: entry.endDate ?? undefined,
    skills: entry.skills ?? [],
  }));
  const cacheEntry: ApifyCacheEntry = {
    openToWork: profile.openToWork === true,
    experience,
    profileSkills: toProfileSkills(profile),
    canonicalLinkedinUrl: linkedinUrl,
  };
  cache.set(normalized, cacheEntry);
}

function buildCompaniesList(input: CompanyEmployeesInput): string[] {
  const companies: string[] = [];
  const linkedin = input.companyLinkedinUrl?.trim();
  if (linkedin) {
    companies.push(normalizeLinkedinCompanyInput(linkedin));
  } else {
    companies.push(input.companyName.trim());
  }
  return companies;
}

async function runCompanyEmployeesActor(
  payload: Record<string, unknown>,
  apiKey: string
): Promise<CompanyEmployeesProfile[]> {
  const endpoint =
    `${APIFY_BASE_URL}/acts/${APIFY_ACTOR_ID}/run-sync-get-dataset-items` +
    `?token=${apiKey}&timeout=${RUN_TIMEOUT_SECONDS}`;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      const response = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        throw new Error(`Apify company employees actor returned HTTP ${response.status}`);
      }
      const data = (await response.json()) as unknown;
      if (!Array.isArray(data)) {
        throw new Error("Invalid Apify company employees response");
      }
      return data as CompanyEmployeesProfile[];
    } catch (error) {
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAYS_MS[attempt]);
        continue;
      }
      throw error;
    }
  }
  return [];
}

async function callCompanyEmployeesActor(
  input: CompanyEmployeesInput,
  apiKey: string
): Promise<CompanyEmployeesProfile[]> {
  const companies = buildCompaniesList(input);
  return runCompanyEmployeesActor({
    profileScraperMode: "Full ($8 per 1k)",
    companyBatchMode: "all_at_once",
    maxItems: input.maxItemsPerCompany ?? 30,
    companies,
    jobTitles: JOB_TITLES,
    pastJobTitles: ["SRE", "Site Reliability"],
    functionIds: ["8"],
    excludeSeniorityLevelIds: EXCLUDE_SENIORITY_LEVEL_IDS,
    excludeFunctionIds: EXCLUDE_FUNCTION_IDS,
  }, apiKey);
}


function mapProfilesToCompanyEmployees(
  profiles: CompanyEmployeesProfile[],
  input: CompanyEmployeesInput
): CompanyEmployeesResult {
  const employees: EnrichedEmployee[] = [];
  const apifyCache: ApifyOpenToWorkCache = new Map();
  const seen = new Set<string>();

  for (const profile of profiles) {
    const employee = mapProfileToEnrichedEmployee(profile, input);
    if (!employee) {
      continue;
    }
    const dedupeKey = employee.id ?? `${employee.name}|${employee.linkedinUrl ?? ""}`;
    if (seen.has(dedupeKey)) {
      continue;
    }
    seen.add(dedupeKey);
    employees.push(employee);
    populateApifyCache(profile, apifyCache);
  }

  return {
    employees,
    apifyCache,
    profileCount: profiles.length,
  };
}

export async function scrapeCompanyEmployees(input: CompanyEmployeesInput): Promise<CompanyEmployeesResult> {
  const apiKey = getRequiredEnv("APIFY_API_KEY");
  const profiles = await callCompanyEmployeesActor(input, apiKey);
  return mapProfilesToCompanyEmployees(profiles, input);
}

export function filterByPastExperienceKeywords(
  pool: EnrichedEmployee[],
  cache: ApifyOpenToWorkCache,
  keywords: string[]
): EnrichedEmployee[] {
  return pool.filter((employee) => hasPastTitleInCache(employee, cache, keywords));
}

function containsAnyKeyword(text: string, keywords: string[]): boolean {
  const lowered = text.toLowerCase();
  return keywords.some((keyword) => lowered.includes(keyword.toLowerCase()));
}

function hasPastTitleInCache(
  employee: EnrichedEmployee,
  cache: ApifyOpenToWorkCache,
  titleKeywords: string[]
): boolean {
  const linkedinUrl = employee.linkedinUrl?.trim() ?? "";
  if (!linkedinUrl) {
    return false;
  }
  const key = normalizeLinkedinUrl(linkedinUrl);
  const cached = cache.get(key);
  if (!cached) {
    return false;
  }
  return cached.experience.some((entry) => {
    const position = entry.position?.trim() ?? "";
    if (!position) {
      return false;
    }
    return containsAnyKeyword(position, titleKeywords);
  });
}

export function filterPoolByStage(
  pool: EnrichedEmployee[],
  cache: ApifyOpenToWorkCache,
  stage: {
    currentTitles?: string[];
    pastTitles?: string[];
    notTitles?: string[];
    notPastTitles?: string[];
  }
): EnrichedEmployee[] {
  return pool.filter((employee) => {
    const title = employee.currentTitle.toLowerCase();
    const hasCurrentFilters = (stage.currentTitles?.length ?? 0) > 0;
    const hasPastFilters = (stage.pastTitles?.length ?? 0) > 0;

    const currentMatch = hasCurrentFilters
      ? stage.currentTitles!.some((keyword) => title.includes(keyword.toLowerCase()))
      : false;
    const pastMatch = hasPastFilters
      ? hasPastTitleInCache(employee, cache, stage.pastTitles ?? [])
      : false;

    const include = (hasCurrentFilters || hasPastFilters) ? (currentMatch || pastMatch) : true;
    if (!include) {
      return false;
    }

    const excludedByCurrent = (stage.notTitles ?? []).some((keyword) => title.includes(keyword.toLowerCase()));
    if (excludedByCurrent) {
      return false;
    }

    const excludedByPast = (stage.notPastTitles?.length ?? 0) > 0
      ? hasPastTitleInCache(employee, cache, stage.notPastTitles ?? [])
      : false;

    return !excludedByPast;
  });
}
