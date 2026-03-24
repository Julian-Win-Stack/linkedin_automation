import { EnrichedEmployee } from "../types/prospect";

const SRE_VARIANT_REGEX = /\b(sre|site reliability|site-reliability|site reliability engineering)\b/i;
const HEAD_OF_RELIABILITY_REGEX = /\bhead of reliability\b/i;
const TIER_1_REGEX = /\b(head|director)\b/i;
const TIER_2_REGEX = /\b(manager|staff)\b/i;
const TIER_3_REGEX = /\b(senior|sr\.?)\b/i;
const TIER_4_MIN_TENURE_MONTHS = 2;
const PAST_SRE_MIN_TENURE_MONTHS = 2;
const PLATFORM_TITLE_REGEX = /\bplatform engineer\b/i;
const PLATFORM_SENIOR_REGEX = /\b(senior|staff|principal|lead|manager|head|director)\b/i;
const PLATFORM_NON_SENIOR_MIN_TENURE_MONTHS = 11;

type SreTier = 1 | 2 | 3 | 4;

function isSreTitle(title: string): boolean {
  return SRE_VARIANT_REGEX.test(title);
}

function toEmployeeKey(employee: EnrichedEmployee): string {
  return employee.id ?? `${employee.name}|${employee.currentTitle}|${employee.linkedinUrl ?? ""}`;
}

function dedupeEmployees(employees: EnrichedEmployee[]): EnrichedEmployee[] {
  const seen = new Set<string>();
  const unique: EnrichedEmployee[] = [];

  for (const employee of employees) {
    const key = toEmployeeKey(employee);
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    unique.push(employee);
  }

  return unique;
}

function classifySreTier(employee: EnrichedEmployee): SreTier | null {
  const title = employee.currentTitle?.trim() ?? "";
  if (!title) {
    return null;
  }

  if (HEAD_OF_RELIABILITY_REGEX.test(title)) {
    return 1;
  }

  if (!isSreTitle(title)) {
    return null;
  }

  if (TIER_1_REGEX.test(title)) {
    return 1;
  }
  if (TIER_2_REGEX.test(title)) {
    return 2;
  }
  if (TIER_3_REGEX.test(title)) {
    return 3;
  }
  return 4;
}

function compareByTrimPriority(a: EnrichedEmployee, b: EnrichedEmployee): number {
  const aHasStartDate = a.startDate != null;
  const bHasStartDate = b.startDate != null;
  if (aHasStartDate !== bHasStartDate) {
    return aHasStartDate ? -1 : 1;
  }

  const aTenure = a.tenure ?? -1;
  const bTenure = b.tenure ?? -1;
  if (aTenure !== bTenure) {
    return bTenure - aTenure;
  }

  const aName = a.name.toLowerCase();
  const bName = b.name.toLowerCase();
  if (aName !== bName) {
    return aName.localeCompare(bName);
  }

  const aLinkedin = a.linkedinUrl ?? "";
  const bLinkedin = b.linkedinUrl ?? "";
  return aLinkedin.localeCompare(bLinkedin);
}

export function selectTopSreForLemlist(employees: EnrichedEmployee[], limit = 7): EnrichedEmployee[] {
  const uniqueEmployees = dedupeEmployees(employees);
  const tierBuckets: Record<SreTier, EnrichedEmployee[]> = {
    1: [],
    2: [],
    3: [],
    4: [],
  };

  for (const employee of uniqueEmployees) {
    const tier = classifySreTier(employee);
    if (!tier) {
      continue;
    }

    if (tier === 4 && employee.tenure !== null && employee.tenure < TIER_4_MIN_TENURE_MONTHS) {
      continue;
    }

    tierBuckets[tier].push(employee);
  }

  for (const tier of [1, 2, 3, 4] as const) {
    tierBuckets[tier].sort(compareByTrimPriority);
  }

  const selected: EnrichedEmployee[] = [];
  for (const tier of [1, 2, 3, 4] as const) {
    if (selected.length >= limit) {
      break;
    }

    const remaining = limit - selected.length;
    selected.push(...tierBuckets[tier].slice(0, remaining));
  }

  return selected;
}

export function isSeniorPlatformEngineer(title: string): boolean {
  return PLATFORM_SENIOR_REGEX.test(title);
}

function comparePlatformBackfillPriority(a: EnrichedEmployee, b: EnrichedEmployee): number {
  const aSenior = isSeniorPlatformEngineer(a.currentTitle);
  const bSenior = isSeniorPlatformEngineer(b.currentTitle);

  if (aSenior !== bSenior) {
    return aSenior ? -1 : 1;
  }

  return compareByTrimPriority(a, b);
}

function selectPastSreBackfillCandidates(
  selected: EnrichedEmployee[],
  candidates: EnrichedEmployee[],
  minimum: number
): EnrichedEmployee[] {
  const selectedKeys = new Set(selected.map((employee) => toEmployeeKey(employee)));
  const eligible = dedupeEmployees(candidates)
    .filter((employee) => !selectedKeys.has(toEmployeeKey(employee)))
    .filter((employee) => {
      if (employee.startDate === null) {
        return true;
      }

      return employee.tenure !== null && employee.tenure >= PAST_SRE_MIN_TENURE_MONTHS;
    })
    .sort(compareByTrimPriority);

  const needed = Math.max(0, minimum - selected.length);
  return eligible.slice(0, needed);
}

function selectPlatformBackfillCandidates(
  selected: EnrichedEmployee[],
  candidates: EnrichedEmployee[],
  minimum: number
): EnrichedEmployee[] {
  const selectedKeys = new Set(selected.map((employee) => toEmployeeKey(employee)));
  const eligible = dedupeEmployees(candidates)
    .filter((employee) => !selectedKeys.has(toEmployeeKey(employee)))
    .filter((employee) => PLATFORM_TITLE_REGEX.test(employee.currentTitle))
    .filter((employee) => {
      if (isSeniorPlatformEngineer(employee.currentTitle)) {
        return true;
      }

      return employee.tenure !== null && employee.tenure >= PLATFORM_NON_SENIOR_MIN_TENURE_MONTHS;
    })
    .sort(comparePlatformBackfillPriority);

  const needed = Math.max(0, minimum - selected.length);
  return eligible.slice(0, needed);
}

interface BackfillOptions {
  minimum?: number;
  max?: number;
}

export function fillToMinimumWithBackfill(
  selectedCurrentSre: EnrichedEmployee[],
  pastSreCandidates: EnrichedEmployee[],
  platformCandidates: EnrichedEmployee[],
  options: BackfillOptions = {}
): EnrichedEmployee[] {
  const minimum = options.minimum ?? 5;
  const max = options.max ?? 7;

  let selected = dedupeEmployees(selectedCurrentSre).slice(0, max);

  if (selected.length === 0) {
    return [];
  }

  if (selected.length >= minimum) {
    return selected;
  }

  const pastSreAdds = selectPastSreBackfillCandidates(selected, pastSreCandidates, minimum);
  selected = dedupeEmployees([...selected, ...pastSreAdds]).slice(0, max);

  if (selected.length >= minimum) {
    return selected;
  }

  const platformAdds = selectPlatformBackfillCandidates(selected, platformCandidates, minimum);
  selected = dedupeEmployees([...selected, ...platformAdds]).slice(0, max);

  return selected;
}
