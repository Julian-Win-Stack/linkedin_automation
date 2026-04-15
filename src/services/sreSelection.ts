import { EnrichedEmployee, ApifyOpenToWorkCache } from "../types/prospect";
import { filterPoolByStage } from "./apifyCompanyEmployees";
import { filterOpenToWorkFromCache, filterFrontendEngineers, filterOutHardwareHeavyPeople, ApifyFilteredReason } from "./apifyClient";

const SRE_VARIANT_REGEX = /\b(sre|site reliability|site-reliability|site reliability engineering)\b/i;
const TIER_1_HEAD_TITLE_REGEX = /\bhead of (reliability|site reliability|sre)\b/i;
const TIER_1_REGEX = /\b(head|director)\b/i;
const TIER_2_REGEX = /\b(manager|staff)\b/i;
const TIER_3_REGEX = /\b(senior|sr\.?)\b/i;
const TIER_4_MIN_TENURE_MONTHS = 2;
const PAST_SRE_MIN_TENURE_MONTHS = 2;
const PLATFORM_TITLE_REGEX = /\bplatform engineer\b/i;
const PLATFORM_SENIOR_REGEX = /\b(senior|staff|principal|lead|manager|head|director)\b/i;
const PLATFORM_NON_SENIOR_MIN_TENURE_MONTHS = 11;
const QA_TITLE_REJECT_REGEX = /\bQA\b/i;

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

  if (TIER_1_HEAD_TITLE_REGEX.test(title)) {
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

export interface KeywordMatchedSelection {
  forLinkedin: EnrichedEmployee[];
  forEmailRecycling: EnrichedEmployee[];
}

export function selectKeywordMatchedByTenure(
  allKeywordMatched: EnrichedEmployee[],
  alreadySelected: EnrichedEmployee[],
  maxTotal: number
): KeywordMatchedSelection {
  const selectedKeys = new Set(alreadySelected.map((emp) => toEmployeeKey(emp)));
  const eligible = dedupeEmployees(allKeywordMatched).filter(
    (emp) => !selectedKeys.has(toEmployeeKey(emp))
  );

  eligible.sort(compareByTrimPriority);

  const slotsAvailable = Math.max(0, maxTotal - alreadySelected.length);
  const forLinkedin = eligible.slice(0, slotsAvailable);
  const forEmailRecycling = eligible.slice(slotsAvailable);

  return { forLinkedin, forEmailRecycling };
}

// ---------------------------------------------------------------------------
// Backfill stages 4–8: Infrastructure → Platform → DevOps → Normal Engineer → Eng Leader
// These run only when the count after stages 1–3 (SRE / SRE-like / Past SRE) is < 5.
// ---------------------------------------------------------------------------

interface BackfillStageConfig {
  label: string;
  currentTitles?: string[];
  pastTitles?: string[];
  notTitles?: string[];
  notPastTitles?: string[];
  splitLeadership: boolean;
  leadershipTitleKeywords?: string[];
  allEngLead?: boolean;
  isNormalEngineer?: boolean;
}

const BACKFILL_LEADERSHIP_KEYWORDS = ["vp", "manager", "director", "head", "chief", "principal"];

export const BACKFILL_STAGES: BackfillStageConfig[] = [
  {
    label: "Infrastructure",
    currentTitles: ["Infrastructure"],
    notTitles: [
      "data", "corporate", "contract", "contractor", "freelance", "freelancer", "junior", "jr",
      "IT", "helpdesk", "desktop", "end user", "workplace", "internal systems", "business systems",
      "information systems", "security", "infosec", "GRC", "governance", "risk", "compliance",
      "IAM", "identity", "trust & safety", "privacy", "analytics", "BI", "business intelligence",
      "support", "customer support", "technical support", "customer success", "business", "sales",
      "trainee", "solutions engineer", "TAM", "operations", "design", "program manager",
      "project manager", "enterprise", "AI", "machine learning", "ml", "automation", "operation",
      "development", "construction", "sysadmin", "system administrator", "administrator",
      "salesforce", "android", "IOS", "network", "search", "information technology", "solution",
    ],
    splitLeadership: true,
    leadershipTitleKeywords: BACKFILL_LEADERSHIP_KEYWORDS,
  },
  {
    label: "Platform",
    currentTitles: [
      "Platform engineering", "Platform engineer", "Platforms Engineering Manager",
      "Director of Software Engineering, Platform", "Director, Engineering (Platform)",
      "Platform Engineering Manager", "VP of Engineering, Platform", "VP, Engineering - Platform",
      "VP, Product Platform & Engineering", "VP of Developer Platform", "VP of Engineering Systems",
      "Head of Platform", "Head of Developer Platform", "Head of Platform & Reliability",
      "Head of Cloud Platform", "Head of Engineering Productivity / Platform",
      "Chief Platform Officer", "backend platform", "cloud platform", "platform cloud",
    ],
    notTitles: [
      "data", "contract", "contractor", "freelance", "freelancer", "junior", "jr",
      "AI", "artificial intelligence", "machine learning", "ml",
      "frontend", "front-end", "front end", "solution",
    ],
    notPastTitles: [
      "client", "account", "sales", "customer", "insight", "research", "marketing",
      "consultant", "analyst", "partner", "commercial", "AI", "artificial intelligence",
      "machine learning", "ml",
    ],
    splitLeadership: true,
    leadershipTitleKeywords: BACKFILL_LEADERSHIP_KEYWORDS,
  },
  {
    label: "DevOps",
    currentTitles: ["DevOps", "Dev Ops"],
    notTitles: [
      "data", "IT", "corporate", "contract", "contractor", "freelance", "freelancer",
      "junior", "jr", "enterprise", "internal systems", "workplace", "end user", "desktop",
      "helpdesk", "release", "management", "deployment", "analytics", "BI", "security",
      "infosec", "GRC", "compliance", "governance", "IAM", "support", "customer", "business",
      "sales", "trainee", "solutions", "consultant", "professional services", "TAM", "project",
      "program", "scrum", "agile", "solution", "representative", "sysops", "salesforce",
      "android", "IOS",
    ],
    splitLeadership: true,
    leadershipTitleKeywords: BACKFILL_LEADERSHIP_KEYWORDS,
  },
  {
    label: "Normal Engineer",
    currentTitles: [
      "Principal engineer", "Staff engineer", "Tech lead",
      "Lead software engineer", "Technical Lead", "Lead Engineer",
    ],
    pastTitles: ["engineer"],
    notTitles: [
      "ml", "machine learning", "data", "contract", "contractor", "freelance", "freelancer",
      "junior", "jr", "frontend", "front-end", "front end", "salesforce", "android", "IOS",
      "battery", "mobile", "desktop", "test", "AI", "artificial intelligence", "hardware", "solution",
    ],
    splitLeadership: false,
    isNormalEngineer: true,
  },
  {
    label: "Eng Leader",
    currentTitles: [
      "VP of Engineering", "Vice President of Engineering", "VP Engineering",
      "Vice President Engineering", "Head of Engineering", "Director of Engineering",
      "Director, Engineering", "Engineering Director", "Engineering Manager",
      "Senior Engineering Manager", "Manager, Engineering", "Head of Software Engineering",
      "VP of Software Engineering", "Director of Software Engineering",
      "Manager of Software Engineering",
    ],
    pastTitles: ["engineer"],
    notTitles: [
      "IT", "information technology", "corporate", "enterprise systems", "internal systems",
      "workplace", "end user", "helpdesk", "desktop", "industrial", "solutions", "mechanical",
      "electrical", "electronics", "hardware", "firmware", "embedded", "manufacturing",
      "production", "plant", "facilities", "network", "telecom", "NOC", "field engineering",
      "security", "infosec", "cybersecurity", "GRC", "compliance", "trust & safety",
      "data", "analytics", "BI", "business intelligence", "research", "applied science",
      "ml", "machine learning", "program", "project", "TPM", "agile", "scrum",
      "salesforce", "android", "IOS", "AI", "artificial intelligence", "junior", "jr", "solution",
    ],
    splitLeadership: false,
    allEngLead: true,
  },
];

function rankAndSelectBackfillCandidates(
  enriched: EnrichedEmployee[],
  slotsAvailable: number
): EnrichedEmployee[] {
  const qualified: EnrichedEmployee[] = [];
  const nullTenure: EnrichedEmployee[] = [];

  for (const employee of enriched) {
    if (employee.tenure === null) {
      nullTenure.push(employee);
    } else {
      qualified.push(employee);
    }
  }

  qualified.sort((a, b) => (b.tenure ?? -1) - (a.tenure ?? -1));
  nullTenure.sort((a, b) => a.name.localeCompare(b.name));

  const selected: EnrichedEmployee[] = [];
  for (const emp of qualified) {
    if (selected.length >= slotsAvailable) break;
    selected.push(emp);
  }
  for (const emp of nullTenure) {
    if (selected.length >= slotsAvailable) break;
    selected.push(emp);
  }
  return selected;
}

function isBackfillLeadershipTitle(title: string, keywords: string[]): boolean {
  const normalized = title.toLowerCase();
  return keywords.some((kw) => normalized.includes(kw));
}

function partitionBackfillLeadership(
  candidates: EnrichedEmployee[],
  keywords: string[]
): { icCandidates: EnrichedEmployee[]; leadershipCandidates: EnrichedEmployee[] } {
  const icCandidates: EnrichedEmployee[] = [];
  const leadershipCandidates: EnrichedEmployee[] = [];
  for (const emp of candidates) {
    if (isBackfillLeadershipTitle(emp.currentTitle, keywords)) {
      leadershipCandidates.push(emp);
    } else {
      icCandidates.push(emp);
    }
  }
  return { icCandidates, leadershipCandidates };
}

export interface BackfillStageCandidate {
  employee: EnrichedEmployee;
  linkedinBucket: "eng" | "engLead";
}

export interface BackfillStagesResult {
  candidates: BackfillStageCandidate[];
  filteredOutReasons: Array<ApifyFilteredReason | "frontend_role" | "hardware_heavy" | "qa_title">;
  warnings: string[];
  normalEngineerApifyWarnings: Array<{ employee: EnrichedEmployee; problem: string }>;
}

/**
 * Runs LinkedIn backfill stages 4–8 (Infrastructure → Platform → DevOps → Normal Engineer → Eng Leader).
 * Only called when the count from stages 1–3 (SRE / SRE-like / Past SRE) is < 5.
 * Fills up to maxTotal (default 5) total candidates.
 */
export function runBackfillStages(
  alreadySelected: EnrichedEmployee[],
  profilePool: EnrichedEmployee[],
  apifyCache: ApifyOpenToWorkCache,
  companyInfo: { companyName: string; companyDomain: string },
  maxTotal = 5
): BackfillStagesResult {
  const result: BackfillStagesResult = {
    candidates: [],
    filteredOutReasons: [],
    warnings: [],
    normalEngineerApifyWarnings: [],
  };

  const selectedKeys = new Set(alreadySelected.map(toEmployeeKey));
  const addedKeys = new Set<string>();

  function isAlreadySelected(emp: EnrichedEmployee): boolean {
    const key = toEmployeeKey(emp);
    return selectedKeys.has(key) || addedKeys.has(key);
  }

  function currentTotal(): number {
    return alreadySelected.length + result.candidates.length;
  }

  for (const stage of BACKFILL_STAGES) {
    if (currentTotal() >= maxTotal) break;

    const stagePool = filterPoolByStage(profilePool, apifyCache, {
      currentTitles: stage.currentTitles,
      pastTitles: stage.pastTitles,
      notTitles: stage.notTitles,
      notPastTitles: stage.notPastTitles,
    });

    const deduped = stagePool.filter((emp) => !isAlreadySelected(emp));
    if (deduped.length === 0) continue;

    const qaKept: EnrichedEmployee[] = [];
    for (const emp of deduped) {
      if (QA_TITLE_REJECT_REGEX.test(emp.currentTitle ?? "")) {
        result.filteredOutReasons.push("qa_title");
      } else {
        qaKept.push(emp);
      }
    }
    if (qaKept.length === 0) continue;

    const apifyResult = filterOpenToWorkFromCache(qaKept, apifyCache, companyInfo);
    for (const w of apifyResult.warnings) result.warnings.push(w);
    for (const { reason } of apifyResult.filteredOut) result.filteredOutReasons.push(reason);

    const hardwareResult = filterOutHardwareHeavyPeople(apifyResult.kept, apifyCache);
    for (const _emp of hardwareResult.rejected) result.filteredOutReasons.push("hardware_heavy");

    let candidatesForRanking = hardwareResult.kept;
    if (candidatesForRanking.length === 0) continue;

    if (stage.isNormalEngineer) {
      const frontendResult = filterFrontendEngineers(candidatesForRanking, apifyCache, companyInfo);
      for (const _emp of frontendResult.rejectedFrontend) result.filteredOutReasons.push("frontend_role");
      for (const wc of frontendResult.warningCandidates) {
        result.normalEngineerApifyWarnings.push({ employee: wc.employee, problem: wc.problem });
      }
      candidatesForRanking = frontendResult.kept;
      if (candidatesForRanking.length === 0) continue;
    }

    const slots = maxTotal - currentTotal();

    if (stage.allEngLead) {
      const picked = rankAndSelectBackfillCandidates(candidatesForRanking, slots);
      for (const emp of picked) {
        result.candidates.push({ employee: emp, linkedinBucket: "engLead" });
        addedKeys.add(toEmployeeKey(emp));
      }
    } else if (stage.splitLeadership && stage.leadershipTitleKeywords) {
      const { icCandidates, leadershipCandidates } = partitionBackfillLeadership(
        candidatesForRanking,
        stage.leadershipTitleKeywords
      );
      const icPicked = rankAndSelectBackfillCandidates(icCandidates, slots);
      for (const emp of icPicked) {
        result.candidates.push({ employee: emp, linkedinBucket: "eng" });
        addedKeys.add(toEmployeeKey(emp));
      }
      const slotsAfterIc = maxTotal - currentTotal();
      if (slotsAfterIc > 0) {
        const leadPicked = rankAndSelectBackfillCandidates(leadershipCandidates, slotsAfterIc);
        for (const emp of leadPicked) {
          result.candidates.push({ employee: emp, linkedinBucket: "engLead" });
          addedKeys.add(toEmployeeKey(emp));
        }
      }
    } else {
      const picked = rankAndSelectBackfillCandidates(candidatesForRanking, slots);
      for (const emp of picked) {
        result.candidates.push({ employee: emp, linkedinBucket: "eng" });
        addedKeys.add(toEmployeeKey(emp));
      }
    }
  }

  return result;
}
