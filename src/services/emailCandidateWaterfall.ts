import { ResolvedCompany } from "./getCompany";
import { bulkEnrichPeople, EnrichmentCache } from "./bulkEnrichPeople";
import { searchEmailCandidatePeople, searchEmailCandidatePeopleCached, ApolloSearchCache, PeopleSearchFilters } from "./searchPeople";
import { EnrichedEmployee, Prospect, ApifyOpenToWorkCache } from "../types/prospect";
import { scrapeAndFilterOpenToWork, splitByTenure, filterFrontendEngineers } from "./apifyClient";

export type EmailCampaignBucket = "sre" | "eng" | "engLead";

export interface EmailSearchStageConfig {
  currentTitles?: string[];
  pastTitles?: string[];
  notTitles?: string[];
  notPastTitles?: string[];
  minTenureMonths: number;
  campaignBucket: EmailCampaignBucket;
  splitLeadership?: boolean;
  leadershipBucket?: EmailCampaignBucket;
  leadershipTitleKeywords?: string[];
}

export interface TaggedEmailCandidate {
  employee: EnrichedEmployee;
  campaignBucket: EmailCampaignBucket;
}

export type EmailWaterfallFilteredReason = "open_to_work" | "frontend_role" | "contract_employment";

export interface FilteredEmailCandidate {
  employee: EnrichedEmployee;
  reason: EmailWaterfallFilteredReason;
}

export interface EmailWaterfallResult {
  candidates: TaggedEmailCandidate[];
  filteredOutCandidates: FilteredEmailCandidate[];
  warnings: string[];
  normalEngineerApifyWarnings: NormalEngineerApifyWarningCandidate[];
}

export interface EmailWaterfallOptions {
  rawSreCount?: number;
  apolloSearchCache?: ApolloSearchCache;
  recycledKeywordMatched?: EnrichedEmployee[];
}

export interface NormalEngineerApifyWarningCandidate {
  employee: EnrichedEmployee;
  problem: string;
}

const MAX_PER_COMPANY = 7;
const MAX_SEARCH_RESULTS = 30;
const LINE_WIDTH = 62;
const HEAVY_LINE = "═".repeat(LINE_WIDTH);
const LIGHT_LINE = "─".repeat(LINE_WIDTH);
const LEADERSHIP_TITLE_KEYWORDS = ["vp", "manager", "director", "head", "chief", "principal"];
const LINKEDIN_LEADERSHIP_TITLE_KEYWORDS = ["director", "vp", "svp", "head", "chief"];
const NORMAL_ENGINEER_STAGE_LABEL = "Normal Engineer Search";
const SPLIT_LEADERSHIP_BUCKET: EmailCampaignBucket = "engLead";
const MIN_SRE_COUNT_FOR_EMAIL_SRE_STAGES = 8;

const EMAIL_CANDIDATE_STAGES: EmailSearchStageConfig[] = [
  {
    currentTitles: [
      "site reliability",
      "SRE",
      "Site Reliability Engineer",
      "Site Reliability Engineering",
      "Head of Reliability",
      "observability",
    ],
    notTitles: ["contract", "contractor", "freelance", "freelancer", "junior", "jr"],
    minTenureMonths: 2,
    campaignBucket: "sre",
    splitLeadership: true,
    leadershipBucket: SPLIT_LEADERSHIP_BUCKET,
    leadershipTitleKeywords: LINKEDIN_LEADERSHIP_TITLE_KEYWORDS,
  },
  {
    pastTitles: [
      "site reliability",
      "SRE",
      "Site Reliability Engineer",
      "Site Reliability Engineering",
      "Head of Reliability",
      "observability",
    ],
    notTitles: ["contract", "contractor", "freelance", "freelancer", "junior", "jr"],
    minTenureMonths: 2,
    campaignBucket: "sre",
    splitLeadership: true,
    leadershipBucket: SPLIT_LEADERSHIP_BUCKET,
    leadershipTitleKeywords: LINKEDIN_LEADERSHIP_TITLE_KEYWORDS,
  },
  {
    currentTitles: ["Infrastructure"],
    notTitles: [
      "data",
      "corporate",
      "contract",
      "contractor",
      "freelance",
      "freelancer",
      "junior",
      "jr",
      "IT",
      "helpdesk",
      "desktop",
      "end user",
      "workplace",
      "internal systems",
      "business systems",
      "information systems",
      "security",
      "infosec",
      "GRC",
      "governance",
      "risk",
      "compliance",
      "IAM",
      "identity",
      "trust & safety",
      "privacy",
      "analytics",
      "BI",
      "business intelligence",
      "support",
      "customer support",
      "technical support",
      "customer success",
      "business",
      "sales",
      "trainee",
      "solutions engineer",
      "TAM",
      "operations",
      "design",
      "program manager",
      "project manager",
      "enterprise",
      "AI",
      "machine learning",
      "ml",
      "automation",
      "operation",
      "development",
      "construction",
      "sysadmin",
      "system administrator",
      "administrator",
      "salesforce",
      "android",
      "IOS",
      "network",
      "search",
      "information technology",
      "solution",
    ],
    minTenureMonths: 11,
    campaignBucket: "eng",
    splitLeadership: true,
    leadershipBucket: SPLIT_LEADERSHIP_BUCKET,
  },
  {
    currentTitles: [
      "Platform engineering",
      "Platform engineer",
      "Platforms Engineering Manager",
      "Director of Software Engineering, Platform",
      "Director, Engineering (Platform)",
      "Platform Engineering Manager",
      "VP of Engineering, Platform",
      "VP, Engineering - Platform",
      "VP, Product Platform & Engineering",
      "VP of Developer Platform",
      "VP of Engineering Systems",
      "Head of Platform",
      "Head of Developer Platform",
      "Head of Platform & Reliability",
      "Head of Cloud Platform",
      "Head of Engineering Productivity / Platform",
      "Chief Platform Officer",
      "backend platform",
      "cloud platform",
      "platform cloud",
    ],
    notTitles: [
      "data",
      "contract",
      "contractor",
      "freelance",
      "freelancer",
      "junior",
      "jr",
      "AI",
      "artificial intelligence",
      "machine learning",
      "ml",
      "frontend",
      "front-end",
      "front end",
      "solution",
    ],
    notPastTitles: [
      "client",
      "account",
      "sales",
      "customer",
      "insight",
      "research",
      "marketing",
      "consultant",
      "analyst",
      "partner",
      "commercial",
      "AI",
      "artificial intelligence",
      "machine learning",
      "ml",
    ],
    minTenureMonths: 11,
    campaignBucket: "eng",
    splitLeadership: true,
    leadershipBucket: SPLIT_LEADERSHIP_BUCKET,
  },
  {
    currentTitles: ["DevOps", "Dev Ops"],
    notTitles: [
      "data",
      "IT",
      "corporate",
      "contract",
      "contractor",
      "freelance",
      "freelancer",
      "junior",
      "jr",
      "enterprise", 
      "internal systems",
      "workplace",
      "end user",
      "desktop",
      "helpdesk",
      "release",
      "management",
      "deployment",
      "analytics",
      "BI",
      "security",
      "infosec",
      "GRC",
      "compliance",
      "governance",
      "IAM",
      "support",
      "customer",
      "business",
      "sales",
      "trainee",
      "solutions",
      "consultant",
      "professional services",
      "TAM",
      "project",
      "program",
      "scrum",
      "agile",
      "solution",
      "representative",
      "sysops",
      "salesforce",
      "android",
      "IOS",
      "solution",
    ],
    minTenureMonths: 11,
    campaignBucket: "eng",
    splitLeadership: true,
    leadershipBucket: SPLIT_LEADERSHIP_BUCKET,
  },
  {
    currentTitles: [
      "Principal engineer",
      "Staff engineer",
      "Tech lead",
      "Lead software engineer",
      "Technical Lead",
      "Lead Engineer",
    ],
    pastTitles: ["engineer"],
    notTitles: [
      "ml",
      "machine learning",
      "data",
      "contract",
      "contractor",
      "freelance",
      "freelancer",
      "junior",
      "jr",
      "frontend",
      "front-end",
      "front end",
      "salesforce",
      "android",
      "IOS",
      "battery",
      "mobile",
      "desktop",
      "test",
      "AI",
      "artificial intelligence",
      "hardware",
      "solution",
    ],
    minTenureMonths: 11,
    campaignBucket: "eng",
  },
  {
    currentTitles: [
      "VP of Engineering",
      "Vice President of Engineering",
      "VP Engineering",
      "Vice President Engineering",
      "Head of Engineering",
      "Director of Engineering",
      "Director, Engineering",
      "Engineering Director",
      "Engineering Manager",
      "Senior Engineering Manager",
      "Manager, Engineering",
      "Head of Software Engineering",
      "VP of Software Engineering",
      "Director of Software Engineering",
      "Manager of Software Engineering",
    ],
    pastTitles: ["engineer"],
    notTitles: [
      "IT",
      "information technology",
      "corporate",
      "enterprise systems",
      "internal systems",
      "workplace",
      "end user",
      "helpdesk",
      "desktop",
      "industrial",
      "solutions",
      "mechanical",
      "electrical",
      "electronics",
      "hardware",
      "firmware",
      "embedded",
      "manufacturing",
      "production",
      "plant",
      "facilities",
      "network",
      "telecom",
      "NOC",
      "field engineering",
      "security",
      "infosec",
      "cybersecurity",
      "GRC",
      "compliance",
      "trust & safety",
      "data",
      "analytics",
      "BI",
      "business intelligence",
      "research",
      "applied science",
      "ml",
      "machine learning",
      "program",
      "project",
      "TPM",
      "agile",
      "scrum",
      "salesforce",
      "android",
      "IOS",
      "AI",
      "artificial intelligence",
      "junior",
      "jr",
      "solution",
    ],
    minTenureMonths: 11,
    campaignBucket: "engLead",
  },
];

const STAGE_LABELS = [
  "SRE Search",
  "Past SRE Search",
  "Infrastructure Search",
  "Platform Search",
  "DevOps Search",
  "Normal Engineer Search",
  "Eng Leader Search",
];

export const LINKEDIN_KEYWORD_STAGE_INFRA = EMAIL_CANDIDATE_STAGES[2];
export const LINKEDIN_KEYWORD_STAGE_DEVOPS = EMAIL_CANDIDATE_STAGES[4];
export const LINKEDIN_KEYWORD_STAGE_NORMAL_ENG = EMAIL_CANDIDATE_STAGES[5];

function print(line: string): void {
  process.stdout.write(line + "\n");
}

function toEmployeeKey(employee: EnrichedEmployee): string {
  return employee.id ?? `${employee.name}|${employee.currentTitle}|${employee.linkedinUrl ?? ""}`;
}

function addEmployeeIdentifiers(set: Set<string>, employee: EnrichedEmployee): void {
  set.add(toEmployeeKey(employee));
  if (employee.id) {
    set.add(employee.id);
  }
}

function hasEmployeeIdentifier(set: Set<string>, employee: EnrichedEmployee): boolean {
  if (set.has(toEmployeeKey(employee))) {
    return true;
  }
  if (employee.id && set.has(employee.id)) {
    return true;
  }
  return false;
}

function dedupeProspectsById(items: Prospect[]): Prospect[] {
  const seen = new Set<string>();
  const deduped: Prospect[] = [];

  for (const item of items) {
    if (seen.has(item.id)) {
      continue;
    }
    seen.add(item.id);
    deduped.push(item);
  }

  return deduped;
}

function rankAndSelectCandidates(
  enriched: EnrichedEmployee[],
  minTenureMonths: number,
  slotsAvailable: number
): { selected: EnrichedEmployee[]; qualifiedCount: number; nullTenureCount: number; belowMinCount: number; fillerCount: number } {
  const qualified: EnrichedEmployee[] = [];
  const nullTenure: EnrichedEmployee[] = [];
  let belowMinCount = 0;

  for (const employee of enriched) {
    if (employee.tenure === null) {
      nullTenure.push(employee);
      continue;
    }
    if (employee.tenure >= minTenureMonths) {
      qualified.push(employee);
    } else {
      belowMinCount += 1;
    }
  }

  qualified.sort((a, b) => (b.tenure ?? -1) - (a.tenure ?? -1));
  nullTenure.sort((a, b) => a.name.localeCompare(b.name));

  const selected: EnrichedEmployee[] = [];

  for (const emp of qualified) {
    if (selected.length >= slotsAvailable) {
      break;
    }
    selected.push(emp);
  }

  const qualifiedSelected = selected.length;

  for (const emp of nullTenure) {
    if (selected.length >= slotsAvailable) {
      break;
    }
    selected.push(emp);
  }

  return {
    selected,
    qualifiedCount: qualified.length,
    nullTenureCount: nullTenure.length,
    belowMinCount,
    fillerCount: selected.length - qualifiedSelected,
  };
}

function printStageHeader(stageIndex: number): void {
  const label = STAGE_LABELS[stageIndex] ?? `Stage ${stageIndex + 1}`;
  const totalStages = EMAIL_CANDIDATE_STAGES.length;
  const tag = `Stage ${stageIndex + 1}/${totalStages}: ${label}`;
  const padding = LIGHT_LINE.length - tag.length - 5;
  print("");
  print(`─── ${tag} ${"─".repeat(Math.max(1, padding))}`);
}

function printStageSkip(reason: string): void {
  print(`    SKIPPED — ${reason}`);
  print(LIGHT_LINE);
}

function printPeopleTable(selected: EnrichedEmployee[], bucket: EmailCampaignBucket): void {
  print("");
  print(`    ${"Name".padEnd(22)}${"Title".padEnd(24)}${"Tenure".padEnd(10)}Bucket`);
  print(`    ${"─".repeat(22)}${"─".repeat(24)}${"─".repeat(10)}${"─".repeat(6)}`);
  for (const emp of selected) {
    const name = emp.name.length > 20 ? emp.name.slice(0, 19) + "…" : emp.name;
    const title = emp.currentTitle.length > 22 ? emp.currentTitle.slice(0, 21) + "…" : emp.currentTitle;
    const tenure = emp.tenure !== null ? `${emp.tenure}mo` : "—";
    print(`    ${name.padEnd(22)}${title.padEnd(24)}${tenure.padEnd(10)}${bucket}`);
  }
}

function isLeadershipRoleTitle(title: string, leadershipTitleKeywords: string[] = LEADERSHIP_TITLE_KEYWORDS): boolean {
  const normalized = title.toLowerCase();
  return leadershipTitleKeywords.some((keyword) => normalized.includes(keyword));
}

function partitionLeadershipCandidates(
  candidates: EnrichedEmployee[],
  leadershipTitleKeywords: string[] = LEADERSHIP_TITLE_KEYWORDS
): {
  icCandidates: EnrichedEmployee[];
  leadershipCandidates: EnrichedEmployee[];
} {
  const icCandidates: EnrichedEmployee[] = [];
  const leadershipCandidates: EnrichedEmployee[] = [];
  for (const employee of candidates) {
    if (isLeadershipRoleTitle(employee.currentTitle, leadershipTitleKeywords)) {
      leadershipCandidates.push(employee);
    } else {
      icCandidates.push(employee);
    }
  }
  return { icCandidates, leadershipCandidates };
}

export async function runEmailCandidateWaterfall(
  company: ResolvedCompany,
  linkedinAttemptedKeys: Set<string>,
  enrichmentCache: EnrichmentCache,
  filters: PeopleSearchFilters,
  apifyCache: ApifyOpenToWorkCache,
  options: EmailWaterfallOptions = {}
): Promise<EmailWaterfallResult> {
  const listA: TaggedEmailCandidate[] = [];
  const listAKeys = new Set<string>();
  const filteredOutCandidates: FilteredEmailCandidate[] = [];
  const warnings: string[] = [];
  const normalEngineerApifyWarnings: NormalEngineerApifyWarningCandidate[] = [];
  const rawSreCount = options.rawSreCount ?? Number.POSITIVE_INFINITY;
  const apolloSearchCache = options.apolloSearchCache;
  const recycledKeywordMatched = options.recycledKeywordMatched ?? [];

  print("");
  print(HEAVY_LINE);
  print(`  EMAIL WATERFALL — ${company.companyName} (${company.domain})`);
  print(`  LinkedIn exclusion keys: ${linkedinAttemptedKeys.size}`);
  if (recycledKeywordMatched.length > 0) {
    print(`  Recycled keyword-matched candidates: ${recycledKeywordMatched.length}`);
  }
  print(HEAVY_LINE);

  for (let stageIndex = 0; stageIndex < EMAIL_CANDIDATE_STAGES.length; stageIndex += 1) {
    const stage = EMAIL_CANDIDATE_STAGES[stageIndex];
    const stageLabel = STAGE_LABELS[stageIndex] ?? `Stage ${stageIndex + 1}`;
    const isSreSearchStage = stageLabel === "SRE Search" || stageLabel === "Past SRE Search";

    if (stageIndex === 1 && recycledKeywordMatched.length > 0 && listA.length < MAX_PER_COMPANY) {
      print("");
      print(`─── Recycled Keyword-Matched (from LinkedIn overflow) ${"─".repeat(8)}`);
      const slotsForRecycled = MAX_PER_COMPANY - listA.length;
      let recycledAdded = 0;
      for (const emp of recycledKeywordMatched) {
        if (recycledAdded >= slotsForRecycled) break;
        if (hasEmployeeIdentifier(linkedinAttemptedKeys, emp)) continue;
        if (hasEmployeeIdentifier(listAKeys, emp)) continue;
        listA.push({ employee: emp, campaignBucket: "sre" });
        addEmployeeIdentifiers(listAKeys, emp);
        recycledAdded += 1;
      }
      print(`    Inserted ${recycledAdded} recycled keyword-matched candidates (SRE bucket)`);
      print(`    ▸ List A: ${listA.length} / ${MAX_PER_COMPANY}`);
      print(LIGHT_LINE);
    }

    if (isSreSearchStage && rawSreCount < MIN_SRE_COUNT_FOR_EMAIL_SRE_STAGES) {
      printStageHeader(stageIndex);
      printStageSkip(
        `skipped because pre-filter SRE count is ${rawSreCount} (< ${MIN_SRE_COUNT_FOR_EMAIL_SRE_STAGES})`
      );
      continue;
    }

    if (listA.length >= MAX_PER_COMPANY) {
      printStageHeader(stageIndex);
      printStageSkip(`List A already full (${listA.length}/${MAX_PER_COMPANY})`);
      break;
    }

    printStageHeader(stageIndex);

    const currentTitlesStr = (stage.currentTitles ?? []).join(", ") || "—";
    const pastTitlesStr = (stage.pastTitles ?? []).join(", ") || "—";
    const notTitlesStr = (stage.notTitles ?? []).join(", ") || "—";
    print(`    Titles (current) : ${currentTitlesStr}`);
    print(`    Titles (past)    : ${pastTitlesStr}`);
    print(`    Titles (exclude) : ${notTitlesStr}`);
    print(`    Min tenure       : ${stage.minTenureMonths} months`);
    print(`    Campaign bucket  : ${stage.campaignBucket}`);
    print("");

    const searchParams = { currentTitles: stage.currentTitles, pastTitles: stage.pastTitles, notTitles: stage.notTitles, notPastTitles: stage.notPastTitles };
    const rawProspects = apolloSearchCache
      ? await searchEmailCandidatePeopleCached(company, MAX_SEARCH_RESULTS, searchParams, filters, apolloSearchCache)
      : await searchEmailCandidatePeople(company, MAX_SEARCH_RESULTS, searchParams, filters);

    const prospects = dedupeProspectsById(rawProspects);

    print(`    Search       ${String(rawProspects.length).padStart(3)} raw → ${prospects.length} after self-dedup`);

    if (prospects.length === 0) {
      printStageSkip("0 prospects from search");
      continue;
    }

    const deduped = prospects.filter((prospect) => {
      return !linkedinAttemptedKeys.has(prospect.id) && !listAKeys.has(prospect.id);
    });

    const removedCount = prospects.length - deduped.length;
    print(`    Dedup        ${String(prospects.length).padStart(3)} → ${deduped.length}  (removed ${removedCount})`);

    if (deduped.length === 0) {
      printStageSkip("all prospects removed by dedup");
      continue;
    }

    const enriched = await bulkEnrichPeople(deduped, enrichmentCache);

    print(`    Enrichment   ${String(deduped.length).padStart(3)} sent → ${enriched.length} returned`);

    if (enriched.length === 0) {
      printStageSkip("0 enriched results");
      continue;
    }

    const filtered = enriched.filter((employee) => {
      const key = toEmployeeKey(employee);
      return !linkedinAttemptedKeys.has(key) && !listAKeys.has(key);
    });

    if (filtered.length < enriched.length) {
      print(`    Post-dedup   ${String(enriched.length).padStart(3)} → ${filtered.length}  (removed ${enriched.length - filtered.length} by enriched-key dedup)`);
    }

    if (filtered.length === 0) {
      printStageSkip("all enriched removed by post-enrich dedup");
      continue;
    }

    const { eligible: tenureEligible, droppedByTenure } = splitByTenure(filtered, stage.minTenureMonths);
    if (droppedByTenure.length > 0) {
      print(`    Pre-tenure   ${String(filtered.length).padStart(3)} → ${tenureEligible.length}  (dropped ${droppedByTenure.length} below ${stage.minTenureMonths}mo)`);
    }

    if (tenureEligible.length === 0) {
      printStageSkip("all candidates dropped by tenure filter");
      continue;
    }

    const isNormalEngineerStage = STAGE_LABELS[stageIndex] === NORMAL_ENGINEER_STAGE_LABEL;
    const {
      kept: apifyFiltered,
      warnings: apifyWarnings,
      filteredOut: apifyFilteredOut,
    } = await scrapeAndFilterOpenToWork(tenureEligible, apifyCache, {
      companyName: company.companyName,
      companyDomain: company.domain,
    });
    warnings.push(...apifyWarnings);
    filteredOutCandidates.push(...apifyFilteredOut);

    if (apifyFiltered.length === 0) {
      printStageSkip("all candidates removed by openToWork filter");
      continue;
    }

    let candidatesForRanking = apifyFiltered;

    if (isNormalEngineerStage) {
      const frontendResult = filterFrontendEngineers(apifyFiltered, apifyCache);
      candidatesForRanking = frontendResult.kept;
      filteredOutCandidates.push(
        ...frontendResult.rejectedFrontend.map((employee) => ({
          employee,
          reason: "frontend_role" as const,
        }))
      );
      normalEngineerApifyWarnings.push(
        ...frontendResult.warningCandidates.map((warningCandidate) => ({
          employee: warningCandidate.employee,
          problem: warningCandidate.problem,
        }))
      );

      if (frontendResult.rejectedFrontend.length > 0) {
        print(`    Frontend     ${apifyFiltered.length} → ${frontendResult.kept.length}  (rejected ${frontendResult.rejectedFrontend.length} frontend)`);
      }

      if (candidatesForRanking.length === 0) {
        printStageSkip("all candidates removed by frontend keyword filter");
        continue;
      }
    }

    if (stage.splitLeadership) {
      const { icCandidates, leadershipCandidates } = partitionLeadershipCandidates(
        candidatesForRanking,
        stage.leadershipTitleKeywords
      );
      const icSlots = MAX_PER_COMPANY - listA.length;
      const icResult = rankAndSelectCandidates(icCandidates, stage.minTenureMonths, icSlots);
      print(
        `    IC Tenure    ${icResult.qualifiedCount} qualified · ${icResult.nullTenureCount} null (filler) · ${icResult.belowMinCount} below min (dropped)`
      );
      print(
        `    IC Selection ${icSlots} slots → ${icResult.selected.length} picked (${icResult.selected.length - icResult.fillerCount} qualified + ${icResult.fillerCount} fillers)`
      );
      printPeopleTable(icResult.selected, stage.campaignBucket);
      for (const employee of icResult.selected) {
        listA.push({ employee, campaignBucket: stage.campaignBucket });
        addEmployeeIdentifiers(listAKeys, employee);
      }

      const leadershipSlots = MAX_PER_COMPANY - listA.length;
      if (leadershipSlots > 0 && stage.leadershipBucket) {
        const leadershipResult = rankAndSelectCandidates(
          leadershipCandidates,
          stage.minTenureMonths,
          leadershipSlots
        );
        print(
          `    Lead Tenure  ${leadershipResult.qualifiedCount} qualified · ${leadershipResult.nullTenureCount} null (filler) · ${leadershipResult.belowMinCount} below min (dropped)`
        );
        print(
          `    Lead Select  ${leadershipSlots} slots → ${leadershipResult.selected.length} picked (${leadershipResult.selected.length - leadershipResult.fillerCount} qualified + ${leadershipResult.fillerCount} fillers)`
        );
        printPeopleTable(leadershipResult.selected, stage.leadershipBucket);
        for (const employee of leadershipResult.selected) {
          listA.push({ employee, campaignBucket: stage.leadershipBucket });
          addEmployeeIdentifiers(listAKeys, employee);
        }
      }
    } else {
      const slotsAvailable = MAX_PER_COMPANY - listA.length;
      const result = rankAndSelectCandidates(candidatesForRanking, stage.minTenureMonths, slotsAvailable);
      print(
        `    Tenure       ${result.qualifiedCount} qualified · ${result.nullTenureCount} null (filler) · ${result.belowMinCount} below min (dropped)`
      );
      print(
        `    Selection    ${slotsAvailable} slots → ${result.selected.length} picked (${result.selected.length - result.fillerCount} qualified + ${result.fillerCount} fillers)`
      );
      printPeopleTable(result.selected, stage.campaignBucket);
      for (const employee of result.selected) {
        listA.push({ employee, campaignBucket: stage.campaignBucket });
        addEmployeeIdentifiers(listAKeys, employee);
      }
    }

    print("");
    print(`    ▸ List A: ${listA.length} / ${MAX_PER_COMPANY}`);
    print(LIGHT_LINE);
  }

  const sreCt = listA.filter((c) => c.campaignBucket === "sre").length;
  const engCt = listA.filter((c) => c.campaignBucket === "eng").length;
  const engLeadCt = listA.filter((c) => c.campaignBucket === "engLead").length;

  print("");
  print(HEAVY_LINE);
  print(`  WATERFALL COMPLETE — ${company.companyName}`);
  print(`  Total: ${listA.length}  ·  SRE: ${sreCt}  ·  ENG: ${engCt}  ·  ENG_LEAD: ${engLeadCt}`);
  print(HEAVY_LINE);
  print("");

  return { candidates: listA, filteredOutCandidates, warnings, normalEngineerApifyWarnings };
}
