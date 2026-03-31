import { ResolvedCompany } from "./getCompany";
import { bulkEnrichPeople, EnrichmentCache } from "./bulkEnrichPeople";
import { searchEmailCandidatePeople, PeopleSearchFilters } from "./searchPeople";
import { EnrichedEmployee, Prospect } from "../types/prospect";

export type EmailCampaignBucket = "sre" | "eng" | "engLead";

interface EmailSearchStageConfig {
  currentTitles?: string[];
  pastTitles?: string[];
  notTitles?: string[];
  minTenureMonths: number;
  campaignBucket: EmailCampaignBucket;
}

export interface TaggedEmailCandidate {
  employee: EnrichedEmployee;
  campaignBucket: EmailCampaignBucket;
}

export interface EmailWaterfallResult {
  candidates: TaggedEmailCandidate[];
}

const MAX_PER_COMPANY = 7;
const MAX_SEARCH_RESULTS = 30;
const LINE_WIDTH = 62;
const HEAVY_LINE = "═".repeat(LINE_WIDTH);
const LIGHT_LINE = "─".repeat(LINE_WIDTH);

const EMAIL_CANDIDATE_STAGES: EmailSearchStageConfig[] = [
  {
    currentTitles: ["site reliability", "SRE", "Site Reliability Engineer"],
    minTenureMonths: 2,
    campaignBucket: "sre",
  },
  {
    pastTitles: ["site reliability", "SRE", "Site Reliability Engineer"],
    minTenureMonths: 2,
    campaignBucket: "sre",
  },
  {
    currentTitles: ["Infrastructure"],
    notTitles: ["data"],
    minTenureMonths: 11,
    campaignBucket: "eng",
  },
  {
    currentTitles: [
      "Platform engineering",
      "Platform engineer",
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
    ],
    notTitles: ["data"],
    minTenureMonths: 11,
    campaignBucket: "eng",
  },
  {
    currentTitles: ["DevOps", "Dev Ops", "Principal engineer", "Staff engineer", "Tech lead", "Lead software engineer"],
    notTitles: ["data"],
    minTenureMonths: 11,
    campaignBucket: "eng",
  },
  {
    currentTitles: ["Vp", "Head of", "Svp", "vice president", "Director", "Senior vice president", "Manager"],
    pastTitles: ["engineer"],
    notTitles: ["data"],
    minTenureMonths: 11,
    campaignBucket: "engLead",
  },
];

const STAGE_LABELS = [
  "SRE Search",
  "Past SRE Search",
  "Infrastructure Search",
  "Platform Search",
  "Normal Engineer Search",
  "Eng Leader Search",
];

function print(line: string): void {
  process.stdout.write(line + "\n");
}

function toEmployeeKey(employee: EnrichedEmployee): string {
  return employee.id ?? `${employee.name}|${employee.currentTitle}|${employee.linkedinUrl ?? ""}`;
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
  const label = STAGE_LABELS[stageIndex];
  const tag = `Stage ${stageIndex + 1}/6: ${label}`;
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

export async function runEmailCandidateWaterfall(
  company: ResolvedCompany,
  linkedinAttemptedKeys: Set<string>,
  enrichmentCache: EnrichmentCache,
  filters: PeopleSearchFilters
): Promise<EmailWaterfallResult> {
  const listA: TaggedEmailCandidate[] = [];
  const listAKeys = new Set<string>();

  print("");
  print(HEAVY_LINE);
  print(`  EMAIL WATERFALL — ${company.companyName} (${company.domain})`);
  print(`  LinkedIn exclusion keys: ${linkedinAttemptedKeys.size}`);
  print(HEAVY_LINE);

  for (let stageIndex = 0; stageIndex < EMAIL_CANDIDATE_STAGES.length; stageIndex += 1) {
    const stage = EMAIL_CANDIDATE_STAGES[stageIndex];

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

    const rawProspects = await searchEmailCandidatePeople(
      company,
      MAX_SEARCH_RESULTS,
      { currentTitles: stage.currentTitles, pastTitles: stage.pastTitles, notTitles: stage.notTitles },
      filters
    );

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

    const slotsAvailable = MAX_PER_COMPANY - listA.length;
    const result = rankAndSelectCandidates(filtered, stage.minTenureMonths, slotsAvailable);

    print(`    Tenure       ${result.qualifiedCount} qualified · ${result.nullTenureCount} null (filler) · ${result.belowMinCount} below min (dropped)`);
    print(`    Selection    ${slotsAvailable} slots → ${result.selected.length} picked (${result.selected.length - result.fillerCount} qualified + ${result.fillerCount} fillers)`);

    printPeopleTable(result.selected, stage.campaignBucket);

    for (const employee of result.selected) {
      const key = toEmployeeKey(employee);
      listA.push({ employee, campaignBucket: stage.campaignBucket });
      listAKeys.add(key);
      if (employee.id) {
        listAKeys.add(employee.id);
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

  return { candidates: listA };
}
