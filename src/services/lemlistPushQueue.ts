import { EnrichedEmployee, LemlistFailedLead, LemlistPushMeta } from "../types/prospect";
import {
  createLeadInCampaign,
  getLemlistLinkedinCampaignIdsForUser,
  LemlistCreateLeadPayload,
} from "./lemlistClient";
import { SelectedUser } from "../shared/selectedUser";

const RATE_LIMIT_REQUESTS = 20;
const RATE_LIMIT_WINDOW_MS = 2_000;
const DEFAULT_LEMLIST_QUERY = {
  deduplicate: false,
  linkedinEnrichment: false,
  findEmail: false,
  verifyEmail: false,
  findPhone: false,
};

type QueueTask = () => Promise<void>;

export type LinkedinCampaignBucket = "sre" | "eng";

export interface TaggedLinkedinCandidate {
  employee: EnrichedEmployee;
  linkedinBucket: LinkedinCampaignBucket;
}

let queueTail: Promise<void> = Promise.resolve();

function enqueue(task: QueueTask): Promise<void> {
  const run = queueTail.then(task, task);
  queueTail = run.catch(() => undefined);
  return run;
}

function splitName(name: string): { firstName: string; lastName?: string } | null {
  const trimmed = name.trim();
  if (!trimmed) {
    return null;
  }

  const parts = trimmed.split(/\s+/);
  const firstName = parts[0];
  const lastName = parts.slice(1).join(" ").trim();
  return {
    firstName,
    ...(lastName ? { lastName } : {}),
  };
}

function toLeadPayload(
  employee: EnrichedEmployee,
  companyName: string,
  companyDomain: string
): LemlistCreateLeadPayload | null {
  const parsedName = splitName(employee.name);
  if (!parsedName) {
    return null;
  }

  return {
    firstName: parsedName.firstName,
    lastName: parsedName.lastName,
    companyName,
    jobTitle: employee.currentTitle,
    linkedinUrl: employee.linkedinUrl ?? undefined,
    companyDomain,
  };
}

function groupByBucket(candidates: TaggedLinkedinCandidate[]): Record<LinkedinCampaignBucket, EnrichedEmployee[]> {
  const buckets: Record<LinkedinCampaignBucket, EnrichedEmployee[]> = {
    sre: [],
    eng: [],
  };

  for (const candidate of candidates) {
    buckets[candidate.linkedinBucket].push(candidate.employee);
  }

  return buckets;
}

export async function pushPeopleToLemlistCampaign(
  candidates: TaggedLinkedinCandidate[],
  companyName: string,
  companyDomain: string,
  selectedUser: SelectedUser
): Promise<LemlistPushMeta> {
  return new Promise<LemlistPushMeta>((resolve, reject) => {
    void enqueue(async () => {
      try {
        const campaignIds = getLemlistLinkedinCampaignIdsForUser(selectedUser);
        const grouped = groupByBucket(candidates);
        const failedItems: LemlistFailedLead[] = [];
        const successItems: string[] = [];
        let successful = 0;
        let sentInWindow = 0;
        let windowStartedAt = Date.now();

        console.log(
          `[Lemlist] Campaign routing (${companyName}) user=${selectedUser}: SRE=${grouped.sre.length}, ENG=${grouped.eng.length}`
        );

        const bucketOrder: Array<{ campaignId: string; people: EnrichedEmployee[] }> = [
          { campaignId: campaignIds.sreCampaignId, people: grouped.sre },
          { campaignId: campaignIds.engCampaignId, people: grouped.eng },
        ];

        for (const bucket of bucketOrder) {
          for (const employee of bucket.people) {
            const now = Date.now();
            const elapsed = now - windowStartedAt;
            if (elapsed >= RATE_LIMIT_WINDOW_MS) {
              sentInWindow = 0;
              windowStartedAt = now;
            } else if (sentInWindow >= RATE_LIMIT_REQUESTS) {
              const waitMs = RATE_LIMIT_WINDOW_MS - elapsed;
              await new Promise((timerResolve) => setTimeout(timerResolve, waitMs));
              sentInWindow = 0;
              windowStartedAt = Date.now();
            }

            const payload = toLeadPayload(employee, companyName, companyDomain);
            if (!payload) {
              failedItems.push({
                name: employee.name,
                error: "Missing valid lead name for Lemlist payload.",
              });
              continue;
            }

            try {
              await createLeadInCampaign(bucket.campaignId, payload, DEFAULT_LEMLIST_QUERY);
              successful += 1;
              successItems.push(employee.name);
              sentInWindow += 1;
            } catch (error) {
              const message = error instanceof Error ? error.message : "Unknown Lemlist push error.";
              failedItems.push({
                name: employee.name,
                error: message,
              });
            }
          }
        }

        resolve({
          attempted: candidates.length,
          successful,
          failed: failedItems.length,
          successItems,
          failedItems,
        });
      } catch (error) {
        reject(error);
      }
    });
  });
}
