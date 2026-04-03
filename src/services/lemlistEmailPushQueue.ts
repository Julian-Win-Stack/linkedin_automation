import {
  EnrichedEmployee,
  LemlistFailedLead,
  LemlistPushMeta,
  LemlistPushOutcome,
} from "../types/prospect";
import {
  createLeadInCampaign,
  getLemlistEmailCampaignIdsForUser,
  LemlistCreateLeadPayload,
} from "./lemlistClient";
import { SelectedUser } from "../shared/selectedUser";
import { EmailCampaignBucket, TaggedEmailCandidate } from "./emailCandidateWaterfall";

const RATE_LIMIT_REQUESTS = 20;
const RATE_LIMIT_WINDOW_MS = 2_000;
const DEFAULT_LEMLIST_QUERY = {
  deduplicate: false,
  linkedinEnrichment: false,
  findEmail: false,
  verifyEmail: false,
  findPhone: false,
};
const LEMLIST_ERROR_COLOR = "\x1b[31m";
const ANSI_RESET = "\x1b[0m";
const ALREADY_IN_CAMPAIGN_ERROR = "lead already in the campaign";

type QueueTask = () => Promise<void>;

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

  if (!employee.email || employee.email.trim().length === 0) {
    return null;
  }

  return {
    email: employee.email,
    firstName: parsedName.firstName,
    lastName: parsedName.lastName,
    companyName,
    companyDomain,
    jobTitle: employee.currentTitle,
    linkedinUrl: employee.linkedinUrl ?? undefined,
  };
}

function groupByBucket(
  candidates: TaggedEmailCandidate[]
): Record<EmailCampaignBucket, EnrichedEmployee[]> {
  const groups: Record<EmailCampaignBucket, EnrichedEmployee[]> = {
    sre: [],
    eng: [],
    engLead: [],
  };

  for (const { employee, campaignBucket } of candidates) {
    groups[campaignBucket].push(employee);
  }

  return groups;
}

function toEmployeeKey(employee: EnrichedEmployee): string {
  return employee.id ?? `${employee.name}|${employee.currentTitle}|${employee.linkedinUrl ?? ""}`;
}

function isAlreadyInCampaignError(message: string): boolean {
  return message.toLowerCase().includes(ALREADY_IN_CAMPAIGN_ERROR);
}

export async function pushPeopleToLemlistEmailCampaign(
  candidates: TaggedEmailCandidate[],
  companyName: string,
  companyDomain: string,
  selectedUser: SelectedUser
): Promise<LemlistPushMeta> {
  return new Promise<LemlistPushMeta>((resolve, reject) => {
    void enqueue(async () => {
      try {
        const campaignIds = getLemlistEmailCampaignIdsForUser(selectedUser);
        const grouped = groupByBucket(candidates);
        const failedItems: LemlistFailedLead[] = [];
        const successItems: string[] = [];
        const outcomes: LemlistPushOutcome[] = [];
        let successful = 0;
        let sentInWindow = 0;
        let windowStartedAt = Date.now();

        console.log(
          `[Lemlist][Email] Campaign routing (${companyName}) user=${selectedUser}: SRE=${grouped.sre.length}, ENG=${grouped.eng.length}, ENG_LEAD=${grouped.engLead.length}`
        );

        const buckets: Array<{ campaignId: string; people: EnrichedEmployee[] }> = [
          { campaignId: campaignIds.sreEmailCampaignId, people: grouped.sre },
          { campaignId: campaignIds.engEmailCampaignId, people: grouped.eng },
          { campaignId: campaignIds.engLeadEmailCampaignId, people: grouped.engLead },
        ];

        for (const bucket of buckets) {
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
              if (!employee.email || employee.email.trim().length === 0) {
                const failureMessage = "Missing email for Lemlist email campaign payload.";
                console.log(
                  `[EmailPush][MissingEmail] company=${companyName} person=${employee.name} title=${employee.currentTitle}`
                );
                failedItems.push({
                  name: employee.name,
                  error: failureMessage,
                });
                outcomes.push({
                  key: toEmployeeKey(employee),
                  name: employee.name,
                  title: employee.currentTitle,
                  linkedinUrl: employee.linkedinUrl ?? null,
                  status: "failed",
                  error: failureMessage,
                });
                continue;
              }

              const failureMessage = "Missing valid lead name for Lemlist payload.";
              failedItems.push({
                name: employee.name,
                error: failureMessage,
              });
              outcomes.push({
                key: toEmployeeKey(employee),
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                status: "failed",
                error: failureMessage,
              });
              continue;
            }

            try {
              await createLeadInCampaign(bucket.campaignId, payload, DEFAULT_LEMLIST_QUERY);
              successful += 1;
              successItems.push(employee.name);
              sentInWindow += 1;
              outcomes.push({
                key: toEmployeeKey(employee),
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                status: "succeed",
              });
            } catch (error) {
              const message = error instanceof Error ? error.message : "Unknown Lemlist push error.";
              if (isAlreadyInCampaignError(message)) {
                successful += 1;
                successItems.push(employee.name);
                outcomes.push({
                  key: toEmployeeKey(employee),
                  name: employee.name,
                  title: employee.currentTitle,
                  linkedinUrl: employee.linkedinUrl ?? null,
                  status: "succeed",
                });
                continue;
              }
              console.error(
                `${LEMLIST_ERROR_COLOR}[Lemlist][Email][ERROR] company=${companyName} person=${employee.name} campaign=${bucket.campaignId} message=${message}${ANSI_RESET}`
              );
              failedItems.push({
                name: employee.name,
                error: message,
              });
              outcomes.push({
                key: toEmployeeKey(employee),
                name: employee.name,
                title: employee.currentTitle,
                linkedinUrl: employee.linkedinUrl ?? null,
                status: "failed",
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
          outcomes,
        });
      } catch (error) {
        reject(error);
      }
    });
  });
}
