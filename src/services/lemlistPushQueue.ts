import { EnrichedEmployee, LemlistFailedLead, LemlistPushMeta } from "../types/prospect";
import { createLeadInCampaign, getLemlistCampaignId, LemlistCreateLeadPayload } from "./lemlistClient";

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

export async function pushPeopleToLemlistCampaign(
  employees: EnrichedEmployee[],
  companyName: string,
  companyDomain: string
): Promise<LemlistPushMeta> {
  return new Promise<LemlistPushMeta>((resolve, reject) => {
    void enqueue(async () => {
      try {
        const campaignId = getLemlistCampaignId();
        const failedItems: LemlistFailedLead[] = [];
        const successItems: string[] = [];
        let successful = 0;
        let sentInWindow = 0;
        let windowStartedAt = Date.now();

        for (const employee of employees) {
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
            await createLeadInCampaign(campaignId, payload, DEFAULT_LEMLIST_QUERY);
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

        resolve({
          attempted: employees.length,
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
