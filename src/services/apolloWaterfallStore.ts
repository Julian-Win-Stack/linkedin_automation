interface WaterfallPersonPayload {
  id?: string | number;
  emails?: Array<{ email?: string } | string>;
  waterfall?: {
    emails?: Array<{
      emails?: string[];
      vendors?: Array<{
        emails?: string[];
      }>;
    }>;
  };
}

interface WaterfallWebhookPayload {
  request_id?: string | number;
  people?: WaterfallPersonPayload[];
}

interface PendingWaterfallRequest {
  requestId: string;
  personIds: Set<string>;
  createdAtMs: number;
  resolvedAtMs?: number;
  recoveredEmailsByPersonId: Map<string, string>;
}

const pendingRequests = new Map<string, PendingWaterfallRequest>();
const bufferedCallbacks = new Map<string, { payload: unknown; receivedAtMs: number }>();
const MAX_REQUEST_AGE_MS = 2 * 60 * 60 * 1000;
const WAIT_POLL_INTERVAL_MS = 500;

function cleanupStaleRequests(nowMs: number): void {
  for (const [requestId, request] of pendingRequests.entries()) {
    if (nowMs - request.createdAtMs > MAX_REQUEST_AGE_MS) {
      pendingRequests.delete(requestId);
    }
  }
  for (const [requestId, buffered] of bufferedCallbacks.entries()) {
    if (nowMs - buffered.receivedAtMs > MAX_REQUEST_AGE_MS) {
      bufferedCallbacks.delete(requestId);
    }
  }
}

function toApolloRequestId(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const record = payload as WaterfallWebhookPayload;
  if (typeof record.request_id === "string" || typeof record.request_id === "number") {
    return String(record.request_id);
  }

  return null;
}

function toPersonEmails(payload: unknown): Map<string, string> {
  const result = new Map<string, string>();
  if (!payload || typeof payload !== "object") {
    return result;
  }

  const record = payload as WaterfallWebhookPayload;
  if (!Array.isArray(record.people)) {
    return result;
  }

  for (const person of record.people) {
    const personId =
      typeof person?.id === "string" || typeof person?.id === "number"
        ? String(person.id).trim()
        : "";
    if (!personId) {
      continue;
    }

    const directEmails = Array.isArray(person.emails)
      ? person.emails
          .map((emailRecord) => {
            if (typeof emailRecord === "string") {
              return emailRecord.trim();
            }
            return typeof emailRecord?.email === "string" ? emailRecord.email.trim() : "";
          })
          .filter((email) => email.length > 0)
      : [];

    const waterfallEmails = Array.isArray(person.waterfall?.emails)
      ? person.waterfall.emails
          .flatMap((emailGroup) => {
            const groupEmails = Array.isArray(emailGroup.emails)
              ? emailGroup.emails.map((email) => (typeof email === "string" ? email.trim() : ""))
              : [];
            const vendorEmails = Array.isArray(emailGroup.vendors)
              ? emailGroup.vendors.flatMap((vendor) =>
                  Array.isArray(vendor.emails)
                    ? vendor.emails.map((email) =>
                        typeof email === "string" ? email.trim() : ""
                      )
                    : []
                )
              : [];
            return [...groupEmails, ...vendorEmails];
          })
          .filter((email) => email.length > 0)
      : [];

    const firstEmail = [...directEmails, ...waterfallEmails].find((email) => email.length > 0);

    if (firstEmail) {
      result.set(personId, firstEmail);
    }
  }

  return result;
}

export function registerPendingWaterfallRequest(
  requestId: string,
  personIds: string[]
): { appliedBufferedCallback: boolean; recoveredEmailCount: number } {
  const nowMs = Date.now();
  cleanupStaleRequests(nowMs);

  const normalizedPersonIds = personIds.map((id) => id.trim()).filter((id) => id.length > 0);
  pendingRequests.set(requestId, {
    requestId,
    personIds: new Set(normalizedPersonIds),
    createdAtMs: nowMs,
    recoveredEmailsByPersonId: new Map(),
  });

  const buffered = bufferedCallbacks.get(requestId);
  if (!buffered) {
    return { appliedBufferedCallback: false, recoveredEmailCount: 0 };
  }
  bufferedCallbacks.delete(requestId);
  const processed = processApolloWaterfallWebhook(buffered.payload);
  return {
    appliedBufferedCallback: true,
    recoveredEmailCount: processed.recoveredEmailCount,
  };
}

export function processApolloWaterfallWebhook(
  payload: unknown,
  trackedRequestId?: string
): {
  apolloRequestId: string | null;
  requestId: string | null;
  recoveredEmailCount: number;
} {
  const nowMs = Date.now();
  cleanupStaleRequests(nowMs);
  const apolloRequestId = toApolloRequestId(payload);
  const requestId = trackedRequestId?.trim() || apolloRequestId;
  if (!requestId) {
    return {
      apolloRequestId,
      requestId: null,
      recoveredEmailCount: 0,
    };
  }

  const request = pendingRequests.get(requestId);
  if (!request) {
    bufferedCallbacks.set(requestId, {
      payload,
      receivedAtMs: nowMs,
    });
    return {
      apolloRequestId,
      requestId,
      recoveredEmailCount: 0,
    };
  }

  const personEmails = toPersonEmails(payload);
  let recovered = 0;
  for (const [personId, email] of personEmails.entries()) {
    if (request.personIds.size > 0 && !request.personIds.has(personId)) {
      continue;
    }
    request.recoveredEmailsByPersonId.set(personId, email);
    recovered += 1;
  }
  request.resolvedAtMs = nowMs;

  return {
    apolloRequestId,
    requestId,
    recoveredEmailCount: recovered,
  };
}

export async function waitForWaterfallRequests(
  requestIds: string[],
  timeoutMs: number
): Promise<{ completedRequestCount: number; timedOut: boolean }> {
  const uniqueIds = [...new Set(requestIds)];
  if (uniqueIds.length === 0) {
    return {
      completedRequestCount: 0,
      timedOut: false,
    };
  }

  const startedAtMs = Date.now();
  const deadlineMs = startedAtMs + Math.max(timeoutMs, 0);
  let lastCompletedCount = -1;
  while (Date.now() <= deadlineMs) {
    const completed = uniqueIds.filter((requestId) => pendingRequests.get(requestId)?.resolvedAtMs != null);
    if (completed.length !== lastCompletedCount) {
      console.log(
        `[ApolloWaterfall] Wait progress: completed=${completed.length}/${uniqueIds.length}`
      );
      lastCompletedCount = completed.length;
    }
    if (completed.length === uniqueIds.length) {
      return {
        completedRequestCount: completed.length,
        timedOut: false,
      };
    }
    await new Promise((resolve) => setTimeout(resolve, WAIT_POLL_INTERVAL_MS));
  }

  const completedAtTimeout = uniqueIds.filter((requestId) => pendingRequests.get(requestId)?.resolvedAtMs != null);
  return {
    completedRequestCount: completedAtTimeout.length,
    timedOut: true,
  };
}

export function getRecoveredEmailsForRequests(requestIds: string[]): Map<string, string> {
  const merged = new Map<string, string>();
  const uniqueIds = [...new Set(requestIds)];
  for (const requestId of uniqueIds) {
    const request = pendingRequests.get(requestId);
    if (!request) {
      continue;
    }
    for (const [personId, email] of request.recoveredEmailsByPersonId.entries()) {
      if (!merged.has(personId)) {
        merged.set(personId, email);
      }
    }
    pendingRequests.delete(requestId);
  }
  return merged;
}

