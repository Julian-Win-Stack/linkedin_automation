import { EnrichedEmployee } from "../types/prospect";
import { bulkEnrichData, getEnrichmentResult, LemlistBulkEnrichmentRequest } from "./lemlistClient";

const BULK_MAX_REQUESTS = 500;
const POLL_BATCH_SIZE = 20;
const POLL_INTERVAL_MS = 2_000;
const POLL_TIMEOUT_MS = 2 * 60 * 1000;

export interface MissingEmailCandidate {
  employee: EnrichedEmployee;
  companyName: string;
  companyDomain: string;
}

export interface LemlistEmailEnrichmentSummary {
  attempted: number;
  accepted: number;
  recovered: number;
  notFound: number;
}

interface PendingEnrichment {
  enrichId: string;
  candidate: MissingEmailCandidate;
}

function splitName(name: string): { firstName: string; lastName: string } | null {
  const trimmed = name.trim();
  if (!trimmed) {
    return null;
  }

  const parts = trimmed.split(/\s+/);
  if (parts.length < 2) {
    return null;
  }

  return {
    firstName: parts[0],
    lastName: parts.slice(1).join(" "),
  };
}

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function extractFoundEmail(payload: unknown): { email: string | null; done: boolean; notFound: boolean } {
  if (!payload || typeof payload !== "object") {
    return { email: null, done: false, notFound: false };
  }

  const record = payload as Record<string, unknown>;
  const status = typeof record.enrichmentStatus === "string" ? record.enrichmentStatus.toLowerCase() : "";
  if (status !== "done") {
    return { email: null, done: false, notFound: false };
  }

  const data = record.data;
  if (!data || typeof data !== "object") {
    return { email: null, done: true, notFound: true };
  }

  const dataRecord = data as Record<string, unknown>;
  const directEmail = dataRecord.email;
  if (directEmail && typeof directEmail === "object") {
    const emailRecord = directEmail as Record<string, unknown>;
    const found = typeof emailRecord.email === "string" ? emailRecord.email.trim() : "";
    if (found.length > 0) {
      return { email: found, done: true, notFound: false };
    }
    if (emailRecord.notFound === true) {
      return { email: null, done: true, notFound: true };
    }
  }

  const findEmail = dataRecord.find_email;
  if (findEmail && typeof findEmail === "object") {
    const findEmailRecord = findEmail as Record<string, unknown>;
    const found = typeof findEmailRecord.email === "string" ? findEmailRecord.email.trim() : "";
    if (found.length > 0) {
      return { email: found, done: true, notFound: false };
    }
    const findEmailStatus =
      typeof findEmailRecord.status === "string" ? findEmailRecord.status.toLowerCase() : "";
    if (findEmailStatus.includes("not_found") || findEmailStatus.includes("not found")) {
      return { email: null, done: true, notFound: true };
    }
  }

  return { email: null, done: true, notFound: true };
}

export async function enrichMissingEmailsWithLemlist(
  candidates: MissingEmailCandidate[]
): Promise<LemlistEmailEnrichmentSummary> {
  const pendingByMetadata = new Map<string, MissingEmailCandidate>();
  const pendingByEnrichId = new Map<string, MissingEmailCandidate>();
  let attempted = 0;
  let accepted = 0;
  let recovered = 0;
  let notFound = 0;

  const requests: LemlistBulkEnrichmentRequest[] = [];
  for (const candidate of candidates) {
    const employee = candidate.employee;
    if (employee.email?.trim()) {
      continue;
    }

    const names = splitName(employee.name);
    if (!names || !candidate.companyName.trim() || !candidate.companyDomain.trim()) {
      continue;
    }

    const metadataId = `${employee.id ?? `${employee.name}:${candidate.companyDomain}`}:${attempted}`;
    attempted += 1;
    pendingByMetadata.set(metadataId, candidate);
    requests.push({
      input: {
        firstName: names.firstName,
        lastName: names.lastName,
        companyName: candidate.companyName.trim(),
        companyDomain: candidate.companyDomain.trim(),
      },
      enrichmentRequests: ["find_email"],
      metadata: { metadataId },
    });
  }

  if (requests.length === 0) {
    return { attempted: 0, accepted: 0, recovered: 0, notFound: 0 };
  }

  for (const requestBatch of chunkArray(requests, BULK_MAX_REQUESTS)) {
    const responseItems = await bulkEnrichData(requestBatch);
    for (const item of responseItems) {
      const metadata =
        item.metadata && typeof item.metadata === "object"
          ? (item.metadata as Record<string, unknown>)
          : null;
      const metadataId = typeof metadata?.metadataId === "string" ? metadata.metadataId : "";
      if (!metadataId) {
        continue;
      }

      const candidate = pendingByMetadata.get(metadataId);
      if (!candidate) {
        continue;
      }

      if (item.error || !item.id) {
        notFound += 1;
        continue;
      }

      accepted += 1;
      pendingByEnrichId.set(item.id, candidate);
    }
  }

  const deadline = Date.now() + POLL_TIMEOUT_MS;
  while (pendingByEnrichId.size > 0 && Date.now() < deadline) {
    const currentBatch = [...pendingByEnrichId.entries()].slice(0, POLL_BATCH_SIZE);
    await Promise.all(
      currentBatch.map(async ([enrichId, candidate]) => {
        try {
          const result = await getEnrichmentResult(enrichId, 0);
          const extracted = extractFoundEmail(result);
          if (!extracted.done) {
            return;
          }

          pendingByEnrichId.delete(enrichId);
          if (extracted.email) {
            candidate.employee.email = extracted.email;
            recovered += 1;
            return;
          }

          if (extracted.notFound) {
            notFound += 1;
          }
        } catch {
          // Keep pending and retry until timeout window.
        }
      })
    );

    if (pendingByEnrichId.size > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
    }
  }

  notFound += pendingByEnrichId.size;
  return { attempted, accepted, recovered, notFound };
}
