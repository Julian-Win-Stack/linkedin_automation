import express from "express";
import { processApolloWaterfallWebhook } from "../services/apolloWaterfallStore";

const apolloWebhookRouter = express.Router();
const seenRequestIds = new Map<string, number>();
const MAX_CACHE_AGE_MS = 60 * 60 * 1000;

function cleanupSeenRequestIds(nowMs: number): void {
  for (const [requestId, timestampMs] of seenRequestIds.entries()) {
    if (nowMs - timestampMs > MAX_CACHE_AGE_MS) {
      seenRequestIds.delete(requestId);
    }
  }
}

function toRequestId(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") {
    return null;
  }

  const record = payload as Record<string, unknown>;
  const rawRequestId = record.request_id;
  if (typeof rawRequestId === "string" || typeof rawRequestId === "number") {
    return String(rawRequestId);
  }

  return null;
}

function toWebhookSummary(payload: unknown): string {
  if (!payload || typeof payload !== "object") {
    return "summary=unavailable";
  }

  const record = payload as Record<string, unknown>;
  const totalRequested =
    typeof record.total_requested_enrichments === "number"
      ? record.total_requested_enrichments
      : "n/a";
  const recordsEnriched =
    typeof record.records_enriched === "number" ? record.records_enriched : "n/a";
  const emailEnriched =
    typeof record.email_records_enriched === "number"
      ? record.email_records_enriched
      : "n/a";
  const emailNotFound =
    typeof record.email_records_not_found === "number"
      ? record.email_records_not_found
      : "n/a";
  const status = typeof record.status === "string" ? record.status : "n/a";

  return `status=${status} total_requested=${totalRequested} records_enriched=${recordsEnriched} email_records_enriched=${emailEnriched} email_records_not_found=${emailNotFound}`;
}

apolloWebhookRouter.post("/webhooks/apollo/waterfall", (req, res) => {
  const configuredSecret = process.env.APOLLO_WEBHOOK_SECRET?.trim();
  if (configuredSecret) {
    const token = typeof req.query.token === "string" ? req.query.token.trim() : "";
    if (token !== configuredSecret) {
      return res.status(401).json({ error: "Invalid Apollo webhook token." });
    }
  }

  const nowMs = Date.now();
  cleanupSeenRequestIds(nowMs);
  const requestId = toRequestId(req.body);
  const trackedRequestId =
    typeof req.query.client_req_id === "string" ? req.query.client_req_id.trim() : "";
  const dedupeKey = trackedRequestId || requestId || "";

  if (dedupeKey) {
    const alreadySeen = seenRequestIds.has(dedupeKey);
    seenRequestIds.set(dedupeKey, nowMs);
    if (alreadySeen) {
      console.log(
        `[ApolloWebhook] Duplicate waterfall callback ignored. dedupe_key=${dedupeKey} apollo_request_id=${requestId ?? "none"} client_req_id=${trackedRequestId || "none"}`
      );
      return res.status(200).json({ ok: true, duplicate: true });
    }
    const processed = processApolloWaterfallWebhook(req.body, trackedRequestId || undefined);
    console.log(
      `[ApolloWebhook] Waterfall callback received. request_id=${processed.requestId ?? "none"} apollo_request_id=${processed.apolloRequestId ?? "none"} client_req_id=${trackedRequestId || "none"} recovered_emails=${processed.recoveredEmailCount}`
    );
    console.log(`[ApolloWebhook] ${toWebhookSummary(req.body)}`);
  } else {
    console.log("[ApolloWebhook] Waterfall callback received without request_id.");
  }

  return res.status(200).json({ ok: true });
});

export default apolloWebhookRouter;
