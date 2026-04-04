import { randomUUID } from "node:crypto";
import { Router } from "express";
import multer from "multer";
import { loadPipelineConfig } from "../config/pipelineConfig";
import { createJob, getJob, markJobCancelled } from "../jobs/jobStore";
import { runResearchPipeline } from "../jobs/researchPipeline";
import { isSelectedUser, SelectedUser } from "../shared/selectedUser";
import { generateCampaignPdf } from "../services/pdfReportGenerator";
import {
  claimNextQueuedItemForUser,
  completeQueueItem,
  enqueueQueueItem,
  getQueueItemById,
  listQueueItemsForUser,
  recoverRunningItemsToQueued,
  setQueueItemJobId,
  toQueueLabel,
} from "../services/queueStore";
import { getWeeklySuccessCounts } from "../services/weeklySuccessStore";

const upload = multer({ storage: multer.memoryStorage() });
const router = Router();
const processingUsers = new Set<SelectedUser>();
let queueRecovered = false;

function getWeekStartMsLocal(nowMs = Date.now()): number {
  const now = new Date(nowMs);
  now.setHours(0, 0, 0, 0);
  const dayOfWeek = now.getDay();
  const daysSinceSaturday = (dayOfWeek + 1) % 7;
  return now.getTime() - daysSinceSaturday * 24 * 60 * 60 * 1000;
}

function ensureQueueRecovery(): void {
  if (queueRecovered) {
    return;
  }
  recoverRunningItemsToQueued();
  queueRecovered = true;
}

async function processUserQueue(selectedUser: SelectedUser): Promise<void> {
  const config = loadPipelineConfig();
  while (true) {
    const queueItem = claimNextQueuedItemForUser(selectedUser);
    if (!queueItem) {
      return;
    }

    const jobId = createJob();
    setQueueItemJobId(queueItem.queueItemId, jobId);

    await runResearchPipeline(jobId, queueItem.csvInput, config, selectedUser, queueItem.weekStartMs);

    const latestQueueItem = getQueueItemById(queueItem.queueItemId);
    if (!latestQueueItem || latestQueueItem.status === "cancelled") {
      continue;
    }
    const job = getJob(jobId);
    if (!job) {
      completeQueueItem(queueItem.queueItemId, {
        status: "error",
        errorMessage: "Job state not found after processing.",
      });
      continue;
    }
    if (job.status === "done") {
      completeQueueItem(queueItem.queueItemId, {
        status: "done",
        csvOutputBase64: job.csvBase64 ?? null,
        summary: job.summary ?? null,
        warnings: job.warnings,
        skippedCompanies: job.skippedCompanies,
        rejectedCompanies: job.rejectedCompanies,
        rejectedReason: job.rejectedReason ?? null,
        campaignPushData: job.campaignPushData ?? null,
      });
      continue;
    }
    if (job.status === "cancelled") {
      completeQueueItem(queueItem.queueItemId, {
        status: "cancelled",
        warnings: job.warnings,
        errorMessage: job.message ?? "Queue item cancelled.",
      });
      continue;
    }
    completeQueueItem(queueItem.queueItemId, {
      status: "error",
      summary: job.summary ?? null,
      warnings: job.warnings,
      skippedCompanies: job.skippedCompanies,
      rejectedCompanies: job.rejectedCompanies,
      rejectedReason: job.rejectedReason ?? null,
      errorMessage: job.error ?? "Queue item failed.",
      campaignPushData: job.campaignPushData ?? null,
    });
  }
}

function triggerUserQueue(selectedUser: SelectedUser): void {
  ensureQueueRecovery();
  if (processingUsers.has(selectedUser)) {
    return;
  }
  processingUsers.add(selectedUser);
  void processUserQueue(selectedUser).finally(() => {
    processingUsers.delete(selectedUser);
  });
}

router.post("/research", upload.single("csv"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No CSV file provided. Use form field 'csv'." });
  }

  try {
    const selectedUserRaw = typeof req.body?.selectedUser === "string" ? req.body.selectedUser : "";
    const normalizedSelectedUser = selectedUserRaw.trim().toLowerCase();
    if (!isSelectedUser(normalizedSelectedUser)) {
      return res.status(400).json({
        error: "selectedUser is required and must be one of: raihan, cherry, julian.",
      });
    }
    const selectedUser: SelectedUser = normalizedSelectedUser;

    const csvBuffer = req.file.buffer.toString("utf8");
    const config = loadPipelineConfig();
    const firstLine = csvBuffer.split("\n")[0] ?? "";
    const headers = firstLine.split(",").map((header) => header.trim().replace(/^"|"$/g, ""));
    const hasNameColumn = headers.includes(config.nameColumn);
    const hasDomainColumn = headers.includes(config.domainColumn);
    const hasApolloAccountIdColumn = headers.includes(config.apolloAccountIdColumn);

    if (!hasNameColumn || (!hasDomainColumn && !hasApolloAccountIdColumn)) {
      return res.status(400).json({
        error: `CSV must have "${config.nameColumn}" and at least one of "${config.domainColumn}" or "${config.apolloAccountIdColumn}". Found: ${headers.join(", ")}`,
      });
    }

    const weekStartMsRaw = typeof req.body?.weekStartMs === "string" ? req.body.weekStartMs : "";
    const parsedWeekStartMs = Number(weekStartMsRaw);
    const weekStartMs = Number.isFinite(parsedWeekStartMs) && parsedWeekStartMs >= 0
      ? parsedWeekStartMs
      : getWeekStartMsLocal();

    const queueItem = enqueueQueueItem({
      queueItemId: randomUUID(),
      selectedUser,
      csvInput: csvBuffer,
      weekStartMs,
    });
    triggerUserQueue(selectedUser);
    return res.status(200).json({
      queueItemId: queueItem.queueItemId,
      queueOrder: queueItem.queueOrder,
      queueLabel: toQueueLabel(queueItem.queueOrder),
      status: queueItem.status,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected server error.";
    if (message.includes("Queue limit reached")) {
      return res.status(409).json({ error: message });
    }
    return res.status(500).json({ error: message });
  }
});

router.get("/queue", (req, res) => {
  const selectedUserRaw = typeof req.query?.selectedUser === "string" ? req.query.selectedUser : "";
  const normalizedSelectedUser = selectedUserRaw.trim().toLowerCase();
  if (!isSelectedUser(normalizedSelectedUser)) {
    return res.status(400).json({
      error: "selectedUser is required and must be one of: raihan, cherry, julian.",
    });
  }
  const selectedUser: SelectedUser = normalizedSelectedUser;
  ensureQueueRecovery();
  const items = listQueueItemsForUser(selectedUser).map((item) => {
    const runningJob = item.jobId ? getJob(item.jobId) : undefined;
    const liveStatus = item.status === "running" && runningJob?.status === "error" ? "error" : item.status;
    return {
      queueItemId: item.queueItemId,
      queueOrder: item.queueOrder,
      queueLabel: toQueueLabel(item.queueOrder),
      status: liveStatus,
      createdAtMs: item.createdAtMs,
      updatedAtMs: item.updatedAtMs,
      startedAtMs: item.startedAtMs,
      completedAtMs: item.completedAtMs,
      summary: item.summary,
      warnings: item.warnings,
      skippedCompanies: item.skippedCompanies,
      rejectedCompanies: item.rejectedCompanies,
      rejectedReason: item.rejectedReason,
      errorMessage: item.errorMessage ?? runningJob?.error ?? null,
      progressMessage: item.status === "running" ? (runningJob?.message ?? null) : null,
      currentRow: item.status === "running" ? (runningJob?.currentRow ?? null) : null,
      totalRows: item.status === "running" ? (runningJob?.totalRows ?? null) : null,
      hasCsv: Boolean(item.csvOutputBase64),
      hasPdf: Boolean(item.campaignPushData),
    };
  });
  return res.status(200).json({ items });
});

router.get("/status/:jobId", (req, res) => {
  const job = getJob(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }

  if (job.status === "done") {
    return res.status(200).json({
      status: "done",
      csv: job.csvBase64 ?? "",
      warnings: job.warnings,
      skippedCompanies: job.skippedCompanies,
      rejectedCompanies: job.rejectedCompanies,
      rejectedReason: job.rejectedReason,
      summary: job.summary,
    });
  }

  if (job.status === "error") {
    return res.status(200).json({
      status: "error",
      error: job.error ?? "Unknown error",
    });
  }

  if (job.status === "cancelled") {
    return res.status(200).json({
      status: "error",
      error: job.message ?? "Job was cancelled",
    });
  }

  return res.status(200).json({
    status: job.status,
    message: job.message,
    totalRows: job.totalRows,
    currentRow: job.currentRow,
    warnings: job.warnings,
  });
});

router.get("/weekly-counts", (req, res) => {
  const selectedUserRaw = typeof req.query?.selectedUser === "string" ? req.query.selectedUser : "";
  const normalizedSelectedUser = selectedUserRaw.trim().toLowerCase();
  if (!isSelectedUser(normalizedSelectedUser)) {
    return res.status(400).json({
      error: "selectedUser is required and must be one of: raihan, cherry, julian.",
    });
  }
  const selectedUser: SelectedUser = normalizedSelectedUser;

  const weekStartMsRaw = typeof req.query?.weekStartMs === "string" ? req.query.weekStartMs : "";
  const weekStartMs = Number(weekStartMsRaw);
  if (!Number.isFinite(weekStartMs) || weekStartMs < 0) {
    return res.status(400).json({
      error: "weekStartMs query param is required and must be a valid timestamp in milliseconds.",
    });
  }

  const counts = getWeeklySuccessCounts({
    selectedUser,
    weekStartMs,
  });

  return res.status(200).json(counts);
});

router.post("/cancel/:jobId", (req, res) => {
  const job = getJob(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }
  if (job.status === "done" || job.status === "error" || job.status === "cancelled") {
    return res.status(409).json({ error: `Cannot cancel a job in status "${job.status}"` });
  }

  markJobCancelled(req.params.jobId);
  return res.status(200).json({ status: "cancelled" });
});

router.post("/queue/:queueItemId/cancel", (req, res) => {
  const item = getQueueItemById(req.params.queueItemId);
  if (!item) {
    return res.status(404).json({ error: "Queue item not found" });
  }
  if (item.status === "done" || item.status === "error" || item.status === "cancelled") {
    return res.status(409).json({ error: `Cannot cancel a queue item in status "${item.status}"` });
  }

  if (item.status === "running" && item.jobId) {
    markJobCancelled(item.jobId);
  }
  completeQueueItem(item.queueItemId, {
    status: "cancelled",
    warnings: item.warnings,
    skippedCompanies: item.skippedCompanies,
    rejectedCompanies: item.rejectedCompanies,
    rejectedReason: item.rejectedReason,
    errorMessage: "Queue item cancelled by user.",
  });
  triggerUserQueue(item.selectedUser);
  return res.status(200).json({ status: "cancelled" });
});

router.get("/pdf/:jobId", (req, res) => {
  const job = getJob(req.params.jobId);
  if (!job) {
    return res.status(404).json({ error: "Job not found" });
  }
  if (job.status !== "done" || !job.campaignPushData) {
    return res.status(400).json({ error: "PDF is not available for this job." });
  }

  const doc = generateCampaignPdf(job.campaignPushData);

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", 'attachment; filename="campaign-push-report.pdf"');
  doc.pipe(res);
  doc.end();
});

router.get("/queue/:queueItemId/csv", (req, res) => {
  const item = getQueueItemById(req.params.queueItemId);
  if (!item) {
    return res.status(404).json({ error: "Queue item not found" });
  }
  if (!item.csvOutputBase64) {
    return res.status(400).json({ error: "CSV is not available for this queue item." });
  }
  const csv = Buffer.from(item.csvOutputBase64, "base64");
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", `attachment; filename="${toQueueLabel(item.queueOrder)}.csv"`);
  return res.status(200).send(csv);
});

router.get("/queue/:queueItemId/pdf", (req, res) => {
  const item = getQueueItemById(req.params.queueItemId);
  if (!item) {
    return res.status(404).json({ error: "Queue item not found" });
  }
  if (!item.campaignPushData) {
    return res.status(400).json({ error: "PDF is not available for this queue item." });
  }
  const doc = generateCampaignPdf(item.campaignPushData);
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", `attachment; filename="${toQueueLabel(item.queueOrder)}.pdf"`);
  doc.pipe(res);
  doc.end();
});

export default router;
