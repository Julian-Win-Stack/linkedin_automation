import { Router } from "express";
import multer from "multer";
import { loadPipelineConfig } from "../config/pipelineConfig";
import { createJob, getJob, markJobCancelled } from "../jobs/jobStore";
import { runResearchPipeline } from "../jobs/researchPipeline";
import { isSelectedUser, SelectedUser } from "../shared/selectedUser";
import { generateCampaignPdf } from "../services/pdfReportGenerator";

const upload = multer({ storage: multer.memoryStorage() });
const router = Router();

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

    const jobId = createJob();
    void runResearchPipeline(jobId, csvBuffer, config, selectedUser);
    return res.status(200).json({ jobId });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected server error.";
    return res.status(500).json({ error: message });
  }
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
      rejectsCsv: job.rejectsCsvBase64 ?? "",
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

export default router;
