import { Router } from "express";
import multer from "multer";
import { loadPipelineConfig } from "../config/pipelineConfig";
import { createJob, getJob, markJobCancelled } from "../jobs/jobStore";
import { runResearchPipeline } from "../jobs/researchPipeline";

const upload = multer({ storage: multer.memoryStorage() });
const router = Router();

router.post("/research", upload.single("csv"), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: "No CSV file provided. Use form field 'csv'." });
  }

  try {
    const csvBuffer = req.file.buffer.toString("utf8");
    const config = loadPipelineConfig();
    const firstLine = csvBuffer.split("\n")[0] ?? "";
    const headers = firstLine.split(",").map((header) => header.trim().replace(/^"|"$/g, ""));
    if (!headers.includes(config.nameColumn) || !headers.includes(config.domainColumn)) {
      return res.status(400).json({
        error: `CSV must have columns "${config.nameColumn}" and "${config.domainColumn}". Found: ${headers.join(", ")}`,
      });
    }

    const jobId = createJob();
    void runResearchPipeline(jobId, csvBuffer, config);
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

export default router;
