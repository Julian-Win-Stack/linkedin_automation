import { Router } from "express";
import { isSelectedUser } from "../shared/selectedUser";
import {
  getWeeklySuccessCounts,
  insertWeeklySuccessAdjustment,
} from "../services/weeklySuccessStore";

const router = Router();

function getAdminApiKey(): string {
  return process.env.ADMIN_API_KEY?.trim() ?? "";
}

function getWeekStartMsLocal(nowMs = Date.now()): number {
  const now = new Date(nowMs);
  now.setHours(0, 0, 0, 0);
  const dayOfWeek = now.getDay();
  const daysSinceSaturday = (dayOfWeek + 1) % 7;
  return now.getTime() - daysSinceSaturday * 24 * 60 * 60 * 1000;
}

router.post("/admin/adjust-weekly-counts", (req, res) => {
  const adminKey = getAdminApiKey();
  if (!adminKey) {
    return res.status(503).json({ error: "ADMIN_API_KEY is not configured on this server." });
  }

  const providedKey = typeof req.headers["x-admin-key"] === "string" ? req.headers["x-admin-key"] : "";
  if (providedKey !== adminKey) {
    return res.status(401).json({ error: "Invalid admin key." });
  }

  const {
    selectedUser: rawUser,
    targetLinkedinCount,
    targetCompaniesReachedOutToCount,
    weekStartMs: rawWeekStartMs,
  } = req.body ?? {};

  const normalizedUser = typeof rawUser === "string" ? rawUser.trim().toLowerCase() : "";
  if (!isSelectedUser(normalizedUser)) {
    return res.status(400).json({ error: "selectedUser must be one of: raihan, cherry, julian." });
  }

  if (typeof targetLinkedinCount !== "number" || !Number.isInteger(targetLinkedinCount) || targetLinkedinCount < 0) {
    return res.status(400).json({ error: "targetLinkedinCount must be a non-negative integer." });
  }

  if (
    typeof targetCompaniesReachedOutToCount !== "number" ||
    !Number.isInteger(targetCompaniesReachedOutToCount) ||
    targetCompaniesReachedOutToCount < 0
  ) {
    return res
      .status(400)
      .json({ error: "targetCompaniesReachedOutToCount must be a non-negative integer." });
  }

  const nowMs = Date.now();
  const weekStartMs =
    typeof rawWeekStartMs === "number" && Number.isFinite(rawWeekStartMs) && rawWeekStartMs >= 0
      ? rawWeekStartMs
      : getWeekStartMsLocal(nowMs);

  const current = getWeeklySuccessCounts({ selectedUser: normalizedUser, weekStartMs });
  const linkedinDelta = targetLinkedinCount - current.linkedinCount;
  const companiesReachedOutToDelta =
    targetCompaniesReachedOutToCount - current.companiesReachedOutToCount;

  if (linkedinDelta !== 0 || companiesReachedOutToDelta !== 0) {
    insertWeeklySuccessAdjustment({
      selectedUser: normalizedUser,
      linkedinDelta,
      companiesReachedOutToDelta,
      nowMs,
    });
  }

  return res.status(200).json({
    selectedUser: normalizedUser,
    previousLinkedinCount: current.linkedinCount,
    previousCompaniesReachedOutToCount: current.companiesReachedOutToCount,
    newLinkedinCount: targetLinkedinCount,
    newCompaniesReachedOutToCount: targetCompaniesReachedOutToCount,
    adjustedLinkedinBy: linkedinDelta,
    adjustedCompaniesReachedOutToBy: companiesReachedOutToDelta,
  });
});

export default router;
