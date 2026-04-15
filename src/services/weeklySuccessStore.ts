import path from "node:path";
import fs from "node:fs";
import Database from "better-sqlite3";
import type { SelectedUser } from "../shared/selectedUser";

const DEFAULT_DB_DIR = path.join(process.cwd(), "data");
const DEFAULT_DB_PATH = path.join(DEFAULT_DB_DIR, "weekly-success.sqlite");

interface WeeklySuccessRow {
  selected_user: SelectedUser;
  completed_at_ms: number;
  linkedin_success_count: number;
  companies_reached_out_to: number;
}

let db: Database.Database | null = null;

function getDbPath(): string {
  const rawPath = process.env.WEEKLY_SUCCESS_SQLITE_PATH?.trim();
  return rawPath && rawPath.length > 0 ? rawPath : DEFAULT_DB_PATH;
}

function ensureDb(): Database.Database {
  if (db) {
    return db;
  }

  const dbPath = getDbPath();
  const dir = path.dirname(dbPath);
  fs.mkdirSync(dir, { recursive: true });

  const instance = new Database(dbPath);
  instance.pragma("journal_mode = WAL");
  instance.exec(`
    CREATE TABLE IF NOT EXISTS weekly_success_job (
      job_id TEXT PRIMARY KEY,
      selected_user TEXT NOT NULL,
      completed_at_ms INTEGER NOT NULL,
      linkedin_success_count INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_weekly_success_user_time
      ON weekly_success_job(selected_user, completed_at_ms);
  `);
  try {
    instance.exec(
      `ALTER TABLE weekly_success_job ADD COLUMN companies_reached_out_to INTEGER NOT NULL DEFAULT 0`
    );
  } catch {
    // column already exists
  }
  try {
    instance.exec(`ALTER TABLE weekly_success_job DROP COLUMN email_success_count`);
  } catch {
    // column already dropped or never existed
  }
  db = instance;
  return instance;
}

export function saveWeeklySuccessForJob(input: {
  jobId: string;
  selectedUser: SelectedUser;
  completedAtMs: number;
  linkedinSuccessCount: number;
  companiesReachedOutToCount: number;
}): void {
  const instance = ensureDb();
  const stmt = instance.prepare(`
    INSERT INTO weekly_success_job (
      job_id,
      selected_user,
      completed_at_ms,
      linkedin_success_count,
      companies_reached_out_to
    )
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(job_id) DO UPDATE SET
      selected_user = excluded.selected_user,
      completed_at_ms = excluded.completed_at_ms,
      linkedin_success_count = excluded.linkedin_success_count,
      companies_reached_out_to = excluded.companies_reached_out_to
  `);

  stmt.run(
    input.jobId,
    input.selectedUser,
    input.completedAtMs,
    input.linkedinSuccessCount,
    input.companiesReachedOutToCount
  );
}

export function getWeeklySuccessCounts(input: {
  selectedUser: SelectedUser;
  weekStartMs: number;
}): { linkedinCount: number; companiesReachedOutToCount: number } {
  const instance = ensureDb();
  const weekEndExclusiveMs = input.weekStartMs + 7 * 24 * 60 * 60 * 1000;
  const stmt = instance.prepare<
    [SelectedUser, number, number],
    {
      linkedin_total: number | null;
      companies_reached_out_to_total: number | null;
    }
  >(`
    SELECT
      SUM(linkedin_success_count) AS linkedin_total,
      SUM(companies_reached_out_to) AS companies_reached_out_to_total
    FROM weekly_success_job
    WHERE selected_user = ?
      AND completed_at_ms >= ?
      AND completed_at_ms < ?
  `);
  const row = stmt.get(input.selectedUser, input.weekStartMs, weekEndExclusiveMs);

  return {
    linkedinCount: Number(row?.linkedin_total ?? 0),
    companiesReachedOutToCount: Number(row?.companies_reached_out_to_total ?? 0),
  };
}

export function insertWeeklySuccessAdjustment(input: {
  selectedUser: SelectedUser;
  linkedinDelta: number;
  companiesReachedOutToDelta: number;
  nowMs: number;
}): void {
  const instance = ensureDb();
  const stmt = instance.prepare(`
    INSERT INTO weekly_success_job (
      job_id,
      selected_user,
      completed_at_ms,
      linkedin_success_count,
      companies_reached_out_to
    )
    VALUES (?, ?, ?, ?, ?)
  `);
  stmt.run(
    `manual-adj-${input.nowMs}-${Math.random().toString(36).slice(2, 8)}`,
    input.selectedUser,
    input.nowMs,
    input.linkedinDelta,
    input.companiesReachedOutToDelta
  );
}

export function __resetWeeklySuccessStoreForTests(): void {
  if (!db) {
    return;
  }
  db.close();
  db = null;
}
