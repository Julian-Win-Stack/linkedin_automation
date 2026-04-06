import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import type { SelectedUser } from "../shared/selectedUser";
import type { CampaignPushData, JobSummary } from "../jobs/jobStore";

const DEFAULT_DB_DIR = path.join(process.cwd(), "data");
const DEFAULT_DB_PATH = path.join(DEFAULT_DB_DIR, "weekly-success.sqlite");
const MAX_ACTIVE_QUEUE_ITEMS_PER_USER = 10;

export type QueueItemStatus = "queued" | "running" | "done" | "error" | "cancelled";

type QueueItemRow = {
  queue_item_id: string;
  selected_user: SelectedUser;
  queue_order: number;
  status: QueueItemStatus;
  week_start_ms: number;
  csv_input: string;
  job_id: string | null;
  csv_output_base64: string | null;
  summary_json: string | null;
  warnings_json: string | null;
  skipped_companies_json: string | null;
  rejected_companies_json: string | null;
  rejected_reason: string | null;
  error_message: string | null;
  campaign_push_data_json: string | null;
  created_at_ms: number;
  updated_at_ms: number;
  started_at_ms: number | null;
  completed_at_ms: number | null;
};

export type QueueItem = {
  queueItemId: string;
  selectedUser: SelectedUser;
  queueOrder: number;
  status: QueueItemStatus;
  weekStartMs: number;
  csvInput: string;
  jobId: string | null;
  csvOutputBase64: string | null;
  summary: JobSummary | null;
  warnings: string[];
  skippedCompanies: string[];
  rejectedCompanies: string[];
  rejectedReason: string | null;
  errorMessage: string | null;
  campaignPushData: CampaignPushData | null;
  createdAtMs: number;
  updatedAtMs: number;
  startedAtMs: number | null;
  completedAtMs: number | null;
};

let db: Database.Database | null = null;

function getDbPath(): string {
  const rawPath = process.env.WEEKLY_SUCCESS_SQLITE_PATH?.trim();
  return rawPath && rawPath.length > 0 ? rawPath : DEFAULT_DB_PATH;
}

function parseJson<T>(value: string | null): T | null {
  if (!value) {
    return null;
  }
  try {
    return JSON.parse(value) as T;
  } catch {
    return null;
  }
}

function rowToQueueItem(row: QueueItemRow): QueueItem {
  return {
    queueItemId: row.queue_item_id,
    selectedUser: row.selected_user,
    queueOrder: row.queue_order,
    status: row.status,
    weekStartMs: row.week_start_ms,
    csvInput: row.csv_input,
    jobId: row.job_id,
    csvOutputBase64: row.csv_output_base64,
    summary: parseJson<JobSummary>(row.summary_json),
    warnings: parseJson<string[]>(row.warnings_json) ?? [],
    skippedCompanies: parseJson<string[]>(row.skipped_companies_json) ?? [],
    rejectedCompanies: parseJson<string[]>(row.rejected_companies_json) ?? [],
    rejectedReason: row.rejected_reason,
    errorMessage: row.error_message,
    campaignPushData: parseJson<CampaignPushData>(row.campaign_push_data_json),
    createdAtMs: row.created_at_ms,
    updatedAtMs: row.updated_at_ms,
    startedAtMs: row.started_at_ms,
    completedAtMs: row.completed_at_ms,
  };
}

function ensureDb(): Database.Database {
  if (db) {
    return db;
  }
  const dbPath = getDbPath();
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
  const instance = new Database(dbPath);
  instance.pragma("journal_mode = WAL");
  instance.exec(`
    CREATE TABLE IF NOT EXISTS queue_items (
      queue_item_id TEXT PRIMARY KEY,
      selected_user TEXT NOT NULL,
      queue_order INTEGER NOT NULL,
      status TEXT NOT NULL,
      week_start_ms INTEGER NOT NULL,
      csv_input TEXT NOT NULL,
      job_id TEXT,
      csv_output_base64 TEXT,
      summary_json TEXT,
      warnings_json TEXT,
      skipped_companies_json TEXT,
      rejected_companies_json TEXT,
      rejected_reason TEXT,
      error_message TEXT,
      campaign_push_data_json TEXT,
      created_at_ms INTEGER NOT NULL,
      updated_at_ms INTEGER NOT NULL,
      started_at_ms INTEGER,
      completed_at_ms INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_queue_items_user_status
      ON queue_items(selected_user, status, queue_order);
  `);
  db = instance;
  return instance;
}

export function toQueueLabel(queueOrder: number): string {
  const rem10 = queueOrder % 10;
  const rem100 = queueOrder % 100;
  let suffix = "th";
  if (rem10 === 1 && rem100 !== 11) suffix = "st";
  else if (rem10 === 2 && rem100 !== 12) suffix = "nd";
  else if (rem10 === 3 && rem100 !== 13) suffix = "rd";
  return `${queueOrder}${suffix} queue`;
}

export function enqueueQueueItem(input: {
  queueItemId: string;
  selectedUser: SelectedUser;
  csvInput: string;
  weekStartMs: number;
}): QueueItem {
  const instance = ensureDb();
  const nowMs = Date.now();
  const tx = instance.transaction(() => {
    const activeCountStmt = instance.prepare<[SelectedUser], { count: number }>(`
      SELECT COUNT(*) as count
      FROM queue_items
      WHERE selected_user = ?
        AND status IN ('queued', 'running')
    `);
    const activeCount = Number(activeCountStmt.get(input.selectedUser)?.count ?? 0);
    if (activeCount >= MAX_ACTIVE_QUEUE_ITEMS_PER_USER) {
      throw new Error(`Queue limit reached (${MAX_ACTIVE_QUEUE_ITEMS_PER_USER}) for ${input.selectedUser}.`);
    }

    const nextOrderStmt = instance.prepare<[SelectedUser], { next_order: number }>(`
      SELECT COALESCE(MAX(queue_order), 0) + 1 AS next_order
      FROM queue_items
      WHERE selected_user = ?
    `);
    const queueOrder = Number(nextOrderStmt.get(input.selectedUser)?.next_order ?? 1);

    const insertStmt = instance.prepare(`
      INSERT INTO queue_items (
        queue_item_id,
        selected_user,
        queue_order,
        status,
        week_start_ms,
        csv_input,
        created_at_ms,
        updated_at_ms
      )
      VALUES (?, ?, ?, 'queued', ?, ?, ?, ?)
    `);
    insertStmt.run(
      input.queueItemId,
      input.selectedUser,
      queueOrder,
      input.weekStartMs,
      input.csvInput,
      nowMs,
      nowMs
    );
  });
  tx();

  const created = getQueueItemById(input.queueItemId);
  if (!created) {
    throw new Error("Failed to create queue item.");
  }
  return created;
}

export function recoverRunningItemsToQueued(): number {
  const instance = ensureDb();
  const nowMs = Date.now();
  const stmt = instance.prepare(`
    UPDATE queue_items
    SET status = 'queued',
        updated_at_ms = ?,
        job_id = NULL,
        started_at_ms = NULL
    WHERE status = 'running'
  `);
  const result = stmt.run(nowMs);
  return Number(result.changes ?? 0);
}

export function claimNextQueuedItemForUser(selectedUser: SelectedUser): QueueItem | null {
  const instance = ensureDb();
  const nowMs = Date.now();
  const tx = instance.transaction(() => {
    const hasRunningStmt = instance.prepare<[SelectedUser], { count: number }>(`
      SELECT COUNT(*) as count
      FROM queue_items
      WHERE selected_user = ?
        AND status = 'running'
    `);
    const runningCount = Number(hasRunningStmt.get(selectedUser)?.count ?? 0);
    if (runningCount > 0) {
      return null;
    }

    const nextStmt = instance.prepare<[SelectedUser], QueueItemRow>(`
      SELECT *
      FROM queue_items
      WHERE selected_user = ?
        AND status = 'queued'
      ORDER BY queue_order ASC
      LIMIT 1
    `);
    const row = nextStmt.get(selectedUser);
    if (!row) {
      return null;
    }

    const updateStmt = instance.prepare(`
      UPDATE queue_items
      SET status = 'running',
          updated_at_ms = ?,
          started_at_ms = ?
      WHERE queue_item_id = ?
    `);
    updateStmt.run(nowMs, nowMs, row.queue_item_id);

    const selectedStmt = instance.prepare<[string], QueueItemRow>(`
      SELECT * FROM queue_items WHERE queue_item_id = ?
    `);
    return selectedStmt.get(row.queue_item_id) ?? null;
  });

  const claimed = tx();
  return claimed ? rowToQueueItem(claimed) : null;
}

export function setQueueItemJobId(queueItemId: string, jobId: string): void {
  const instance = ensureDb();
  const stmt = instance.prepare(`
    UPDATE queue_items
    SET job_id = ?, updated_at_ms = ?
    WHERE queue_item_id = ?
  `);
  stmt.run(jobId, Date.now(), queueItemId);
}

export function completeQueueItem(queueItemId: string, input: {
  status: "done" | "error" | "cancelled";
  csvOutputBase64?: string | null;
  summary?: JobSummary | null;
  warnings?: string[];
  skippedCompanies?: string[];
  rejectedCompanies?: string[];
  rejectedReason?: string | null;
  errorMessage?: string | null;
  campaignPushData?: CampaignPushData | null;
}): void {
  const instance = ensureDb();
  const nowMs = Date.now();
  const stmt = instance.prepare(`
    UPDATE queue_items
    SET status = ?,
        csv_output_base64 = ?,
        summary_json = ?,
        warnings_json = ?,
        skipped_companies_json = ?,
        rejected_companies_json = ?,
        rejected_reason = ?,
        error_message = ?,
        campaign_push_data_json = ?,
        updated_at_ms = ?,
        completed_at_ms = ?
    WHERE queue_item_id = ?
  `);
  stmt.run(
    input.status,
    input.csvOutputBase64 ?? null,
    input.summary ? JSON.stringify(input.summary) : null,
    JSON.stringify(input.warnings ?? []),
    JSON.stringify(input.skippedCompanies ?? []),
    JSON.stringify(input.rejectedCompanies ?? []),
    input.rejectedReason ?? null,
    input.errorMessage ?? null,
    input.campaignPushData ? JSON.stringify(input.campaignPushData) : null,
    nowMs,
    nowMs,
    queueItemId
  );
}

export function listQueueItemsForUser(selectedUser: SelectedUser): QueueItem[] {
  const instance = ensureDb();
  const stmt = instance.prepare<[SelectedUser], QueueItemRow>(`
    SELECT *
    FROM queue_items
    WHERE selected_user = ?
    ORDER BY queue_order ASC
  `);
  return stmt.all(selectedUser).map(rowToQueueItem);
}

export function clearFinishedQueueItemsForUser(selectedUser: SelectedUser): number {
  const instance = ensureDb();
  const stmt = instance.prepare<[SelectedUser]>(`
    DELETE FROM queue_items
    WHERE selected_user = ?
      AND status IN ('done', 'error')
  `);
  const result = stmt.run(selectedUser);
  return Number(result.changes ?? 0);
}

export function getQueueItemById(queueItemId: string): QueueItem | null {
  const instance = ensureDb();
  const stmt = instance.prepare<[string], QueueItemRow>(`
    SELECT *
    FROM queue_items
    WHERE queue_item_id = ?
    LIMIT 1
  `);
  const row = stmt.get(queueItemId);
  return row ? rowToQueueItem(row) : null;
}

export function getQueueItemByJobId(jobId: string): QueueItem | null {
  const instance = ensureDb();
  const stmt = instance.prepare<[string], QueueItemRow>(`
    SELECT *
    FROM queue_items
    WHERE job_id = ?
    LIMIT 1
  `);
  const row = stmt.get(jobId);
  return row ? rowToQueueItem(row) : null;
}

export function __resetQueueStoreForTests(): void {
  if (!db) {
    return;
  }
  db.close();
  db = null;
}
