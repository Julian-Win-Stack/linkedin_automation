import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { afterEach, describe, expect, it } from "vitest";
import {
  __resetWeeklySuccessStoreForTests,
  getWeeklySuccessCounts,
  saveWeeklySuccessForJob,
} from "../src/services/weeklySuccessStore";

function makeTempDbPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "weekly-success-"));
  return path.join(dir, "test.sqlite");
}

describe("weeklySuccessStore", () => {
  afterEach(() => {
    __resetWeeklySuccessStoreForTests();
    delete process.env.WEEKLY_SUCCESS_SQLITE_PATH;
  });

  it("stores and aggregates weekly counts by user and window", () => {
    process.env.WEEKLY_SUCCESS_SQLITE_PATH = makeTempDbPath();
    const weekStartMs = new Date(2026, 3, 4, 0, 0, 0, 0).getTime();
    const inWeek = weekStartMs + 2 * 24 * 60 * 60 * 1000;
    const outOfWeek = weekStartMs + 8 * 24 * 60 * 60 * 1000;

    saveWeeklySuccessForJob({
      jobId: "job-1",
      selectedUser: "julian",
      completedAtMs: inWeek,
      linkedinSuccessCount: 3,
      emailSuccessCount: 4,
    });
    saveWeeklySuccessForJob({
      jobId: "job-2",
      selectedUser: "julian",
      completedAtMs: inWeek,
      linkedinSuccessCount: 2,
      emailSuccessCount: 1,
    });
    saveWeeklySuccessForJob({
      jobId: "job-3",
      selectedUser: "raihan",
      completedAtMs: inWeek,
      linkedinSuccessCount: 10,
      emailSuccessCount: 10,
    });
    saveWeeklySuccessForJob({
      jobId: "job-4",
      selectedUser: "julian",
      completedAtMs: outOfWeek,
      linkedinSuccessCount: 99,
      emailSuccessCount: 99,
    });

    const totals = getWeeklySuccessCounts({
      selectedUser: "julian",
      weekStartMs,
    });
    expect(totals).toEqual({
      linkedinCount: 5,
      emailCount: 5,
    });
  });

  it("upserts same job id to prevent double counting", () => {
    process.env.WEEKLY_SUCCESS_SQLITE_PATH = makeTempDbPath();
    const weekStartMs = new Date(2026, 3, 4, 0, 0, 0, 0).getTime();
    const inWeek = weekStartMs + 1 * 24 * 60 * 60 * 1000;

    saveWeeklySuccessForJob({
      jobId: "job-dup",
      selectedUser: "cherry",
      completedAtMs: inWeek,
      linkedinSuccessCount: 1,
      emailSuccessCount: 1,
    });
    saveWeeklySuccessForJob({
      jobId: "job-dup",
      selectedUser: "cherry",
      completedAtMs: inWeek,
      linkedinSuccessCount: 7,
      emailSuccessCount: 9,
    });

    const totals = getWeeklySuccessCounts({
      selectedUser: "cherry",
      weekStartMs,
    });
    expect(totals).toEqual({
      linkedinCount: 7,
      emailCount: 9,
    });
  });
});
