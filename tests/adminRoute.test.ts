import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import adminRouter from "../src/routes/admin";

const getWeeklySuccessCountsMock = vi.fn();
const insertWeeklySuccessAdjustmentMock = vi.fn();

vi.mock("../src/services/weeklySuccessStore", () => ({
  getWeeklySuccessCounts: (...args: unknown[]) => getWeeklySuccessCountsMock(...args),
  insertWeeklySuccessAdjustment: (...args: unknown[]) => insertWeeklySuccessAdjustmentMock(...args),
}));

function createTestApp() {
  const app = express();
  app.use(express.json());
  app.use(adminRouter);
  return app;
}

describe("POST /admin/adjust-weekly-counts", () => {
  beforeEach(() => {
    getWeeklySuccessCountsMock.mockReset();
    insertWeeklySuccessAdjustmentMock.mockReset();
    getWeeklySuccessCountsMock.mockReturnValue({
      linkedinCount: 0,
      companiesReachedOutToCount: 0,
    });
    process.env.ADMIN_API_KEY = "test-secret-key";
  });

  it("returns 401 when admin key is missing", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .send({
        selectedUser: "raihan",
        targetLinkedinCount: 20,
        targetCompaniesReachedOutToCount: 30,
      });

    expect(response.status).toBe(401);
    expect(response.body.error).toContain("Invalid admin key");
  });

  it("returns 401 when admin key is wrong", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "wrong-key")
      .send({
        selectedUser: "raihan",
        targetLinkedinCount: 20,
        targetCompaniesReachedOutToCount: 30,
      });

    expect(response.status).toBe(401);
    expect(response.body.error).toContain("Invalid admin key");
  });

  it("returns 503 when ADMIN_API_KEY is not configured", async () => {
    delete process.env.ADMIN_API_KEY;
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "anything")
      .send({
        selectedUser: "raihan",
        targetLinkedinCount: 20,
        targetCompaniesReachedOutToCount: 30,
      });

    expect(response.status).toBe(503);
    expect(response.body.error).toContain("ADMIN_API_KEY is not configured");
  });

  it("returns 400 when selectedUser is invalid", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({
        selectedUser: "notauser",
        targetLinkedinCount: 20,
        targetCompaniesReachedOutToCount: 30,
      });

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("selectedUser");
  });

  it("returns 400 when targetLinkedinCount is not a non-negative integer", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({
        selectedUser: "raihan",
        targetLinkedinCount: -5,
        targetCompaniesReachedOutToCount: 30,
      });

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("targetLinkedinCount");
  });

  it("returns 400 when targetCompaniesReachedOutToCount is missing", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({
        selectedUser: "raihan",
        targetLinkedinCount: 20,
      });

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("targetCompaniesReachedOutToCount");
  });

  it("adjusts counts and returns previous and new totals", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({
      linkedinCount: 2,
      companiesReachedOutToCount: 5,
    });
    const app = createTestApp();

    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({
        selectedUser: "raihan",
        targetLinkedinCount: 20,
        targetCompaniesReachedOutToCount: 30,
      });

    expect(response.status).toBe(200);
    expect(response.body.selectedUser).toBe("raihan");
    expect(response.body.previousLinkedinCount).toBe(2);
    expect(response.body.previousCompaniesReachedOutToCount).toBe(5);
    expect(response.body.newLinkedinCount).toBe(20);
    expect(response.body.newCompaniesReachedOutToCount).toBe(30);
    expect(response.body.adjustedLinkedinBy).toBe(18);
    expect(response.body.adjustedCompaniesReachedOutToBy).toBe(25);
  });

  it("calls insertWeeklySuccessAdjustment with the correct delta", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({
      linkedinCount: 2,
      companiesReachedOutToCount: 5,
    });
    const app = createTestApp();

    await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({
        selectedUser: "cherry",
        targetLinkedinCount: 20,
        targetCompaniesReachedOutToCount: 30,
      });

    expect(insertWeeklySuccessAdjustmentMock).toHaveBeenCalledTimes(1);
    const callArgs = insertWeeklySuccessAdjustmentMock.mock.calls[0][0] as {
      selectedUser: string;
      linkedinDelta: number;
      companiesReachedOutToDelta: number;
    };
    expect(callArgs.selectedUser).toBe("cherry");
    expect(callArgs.linkedinDelta).toBe(18);
    expect(callArgs.companiesReachedOutToDelta).toBe(25);
  });

  it("does not call insertWeeklySuccessAdjustment when counts are already at target", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({
      linkedinCount: 20,
      companiesReachedOutToCount: 30,
    });
    const app = createTestApp();

    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({
        selectedUser: "raihan",
        targetLinkedinCount: 20,
        targetCompaniesReachedOutToCount: 30,
      });

    expect(response.status).toBe(200);
    expect(response.body.adjustedLinkedinBy).toBe(0);
    expect(response.body.adjustedCompaniesReachedOutToBy).toBe(0);
    expect(insertWeeklySuccessAdjustmentMock).not.toHaveBeenCalled();
  });

  it("supports negative delta to reduce counts", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({
      linkedinCount: 50,
      companiesReachedOutToCount: 60,
    });
    const app = createTestApp();

    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({
        selectedUser: "julian",
        targetLinkedinCount: 10,
        targetCompaniesReachedOutToCount: 15,
      });

    expect(response.status).toBe(200);
    expect(response.body.adjustedLinkedinBy).toBe(-40);
    expect(response.body.adjustedCompaniesReachedOutToBy).toBe(-45);

    const callArgs = insertWeeklySuccessAdjustmentMock.mock.calls[0][0] as {
      linkedinDelta: number;
      companiesReachedOutToDelta: number;
    };
    expect(callArgs.linkedinDelta).toBe(-40);
    expect(callArgs.companiesReachedOutToDelta).toBe(-45);
  });

  it("accepts case-insensitive selectedUser", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({
        selectedUser: "RAIHAN",
        targetLinkedinCount: 0,
        targetCompaniesReachedOutToCount: 0,
      });

    expect(response.status).toBe(200);
    expect(response.body.selectedUser).toBe("raihan");
  });
});
