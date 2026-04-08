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
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 0, emailCount: 0 });
    process.env.ADMIN_API_KEY = "test-secret-key";
  });

  it("returns 401 when admin key is missing", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .send({ selectedUser: "raihan", targetLinkedinCount: 20, targetEmailCount: 70 });

    expect(response.status).toBe(401);
    expect(response.body.error).toContain("Invalid admin key");
  });

  it("returns 401 when admin key is wrong", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "wrong-key")
      .send({ selectedUser: "raihan", targetLinkedinCount: 20, targetEmailCount: 70 });

    expect(response.status).toBe(401);
    expect(response.body.error).toContain("Invalid admin key");
  });

  it("returns 503 when ADMIN_API_KEY is not configured", async () => {
    delete process.env.ADMIN_API_KEY;
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "anything")
      .send({ selectedUser: "raihan", targetLinkedinCount: 20, targetEmailCount: 70 });

    expect(response.status).toBe(503);
    expect(response.body.error).toContain("ADMIN_API_KEY is not configured");
  });

  it("returns 400 when selectedUser is invalid", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({ selectedUser: "notauser", targetLinkedinCount: 20, targetEmailCount: 70 });

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("selectedUser");
  });

  it("returns 400 when targetLinkedinCount is not a non-negative integer", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({ selectedUser: "raihan", targetLinkedinCount: -5, targetEmailCount: 70 });

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("targetLinkedinCount");
  });

  it("returns 400 when targetEmailCount is missing", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({ selectedUser: "raihan", targetLinkedinCount: 20 });

    expect(response.status).toBe(400);
    expect(response.body.error).toContain("targetEmailCount");
  });

  it("adjusts counts and returns previous and new totals", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 2, emailCount: 50 });
    const app = createTestApp();

    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({ selectedUser: "raihan", targetLinkedinCount: 20, targetEmailCount: 70 });

    expect(response.status).toBe(200);
    expect(response.body.selectedUser).toBe("raihan");
    expect(response.body.previousLinkedinCount).toBe(2);
    expect(response.body.previousEmailCount).toBe(50);
    expect(response.body.newLinkedinCount).toBe(20);
    expect(response.body.newEmailCount).toBe(70);
    expect(response.body.adjustedLinkedinBy).toBe(18);
    expect(response.body.adjustedEmailBy).toBe(20);
  });

  it("calls insertWeeklySuccessAdjustment with the correct delta", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 2, emailCount: 50 });
    const app = createTestApp();

    await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({ selectedUser: "cherry", targetLinkedinCount: 20, targetEmailCount: 70 });

    expect(insertWeeklySuccessAdjustmentMock).toHaveBeenCalledTimes(1);
    const callArgs = insertWeeklySuccessAdjustmentMock.mock.calls[0][0] as {
      selectedUser: string;
      linkedinDelta: number;
      emailDelta: number;
    };
    expect(callArgs.selectedUser).toBe("cherry");
    expect(callArgs.linkedinDelta).toBe(18);
    expect(callArgs.emailDelta).toBe(20);
  });

  it("does not call insertWeeklySuccessAdjustment when counts are already at target", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 20, emailCount: 70 });
    const app = createTestApp();

    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({ selectedUser: "raihan", targetLinkedinCount: 20, targetEmailCount: 70 });

    expect(response.status).toBe(200);
    expect(response.body.adjustedLinkedinBy).toBe(0);
    expect(response.body.adjustedEmailBy).toBe(0);
    expect(insertWeeklySuccessAdjustmentMock).not.toHaveBeenCalled();
  });

  it("supports negative delta to reduce counts", async () => {
    getWeeklySuccessCountsMock.mockReturnValue({ linkedinCount: 50, emailCount: 80 });
    const app = createTestApp();

    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({ selectedUser: "julian", targetLinkedinCount: 10, targetEmailCount: 30 });

    expect(response.status).toBe(200);
    expect(response.body.adjustedLinkedinBy).toBe(-40);
    expect(response.body.adjustedEmailBy).toBe(-50);

    const callArgs = insertWeeklySuccessAdjustmentMock.mock.calls[0][0] as {
      linkedinDelta: number;
      emailDelta: number;
    };
    expect(callArgs.linkedinDelta).toBe(-40);
    expect(callArgs.emailDelta).toBe(-50);
  });

  it("accepts case-insensitive selectedUser", async () => {
    const app = createTestApp();
    const response = await request(app)
      .post("/admin/adjust-weekly-counts")
      .set("X-Admin-Key", "test-secret-key")
      .send({ selectedUser: "RAIHAN", targetLinkedinCount: 0, targetEmailCount: 0 });

    expect(response.status).toBe(200);
    expect(response.body.selectedUser).toBe("raihan");
  });
});
