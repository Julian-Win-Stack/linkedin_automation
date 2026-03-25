import express from "express";
import request from "supertest";
import { beforeEach, describe, expect, it, vi } from "vitest";
import apolloWebhookRouter from "../src/routes/apolloWebhook";

const processApolloWaterfallWebhookMock = vi.fn();

vi.mock("../src/services/apolloWaterfallStore", () => ({
  processApolloWaterfallWebhook: (...args: unknown[]) => processApolloWaterfallWebhookMock(...args),
}));

function createTestApp() {
  const app = express();
  app.use(express.json());
  app.use(apolloWebhookRouter);
  return app;
}

describe("apollo webhook route", () => {
  beforeEach(() => {
    delete process.env.APOLLO_WEBHOOK_SECRET;
    vi.restoreAllMocks();
    processApolloWaterfallWebhookMock.mockReset();
    processApolloWaterfallWebhookMock.mockReturnValue({
      requestId: "req_1",
      recoveredEmailCount: 0,
    });
  });

  it("accepts webhook payload and returns ok", async () => {
    const app = createTestApp();
    const response = await request(app).post("/webhooks/apollo/waterfall").send({ request_id: "req_1" });

    expect(response.status).toBe(200);
    expect(response.body.ok).toBe(true);
    expect(processApolloWaterfallWebhookMock).toHaveBeenCalledTimes(1);
  });

  it("returns duplicate=true when request_id is repeated", async () => {
    const app = createTestApp();
    await request(app).post("/webhooks/apollo/waterfall").send({ request_id: "req_2" });
    const secondResponse = await request(app).post("/webhooks/apollo/waterfall").send({ request_id: "req_2" });

    expect(secondResponse.status).toBe(200);
    expect(secondResponse.body.duplicate).toBe(true);
    expect(processApolloWaterfallWebhookMock).toHaveBeenCalledTimes(1);
  });

  it("rejects webhook request when token does not match configured secret", async () => {
    process.env.APOLLO_WEBHOOK_SECRET = "expected_token";
    const app = createTestApp();

    const response = await request(app).post("/webhooks/apollo/waterfall?token=wrong").send({ request_id: "req_3" });

    expect(response.status).toBe(401);
    expect(response.body.error).toBe("Invalid Apollo webhook token.");
  });
});
