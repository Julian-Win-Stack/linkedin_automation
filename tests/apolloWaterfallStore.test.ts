import { describe, expect, it } from "vitest";
import {
  getRecoveredEmailsForRequests,
  processApolloWaterfallWebhook,
  registerPendingWaterfallRequest,
  waitForWaterfallRequests,
} from "../src/services/apolloWaterfallStore";

describe("apolloWaterfallStore", () => {
  it("registers request, processes webhook, and returns recovered emails", async () => {
    registerPendingWaterfallRequest("req_store_1", ["person_a", "person_b"]);
    const processResult = processApolloWaterfallWebhook({
      request_id: "req_store_1",
      people: [
        {
          id: "person_a",
          emails: [{ email: "a@example.com" }],
        },
      ],
    });

    expect(processResult.requestId).toBe("req_store_1");
    expect(processResult.recoveredEmailCount).toBe(1);

    const waitResult = await waitForWaterfallRequests(["req_store_1"], 1000);
    expect(waitResult.timedOut).toBe(false);
    expect(waitResult.completedRequestCount).toBe(1);

    const recovered = getRecoveredEmailsForRequests(["req_store_1"]);
    expect(recovered.get("person_a")).toBe("a@example.com");
  });

  it("times out when callback does not arrive in time", async () => {
    registerPendingWaterfallRequest("req_store_timeout", ["person_x"]);
    const waitResult = await waitForWaterfallRequests(["req_store_timeout"], 0);
    expect(waitResult.timedOut).toBe(true);
  });

  it("applies callback that arrived before registration", async () => {
    processApolloWaterfallWebhook({
      request_id: "req_buffered_1",
      people: [
        {
          id: "person_z",
          emails: [{ email: "z@example.com" }],
        },
      ],
    });

    const registration = registerPendingWaterfallRequest("req_buffered_1", ["person_z"]);
    expect(registration.appliedBufferedCallback).toBe(true);
    expect(registration.recoveredEmailCount).toBe(1);

    const waitResult = await waitForWaterfallRequests(["req_buffered_1"], 1000);
    expect(waitResult.timedOut).toBe(false);
    expect(waitResult.completedRequestCount).toBe(1);

    const recovered = getRecoveredEmailsForRequests(["req_buffered_1"]);
    expect(recovered.get("person_z")).toBe("z@example.com");
  });

  it("extracts emails from waterfall vendors payload", async () => {
    registerPendingWaterfallRequest("req_vendor_1", ["person_vendor"]);
    const processResult = processApolloWaterfallWebhook({
      request_id: "req_vendor_1",
      people: [
        {
          id: "person_vendor",
          waterfall: {
            emails: [
              {
                vendors: [{ emails: ["vendor@example.com"] }],
              },
            ],
          },
        },
      ],
    });

    expect(processResult.recoveredEmailCount).toBe(1);
    const recovered = getRecoveredEmailsForRequests(["req_vendor_1"]);
    expect(recovered.get("person_vendor")).toBe("vendor@example.com");
  });
});

