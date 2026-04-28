import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  __resetQueueStoreForTests,
  claimNextQueuedItemForUser,
  completeQueueItem,
  enqueueQueueItem,
  getQueueItemById,
  listQueueItemsForUser,
  recoverRunningItemsToQueued,
  setQueueItemJobId,
  toQueueLabel,
} from "../src/services/queueStore";

function makeTempDbPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "queue-store-"));
  return path.join(dir, "test.sqlite");
}

describe("queueStore", () => {
  afterEach(() => {
    __resetQueueStoreForTests();
    delete process.env.WEEKLY_SUCCESS_SQLITE_PATH;
  });

  it("enqueues in stable order and limits active items to 10", () => {
    process.env.WEEKLY_SUCCESS_SQLITE_PATH = makeTempDbPath();
    const weekStartMs = Date.now();

    for (let i = 1; i <= 10; i += 1) {
      const item = enqueueQueueItem({
        queueItemId: `q-${i}`,
        selectedUser: "julian",
        csvInput: `Company Name,Website\nAcme ${i},acme${i}.com\n`,
        weekStartMs,
      });
      expect(item.queueOrder).toBe(i);
    }

    expect(() =>
      enqueueQueueItem({
        queueItemId: "q-11",
        selectedUser: "julian",
        csvInput: "Company Name,Website\nAcme 11,acme11.com\n",
        weekStartMs,
      })
    ).toThrow("Queue limit reached");
  });

  it("claims one queued item at a time per user", () => {
    process.env.WEEKLY_SUCCESS_SQLITE_PATH = makeTempDbPath();
    const weekStartMs = Date.now();

    enqueueQueueItem({
      queueItemId: "q-1",
      selectedUser: "raihan",
      csvInput: "Company Name,Website\nA,a.com\n",
      weekStartMs,
    });
    enqueueQueueItem({
      queueItemId: "q-2",
      selectedUser: "raihan",
      csvInput: "Company Name,Website\nB,b.com\n",
      weekStartMs,
    });

    const first = claimNextQueuedItemForUser("raihan");
    expect(first?.queueItemId).toBe("q-1");
    expect(first?.status).toBe("running");

    const blocked = claimNextQueuedItemForUser("raihan");
    expect(blocked).toBeNull();

    setQueueItemJobId("q-1", "job-1");
    completeQueueItem("q-1", { status: "done", warnings: [] });

    const second = claimNextQueuedItemForUser("raihan");
    expect(second?.queueItemId).toBe("q-2");
  });

  it("stores completion artifacts and recovers running rows after restart", () => {
    process.env.WEEKLY_SUCCESS_SQLITE_PATH = makeTempDbPath();
    const weekStartMs = Date.now();

    enqueueQueueItem({
      queueItemId: "q-1",
      selectedUser: "cherry",
      csvInput: "Company Name,Website\nA,a.com\n",
      weekStartMs,
    });
    claimNextQueuedItemForUser("cherry");
    setQueueItemJobId("q-1", "job-1");

    const recoveredCount = recoverRunningItemsToQueued();
    expect(recoveredCount).toBe(1);

    const runningAgain = claimNextQueuedItemForUser("cherry");
    expect(runningAgain?.queueItemId).toBe("q-1");
    setQueueItemJobId("q-1", "job-1");

    completeQueueItem("q-1", {
      status: "done",
      csvOutputBase64: Buffer.from("a,b\n1,2\n", "utf8").toString("base64"),
      warnings: ["warn-1"],
    });
    const completed = getQueueItemById("q-1");
    expect(completed?.status).toBe("done");
    expect(completed?.csvOutputBase64).toBeTruthy();
    expect(completed?.warnings).toEqual(["warn-1"]);
  });

  it("round-trips companiesMissingApolloAccountId through completeQueueItem", () => {
    process.env.WEEKLY_SUCCESS_SQLITE_PATH = makeTempDbPath();
    const weekStartMs = Date.now();

    enqueueQueueItem({
      queueItemId: "q-1",
      selectedUser: "julian",
      csvInput: "Company Name,Website,Apollo Account Id\nAcme,acme.com,\n",
      weekStartMs,
    });
    claimNextQueuedItemForUser("julian");
    setQueueItemJobId("q-1", "job-1");

    const companies = [
      { name: "Acme", website: "acme.com" },
      { name: "Bravo Corp", website: "" },
    ];
    completeQueueItem("q-1", {
      status: "done",
      companiesMissingApolloAccountId: companies,
      warnings: [],
    });

    const item = getQueueItemById("q-1");
    expect(item?.companiesMissingApolloAccountId).toEqual(companies);
  });

  it("formats queue labels correctly", () => {
    expect(toQueueLabel(1)).toBe("1st queue");
    expect(toQueueLabel(2)).toBe("2nd queue");
    expect(toQueueLabel(3)).toBe("3rd queue");
    expect(toQueueLabel(4)).toBe("4th queue");
    expect(toQueueLabel(11)).toBe("11th queue");
    expect(toQueueLabel(22)).toBe("22nd queue");
    expect(toQueueLabel(103)).toBe("103rd queue");
  });

  it("keeps queues isolated per user", () => {
    process.env.WEEKLY_SUCCESS_SQLITE_PATH = makeTempDbPath();
    const weekStartMs = Date.now();
    enqueueQueueItem({
      queueItemId: "q-j",
      selectedUser: "julian",
      csvInput: "Company Name,Website\nA,a.com\n",
      weekStartMs,
    });
    enqueueQueueItem({
      queueItemId: "q-r",
      selectedUser: "raihan",
      csvInput: "Company Name,Website\nB,b.com\n",
      weekStartMs,
    });

    const julianItems = listQueueItemsForUser("julian");
    const raihanItems = listQueueItemsForUser("raihan");
    expect(julianItems.map((item) => item.queueItemId)).toEqual(["q-j"]);
    expect(raihanItems.map((item) => item.queueItemId)).toEqual(["q-r"]);
  });
});
