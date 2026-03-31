import { beforeEach, describe, expect, it, vi } from "vitest";
import { pushPeopleToLemlistCampaign, TaggedLinkedinCandidate } from "../src/services/lemlistPushQueue";

const createLeadInCampaignMock = vi.fn();
const getLemlistLinkedinCampaignIdsForUserMock = vi.fn();

vi.mock("../src/services/lemlistClient", () => ({
  createLeadInCampaign: (...args: unknown[]) => createLeadInCampaignMock(...args),
  getLemlistLinkedinCampaignIdsForUser: (...args: unknown[]) =>
    getLemlistLinkedinCampaignIdsForUserMock(...args),
}));

describe("pushPeopleToLemlistCampaign", () => {
  beforeEach(() => {
    createLeadInCampaignMock.mockReset();
    getLemlistLinkedinCampaignIdsForUserMock.mockReset();
    getLemlistLinkedinCampaignIdsForUserMock.mockReturnValue({
      sreCampaignId: "cam_sre",
      engCampaignId: "cam_eng",
    });
  });

  it("routes sre-bucketed candidates to SRE campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedLinkedinCandidate[] = [
      {
        employee: {
          startDate: "2024-01-01",
          endDate: null,
          name: "Jane Doe",
          linkedinUrl: "https://linkedin.com/in/jane",
          currentTitle: "SRE",
          tenure: 12,
        },
        linkedinBucket: "sre",
      },
    ];

    const result = await pushPeopleToLemlistCampaign(candidates, "Acme", "acme.com", "julian");

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_sre",
      {
        firstName: "Jane",
        lastName: "Doe",
        companyName: "Acme",
        jobTitle: "SRE",
        linkedinUrl: "https://linkedin.com/in/jane",
        companyDomain: "acme.com",
      },
      {
        deduplicate: false,
        linkedinEnrichment: false,
        findEmail: false,
        verifyEmail: false,
        findPhone: false,
      }
    );
    expect(result).toEqual({
      attempted: 1,
      successful: 1,
      failed: 0,
      successItems: ["Jane Doe"],
      failedItems: [],
    });
  });

  it("routes eng-bucketed candidates to Eng campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedLinkedinCandidate[] = [
      {
        employee: {
          startDate: "2024-01-01",
          endDate: null,
          name: "Platform Person",
          linkedinUrl: "https://linkedin.com/in/platform",
          currentTitle: "Platform Engineer",
          tenure: 12,
        },
        linkedinBucket: "eng",
      },
    ];

    await pushPeopleToLemlistCampaign(candidates, "Acme", "acme.com", "julian");

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng",
      {
        firstName: "Platform",
        lastName: "Person",
        companyName: "Acme",
        jobTitle: "Platform Engineer",
        linkedinUrl: "https://linkedin.com/in/platform",
        companyDomain: "acme.com",
      },
      expect.any(Object)
    );
  });

  it("routes mixed buckets to correct campaigns in order", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedLinkedinCandidate[] = [
      {
        employee: {
          startDate: "2024-01-01",
          endDate: null,
          name: "SRE Person",
          linkedinUrl: "https://linkedin.com/in/sre",
          currentTitle: "Site Reliability Engineer",
          tenure: 12,
        },
        linkedinBucket: "sre",
      },
      {
        employee: {
          startDate: "2024-01-01",
          endDate: null,
          name: "Platform Person",
          linkedinUrl: "https://linkedin.com/in/platform",
          currentTitle: "Platform Engineer",
          tenure: 12,
        },
        linkedinBucket: "eng",
      },
    ];

    const result = await pushPeopleToLemlistCampaign(candidates, "Acme", "acme.com", "julian");

    expect(createLeadInCampaignMock).toHaveBeenCalledTimes(2);
    expect(createLeadInCampaignMock).toHaveBeenNthCalledWith(
      1,
      "cam_sre",
      expect.objectContaining({ jobTitle: "Site Reliability Engineer" }),
      expect.any(Object)
    );
    expect(createLeadInCampaignMock).toHaveBeenNthCalledWith(
      2,
      "cam_eng",
      expect.objectContaining({ jobTitle: "Platform Engineer" }),
      expect.any(Object)
    );
    expect(result.attempted).toBe(2);
    expect(result.successful).toBe(2);
  });

  it("keeps successful pushes and reports failed pushes across buckets", async () => {
    createLeadInCampaignMock
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error("Lemlist API error (404): Campaign not found"));

    const candidates: TaggedLinkedinCandidate[] = [
      {
        employee: {
          startDate: "2024-01-01",
          endDate: null,
          name: "Jane Doe",
          linkedinUrl: "https://linkedin.com/in/jane",
          currentTitle: "SRE",
          tenure: 12,
        },
        linkedinBucket: "sre",
      },
      {
        employee: {
          startDate: "2024-01-01",
          endDate: null,
          name: "John Doe",
          linkedinUrl: "https://linkedin.com/in/john",
          currentTitle: "Platform Engineer",
          tenure: 12,
        },
        linkedinBucket: "eng",
      },
    ];

    const result = await pushPeopleToLemlistCampaign(candidates, "Acme", "acme.com", "julian");

    expect(result.attempted).toBe(2);
    expect(result.successful).toBe(1);
    expect(result.failed).toBe(1);
    expect(result.successItems).toEqual(["Jane Doe"]);
    expect(result.failedItems[0]).toEqual({
      name: "John Doe",
      error: "Lemlist API error (404): Campaign not found",
    });
    expect(createLeadInCampaignMock).toHaveBeenNthCalledWith(
      1,
      "cam_sre",
      expect.objectContaining({ jobTitle: "SRE" }),
      expect.any(Object)
    );
    expect(createLeadInCampaignMock).toHaveBeenNthCalledWith(
      2,
      "cam_eng",
      expect.objectContaining({ jobTitle: "Platform Engineer" }),
      expect.any(Object)
    );
  });

  it("does not apply tenure filtering in queue layer", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedLinkedinCandidate[] = [
      {
        employee: {
          startDate: "2024-01-01",
          endDate: null,
          name: "Short Stay",
          linkedinUrl: "https://linkedin.com/in/short",
          currentTitle: "SRE",
          tenure: 1,
        },
        linkedinBucket: "sre",
      },
      {
        employee: {
          startDate: "2024-01-01",
          endDate: null,
          name: "Eligible Person",
          linkedinUrl: "https://linkedin.com/in/eligible",
          currentTitle: "SRE",
          tenure: 2,
        },
        linkedinBucket: "sre",
      },
    ];

    const result = await pushPeopleToLemlistCampaign(candidates, "Acme", "acme.com", "julian");

    expect(createLeadInCampaignMock).toHaveBeenCalledTimes(2);
    expect(result.attempted).toBe(2);
    expect(result.successful).toBe(2);
    expect(result.failed).toBe(0);
    expect(result.successItems).toEqual(["Short Stay", "Eligible Person"]);
  });
});
