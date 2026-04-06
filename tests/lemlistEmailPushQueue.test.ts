import { beforeEach, describe, expect, it, vi } from "vitest";
import { pushPeopleToLemlistEmailCampaign } from "../src/services/lemlistEmailPushQueue";
import { TaggedEmailCandidate } from "../src/services/emailCandidateWaterfall";

const createLeadInCampaignMock = vi.fn();
const getLemlistEmailCampaignIdsForUserMock = vi.fn();

vi.mock("../src/services/lemlistClient", () => ({
  createLeadInCampaign: (...args: unknown[]) => createLeadInCampaignMock(...args),
  getLemlistEmailCampaignIdsForUser: (...args: unknown[]) =>
    getLemlistEmailCampaignIdsForUserMock(...args),
}));

describe("pushPeopleToLemlistEmailCampaign", () => {
  beforeEach(() => {
    createLeadInCampaignMock.mockReset();
    getLemlistEmailCampaignIdsForUserMock.mockReset();
    getLemlistEmailCampaignIdsForUserMock.mockReturnValue({
      sreEmailCampaignId: "cam_sre_email",
      engLeadEmailCampaignId: "cam_eng_lead_email",
      engEmailCampaignId: "cam_eng_email",
    });
    vi.restoreAllMocks();
  });

  it("routes sre bucket candidates to SRE email campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedEmailCandidate[] = [
      {
        employee: {
          id: "person-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "SRE Person",
          email: "sre.person@example.com",
          linkedinUrl: "https://linkedin.com/in/sre",
          currentTitle: "SRE",
          headline: "",
          tenure: 24,
        },
        campaignBucket: "sre",
      },
    ];

    await pushPeopleToLemlistEmailCampaign(candidates, "Acme", "acme.com", "julian");

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_sre_email",
      expect.objectContaining({
        email: "sre.person@example.com",
        jobTitle: "SRE",
      }),
      expect.any(Object)
    );
  });

  it("routes eng bucket candidates to ENG email campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedEmailCandidate[] = [
      {
        employee: {
          id: "person-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "Engineer Person",
          email: "engineer.person@example.com",
          linkedinUrl: "https://linkedin.com/in/engineer",
          currentTitle: "Platform Engineer",
          headline: "",
          tenure: 24,
        },
        campaignBucket: "eng",
      },
    ];

    await pushPeopleToLemlistEmailCampaign(candidates, "Acme", "acme.com", "julian");

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng_email",
      expect.objectContaining({
        email: "engineer.person@example.com",
        jobTitle: "Platform Engineer",
      }),
      expect.any(Object)
    );
  });

  it("routes engLead bucket candidates to ENG_LEAD email campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedEmailCandidate[] = [
      {
        employee: {
          id: "person-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "Lead Person",
          email: "lead.person@example.com",
          linkedinUrl: "https://linkedin.com/in/lead",
          currentTitle: "Head of Engineering",
          headline: "",
          tenure: 24,
        },
        campaignBucket: "engLead",
      },
    ];

    await pushPeopleToLemlistEmailCampaign(candidates, "Acme", "acme.com", "julian");

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng_lead_email",
      expect.objectContaining({
        email: "lead.person@example.com",
        jobTitle: "Head of Engineering",
      }),
      expect.any(Object)
    );
  });

  it("routes mixed buckets to correct campaigns", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedEmailCandidate[] = [
      {
        employee: {
          id: "p-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "SRE One",
          email: "sre@example.com",
          linkedinUrl: null,
          currentTitle: "SRE",
          headline: "",
          tenure: 5,
        },
        campaignBucket: "sre",
      },
      {
        employee: {
          id: "p-2",
          startDate: "2024-01-01",
          endDate: null,
          name: "Infra One",
          email: "infra@example.com",
          linkedinUrl: null,
          currentTitle: "Infrastructure",
          headline: "",
          tenure: 15,
        },
        campaignBucket: "eng",
      },
      {
        employee: {
          id: "p-3",
          startDate: "2024-01-01",
          endDate: null,
          name: "VP One",
          email: "vp@example.com",
          linkedinUrl: null,
          currentTitle: "VP of Engineering",
          headline: "",
          tenure: 24,
        },
        campaignBucket: "engLead",
      },
    ];

    await pushPeopleToLemlistEmailCampaign(candidates, "Acme", "acme.com", "julian");

    expect(createLeadInCampaignMock).toHaveBeenCalledTimes(3);
    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_sre_email",
      expect.objectContaining({ email: "sre@example.com" }),
      expect.any(Object)
    );
    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng_email",
      expect.objectContaining({ email: "infra@example.com" }),
      expect.any(Object)
    );
    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng_lead_email",
      expect.objectContaining({ email: "vp@example.com" }),
      expect.any(Object)
    );
  });

  it("silently skips people with missing email — not counted as failed or succeeded", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const candidates: TaggedEmailCandidate[] = [
      {
        employee: {
          id: "person-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "No Email",
          email: null,
          linkedinUrl: "https://linkedin.com/in/no-email",
          currentTitle: "SRE",
          headline: "",
          tenure: 24,
        },
        campaignBucket: "sre",
      },
    ];

    const result = await pushPeopleToLemlistEmailCampaign(
      candidates,
      "Acme",
      "acme.com",
      "julian"
    );

    expect(createLeadInCampaignMock).not.toHaveBeenCalled();
    expect(result.attempted).toBe(1);
    expect(result.successful).toBe(0);
    expect(result.failed).toBe(0);
    expect(result.failedItems).toEqual([]);
    expect(result.outcomes).toEqual([
      expect.objectContaining({
        name: "No Email",
        status: "skipped",
        error: "Missing email for Lemlist email campaign payload.",
      }),
    ]);
  });

  it('does not count "Lead already in the campaign" as email success or failure', async () => {
    createLeadInCampaignMock.mockRejectedValueOnce(
      new Error("Lemlist API error (400): Lead already in the campaign")
    );

    const candidates: TaggedEmailCandidate[] = [
      {
        employee: {
          id: "person-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "Already Added",
          email: "already.added@example.com",
          linkedinUrl: "https://linkedin.com/in/already-added",
          currentTitle: "Staff Engineer",
          headline: "",
          tenure: 24,
        },
        campaignBucket: "eng",
      },
    ];

    const result = await pushPeopleToLemlistEmailCampaign(candidates, "Acme", "acme.com", "julian");

    expect(result.attempted).toBe(1);
    expect(result.successful).toBe(0);
    expect(result.failed).toBe(0);
    expect(result.successItems).toEqual([]);
    expect(result.failedItems).toEqual([]);
    expect(result.outcomes).toEqual([
      expect.objectContaining({
        name: "Already Added",
        status: "skipped",
      }),
    ]);
    expect(result.outcomes[0]).not.toHaveProperty("error");
  });
});
