import { beforeEach, describe, expect, it, vi } from "vitest";
import { pushPeopleToLemlistCampaign } from "../src/services/lemlistPushQueue";

const createLeadInCampaignMock = vi.fn();
const getLemlistLinkedinCampaignIdsMock = vi.fn();

vi.mock("../src/services/lemlistClient", () => ({
  createLeadInCampaign: (...args: unknown[]) => createLeadInCampaignMock(...args),
  getLemlistLinkedinCampaignIds: (...args: unknown[]) => getLemlistLinkedinCampaignIdsMock(...args),
}));

describe("pushPeopleToLemlistCampaign", () => {
  beforeEach(() => {
    createLeadInCampaignMock.mockReset();
    getLemlistLinkedinCampaignIdsMock.mockReset();
    getLemlistLinkedinCampaignIdsMock.mockReturnValue({
      sreCampaignId: "cam_sre",
      engLeadCampaignId: "cam_eng_lead",
      engCampaignId: "cam_eng",
    });
  });

  it("routes SRE titles to SRE campaign and returns push summary", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    const result = await pushPeopleToLemlistCampaign(
      [
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "Jane Doe",
          linkedinUrl: "https://linkedin.com/in/jane",
          currentTitle: "SRE",
          tenure: 12,
        },
      ],
      "Acme",
      "acme.com"
    );

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

  it("routes remaining leadership titles to eng lead campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    await pushPeopleToLemlistCampaign(
      [
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "Lead Person",
          linkedinUrl: "https://linkedin.com/in/lead",
          currentTitle: "VP of Engineering",
          tenure: 12,
        },
      ],
      "Acme",
      "acme.com"
    );

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng_lead",
      {
        firstName: "Lead",
        lastName: "Person",
        companyName: "Acme",
        jobTitle: "VP of Engineering",
        linkedinUrl: "https://linkedin.com/in/lead",
        companyDomain: "acme.com",
      },
      expect.any(Object)
    );
  });

  it("routes non-SRE and non-leadership titles to eng campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    await pushPeopleToLemlistCampaign(
      [
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "Engineer Person",
          linkedinUrl: "https://linkedin.com/in/engineer",
          currentTitle: "Software Engineer",
          tenure: 12,
        },
      ],
      "Acme",
      "acme.com"
    );

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng",
      {
        firstName: "Engineer",
        lastName: "Person",
        companyName: "Acme",
        jobTitle: "Software Engineer",
        linkedinUrl: "https://linkedin.com/in/engineer",
        companyDomain: "acme.com",
      },
      expect.any(Object)
    );
  });

  it("uses SRE campaign when title matches both SRE and leadership keywords", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    await pushPeopleToLemlistCampaign(
      [
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "Dual Match",
          linkedinUrl: "https://linkedin.com/in/dual",
          currentTitle: "VP, Head of Site Reliability",
          tenure: 12,
        },
      ],
      "Acme",
      "acme.com"
    );

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_sre",
      expect.objectContaining({
        jobTitle: "VP, Head of Site Reliability",
      }),
      expect.any(Object)
    );
  });

  it("keeps successful pushes and reports failed pushes across routed campaigns", async () => {
    createLeadInCampaignMock
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error("Lemlist API error (404): Campaign not found"));

    const result = await pushPeopleToLemlistCampaign(
      [
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "Jane Doe",
          linkedinUrl: "https://linkedin.com/in/jane",
          currentTitle: "Site Reliability Engineer",
          tenure: 12,
        },
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "John Doe",
          linkedinUrl: "https://linkedin.com/in/john",
          currentTitle: "VP Engineering",
          tenure: 12,
        },
      ],
      "Acme",
      "acme.com"
    );

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
      expect.objectContaining({ jobTitle: "Site Reliability Engineer" }),
      expect.any(Object)
    );
    expect(createLeadInCampaignMock).toHaveBeenNthCalledWith(
      2,
      "cam_eng_lead",
      expect.objectContaining({ jobTitle: "VP Engineering" }),
      expect.any(Object)
    );
  });

  it("does not apply tenure filtering in queue layer", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);
    const result = await pushPeopleToLemlistCampaign(
      [
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "Short Stay",
          linkedinUrl: "https://linkedin.com/in/short",
          currentTitle: "SRE",
          tenure: 1,
        },
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "Eligible Person",
          linkedinUrl: "https://linkedin.com/in/eligible",
          currentTitle: "SRE",
          tenure: 2,
        },
      ],
      "Acme",
      "acme.com"
    );

    expect(createLeadInCampaignMock).toHaveBeenCalledTimes(2);
    expect(result.attempted).toBe(2);
    expect(result.successful).toBe(2);
    expect(result.failed).toBe(0);
    expect(result.successItems).toEqual(["Short Stay", "Eligible Person"]);
  });
});
