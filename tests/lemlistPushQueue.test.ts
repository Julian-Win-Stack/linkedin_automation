import { beforeEach, describe, expect, it, vi } from "vitest";
import { pushPeopleToLemlistCampaign } from "../src/services/lemlistPushQueue";

const createLeadInCampaignMock = vi.fn();
const getLemlistCampaignIdMock = vi.fn();

vi.mock("../src/services/lemlistClient", () => ({
  createLeadInCampaign: (...args: unknown[]) => createLeadInCampaignMock(...args),
  getLemlistCampaignId: (...args: unknown[]) => getLemlistCampaignIdMock(...args),
}));

describe("pushPeopleToLemlistCampaign", () => {
  beforeEach(() => {
    createLeadInCampaignMock.mockReset();
    getLemlistCampaignIdMock.mockReset();
    getLemlistCampaignIdMock.mockReturnValue("cam_123");
  });

  it("pushes leads to lemlist and returns push summary", async () => {
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
      "cam_123",
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

  it("keeps successful pushes and reports failed pushes", async () => {
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
          currentTitle: "SRE",
          tenure: 12,
        },
        {
          startDate: "2024-01-01",
          endDate: null,
          name: "John Doe",
          linkedinUrl: "https://linkedin.com/in/john",
          currentTitle: "SRE",
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
