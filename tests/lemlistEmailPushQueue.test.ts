import { beforeEach, describe, expect, it, vi } from "vitest";
import { pushPeopleToLemlistEmailCampaign } from "../src/services/lemlistEmailPushQueue";

const createLeadInCampaignMock = vi.fn();
const getLemlistEmailCampaignIdsMock = vi.fn();

vi.mock("../src/services/lemlistClient", () => ({
  createLeadInCampaign: (...args: unknown[]) => createLeadInCampaignMock(...args),
  getLemlistEmailCampaignIds: (...args: unknown[]) => getLemlistEmailCampaignIdsMock(...args),
}));

describe("pushPeopleToLemlistEmailCampaign", () => {
  beforeEach(() => {
    createLeadInCampaignMock.mockReset();
    getLemlistEmailCampaignIdsMock.mockReset();
    getLemlistEmailCampaignIdsMock.mockReturnValue({
      engLeadEmailCampaignId: "cam_eng_lead_email",
      engEmailCampaignId: "cam_eng_email",
    });
    vi.restoreAllMocks();
  });

  it("routes leadership titles to eng lead email campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    await pushPeopleToLemlistEmailCampaign(
      [
        {
          id: "person-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "Lead Person",
          email: "lead.person@example.com",
          linkedinUrl: "https://linkedin.com/in/lead",
          currentTitle: "Head of Engineering",
          tenure: 24,
        },
      ],
      "Acme",
      "acme.com"
    );

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng_lead_email",
      {
        email: "lead.person@example.com",
        firstName: "Lead",
        lastName: "Person",
        companyName: "Acme",
        companyDomain: "acme.com",
        jobTitle: "Head of Engineering",
        linkedinUrl: "https://linkedin.com/in/lead",
      },
      expect.any(Object)
    );
  });

  it("routes non-leadership titles to eng email campaign", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);

    await pushPeopleToLemlistEmailCampaign(
      [
        {
          id: "person-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "Engineer Person",
          email: "engineer.person@example.com",
          linkedinUrl: "https://linkedin.com/in/engineer",
          currentTitle: "Platform Engineer",
          tenure: 24,
        },
      ],
      "Acme",
      "acme.com"
    );

    expect(createLeadInCampaignMock).toHaveBeenCalledWith(
      "cam_eng_email",
      expect.objectContaining({
        email: "engineer.person@example.com",
        jobTitle: "Platform Engineer",
      }),
      expect.any(Object)
    );
  });

  it("skips missing-email people and logs company and person", async () => {
    createLeadInCampaignMock.mockResolvedValue(undefined);
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);

    const result = await pushPeopleToLemlistEmailCampaign(
      [
        {
          id: "person-1",
          startDate: "2024-01-01",
          endDate: null,
          name: "No Email",
          email: null,
          linkedinUrl: "https://linkedin.com/in/no-email",
          currentTitle: "Head of Engineering",
          tenure: 24,
        },
      ],
      "Acme",
      "acme.com"
    );

    expect(createLeadInCampaignMock).not.toHaveBeenCalled();
    expect(result.attempted).toBe(1);
    expect(result.successful).toBe(0);
    expect(result.failed).toBe(1);
    expect(result.failedItems[0]).toEqual({
      name: "No Email",
      error: "Missing email for Lemlist email campaign payload.",
    });
    expect(logSpy).toHaveBeenCalledWith(
      "[EmailPush][MissingEmail] company=Acme person=No Email title=Head of Engineering"
    );
  });
});
