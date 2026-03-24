import { beforeEach, describe, expect, it, vi } from "vitest";
import axios from "axios";
import {
  createLeadInCampaign,
  getLemlistLinkedinCampaignIds,
  toBasicAuthHeader,
} from "../src/services/lemlistClient";

const { postMock, createMock } = vi.hoisted(() => {
  const localPostMock = vi.fn();
  const localCreateMock = vi.fn(() => ({ post: localPostMock }));
  return { postMock: localPostMock, createMock: localCreateMock };
});

vi.mock("axios", () => ({
  default: {
    create: createMock,
    isAxiosError: (error: unknown) => {
      return typeof error === "object" && error !== null && "isAxiosError" in error;
    },
  },
}));

describe("lemlistClient", () => {
  beforeEach(() => {
    createMock.mockClear();
    postMock.mockClear();
    process.env.LEMLIST_API_KEY = "test_lemlist_key";
    process.env.LEMLIST_LINKEDIN_SRE_CAMPAIGN_ID = "cam_sre";
    process.env.LEMLIST_LINKEDIN_ENG_LEAD_CAMPAIGN_ID = "cam_eng_lead";
    process.env.LEMLIST_LINKEDIN_ENG_CAMPAIGN_ID = "cam_eng";
  });

  it("builds basic auth header with empty username and api key", () => {
    expect(toBasicAuthHeader("abc123")).toBe("Basic OmFiYzEyMw==");
  });

  it("creates campaign lead with docs-compatible endpoint shape", async () => {
    postMock.mockResolvedValue({ data: {} });

    await createLeadInCampaign(
      "cam_123",
      {
        firstName: "John",
        lastName: "Doe",
        companyName: "Acme",
      },
      {
        deduplicate: false,
        linkedinEnrichment: false,
        findEmail: false,
        verifyEmail: false,
        findPhone: false,
      }
    );

    expect(axios.create).toHaveBeenCalledTimes(1);
    expect(postMock).toHaveBeenCalledWith(
      "/campaigns/cam_123/leads/?deduplicate=false&linkedinEnrichment=false&findEmail=false&verifyEmail=false&findPhone=false",
      {
        firstName: "John",
        lastName: "Doe",
        companyName: "Acme",
      }
    );
  });

  it("fails with clear error when lemlist key is missing", async () => {
    delete process.env.LEMLIST_API_KEY;

    await expect(
      createLeadInCampaign("cam_123", {
        firstName: "John",
      })
    ).rejects.toThrow("Missing LEMLIST_API_KEY environment variable.");
  });

  it("returns all three LinkedIn campaign ids from env", () => {
    expect(getLemlistLinkedinCampaignIds()).toEqual({
      sreCampaignId: "cam_sre",
      engLeadCampaignId: "cam_eng_lead",
      engCampaignId: "cam_eng",
    });
  });

  it("fails fast when a LinkedIn campaign id is missing", () => {
    delete process.env.LEMLIST_LINKEDIN_ENG_CAMPAIGN_ID;
    expect(() => getLemlistLinkedinCampaignIds()).toThrow(
      "Missing LEMLIST_LINKEDIN_ENG_CAMPAIGN_ID environment variable."
    );
  });
});
