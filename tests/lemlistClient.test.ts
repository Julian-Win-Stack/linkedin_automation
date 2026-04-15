import { beforeEach, describe, expect, it, vi } from "vitest";
import axios from "axios";
import {
  bulkEnrichData,
  createLeadInCampaign,
  getEnrichmentResult,
  getLemlistLinkedinCampaignIdsForUser,
  toBasicAuthHeader,
} from "../src/services/lemlistClient";

const { postMock, getMock, createMock } = vi.hoisted(() => {
  const localPostMock = vi.fn();
  const localGetMock = vi.fn();
  const localCreateMock = vi.fn(() => ({ post: localPostMock, get: localGetMock }));
  return { postMock: localPostMock, getMock: localGetMock, createMock: localCreateMock };
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
    getMock.mockClear();
    process.env.LEMLIST_API_KEY = "test_lemlist_key";
    process.env.JULIAN_LINKEDIN_SRE_CAMPAIGN_ID = "cam_sre";
    process.env.JULIAN_LINKEDIN_ENG_CAMPAIGN_ID = "cam_eng";
    process.env.JULIAN_LINKEDIN_ENG_LEAD_CAMPAIGN_ID = "cam_linkedin_eng_lead";
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

  it("calls bulk enrich endpoint with find_email payload", async () => {
    postMock.mockResolvedValueOnce({
      data: [{ id: "enr_1", metadata: { metadataId: "m1" } }],
    });

    const response = await bulkEnrichData([
      {
        input: {
          firstName: "John",
          lastName: "Doe",
          companyName: "Acme",
          companyDomain: "acme.com",
        },
        enrichmentRequests: ["find_email"],
        metadata: { metadataId: "m1" },
      },
    ]);

    expect(postMock).toHaveBeenCalledWith("/v2/enrichments/bulk", [
      {
        input: {
          firstName: "John",
          lastName: "Doe",
          companyName: "Acme",
          companyDomain: "acme.com",
        },
        enrichmentRequests: ["find_email"],
        metadata: { metadataId: "m1" },
      },
    ]);
    expect(response).toEqual([{ id: "enr_1", metadata: { metadataId: "m1" } }]);
  });

  it("fetches enrichment result by id", async () => {
    getMock.mockResolvedValueOnce({
      data: {
        enrichmentStatus: "done",
        data: {
          email: {
            email: "john@acme.com",
            notFound: false,
          },
        },
      },
    });

    const result = await getEnrichmentResult("enr_123");
    expect(getMock).toHaveBeenCalledWith("/enrich/enr_123");
    expect(result).toEqual({
      enrichmentStatus: "done",
      data: {
        email: {
          email: "john@acme.com",
          notFound: false,
        },
      },
    });
  });

  it("fails with clear error when lemlist key is missing", async () => {
    delete process.env.LEMLIST_API_KEY;

    await expect(
      createLeadInCampaign("cam_123", {
        firstName: "John",
      })
    ).rejects.toThrow("Missing LEMLIST_API_KEY environment variable.");
  });

  it("returns LinkedIn campaign ids from resolver wrappers", () => {
    expect(getLemlistLinkedinCampaignIdsForUser("julian")).toEqual({
      sreCampaignId: "cam_sre",
      engLeadCampaignId: "cam_linkedin_eng_lead",
      engCampaignId: "cam_eng",
    });
  });

  it("fails fast when a LinkedIn campaign id is missing", () => {
    delete process.env.JULIAN_LINKEDIN_ENG_CAMPAIGN_ID;
    expect(() => getLemlistLinkedinCampaignIdsForUser("julian")).toThrow(
      "Missing JULIAN_LINKEDIN_ENG_CAMPAIGN_ID environment variable."
    );
  });

});
