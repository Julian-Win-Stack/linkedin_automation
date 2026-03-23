import { describe, expect, it } from "vitest";
import { getCompany } from "../src/services/getCompany";

describe("getCompany", () => {
  it("accepts a domain", async () => {
    const result = await getCompany("rappi.com");
    expect(result).toEqual({
      companyName: "rappi.com",
      domain: "rappi.com",
    });
  });

  it("extracts domain from simple website url", async () => {
    const result = await getCompany("http://www.storyblocks.com");
    expect(result).toEqual({
      companyName: "storyblocks.com",
      domain: "storyblocks.com",
    });
  });

  it("extracts domain from long website url", async () => {
    const result = await getCompany("https://www.storyblocks.com/audio/search?media-type=music");
    expect(result).toEqual({
      companyName: "storyblocks.com",
      domain: "storyblocks.com",
    });
  });

  it("rejects plain company names", async () => {
    await expect(getCompany("Rappi")).rejects.toMatchObject({
      name: "InvalidCompanyInputError",
    });
  });
});
