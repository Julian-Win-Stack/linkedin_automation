import { describe, expect, it } from "vitest";
import { getCompany } from "../src/services/getCompany";

describe("getCompany", () => {
  it("accepts a domain", async () => {
    const result = await getCompany("rappi.com");
    expect(result).toEqual({
      companyName: "rappi.com",
      domain: "rappi.com",
      linkedinUrl: null,
    });
  });

  it("accepts a linkedin company url", async () => {
    const result = await getCompany("https://www.linkedin.com/company/rappi/");
    expect(result).toEqual({
      companyName: "https://www.linkedin.com/company/rappi/",
      domain: null,
      linkedinUrl: "https://www.linkedin.com/company/rappi/",
    });
  });

  it("rejects plain company names", async () => {
    await expect(getCompany("Rappi")).rejects.toMatchObject({
      name: "InvalidCompanyInputError",
    });
  });
});
