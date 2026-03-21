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

  it("rejects linkedin company urls", async () => {
    await expect(getCompany("https://www.linkedin.com/company/rappi/")).rejects.toMatchObject({
      name: "InvalidCompanyInputError",
    });
  });

  it("rejects plain company names", async () => {
    await expect(getCompany("Rappi")).rejects.toMatchObject({
      name: "InvalidCompanyInputError",
    });
  });
});
