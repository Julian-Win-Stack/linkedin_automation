import { describe, expect, it } from "vitest";
import { countProcessableCompanies, readCompanies } from "../src/services/observability/csvReader";

describe("readCompanies", () => {
  it("keeps rows that have apollo account id even when website is blank", async () => {
    const csv = [
      "Company Name,Website,Apollo Account Id",
      "Acme,,apollo-123",
      "Bravo,bravo.com,",
      "Charlie,,",
    ].join("\n");
    const skippedReasons: string[] = [];
    const rows = [];

    for await (const row of readCompanies({
      csvBuffer: csv,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
      onSkipRow: (skipInfo) => skippedReasons.push(skipInfo.reason),
    })) {
      rows.push(row);
    }

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      companyName: "Acme",
      companyDomain: "",
      apolloAccountId: "apollo-123",
    });
    expect(rows[1]).toMatchObject({
      companyName: "Bravo",
      companyDomain: "bravo.com",
      apolloAccountId: undefined,
    });
    expect(skippedReasons).toEqual(["missing_website_and_apollo_account_id"]);
  });

  it("does not skip row when company name is missing but website exists", async () => {
    const csv = [
      "Company Name,Website,Apollo Account Id",
      ",acme.com,",
      ",,apollo-123",
      ",,",
    ].join("\n");
    const skippedReasons: string[] = [];
    const rows = [];

    for await (const row of readCompanies({
      csvBuffer: csv,
      nameColumn: "Company Name",
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
      onSkipRow: (skipInfo) => skippedReasons.push(skipInfo.reason),
    })) {
      rows.push(row);
    }

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({
      companyName: "",
      companyDomain: "acme.com",
      apolloAccountId: undefined,
    });
    expect(rows[1]).toMatchObject({
      companyName: "",
      companyDomain: "",
      apolloAccountId: "apollo-123",
    });
    expect(skippedReasons).toEqual(["missing_website_and_apollo_account_id"]);
  });
});

describe("countProcessableCompanies", () => {
  it("counts only rows that have website or apollo account id", async () => {
    const csv = [
      "Company Name,Website,Apollo Account Id",
      "Acme,acme.com,",
      "Bravo,,apollo-123",
      "Charlie,,",
    ].join("\n");

    const count = await countProcessableCompanies({
      csvBuffer: csv,
      domainColumn: "Website",
      apolloAccountIdColumn: "Apollo Account Id",
    });

    expect(count).toBe(2);
  });
});
