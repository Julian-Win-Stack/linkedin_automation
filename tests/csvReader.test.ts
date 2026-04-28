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
      nameColumn: ["Company Name"],
      domainColumn: ["Website"],
      apolloAccountIdColumn: ["Apollo Account Id"],
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
      nameColumn: ["Company Name"],
      domainColumn: ["Website"],
      apolloAccountIdColumn: ["Apollo Account Id"],
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

  it("matches headers case-insensitively", async () => {
    const csv = [
      "company name,website,apollo account id",
      "Acme,acme.com,apollo-123",
    ].join("\n");
    const rows = [];

    for await (const row of readCompanies({
      csvBuffer: csv,
      nameColumn: ["Company Name"],
      domainColumn: ["Website"],
      apolloAccountIdColumn: ["Apollo Account Id"],
    })) {
      rows.push(row);
    }

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      companyName: "Acme",
      companyDomain: "acme.com",
      apolloAccountId: "apollo-123",
    });
  });

  it("uses fallback candidate when primary header is absent", async () => {
    const csv = [
      "Parent Record > Company name,Website",
      "Acme,acme.com",
    ].join("\n");
    const rows = [];

    for await (const row of readCompanies({
      csvBuffer: csv,
      nameColumn: ["Company Name", "Parent Record > Company name"],
      domainColumn: ["Website"],
    })) {
      rows.push(row);
    }

    expect(rows).toHaveLength(1);
    expect(rows[0].companyName).toBe("Acme");
  });

  it("reads name as empty string when no candidate matches any header", async () => {
    const csv = [
      "Org,Website",
      "Acme,acme.com",
    ].join("\n");
    const rows = [];

    for await (const row of readCompanies({
      csvBuffer: csv,
      nameColumn: ["Company Name"],
      domainColumn: ["Website"],
    })) {
      rows.push(row);
    }

    expect(rows).toHaveLength(1);
    expect(rows[0].companyName).toBe("");
    expect(rows[0].companyDomain).toBe("acme.com");
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
      domainColumn: ["Website"],
      apolloAccountIdColumn: ["Apollo Account Id"],
    });

    expect(count).toBe(2);
  });

  it("counts correctly with case-insensitive header matching", async () => {
    const csv = [
      "company name,WEBSITE,apollo account id",
      "Acme,acme.com,",
      "Bravo,,apollo-123",
      "Charlie,,",
    ].join("\n");

    const count = await countProcessableCompanies({
      csvBuffer: csv,
      domainColumn: ["Website"],
      apolloAccountIdColumn: ["Apollo Account Id"],
    });

    expect(count).toBe(2);
  });
});
