import { beforeEach, describe, expect, it, vi } from "vitest";
import { syncApolloAccountsFromOutputRows } from "../src/services/apolloBulkUpdateAccounts";
import { OutputRow } from "../src/services/observability/csvWriter";

const apolloPostMock = vi.fn();

vi.mock("../src/services/apolloClient", () => ({
  apolloPost: (...args: unknown[]) => apolloPostMock(...args),
}));

function makeRow(overrides: Partial<OutputRow> = {}): OutputRow {
  return {
    company_name: "Acme",
    company_domain: "acme.com",
    company_linkedin_url: "https://linkedin.com/company/acme",
    apollo_account_id: "acc_1",
    observability_tool_research: "Datadog",
    stage: "ChasingPOC",
    sre_count: 4,
    notes: "ready",
    ...overrides,
  };
}

describe("syncApolloAccountsFromOutputRows", () => {
  beforeEach(() => {
    apolloPostMock.mockReset();
    apolloPostMock.mockResolvedValue({ accounts: [{ id: "acc_1" }] });
  });

  it("builds account_attributes with system stage + mapped typed_custom_fields", async () => {
    await syncApolloAccountsFromOutputRows([makeRow()]);

    expect(apolloPostMock).toHaveBeenCalledTimes(1);
    expect(apolloPostMock).toHaveBeenCalledWith(
      "/accounts/bulk_update",
      {
        account_attributes: [
          {
            id: "acc_1",
            account_stage_id: "6971e93a8f17d1001569a9bb",
            typed_custom_fields: {
              "6980e9f46ff5a0002169a12a": "Datadog",
              "6967fde7e9b8720011d25737": "4",
              "696fe565def36a00193ece7e": "ready",
            },
          },
        ],
      },
      0
    );
  });

  it("excludes Company Name, Website and Company Linkedin Url from typed_custom_fields", async () => {
    await syncApolloAccountsFromOutputRows([makeRow()]);

    const typedCustomFields = apolloPostMock.mock.calls[0]?.[1]?.account_attributes?.[0]?.typed_custom_fields;
    expect(Object.values(typedCustomFields ?? {})).not.toContain("Acme");
    expect(Object.values(typedCustomFields ?? {})).not.toContain("acme.com");
    expect(Object.values(typedCustomFields ?? {})).not.toContain("https://linkedin.com/company/acme");
  });

  it("logs colored errors for unknown stage and skips stage while keeping custom fields", async () => {
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    await syncApolloAccountsFromOutputRows([makeRow({ stage: "UnknownStage" })]);

    expect(consoleErrorSpy).toHaveBeenCalled();
    expect(String(consoleErrorSpy.mock.calls[0]?.[0] ?? "")).toContain("\x1b[31m");
    const attributes = apolloPostMock.mock.calls[0]?.[1]?.account_attributes?.[0];
    expect(attributes.account_stage_id).toBeUndefined();
    expect(attributes.typed_custom_fields).toEqual({
      "6980e9f46ff5a0002169a12a": "Datadog",
      "6967fde7e9b8720011d25737": "4",
      "696fe565def36a00193ece7e": "ready",
    });

    consoleErrorSpy.mockRestore();
  });

  it("maps stage names case, underscore, and space insensitively", async () => {
    await syncApolloAccountsFromOutputRows([makeRow({ stage: "chasing poc" })]);

    expect(apolloPostMock).toHaveBeenCalledTimes(1);
    expect(apolloPostMock).toHaveBeenCalledWith(
      "/accounts/bulk_update",
      {
        account_attributes: [
          {
            id: "acc_1",
            account_stage_id: "6971e93a8f17d1001569a9bb",
            typed_custom_fields: {
              "6980e9f46ff5a0002169a12a": "Datadog",
              "6967fde7e9b8720011d25737": "4",
              "696fe565def36a00193ece7e": "ready",
            },
          },
        ],
      },
      0
    );
  });

  it("uses hardcoded Apollo custom field IDs for custom values", async () => {
    await syncApolloAccountsFromOutputRows([makeRow()]);

    expect(apolloPostMock).toHaveBeenCalledTimes(1);
    expect(apolloPostMock.mock.calls[0]?.[1]?.account_attributes?.[0]?.typed_custom_fields).toEqual({
      "6980e9f46ff5a0002169a12a": "Datadog",
      "6967fde7e9b8720011d25737": "4",
      "696fe565def36a00193ece7e": "ready",
    });
  });

  it("skips rows without Apollo Account Id", async () => {
    const result = await syncApolloAccountsFromOutputRows([
      makeRow({ apollo_account_id: "" }),
      makeRow({ apollo_account_id: undefined }),
    ]);

    expect(apolloPostMock).not.toHaveBeenCalled();
    expect(result.skippedMissingAccountIdCount).toBe(2);
    expect(result.updatedAccounts).toBe(0);
  });

  it("skips rows with no stage and no custom field values", async () => {
    const result = await syncApolloAccountsFromOutputRows([
      makeRow({
        observability_tool_research: "",
        sre_count: "",
        notes: "",
        stage: "",
      }),
    ]);

    expect(apolloPostMock).not.toHaveBeenCalled();
    expect(result.skippedNoMappableFieldsCount).toBe(1);
  });

  it("returns UI warning for unmapped stage when stage id is unknown", async () => {
    const result = await syncApolloAccountsFromOutputRows([makeRow({ stage: "UnknownStage" })]);

    expect(result.warnings).toEqual(
      expect.arrayContaining([
        expect.stringContaining('Apollo custom field mapping missing for "Stage"'),
      ])
    );
  });

  it("dedupes duplicate account ids with deterministic last-row-wins", async () => {
    await syncApolloAccountsFromOutputRows([
      makeRow({ apollo_account_id: "acc_dup", stage: "NotActionableNow", notes: "old" }),
      makeRow({ apollo_account_id: "acc_dup", stage: "ChasingPOC", notes: "new" }),
    ]);

    expect(apolloPostMock).toHaveBeenCalledTimes(1);
    const payload = apolloPostMock.mock.calls[0]?.[1];
    expect(payload.account_attributes).toHaveLength(1);
    expect(payload.account_attributes[0]).toMatchObject({
      id: "acc_dup",
      account_stage_id: "6971e93a8f17d1001569a9bb",
      typed_custom_fields: {
        "696fe565def36a00193ece7e": "new",
      },
    });
  });

  it("retries once when a batch fails then succeeds", async () => {
    apolloPostMock
      .mockRejectedValueOnce(new Error("temporary"))
      .mockResolvedValueOnce({ accounts: [{ id: "acc_1" }] });

    const result = await syncApolloAccountsFromOutputRows([makeRow()]);

    expect(apolloPostMock).toHaveBeenCalledTimes(2);
    expect(result.updatedAccounts).toBe(1);
  });

  it("throws after retry fails", async () => {
    apolloPostMock.mockRejectedValue(new Error("still failing"));

    await expect(syncApolloAccountsFromOutputRows([makeRow()])).rejects.toThrow(
      "after 1 retry"
    );
    expect(apolloPostMock).toHaveBeenCalledTimes(2);
  });
});
