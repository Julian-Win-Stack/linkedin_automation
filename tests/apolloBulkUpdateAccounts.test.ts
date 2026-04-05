import { beforeEach, describe, expect, it, vi } from "vitest";
import { syncApolloAccountsFromOutputRows } from "../src/services/apolloBulkUpdateAccounts";
import { OutputRow } from "../src/services/observability/csvWriter";

const apolloPostMock = vi.fn();
const fetchApolloAccountCustomFieldNameToIdMapMock = vi.fn();
const fetchApolloAccountStageNameToIdMapMock = vi.fn();

vi.mock("../src/services/apolloClient", () => ({
  apolloPost: (...args: unknown[]) => apolloPostMock(...args),
  fetchApolloAccountCustomFieldNameToIdMap: (...args: unknown[]) =>
    fetchApolloAccountCustomFieldNameToIdMapMock(...args),
  fetchApolloAccountStageNameToIdMap: (...args: unknown[]) =>
    fetchApolloAccountStageNameToIdMapMock(...args),
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
    fetchApolloAccountCustomFieldNameToIdMapMock.mockReset();
    fetchApolloAccountStageNameToIdMapMock.mockReset();
    fetchApolloAccountCustomFieldNameToIdMapMock.mockResolvedValue(
      new Map<string, string>([
        ["observability_tool", "account.field_observability"],
        ["Number of SREs", "account.field_sre_count"],
        ["Notes", "account.field_notes"],
      ])
    );
    fetchApolloAccountStageNameToIdMapMock.mockResolvedValue(
      new Map<string, string>([
        ["ChasingPOC", "stage_chasing_poc"],
        ["NotActionableNow", "stage_not_actionable"],
      ])
    );
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
            account_stage_id: "stage_chasing_poc",
            typed_custom_fields: {
              "account.field_observability": "Datadog",
              "account.field_sre_count": "4",
              "account.field_notes": "ready",
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

  it("logs colored errors for unmapped headers and skips those values", async () => {
    fetchApolloAccountCustomFieldNameToIdMapMock.mockResolvedValueOnce(new Map<string, string>());
    fetchApolloAccountStageNameToIdMapMock.mockResolvedValueOnce(
      new Map<string, string>([["ChasingPOC", "stage_chasing_poc"]])
    );
    const consoleErrorSpy = vi.spyOn(console, "error").mockImplementation(() => {});

    await syncApolloAccountsFromOutputRows([makeRow()]);

    expect(consoleErrorSpy).toHaveBeenCalled();
    expect(String(consoleErrorSpy.mock.calls[0]?.[0] ?? "")).toContain("\x1b[31m");
    const attributes = apolloPostMock.mock.calls[0]?.[1]?.account_attributes?.[0];
    expect(attributes.account_stage_id).toBe("stage_chasing_poc");
    expect(attributes.typed_custom_fields).toBeUndefined();

    consoleErrorSpy.mockRestore();
  });

  it("maps Apollo fields case, underscore, and space insensitively", async () => {
    fetchApolloAccountCustomFieldNameToIdMapMock.mockResolvedValueOnce(
      new Map<string, string>([
        ["OBSERVABILITY TOOL", "account.field_observability"],
        ["Number_of_SREs", "account.field_sre_count"],
        ["  notes  ", "account.field_notes"],
      ])
    );
    fetchApolloAccountStageNameToIdMapMock.mockResolvedValueOnce(
      new Map<string, string>([
        ["chasing poc", "stage_chasing_poc"],
      ])
    );

    await syncApolloAccountsFromOutputRows([makeRow()]);

    expect(apolloPostMock).toHaveBeenCalledTimes(1);
    expect(apolloPostMock).toHaveBeenCalledWith(
      "/accounts/bulk_update",
      {
        account_attributes: [
          {
            id: "acc_1",
            account_stage_id: "stage_chasing_poc",
            typed_custom_fields: {
              "account.field_observability": "Datadog",
              "account.field_sre_count": "4",
              "account.field_notes": "ready",
            },
          },
        ],
      },
      0
    );
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

  it("skips rows with no mappable fields", async () => {
    fetchApolloAccountCustomFieldNameToIdMapMock.mockResolvedValueOnce(new Map<string, string>());
    fetchApolloAccountStageNameToIdMapMock.mockResolvedValueOnce(new Map<string, string>());
    const result = await syncApolloAccountsFromOutputRows([makeRow()]);

    expect(apolloPostMock).not.toHaveBeenCalled();
    expect(result.skippedNoMappableFieldsCount).toBe(1);
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
      account_stage_id: "stage_chasing_poc",
      typed_custom_fields: {
        "account.field_notes": "new",
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
