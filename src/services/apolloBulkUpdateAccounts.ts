import {
  apolloPost,
} from "./apolloClient";
import { OutputRow } from "./observability/csvWriter";

const APOLLO_BULK_UPDATE_BATCH_SIZE = 500;
const APOLLO_BULK_UPDATE_MAX_PER_REQUEST = 1000;
const APOLLO_ERROR_COLOR = "\x1b[31m";
const APOLLO_WARNING_COLOR = "\x1b[33m";
const ANSI_RESET = "\x1b[0m";
const HARDCODED_CUSTOM_FIELD_IDS: Partial<Record<keyof OutputRow, string>> = {};
const CURRENT_WEEK_CUSTOM_FIELD_ID = "69af02b6aa89be0015250321";

export function formatCurrentWeekLabel(nowMs: number = Date.now()): string {
  const d = new Date(nowMs);
  d.setHours(0, 0, 0, 0);
  const dayOfWeek = d.getDay();
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  d.setDate(d.getDate() - daysSinceMonday);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `Week of ${yyyy}-${mm}-${dd}`;
}
const HARDCODED_STAGE_IDS = new Map<string, string>([
  ["Prospecting", "68cc9bfa8d565f0021b01746"],
  ["ChasingPOC", "6971e93a8f17d1001569a9bb"],
  ["Connected", "69b9aba7bf59020015c614fd"],
  ["Pending Warm Intro", "697ff0833a1eb5002124920a"],
  ["Lead", "68cc9bfa8d565f0021b01748"],
  ["Interest", "697ff09c7203e20021841515"],
  ["NotActionableNow", "68cc9bfa8d565f0021b01749"],
  ["Current Client", "68cc9bfa8d565f0021b01747"],
  ["KeepWarm", "697ff0c840ed7d0011df04bc"],
  ["Do Not Prospect", "68cc9bfa8d565f0021b0174a"],
  ["Competitor's Client", "691e23559a765d0015803b24"],
]);

const COLUMN_KEY_TO_HEADER: Partial<Record<keyof OutputRow, string>> = {
  company_name: "Company Name",
  company_domain: "Website",
  company_linkedin_url: "Company Linkedin Url",
  apollo_account_id: "Apollo Account Id",
  stage: "Stage",
};

const EXCLUDED_HEADERS = new Set<string>([
  "Company Name",
  "Website",
  "Company Linkedin Url",
  "Apollo Account Id",
]);

interface ApolloBulkUpdateAccountAttribute {
  id: string;
  account_stage_id?: string;
  typed_custom_fields?: Record<string, string>;
}

interface ApolloBulkUpdateResponse {
  accounts?: Array<{ id?: string }>;
}

interface BuildPayloadResult {
  accountAttributes: ApolloBulkUpdateAccountAttribute[];
  skippedMissingAccountIdCount: number;
  skippedNoMappableFieldsCount: number;
  duplicateAccountIdCount: number;
  unmappedHeaders: string[];
}

export interface ApolloBulkUpdateSyncResult {
  attemptedRows: number;
  dedupedAccounts: number;
  updatedAccounts: number;
  skippedMissingAccountIdCount: number;
  skippedNoMappableFieldsCount: number;
  duplicateAccountIdCount: number;
  warnings: string[];
}

function toDisplayValue(value: unknown): string | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  const text = String(value).trim();
  return text.length > 0 ? text : undefined;
}

function normalizeMappingToken(value: string): string {
  return value.toLowerCase().replace(/[_\s]+/g, "");
}

function looksLikeApolloId(value: string): boolean {
  return /^[a-f0-9]{24}$/i.test(value);
}

function buildAccountAttributesPayload(
  rows: OutputRow[],
  stageNameToId: Map<string, string>,
  weekLabel: string = formatCurrentWeekLabel()
): BuildPayloadResult {
  const unmappedHeadersSet = new Set<string>();
  const accountIdToAttributes = new Map<string, ApolloBulkUpdateAccountAttribute>();
  const dedupedAccountIdsInOrder: string[] = [];
  const duplicateAccountIds = new Set<string>();
  const normalizedStageNameToId = new Map<string, string>();
  for (const [stageName, stageId] of stageNameToId.entries()) {
    const normalized = normalizeMappingToken(stageName);
    if (normalized.length > 0 && !normalizedStageNameToId.has(normalized)) {
      normalizedStageNameToId.set(normalized, stageId);
    }
  }
  let skippedMissingAccountIdCount = 0;
  let skippedNoMappableFieldsCount = 0;

  for (const row of rows) {
    const accountId = toDisplayValue(row.apollo_account_id);
    if (!accountId) {
      skippedMissingAccountIdCount += 1;
      continue;
    }

    if (!toDisplayValue(row.stage)) {
      skippedNoMappableFieldsCount += 1;
      continue;
    }

    const typedCustomFields: Record<string, string> = {};
    let accountStageId: string | undefined;

    for (const [columnKey, rawValue] of Object.entries(row) as Array<[keyof OutputRow, unknown]>) {
      const header = COLUMN_KEY_TO_HEADER[columnKey];
      if (!header || EXCLUDED_HEADERS.has(header)) {
        continue;
      }

      const displayValue = toDisplayValue(rawValue);
      if (!displayValue) {
        continue;
      }

      if (columnKey === "stage") {
        const mappedStageId = normalizedStageNameToId.get(normalizeMappingToken(displayValue));
        if (mappedStageId) {
          accountStageId = mappedStageId;
        } else if (looksLikeApolloId(displayValue)) {
          accountStageId = displayValue;
        } else {
          unmappedHeadersSet.add(header);
        }
        continue;
      }

      const fieldId = HARDCODED_CUSTOM_FIELD_IDS[columnKey];
      if (!fieldId) {
        unmappedHeadersSet.add(header);
        continue;
      }

      typedCustomFields[fieldId] = displayValue;
    }

    typedCustomFields[CURRENT_WEEK_CUSTOM_FIELD_ID] = weekLabel;

    if (!accountIdToAttributes.has(accountId)) {
      dedupedAccountIdsInOrder.push(accountId);
    } else {
      duplicateAccountIds.add(accountId);
    }

    const attributes: ApolloBulkUpdateAccountAttribute = {
      id: accountId,
    };
    if (accountStageId) {
      attributes.account_stage_id = accountStageId;
    }
    if (Object.keys(typedCustomFields).length > 0) {
      attributes.typed_custom_fields = typedCustomFields;
    }
    accountIdToAttributes.set(accountId, attributes);
  }

  const accountAttributes = dedupedAccountIdsInOrder
    .map((accountId) => accountIdToAttributes.get(accountId))
    .filter((item): item is ApolloBulkUpdateAccountAttribute => Boolean(item));

  return {
    accountAttributes,
    skippedMissingAccountIdCount,
    skippedNoMappableFieldsCount,
    duplicateAccountIdCount: duplicateAccountIds.size,
    unmappedHeaders: [...unmappedHeadersSet].sort((a, b) => a.localeCompare(b)),
  };
}

function chunkArray<T>(items: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let start = 0; start < items.length; start += chunkSize) {
    chunks.push(items.slice(start, start + chunkSize));
  }
  return chunks;
}

async function sendBatchWithSingleRetry(
  batch: ApolloBulkUpdateAccountAttribute[],
  batchIndex: number,
  totalBatches: number
): Promise<void> {
  const requestBody = { account_attributes: batch };
  try {
    await apolloPost<ApolloBulkUpdateResponse>("/accounts/bulk_update", requestBody, 0);
  } catch (firstError) {
    try {
      await apolloPost<ApolloBulkUpdateResponse>("/accounts/bulk_update", requestBody, 0);
    } catch (secondError) {
      const errorMessage = secondError instanceof Error ? secondError.message : "Unknown Apollo bulk update error";
      throw new Error(
        `Apollo bulk update failed for batch ${batchIndex + 1}/${totalBatches} (size=${batch.length}) after 1 retry: ${errorMessage}`
      );
    }

    const firstErrorMessage = firstError instanceof Error ? firstError.message : "Unknown Apollo bulk update error";
    console.warn(
      `${APOLLO_WARNING_COLOR}[Apollo][BulkUpdate][WARN] batch=${batchIndex + 1}/${totalBatches} recovered on retry. first_error=${firstErrorMessage}${ANSI_RESET}`
    );
  }
}

export async function syncApolloAccountsFromOutputRows(rows: OutputRow[]): Promise<ApolloBulkUpdateSyncResult> {
  const warnings: string[] = [];
  const payloadResult = buildAccountAttributesPayload(rows, HARDCODED_STAGE_IDS);

  for (const header of payloadResult.unmappedHeaders) {
    warnings.push(
      `Apollo custom field mapping missing for "${header}". Stage updates can still succeed while this field is skipped.`
    );
    console.error(
      `${APOLLO_ERROR_COLOR}[Apollo][BulkUpdate][ERROR] Unmapped output column "${header}" - skipping values for account updates.${ANSI_RESET}`
    );
  }

  if (payloadResult.duplicateAccountIdCount > 0) {
    warnings.push(
      `Apollo bulk update deduped ${payloadResult.duplicateAccountIdCount} duplicate account ID(s); last row values were used.`
    );
  }

  if (payloadResult.accountAttributes.length === 0) {
    return {
      attemptedRows: rows.length,
      dedupedAccounts: 0,
      updatedAccounts: 0,
      skippedMissingAccountIdCount: payloadResult.skippedMissingAccountIdCount,
      skippedNoMappableFieldsCount: payloadResult.skippedNoMappableFieldsCount,
      duplicateAccountIdCount: payloadResult.duplicateAccountIdCount,
      warnings,
    };
  }

  if (payloadResult.accountAttributes.length > APOLLO_BULK_UPDATE_MAX_PER_REQUEST && APOLLO_BULK_UPDATE_BATCH_SIZE > APOLLO_BULK_UPDATE_MAX_PER_REQUEST) {
    throw new Error("Apollo bulk update batch size is larger than API maximum.");
  }

  const batches = chunkArray(payloadResult.accountAttributes, APOLLO_BULK_UPDATE_BATCH_SIZE);
  let updatedAccounts = 0;

  for (let batchIndex = 0; batchIndex < batches.length; batchIndex += 1) {
    const batch = batches[batchIndex];
    await sendBatchWithSingleRetry(batch, batchIndex, batches.length);
    updatedAccounts += batch.length;
  }

  return {
    attemptedRows: rows.length,
    dedupedAccounts: payloadResult.accountAttributes.length,
    updatedAccounts,
    skippedMissingAccountIdCount: payloadResult.skippedMissingAccountIdCount,
    skippedNoMappableFieldsCount: payloadResult.skippedNoMappableFieldsCount,
    duplicateAccountIdCount: payloadResult.duplicateAccountIdCount,
    warnings,
  };
}

export const __testOnly__ = {
  buildAccountAttributesPayload,
  chunkArray,
  formatCurrentWeekLabel,
  CURRENT_WEEK_CUSTOM_FIELD_ID,
};
