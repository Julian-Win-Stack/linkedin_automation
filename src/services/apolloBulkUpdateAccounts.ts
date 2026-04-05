import { apolloPost, fetchApolloAccountCustomFieldNameToIdMap } from "./apolloClient";
import { OutputRow } from "./observability/csvWriter";

const APOLLO_BULK_UPDATE_BATCH_SIZE = 500;
const APOLLO_BULK_UPDATE_MAX_PER_REQUEST = 1000;
const APOLLO_ERROR_COLOR = "\x1b[31m";
const APOLLO_WARNING_COLOR = "\x1b[33m";
const ANSI_RESET = "\x1b[0m";

const COLUMN_KEY_TO_HEADER: Partial<Record<keyof OutputRow, string>> = {
  company_name: "Company Name",
  company_domain: "Website",
  company_linkedin_url: "Company Linkedin Url",
  apollo_account_id: "Apollo Account Id",
  observability_tool_research: "observability_tool",
  stage: "Stage",
  sre_count: "Number of SREs",
  notes: "Notes",
};

const EXCLUDED_HEADERS = new Set<string>([
  "Company Name",
  "Website",
  "Company Linkedin Url",
  "Apollo Account Id",
]);

interface ApolloBulkUpdateAccountAttribute {
  id: string;
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

function buildNormalizedFieldNameToId(fieldNameToId: Map<string, string>): Map<string, string> {
  const normalizedToFieldId = new Map<string, string>();
  for (const [fieldName, fieldId] of fieldNameToId.entries()) {
    const normalizedFieldName = normalizeMappingToken(fieldName);
    if (normalizedFieldName.length === 0) {
      continue;
    }
    if (!normalizedToFieldId.has(normalizedFieldName)) {
      normalizedToFieldId.set(normalizedFieldName, fieldId);
    }
  }
  return normalizedToFieldId;
}

function buildAccountAttributesPayload(rows: OutputRow[], fieldNameToId: Map<string, string>): BuildPayloadResult {
  const unmappedHeadersSet = new Set<string>();
  const accountIdToAttributes = new Map<string, ApolloBulkUpdateAccountAttribute>();
  const dedupedAccountIdsInOrder: string[] = [];
  const duplicateAccountIds = new Set<string>();
  const normalizedFieldNameToId = buildNormalizedFieldNameToId(fieldNameToId);
  let skippedMissingAccountIdCount = 0;
  let skippedNoMappableFieldsCount = 0;

  for (const row of rows) {
    const accountId = toDisplayValue(row.apollo_account_id);
    if (!accountId) {
      skippedMissingAccountIdCount += 1;
      continue;
    }

    const typedCustomFields: Record<string, string> = {};

    for (const [columnKey, rawValue] of Object.entries(row) as Array<[keyof OutputRow, unknown]>) {
      const header = COLUMN_KEY_TO_HEADER[columnKey];
      if (!header || EXCLUDED_HEADERS.has(header)) {
        continue;
      }

      const displayValue = toDisplayValue(rawValue);
      if (!displayValue) {
        continue;
      }

      const fieldId = normalizedFieldNameToId.get(normalizeMappingToken(header));
      if (!fieldId) {
        unmappedHeadersSet.add(header);
        continue;
      }

      typedCustomFields[fieldId] = displayValue;
    }

    if (Object.keys(typedCustomFields).length === 0) {
      skippedNoMappableFieldsCount += 1;
      continue;
    }

    if (!accountIdToAttributes.has(accountId)) {
      dedupedAccountIdsInOrder.push(accountId);
    } else {
      duplicateAccountIds.add(accountId);
    }

    accountIdToAttributes.set(accountId, {
      id: accountId,
      typed_custom_fields: typedCustomFields,
    });
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
  const fieldNameToId = await fetchApolloAccountCustomFieldNameToIdMap();
  const payloadResult = buildAccountAttributesPayload(rows, fieldNameToId);

  for (const header of payloadResult.unmappedHeaders) {
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
};
