import { Router } from "express";
import { getCompany } from "../services/getCompany";
import { searchPeople } from "../services/searchPeople";
import { Prospect } from "../types/prospect";

const router = Router();

const FILTER_PRESETS = {
  srePlatform: ["SRE", "Site Reliability", "Platform"],
  engineer: ["Engineer"],
} as const;

interface ProspectRequestBody {
  companyUrl?: unknown;
  maxResults?: unknown;
  filterPreset?: unknown;
  titleKeywords?: unknown;
}

class BadRequestError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BadRequestError";
  }
}

function parseMaxResults(rawValue: unknown): number {
  if (rawValue == null) {
    return 100;
  }

  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new BadRequestError("maxResults must be a positive number.");
  }

  return Math.min(Math.floor(parsed), 100);
}

function parseFilterPreset(rawValue: unknown): keyof typeof FILTER_PRESETS {
  if (rawValue == null) {
    return "srePlatform";
  }

  if (rawValue !== "srePlatform" && rawValue !== "engineer") {
    throw new BadRequestError("filterPreset must be either 'srePlatform' or 'engineer'.");
  }

  return rawValue;
}

function parseTitleKeywords(
  rawValue: unknown,
  filterPreset: keyof typeof FILTER_PRESETS
): string[] {
  if (rawValue == null) {
    return [...FILTER_PRESETS[filterPreset]];
  }

  if (!Array.isArray(rawValue)) {
    throw new BadRequestError("titleKeywords must be an array of strings.");
  }

  const keywords = rawValue
    .filter((item) => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);

  if (keywords.length === 0) {
    throw new BadRequestError("titleKeywords must contain at least one non-empty string.");
  }

  return keywords;
}

router.post("/search", async (req, res) => {
  try {
    const body = (req.body ?? {}) as ProspectRequestBody;
    const companyUrl = typeof body.companyUrl === "string" ? body.companyUrl.trim() : "";

    if (!companyUrl) {
      return res.status(400).json({
        error: "companyUrl is required and must be a non-empty string.",
      });
    }

    const maxResults = parseMaxResults(body.maxResults);
    const filterPreset = parseFilterPreset(body.filterPreset);
    const titleKeywords = parseTitleKeywords(body.titleKeywords, filterPreset);

    const company = await getCompany(companyUrl);
    const prospects: Prospect[] = await searchPeople(company, maxResults, titleKeywords);

    return res.status(200).json({
      data: prospects,
      meta: {
        count: prospects.length,
        maxResults,
        companyUrl,
        filterPreset,
        titleKeywords,
      },
    });
  } catch (error) {
    const isCompanyNotFound = error instanceof Error && error.name === "CompanyNotFoundError";
    const isInvalidCompanyInput =
      error instanceof Error && error.name === "InvalidCompanyInputError";
    const isBadRequest = error instanceof Error && error.name === "BadRequestError";

    if (isCompanyNotFound) {
      return res.status(404).json({
        error: error instanceof Error ? error.message : "Company not found in Apollo.",
      });
    }
    if (isInvalidCompanyInput) {
      return res.status(400).json({
        error:
          error instanceof Error
            ? error.message
            : "Invalid company input. Use domain or LinkedIn company URL.",
      });
    }
    if (isBadRequest) {
      return res.status(400).json({
        error: error instanceof Error ? error.message : "Invalid request.",
      });
    }

    const message = error instanceof Error ? error.message : "Unexpected server error.";
    return res.status(500).json({
      error: message,
    });
  }
});

export default router;
