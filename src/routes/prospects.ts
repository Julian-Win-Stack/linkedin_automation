import { Router } from "express";
import { bulkEnrichPeople } from "../services/bulkEnrichPeople";
import { getCompany } from "../services/getCompany";
import { searchPeople } from "../services/searchPeople";
import { EnrichedEmployee, Prospect } from "../types/prospect";

const router = Router();
const SRE_PERSON_TITLES = ["SRE", "Site Reliability"];
const MAX_RESULTS = 30;

interface ProspectRequestBody {
  companyUrl?: unknown;
}

function dedupeProspectsById(prospects: Prospect[]): Prospect[] {
  const seen = new Set<string>();
  const deduped: Prospect[] = [];

  for (const prospect of prospects) {
    if (seen.has(prospect.id)) {
      continue;
    }
    seen.add(prospect.id);
    deduped.push(prospect);
  }

  return deduped;
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

    const company = await getCompany(companyUrl);
    const prospects: Prospect[] = await searchPeople(company, MAX_RESULTS, SRE_PERSON_TITLES);
    const dedupedProspects = dedupeProspectsById(prospects);
    const enrichedEmployees: EnrichedEmployee[] = await bulkEnrichPeople(dedupedProspects);

    return res.status(200).json({
      data: enrichedEmployees,
      meta: {
        count: enrichedEmployees.length,
        searchedCount: prospects.length,
        enrichedCount: enrichedEmployees.length,
        maxResults: MAX_RESULTS,
        companyUrl,
        personTitles: SRE_PERSON_TITLES,
      },
    });
  } catch (error) {
    const isCompanyNotFound = error instanceof Error && error.name === "CompanyNotFoundError";
    const isInvalidCompanyInput =
      error instanceof Error && error.name === "InvalidCompanyInputError";

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
            : "Invalid company input. Use a company domain (example: acme.com).",
      });
    }
    const message = error instanceof Error ? error.message : "Unexpected server error.";
    return res.status(500).json({
      error: message,
    });
  }
});

export default router;
