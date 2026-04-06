import PDFDocument from "pdfkit";
import {
  CampaignPushData,
  CampaignPushEntry,
  FilteredOutCampaignSummary,
  NormalEngineerApifyWarningSummary,
} from "../jobs/jobStore";

interface CampaignSection {
  label: string;
  entries: CampaignPushEntry[];
}

interface CompanyGroup {
  companyName: string;
  entries: CampaignPushEntry[];
}

interface FilteredOutReasonGroup {
  label: string;
  totalCount: number;
  companySummaries: Array<{ companyName: string; count: number }>;
}

const PAGE_MARGIN = 50;
const ACCENT_COLOR = "#1a1a2e" as const;
const SECTION_BG = "#f0f0f5" as const;
const LINK_COLOR = "#2563eb" as const;
const MUTED_COLOR = "#6b7280" as const;
const SUCCESS_BG = "#dcfce7" as const;
const SUCCESS_TEXT = "#166534" as const;
const FAILURE_BG = "#fee2e2" as const;
const FAILURE_TEXT = "#991b1b" as const;
const SKIPPED_BG = "#e5e7eb" as const;
const SKIPPED_TEXT = "#374151" as const;
const COMPANY_GROUP_BG = "#eef2ff" as const;
const COMPANY_GROUP_TEXT = "#312e81" as const;
const FILTER_SECTION_BG = "#eff6ff" as const;
const FILTER_CARD_BG = "#f8fafc" as const;
const FILTER_REASON_BADGE_BG = "#e0e7ff" as const;
const FILTER_REASON_BADGE_TEXT = "#3730a3" as const;
const WARNING_SECTION_BG = "#fff7ed" as const;
const WARNING_CARD_BG = "#fffaf0" as const;
const WARNING_BADGE_BG = "#ffedd5" as const;
const WARNING_BADGE_TEXT = "#9a3412" as const;
const WARNING_TEXT = "#7c2d12" as const;
const UNKNOWN_COMPANY_LABEL = "Unknown company";

function buildSections(data: CampaignPushData): CampaignSection[] {
  const mapping: {
    key: "linkedinSre" | "linkedinEngLead" | "linkedinEng" | "emailSre" | "emailEng" | "emailEngLead";
    label: string;
  }[] = [
    { key: "linkedinSre", label: "LinkedIn — SRE Campaign" },
    { key: "linkedinEngLead", label: "LinkedIn — Engineering Leaders Campaign" },
    { key: "linkedinEng", label: "LinkedIn — Engineering Campaign" },
    { key: "emailSre", label: "Email — SRE Campaign" },
    { key: "emailEng", label: "Email — Engineering Campaign" },
    { key: "emailEngLead", label: "Email — Engineering Leaders Campaign" },
  ];

  return mapping
    .filter(({ key }) => data[key].length > 0)
    .map(({ key, label }) => ({ label, entries: data[key] }));
}

function formatDate(): string {
  return new Date().toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}

function buildFilteredOutReasonGroups(entries: FilteredOutCampaignSummary[]): FilteredOutReasonGroup[] {
  const groups: FilteredOutReasonGroup[] = [];

  const openToWorkSummaries = entries
    .filter((entry) => entry.openToWorkCount > 0)
    .map((entry) => ({ companyName: entry.companyName, count: entry.openToWorkCount }));
  if (openToWorkSummaries.length > 0) {
    groups.push({
      label: "Filtered for OpenToWork",
      totalCount: openToWorkSummaries.reduce((sum, entry) => sum + entry.count, 0),
      companySummaries: openToWorkSummaries,
    });
  }
  const contractEmploymentSummaries = entries
    .filter((entry) => entry.contractEmploymentCount > 0)
    .map((entry) => ({ companyName: entry.companyName, count: entry.contractEmploymentCount }));
  if (contractEmploymentSummaries.length > 0) {
    groups.push({
      label: "Filtered for Contract Employment",
      totalCount: contractEmploymentSummaries.reduce((sum, entry) => sum + entry.count, 0),
      companySummaries: contractEmploymentSummaries,
    });
  }
  const frontendRoleSummaries = entries
    .filter((entry) => entry.frontendRoleCount > 0)
    .map((entry) => ({ companyName: entry.companyName, count: entry.frontendRoleCount }));
  if (frontendRoleSummaries.length > 0) {
    groups.push({
      label: "Filtered for Frontend Role",
      totalCount: frontendRoleSummaries.reduce((sum, entry) => sum + entry.count, 0),
      companySummaries: frontendRoleSummaries,
    });
  }

  return groups;
}

export function generateCampaignPdf(data: CampaignPushData): PDFKit.PDFDocument {
  const doc = new PDFDocument({
    size: "A4",
    margins: { top: PAGE_MARGIN, bottom: PAGE_MARGIN, left: PAGE_MARGIN, right: PAGE_MARGIN },
    bufferPages: true,
    info: {
      Title: "Campaign Push Report",
      Author: "LinkedIn Outreach Automation",
    },
  });

  const pageWidth = doc.page.width - PAGE_MARGIN * 2;

  doc.rect(0, 0, doc.page.width, 110).fill(ACCENT_COLOR);
  doc
    .font("Helvetica-Bold")
    .fontSize(22)
    .fillColor("#ffffff")
    .text("Campaign Push Report", PAGE_MARGIN, 38, { width: pageWidth });
  doc
    .font("Helvetica")
    .fontSize(10)
    .fillColor("#c0c0d0")
    .text(`Generated on ${formatDate()}`, PAGE_MARGIN, 68, { width: pageWidth });

  doc.y = 130;

  const sections = buildSections(data);

  if (sections.length === 0) {
    doc
      .font("Helvetica")
      .fontSize(12)
      .fillColor(MUTED_COLOR)
      .text("No candidates were pushed to any campaigns.", PAGE_MARGIN, doc.y);
    return doc;
  }

  const totalPeople = sections.reduce((sum, s) => sum + s.entries.length, 0);
  doc
    .font("Helvetica")
    .fontSize(10)
    .fillColor(MUTED_COLOR)
    .text(
      `${totalPeople} candidate${totalPeople === 1 ? "" : "s"} across ${sections.length} campaign${sections.length === 1 ? "" : "s"}`,
      PAGE_MARGIN,
      doc.y
    );
  doc.y += 20;

  for (let sIdx = 0; sIdx < sections.length; sIdx++) {
    const section = sections[sIdx];
    renderSection(doc, section, pageWidth, sIdx < sections.length - 1);
  }

  if (data.filteredOutCandidates.length > 0) {
    renderFilteredOutSection(doc, data.filteredOutCandidates, pageWidth);
  }

  if (data.normalEngineerApifyWarnings.length > 0) {
    renderNormalEngineerApifyWarningSection(doc, data.normalEngineerApifyWarnings, pageWidth);
  }

  return doc;
}

function ensureSpace(doc: PDFKit.PDFDocument, needed: number): void {
  const bottomLimit = doc.page.height - doc.page.margins.bottom;
  if (doc.y + needed > bottomLimit) {
    doc.addPage();
  }
}

function normalizeCompanyName(companyName: string | undefined): string {
  const trimmed = (companyName ?? "").trim();
  return trimmed.length > 0 ? trimmed : UNKNOWN_COMPANY_LABEL;
}

function groupEntriesByCompany(entries: CampaignPushEntry[]): CompanyGroup[] {
  const byCompany = new Map<string, CampaignPushEntry[]>();

  for (const entry of entries) {
    const companyName = normalizeCompanyName(entry.companyName);
    const existing = byCompany.get(companyName);
    if (existing) {
      existing.push(entry);
    } else {
      byCompany.set(companyName, [entry]);
    }
  }

  return [...byCompany.entries()].map(([companyName, groupedEntries]) => ({
    companyName,
    entries: groupedEntries,
  }));
}

function renderSection(
  doc: PDFKit.PDFDocument,
  section: CampaignSection,
  pageWidth: number,
  hasMore: boolean
): void {
  ensureSpace(doc, 80);

  doc
    .save()
    .roundedRect(PAGE_MARGIN - 4, doc.y, pageWidth + 8, 30, 4)
    .fill(SECTION_BG)
    .restore();

  doc
    .font("Helvetica-Bold")
    .fontSize(12)
    .fillColor(ACCENT_COLOR)
    .text(section.label, PAGE_MARGIN + 8, doc.y + 8, { width: pageWidth - 16 });

  doc.y += 38;

  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor(MUTED_COLOR)
    .text(`${section.entries.length} candidate${section.entries.length === 1 ? "" : "s"}`, PAGE_MARGIN, doc.y);

  doc.y += 16;

  const companyGroups = groupEntriesByCompany(section.entries);
  for (const group of companyGroups) {
    renderCompanyHeader(doc, group, pageWidth);
    for (const entry of group.entries) {
      renderEntry(doc, entry, pageWidth);
    }
    doc.y += 4;
  }

  if (hasMore) {
    doc.y += 12;
    doc
      .save()
      .moveTo(PAGE_MARGIN, doc.y)
      .lineTo(PAGE_MARGIN + pageWidth, doc.y)
      .strokeColor("#e0e0e8")
      .lineWidth(0.5)
      .stroke()
      .restore();
    doc.y += 18;
  }
}

function renderCompanyHeader(doc: PDFKit.PDFDocument, group: CompanyGroup, pageWidth: number): void {
  ensureSpace(doc, 34);

  const label = `${group.companyName} (${group.entries.length})`;

  doc
    .save()
    .roundedRect(PAGE_MARGIN + 8, doc.y, pageWidth - 16, 22, 4)
    .fill(COMPANY_GROUP_BG)
    .restore();

  doc
    .font("Helvetica-Bold")
    .fontSize(9)
    .fillColor(COMPANY_GROUP_TEXT)
    .text(label, PAGE_MARGIN + 16, doc.y + 7, { width: pageWidth - 32 });

  doc.y += 28;
}

function renderEntry(
  doc: PDFKit.PDFDocument,
  entry: CampaignPushEntry,
  pageWidth: number
): void {
  const hasFailureError = entry.lemlistStatus === "failed" && Boolean(entry.lemlistError);
  ensureSpace(doc, hasFailureError ? 78 : 58);

  const skippedStatusLabel = entry.lemlistError?.toLowerCase().includes("missing email")
    ? "Missing email"
    : "Already in campaign";

  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor("#111827")
    .text(entry.name, PAGE_MARGIN + 8, doc.y, { width: pageWidth - 16 });

  const statusLabel = entry.lemlistStatus === "succeed"
    ? "Lemlist succeed"
    : entry.lemlistStatus === "failed"
      ? "Lemlist failed"
      : skippedStatusLabel;
  const statusBg = entry.lemlistStatus === "succeed"
    ? SUCCESS_BG
    : entry.lemlistStatus === "failed"
      ? FAILURE_BG
      : SKIPPED_BG;
  const statusText = entry.lemlistStatus === "succeed"
    ? SUCCESS_TEXT
    : entry.lemlistStatus === "failed"
      ? FAILURE_TEXT
      : SKIPPED_TEXT;
  const statusY = doc.y + 2;
  const statusX = PAGE_MARGIN + 8;
  const statusFontSize = 8;
  const statusPaddingX = 6;
  const statusPaddingY = 3;
  const statusTextWidth = doc.font("Helvetica-Bold").fontSize(statusFontSize).widthOfString(statusLabel);
  const statusWidth = statusTextWidth + statusPaddingX * 2;
  const statusHeight = statusFontSize + statusPaddingY * 2 + 1;

  doc.save().roundedRect(statusX, statusY, statusWidth, statusHeight, 4).fill(statusBg).restore();
  doc
    .font("Helvetica-Bold")
    .fontSize(statusFontSize)
    .fillColor(statusText)
    .text(statusLabel, statusX + statusPaddingX, statusY + statusPaddingY, {
      width: statusTextWidth + 1,
    });

  doc.y = statusY + statusHeight + 4;
  const detailY = doc.y;
  const title = entry.title || "—";

  if (entry.linkedinUrl) {
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor(MUTED_COLOR)
      .text(`${title}  ·  `, PAGE_MARGIN + 8, detailY, { continued: true, width: pageWidth - 16 });
    doc
      .fillColor(LINK_COLOR)
      .text(entry.linkedinUrl, { link: normalizeLink(entry.linkedinUrl), underline: true });
  } else {
    doc
      .font("Helvetica")
      .fontSize(9)
      .fillColor(MUTED_COLOR)
      .text(title, PAGE_MARGIN + 8, detailY, { width: pageWidth - 16 });
  }

  if ((entry.lemlistStatus === "failed" || entry.lemlistStatus === "skipped") && entry.lemlistError) {
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor(entry.lemlistStatus === "failed" ? FAILURE_TEXT : SKIPPED_TEXT)
      .text(`${entry.lemlistStatus === "failed" ? "Error" : "Reason"}: ${entry.lemlistError}`, PAGE_MARGIN + 8, doc.y + 2, {
        width: pageWidth - 16,
      });
  }

  doc.y += 8;
}

function renderFilteredOutSection(
  doc: PDFKit.PDFDocument,
  entries: FilteredOutCampaignSummary[],
  pageWidth: number
): void {
  const groups = buildFilteredOutReasonGroups(entries);
  if (groups.length === 0) {
    return;
  }

  doc.y += 8;
  ensureSpace(doc, 96);
  doc
    .save()
    .moveTo(PAGE_MARGIN, doc.y)
    .lineTo(PAGE_MARGIN + pageWidth, doc.y)
    .strokeColor("#dbeafe")
    .lineWidth(1)
    .stroke()
    .restore();

  doc.y += 18;
  ensureSpace(doc, 64);
  doc
    .save()
    .roundedRect(PAGE_MARGIN - 4, doc.y, pageWidth + 8, 34, 6)
    .fill(FILTER_SECTION_BG)
    .restore();

  doc
    .font("Helvetica-Bold")
    .fontSize(12)
    .fillColor(ACCENT_COLOR)
    .text("Filtered Out Candidates", PAGE_MARGIN + 8, doc.y + 8, { width: pageWidth - 16 });

  doc.y += 42;
  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor(MUTED_COLOR)
    .text(
      `${groups.reduce((sum, group) => sum + group.totalCount, 0)} candidate summaries recorded across ${entries.length} compan${entries.length === 1 ? "y" : "ies"}`,
      PAGE_MARGIN,
      doc.y
    );
  doc.y += 14;

  for (const group of groups) {
    ensureSpace(doc, 44);
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .fillColor("#1e3a8a")
      .text(group.label, PAGE_MARGIN, doc.y, { width: pageWidth });
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor(MUTED_COLOR)
      .text(`${group.totalCount} candidate${group.totalCount === 1 ? "" : "s"} across ${group.companySummaries.length} compan${group.companySummaries.length === 1 ? "y" : "ies"}`, PAGE_MARGIN, doc.y + 14, { width: pageWidth });
    doc.y += 28;

    for (const entry of group.companySummaries) {
      renderSummaryCountCard(doc, {
        companyName: entry.companyName,
        count: entry.count,
        badgeLabel: "Filtered",
        badgeBg: FILTER_REASON_BADGE_BG,
        badgeText: FILTER_REASON_BADGE_TEXT,
      }, pageWidth, FILTER_CARD_BG);
    }

    doc.y += 4;
  }
}

function renderSummaryCountCard(
  doc: PDFKit.PDFDocument,
  entry: {
    companyName: string;
    count: number;
    badgeLabel: string;
    badgeBg: string;
    badgeText: string;
  },
  pageWidth: number,
  backgroundColor: string
): void {
  ensureSpace(doc, 54);
  const cardX = PAGE_MARGIN + 4;
  const cardY = doc.y;
  const cardWidth = pageWidth - 8;
  const cardHeight = 42;

  doc
    .save()
    .roundedRect(cardX, cardY, cardWidth, cardHeight, 6)
    .fill(backgroundColor)
    .restore();

  const badgeTextWidth = doc.font("Helvetica-Bold").fontSize(7.5).widthOfString(entry.badgeLabel);
  const badgeWidth = badgeTextWidth + 10;
  const badgeX = cardX + cardWidth - badgeWidth - 10;
  const badgeY = cardY + 8;

  doc
    .save()
    .roundedRect(badgeX, badgeY, badgeWidth, 14, 6)
    .fill(entry.badgeBg)
    .restore();
  doc
    .font("Helvetica-Bold")
    .fontSize(7.5)
    .fillColor(entry.badgeText)
    .text(entry.badgeLabel, badgeX + 5, badgeY + 4, { width: badgeTextWidth + 1 });

  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor("#111827")
    .text(entry.companyName, cardX + 10, cardY + 10, { width: Math.max(90, badgeX - (cardX + 16)) });

  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor(MUTED_COLOR)
    .text(`${entry.count} candidate${entry.count === 1 ? "" : "s"}`, cardX + 10, cardY + 26, { width: cardWidth - 20 });

  doc.y = cardY + cardHeight + 8;
}

function renderNormalEngineerApifyWarningSection(
  doc: PDFKit.PDFDocument,
  entries: NormalEngineerApifyWarningSummary[],
  pageWidth: number
): void {
  doc.y += 8;
  ensureSpace(doc, 96);
  doc
    .save()
    .moveTo(PAGE_MARGIN, doc.y)
    .lineTo(PAGE_MARGIN + pageWidth, doc.y)
    .strokeColor("#fed7aa")
    .lineWidth(1)
    .stroke()
    .restore();

  doc.y += 18;
  ensureSpace(doc, 64);
  doc
    .save()
    .roundedRect(PAGE_MARGIN - 4, doc.y, pageWidth + 8, 34, 6)
    .fill(WARNING_SECTION_BG)
    .restore();

  doc
    .font("Helvetica-Bold")
    .fontSize(12)
    .fillColor(WARNING_TEXT)
    .text("Warnings — Normal Engineer Apify Match", PAGE_MARGIN + 8, doc.y + 8, { width: pageWidth - 16 });

  doc.y += 42;
  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor(MUTED_COLOR)
    .text(
      `${entries.reduce((sum, entry) => sum + entry.totalCount, 0)} warning${entries.reduce((sum, entry) => sum + entry.totalCount, 0) === 1 ? "" : "s"} across ${entries.length} compan${entries.length === 1 ? "y" : "ies"}`,
      PAGE_MARGIN,
      doc.y
    );
  doc.y += 14;

  for (const entry of entries) {
    renderNormalEngineerApifyWarningEntry(doc, entry, pageWidth);
  }
}

function renderNormalEngineerApifyWarningEntry(
  doc: PDFKit.PDFDocument,
  entry: NormalEngineerApifyWarningSummary,
  pageWidth: number
): void {
  const problemText = entry.problems
    .map((problem) => `${problem.problem}: ${problem.count}`)
    .join(" | ");
  ensureSpace(doc, 86);
  const cardX = PAGE_MARGIN + 4;
  const cardY = doc.y;
  const cardWidth = pageWidth - 8;
  const cardHeight = 74;

  doc
    .save()
    .roundedRect(cardX, cardY, cardWidth, cardHeight, 6)
    .fill(WARNING_CARD_BG)
    .restore();

  const warningBadgeLabel = "Warning";
  const badgeTextWidth = doc.font("Helvetica-Bold").fontSize(7.5).widthOfString(warningBadgeLabel);
  const badgeWidth = badgeTextWidth + 10;
  const badgeX = cardX + cardWidth - badgeWidth - 10;
  const badgeY = cardY + 8;

  doc
    .save()
    .roundedRect(badgeX, badgeY, badgeWidth, 14, 6)
    .fill(WARNING_BADGE_BG)
    .restore();
  doc
    .font("Helvetica-Bold")
    .fontSize(7.5)
    .fillColor(WARNING_BADGE_TEXT)
    .text(warningBadgeLabel, badgeX + 5, badgeY + 4, { width: badgeTextWidth + 1 });

  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor("#111827")
    .text(entry.companyName, cardX + 10, cardY + 9, { width: Math.max(90, badgeX - (cardX + 16)) });

  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor(MUTED_COLOR)
    .text(`${entry.totalCount} warning${entry.totalCount === 1 ? "" : "s"}`, cardX + 10, cardY + 28, { width: cardWidth - 20 });

  doc
    .font("Helvetica")
    .fontSize(8.5)
    .fillColor(WARNING_TEXT)
    .text(`Problems: ${problemText}`, cardX + 10, cardY + 46, { width: cardWidth - 20 });

  doc.y = cardY + cardHeight + 8;
}

function normalizeLink(url: string): string {
  if (/^https?:\/\//i.test(url)) return url;
  return `https://${url}`;
}
