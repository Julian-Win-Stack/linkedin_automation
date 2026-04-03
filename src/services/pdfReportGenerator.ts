import PDFDocument from "pdfkit";
import {
  CampaignPushData,
  CampaignPushEntry,
  FilteredOutCampaignEntry,
  NormalEngineerApifyWarningEntry,
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
  entries: FilteredOutCampaignEntry[];
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
    key: "linkedinSre" | "linkedinEng" | "emailSre" | "emailEng" | "emailEngLead";
    label: string;
  }[] = [
    { key: "linkedinSre", label: "LinkedIn — SRE Campaign" },
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

function buildFilteredOutReasonGroups(entries: FilteredOutCampaignEntry[]): FilteredOutReasonGroup[] {
  const openToWork = entries.filter((entry) => entry.reason === "open_to_work");
  const frontendRole = entries.filter((entry) => entry.reason === "frontend_role");
  const contractEmployment = entries.filter((entry) => entry.reason === "contract_employment");
  const groups: FilteredOutReasonGroup[] = [];

  if (openToWork.length > 0) {
    groups.push({ label: "Filtered for OpenToWork", entries: openToWork });
  }
  if (contractEmployment.length > 0) {
    groups.push({ label: "Filtered for Contract Employment", entries: contractEmployment });
  }
  if (frontendRole.length > 0) {
    groups.push({ label: "Filtered for Frontend Role", entries: frontendRole });
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

  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor("#111827")
    .text(entry.name, PAGE_MARGIN + 8, doc.y, { width: pageWidth - 16 });

  const statusLabel = entry.lemlistStatus === "succeed" ? "Lemlist succeed" : "Lemlist failed";
  const statusBg = entry.lemlistStatus === "succeed" ? SUCCESS_BG : FAILURE_BG;
  const statusText = entry.lemlistStatus === "succeed" ? SUCCESS_TEXT : FAILURE_TEXT;
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

  if (entry.lemlistStatus === "failed" && entry.lemlistError) {
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor(FAILURE_TEXT)
      .text(`Error: ${entry.lemlistError}`, PAGE_MARGIN + 8, doc.y + 2, { width: pageWidth - 16 });
  }

  doc.y += 8;
}

function renderFilteredOutSection(
  doc: PDFKit.PDFDocument,
  entries: FilteredOutCampaignEntry[],
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
    .text(`${entries.length} candidate${entries.length === 1 ? "" : "s"} were filtered out`, PAGE_MARGIN, doc.y);
  doc.y += 14;

  for (const group of groups) {
    ensureSpace(doc, 44);
    doc
      .font("Helvetica-Bold")
      .fontSize(9)
      .fillColor("#1e3a8a")
      .text(group.label, PAGE_MARGIN, doc.y, { width: pageWidth });
    doc.y += 6;

    for (const entry of group.entries) {
      renderFilteredOutEntry(doc, entry, pageWidth);
    }

    doc.y += 4;
  }
}

function renderFilteredOutEntry(
  doc: PDFKit.PDFDocument,
  entry: FilteredOutCampaignEntry,
  pageWidth: number
): void {
  ensureSpace(doc, 72);
  const cardX = PAGE_MARGIN + 4;
  const cardY = doc.y;
  const cardWidth = pageWidth - 8;
  const cardHeight = 60;

  doc
    .save()
    .roundedRect(cardX, cardY, cardWidth, cardHeight, 6)
    .fill(FILTER_CARD_BG)
    .restore();

  const reasonLabel = entry.reason === "open_to_work"
    ? "Reason: OpenToWork profile"
    : entry.reason === "contract_employment"
      ? "Reason: Contract employment type"
      : "Reason: Frontend-focused role";
  const reasonTextWidth = doc.font("Helvetica-Bold").fontSize(7.5).widthOfString(reasonLabel);
  const badgeWidth = reasonTextWidth + 10;
  const badgeX = cardX + cardWidth - badgeWidth - 10;
  const badgeY = cardY + 8;

  doc
    .save()
    .roundedRect(badgeX, badgeY, badgeWidth, 14, 6)
    .fill(FILTER_REASON_BADGE_BG)
    .restore();
  doc
    .font("Helvetica-Bold")
    .fontSize(7.5)
    .fillColor(FILTER_REASON_BADGE_TEXT)
    .text(reasonLabel, badgeX + 5, badgeY + 4, { width: reasonTextWidth + 1 });

  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor("#111827")
    .text(entry.name, cardX + 10, cardY + 9, { width: Math.max(90, badgeX - (cardX + 16)) });

  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor(MUTED_COLOR)
    .text(entry.title || "—", cardX + 10, cardY + 28, { width: cardWidth - 20 });

  if (entry.linkedinUrl) {
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor(LINK_COLOR)
      .text(entry.linkedinUrl, cardX + 10, cardY + 44, {
        width: cardWidth - 20,
        link: normalizeLink(entry.linkedinUrl),
        underline: true,
      });
  } else {
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor(MUTED_COLOR)
      .text("LinkedIn URL: —", cardX + 10, cardY + 44, { width: cardWidth - 20 });
  }

  doc.y = cardY + cardHeight + 8;
}

function renderNormalEngineerApifyWarningSection(
  doc: PDFKit.PDFDocument,
  entries: NormalEngineerApifyWarningEntry[],
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
    .text(`${entries.length} candidate${entries.length === 1 ? "" : "s"} passed with warning`, PAGE_MARGIN, doc.y);
  doc.y += 14;

  for (const entry of entries) {
    renderNormalEngineerApifyWarningEntry(doc, entry, pageWidth);
  }
}

function renderNormalEngineerApifyWarningEntry(
  doc: PDFKit.PDFDocument,
  entry: NormalEngineerApifyWarningEntry,
  pageWidth: number
): void {
  ensureSpace(doc, 96);
  const cardX = PAGE_MARGIN + 4;
  const cardY = doc.y;
  const cardWidth = pageWidth - 8;
  const cardHeight = 84;

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
    .text(entry.name, cardX + 10, cardY + 9, { width: Math.max(90, badgeX - (cardX + 16)) });

  doc
    .font("Helvetica")
    .fontSize(9)
    .fillColor(MUTED_COLOR)
    .text(entry.title || "—", cardX + 10, cardY + 28, { width: cardWidth - 20 });

  if (entry.linkedinUrl) {
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor(LINK_COLOR)
      .text(entry.linkedinUrl, cardX + 10, cardY + 44, {
        width: cardWidth - 20,
        link: normalizeLink(entry.linkedinUrl),
        underline: true,
      });
  } else {
    doc
      .font("Helvetica")
      .fontSize(8.5)
      .fillColor(MUTED_COLOR)
      .text("LinkedIn URL: —", cardX + 10, cardY + 44, { width: cardWidth - 20 });
  }

  doc
    .font("Helvetica")
    .fontSize(8.5)
    .fillColor(WARNING_TEXT)
    .text(`Problem: ${entry.problem}`, cardX + 10, cardY + 60, { width: cardWidth - 20 });

  doc.y = cardY + cardHeight + 8;
}

function normalizeLink(url: string): string {
  if (/^https?:\/\//i.test(url)) return url;
  return `https://${url}`;
}
