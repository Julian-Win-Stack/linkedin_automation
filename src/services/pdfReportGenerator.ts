import PDFDocument from "pdfkit";
import { CampaignPushData, CampaignPushEntry } from "../jobs/jobStore";

interface CampaignSection {
  label: string;
  entries: CampaignPushEntry[];
}

const PAGE_MARGIN = 50;
const ACCENT_COLOR = "#1a1a2e" as const;
const SECTION_BG = "#f0f0f5" as const;
const LINK_COLOR = "#2563eb" as const;
const MUTED_COLOR = "#6b7280" as const;

function buildSections(data: CampaignPushData): CampaignSection[] {
  const mapping: { key: keyof CampaignPushData; label: string }[] = [
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

  return doc;
}

function ensureSpace(doc: PDFKit.PDFDocument, needed: number): void {
  const bottomLimit = doc.page.height - doc.page.margins.bottom;
  if (doc.y + needed > bottomLimit) {
    doc.addPage();
  }
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

  for (const entry of section.entries) {
    renderEntry(doc, entry, pageWidth);
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

function renderEntry(
  doc: PDFKit.PDFDocument,
  entry: CampaignPushEntry,
  pageWidth: number
): void {
  ensureSpace(doc, 40);

  doc
    .font("Helvetica-Bold")
    .fontSize(10)
    .fillColor("#111827")
    .text(entry.name, PAGE_MARGIN + 8, doc.y, { width: pageWidth - 16 });

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

  doc.y += 8;
}

function normalizeLink(url: string): string {
  if (/^https?:\/\//i.test(url)) return url;
  return `https://${url}`;
}
