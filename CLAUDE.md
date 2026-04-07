# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Development (runs backend + frontend concurrently)
npm run dev

# Backend only (tsx watch, hot reload)
npm run dev:backend

# Frontend only (Vite dev server, port 5173)
npm run dev:frontend

# Production build (TypeScript + frontend)
npm run build

# Start production server
npm start

# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run a single test file
npx vitest run tests/<file>.test.ts

# Lint
npm run lint

# Format
npm run format
```

## Architecture

This is a full-stack LinkedIn outreach automation platform targeting SRE hiring. It takes CSV files of companies, enriches employee data from multiple APIs, filters candidates, and pushes them into Lemlist campaigns.

### Request Flow

1. Frontend (`frontend/src/views/ResearchView.vue`) accepts CSV upload + selected user (raihan/cherry/julian)
2. `POST /research/upload` queues a job via `src/jobs/jobStore.ts` + `src/services/queueStore.ts`
3. `GET /research/status/:jobId` is polled every 2 seconds for progress
4. Backend runs `src/jobs/researchPipeline.ts` — the core 1,100+ line orchestration file

### Pipeline Stages (researchPipeline.ts)

1. CSV parsing (company name, domain, Apollo Account ID)
2. Company research via Azure OpenAI + SearchAPI
3. LinkedIn employee scraping via Apify (two-call strategy: pool fetch → open-to-work filter)
4. Candidate filtering — SRE tier ranking, keyword match, title exclusions (data/frontend roles excluded)
5. Apollo bulk email enrichment
6. Lemlist campaign push (LinkedIn + email channels)
7. PDF report generation via PDFKit

### Three-User Model

Users are `raihan`, `cherry`, `julian` (defined in `src/shared/selectedUser.ts`). Each user has separate:
- Lemlist campaign IDs (env vars prefixed `RAIHAN_`, `CHERRY_`, `JULIAN_`)
- Weekly success counters (`src/services/weeklySuccessStore.ts`)
- Queue isolation (max 10 active items per user)

### Storage

- **In-memory** (`src/jobs/jobStore.ts`): live job state, 60-min TTL, max 20 concurrent jobs
- **SQLite** (`src/services/queueStore.ts`): persistent queue with full audit trail; WAL mode; survives restarts
- **SQLite** (`src/services/weeklySuccessStore.ts`): weekly push counts per user
- Database file: `data/weekly-success.sqlite` (gitignored)

### External API Clients (all in `src/services/`)

| Service | File | Purpose |
|---|---|---|
| Apollo.io | `apolloClient.ts`, `apolloBulkEmailEnrichment.ts` | People search, email enrichment |
| Apify | `apifyClient.ts`, `apifyCompanyEmployees.ts` | LinkedIn scraping, open-to-work filter |
| Lemlist | `lemlistClient.ts`, `lemlistPushQueue.ts`, `lemlistEmailPushQueue.ts` | Campaign execution |
| Attio | `attioClient.ts`, `attioAssertCompanyRecords.ts` | CRM sync |
| Azure OpenAI | `observability/openaiClient.ts` | Company research summaries |
| SearchAPI | `observability/searchApiClient.ts` | Web search for company details |

### SRE Ranking

`src/services/sreSelection.ts` classifies candidates into 4 tiers by title + tenure:
- Tier 1: Head/Director of SRE
- Tier 2: Manager/Staff SRE
- Tier 3: Senior SRE
- Tier 4: Junior SRE

Title filtering keywords (incident management, SLO, postmortem, etc.) are defined inline. Frontend/data engineering titles are explicitly excluded.

### Environment Variables

`.env` contains all secrets: Apollo, Azure OpenAI, Lemlist, Apify, Attio API keys, plus Lemlist campaign IDs per user. See `src/config/env.ts` and `src/config/pipelineConfig.ts` for how they're loaded.

### Test Suite

Tests live in `/tests/` and use Vitest. Coverage includes: jobStore, queueStore, sreSelection, apify filtering, apollo clients, CSV parsing, lemlist push, tenure computation, email enrichment, weekly success tracking.
